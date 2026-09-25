/*
 * The consignment push, per store, as the service runs it.
 *
 * This is what create-all-products.mjs did from a laptop, turned round and
 * put where it belongs. Two things changed in the turning.
 *
 * It reads the store once instead of once per style code. Seven hundred and
 * twenty-four separate questions became two: what is on our location, and
 * what does the store already sell. For a store our size that is seconds
 * rather than minutes, and it is the same two questions however big the
 * catalogue gets.
 *
 * And it can now take things away. The old runner walked our stock and asked
 * the store about each pair, so a pair we no longer had was never asked
 * about and never switched off - a consignor could withdraw their shoe and it
 * stayed for sale. Reading our location first means the store's side of the
 * comparison is complete, so the planner sees both halves: what has to go on,
 * and what has to come off.
 *
 * Everything that decides is still in the modules it was in. This only
 * fetches, calls them in order, and writes.
 */
import { buildDesiredListings, normalizeSku, sizeKey } from "./desiredListings.js";
import { planListings, indexCurrentListings } from "./listingPlan.js";
import { ladderFor } from "./sizeLadders.js";
import { createStockxClient } from "./stockxSizes.js";
import { photoSources, photosForSkus, photosFor } from "./storePhotos.js";
import { createShopifyWriter, applyPlan, productImageUrl } from "./shopifyWriter.js";
import { readStoreState, readOurLocationStock, readFlagSetting } from "./storeState.js";

/*
 * Our consignment stock, whole.
 *
 * Paged, because Supabase answers at most a thousand rows and a silent
 * thousand-row answer once made a full catalogue look like a small one.
 */
export async function fetchConsignmentStock({ supabaseUrl, supabaseKey }) {
  const rows = [];

  for (let from = 0; ; from += 1000) {
    const response = await fetch(
      `${supabaseUrl}/rest/v1/consignment_inventory` +
        `?select=id,sku,size,vat_type,selling_price_suggested,quantity,seller_id,` +
        `seller_record_id,brand,product_name&quantity=gt.0`,
      {
        headers: {
          apikey: supabaseKey,
          Authorization: `Bearer ${supabaseKey}`,
          Range: `${from}-${from + 999}`
        }
      }
    );

    const page = await response.json().catch(() => null);

    /*
      A database that is down has to sound like a database that is down.

      This read treated anything that was not an array as "no more rows" and
      handed back what it had - which for a 503 on the first page is an empty
      shelf. The push then concluded that nothing belonged in any store any
      more and set every quantity to zero, in every shop, on 25-09-2026 while
      Supabase was unreachable.
    */
    if (!response.ok || !Array.isArray(page)) {
      throw new Error(
        `Consignment stock could not be read (${response.status}): ` +
        `${JSON.stringify(page).slice(0, 150)}`
      );
    }

    if (!page.length) break;

    rows.push(...page);

    if (page.length < 1000) break;
  }

  return rows;
}

/*
 * Prices we are not allowed to set.
 *
 * A store that sells the same pair itself owns that product page, and its
 * price is a decision it made. We add our stock to its variant on our own
 * location and leave the number alone; the customer sees one page, one
 * price, and the sum of both stocks.
 *
 * "Sells it itself" is read as stock at any location other than ours. That
 * is not perfect - a store with none left of its own reads as unowned for
 * one run - but it errs towards leaving prices untouched, which is the side
 * to err on.
 */
function dropPricesTheStoreOwns(plan, theirStock) {
  const keep = [];
  const left = [];

  for (const change of plan.setPrices) {
    if (theirStock.get(change.variantId) > 0) {
      left.push(change);
      continue;
    }

    keep.push(change);
  }

  return { setPrices: keep, pricesLeftAlone: left };
}

/*
 * Which variants have the store's flag wrong.
 *
 * The rule is the simplest one there is: we supply this pair, or we do not.
 * Read from what is actually on our location rather than from what the plan
 * meant to do, so a flag that ended up wrong for any reason is put right on
 * the next run instead of staying wrong forever.
 *
 * A variant that has never had the flag reads as null, which is not the same
 * as false and still has to be written.
 */
export function flagChangesFor(rows, flag) {
  if (!flag) return [];

  const changes = [];

  for (const row of rows) {
    const should = row.quantity > 0;
    const is = row.flagValue === null ? null : row.flagValue === "true";

    if (is === should) continue;

    changes.push({ variantId: row.variantId, sku: row.sku, size: row.size, value: should });
  }

  return changes;
}

/*
 * What our location will look like once the plan has run.
 *
 * Only used to say what a dry run would do. After a real run the store is
 * read again instead, because what happened beats what was intended.
 */
function afterPlan(rows, plan) {
  const next = new Map(rows.map((row) => [row.variantId, { ...row }]));

  for (const change of [...plan.setQuantities, ...plan.clearQuantities]) {
    const row = next.get(change.variantId);

    if (row) row.quantity = change.to;
  }

  return [...next.values()];
}

/*
 * One store, all the way through.
 *
 * Dry unless apply is true. The three merchant checkboxes are the answer to
 * what this may do: Consignment Sync at all, Product Sync to create pages
 * the store does not have, Price Sync to set the numbers.
 */
export async function runConsignmentForMerchant({
  merchant,
  graphql,
  stock,
  ladders,
  photos,
  apply = false
}) {
  const fields = merchant.fields || {};

  const consignmentSync = Boolean(fields["Consignment Sync?"]);
  const priceSync = Boolean(fields["Price Sync?"]);
  const productSync = Boolean(fields["Product Sync?"]);

  const base = {
    merchantRecordId: merchant.recordId,
    merchantName: merchant.name,
    consignmentSync,
    priceSync,
    productSync
  };

  if (!consignmentSync) {
    return { ...base, skipped: "Consignment Sync is off" };
  }

  const locationId = fields["Shopify Location ID"];

  if (!locationId) {
    return { ...base, skipped: "No Shopify Location ID" };
  }

  /*
    One store wants a flag on each variant its theme reads. That setting
    lives on the merchant, so a second store with a different arrangement is
    a value in Airtable rather than a change here.
  */
  const flag = readFlagSetting(fields);

  /*
    The store is read BEFORE deciding what belongs on it, not after.

    With Price Sync off we set no prices, so the gate has to judge the
    store's own asking price, and that means knowing it. This ran the other
    way round and handed buildDesiredListings an empty price map, which made
    every single pair come back as "the store has no price for this" - so a
    store that sets its own prices could never be supplied at all. The two
    calls only had to swap places.

    Its own catalogue is read as well when the store has a location besides
    ours, which is the closest thing to "does it keep its own stock" that
    does not depend on somebody ticking a box.
  */
  const state = await readStoreState(graphql, { locationId, flag });

  const currentPrices = new Map(
    state.rows
      .filter((row) => row.price > 0)
      .map((row) => [`${normalizeSku(row.sku)}|${sizeKey(row.size)}`, row.price])
  );

  const { listings, rejected } = buildDesiredListings({
    inventoryRows: stock,
    merchantFields: fields,
    currentPrices,
    priceSync,

    /*
      A store that consigns to us should not be sold its own shoes. The
      merchant's own seller record is left out of the cheapest-per-pair
      choice entirely.
    */
    excludeSellerRecordId: (fields["Seller ID"] || [])[0] || null
  });

  const current = indexCurrentListings(state.rows);

  const plan = planListings({
    desired: listings,
    current,
    sizeLadders: ladders,
    productSync,
    priceSync
  });

  const { setPrices, pricesLeftAlone } = dropPricesTheStoreOwns(plan, state.theirStock);

  plan.setPrices = setPrices;

  /*
    Variants the store made itself, which we are about to put stock on.

    Shopify refuses a quantity at a location where the item has no level, so
    these need connecting and tracking turned on first. The old Make scenario
    did this with the REST connect endpoint; the difference is that this only
    fires for the handful that need it instead of on every single row.
  */
  plan.activate = plan.setQuantities.filter(
    (change) => change.to > 0 && !state.ourVariantIds.has(change.variantId)
  );

  const counts = {
    onOurLocation: state.ourRows,
    storeCatalogueRows: state.catalogueRows,
    wanted: listings.length,
    rejected: rejected.length,
    createProducts: plan.createProducts.length,
    addSizes: plan.addSizes.length,
    setQuantities: plan.setQuantities.length,
    setPrices: plan.setPrices.length,
    clearQuantities: plan.clearQuantities.length,
    rejectedReasons: rejected.reduce((seen, row) => {
      seen[row.reason] = (seen[row.reason] || 0) + 1;
      return seen;
    }, {}),
    activate: plan.activate.length,
    pricesLeftAlone: pricesLeftAlone.length
  };

  if (!apply) {
    return {
      ...base,
      dryRun: true,
      ...counts,
      setFlags: flagChangesFor(afterPlan(state.rows, plan), flag).length
    };
  }

  const writer = createShopifyWriter({ graphql, locationId, apply: true });

  const photosBySku = new Map(
    plan.createProducts.map((entry) => [entry.sku, photosFor(photos, entry.sku)])
  );

  const report = await applyPlan(plan, writer, { photosBySku });

  /*
    The flag, set from what the store ended up with rather than from what we
    asked for. That costs one more read of our location, and it means a
    product created in this same run gets its flag in this same run.
  */
  let flagsSet = 0;

  if (flag) {
    const after = await readOurLocationStock(graphql, locationId, flag);
    const changes = flagChangesFor(after, flag);

    await writer.setFlags(changes, flag);

    flagsSet = changes.length;
  }

  return {
    ...base,
    dryRun: false,
    ...counts,
    setFlags: flagsSet,
    done: report.done.length,
    problems: report.problems.slice(0, 10)
  };
}

/*
 * Every store with the box ticked.
 *
 * Our stock, the size ladders and the photographs are fetched once and
 * handed to each store, because they do not differ per store and asking
 * again per store is what made the old script slow.
 *
 * One store's failure stays that store's failure, same as the nightly
 * product sync.
 */
export async function runConsignmentForAll({
  merchants,
  photoSourceMerchants,
  graphqlFor,
  supabaseUrl,
  supabaseKey,
  apply = false,
  onProgress = () => {}
}) {
  const wanted = merchants.filter((m) => Boolean(m.fields?.["Consignment Sync?"]));

  if (!wanted.length) {
    return { merchants: 0, results: [], message: "No merchant has Consignment Sync ticked" };
  }

  const stock = await fetchConsignmentStock({ supabaseUrl, supabaseKey });

  /*
    An empty shelf is never a fact worth acting on. We always hold stock, so
    nothing to list means something went wrong upstream - and acting on it
    would clear every store. Stopping here costs one run; the next one half
    an hour later puts it right.
  */
  if (!stock.length) {
    return { merchants: 0, results: [], message: "Consignment stock came back empty - nothing was touched" };
  }

  const skus = [...new Set(stock.map((row) => String(row.sku || "").trim().toUpperCase()))]
    .filter(Boolean)
    .sort();

  const { ladders } = await ladderFor(skus, { stockx: createStockxClient() });
  const { photos } = await photosForSkus(skus, { sources: photoSources(photoSourceMerchants) });

  const results = [];

  for (const merchant of wanted) {
    onProgress({ merchant: merchant.name, stage: "start" });

    try {
      results.push(
        await runConsignmentForMerchant({
          merchant,
          graphql: graphqlFor(merchant),
          stock,
          ladders,
          photos,
          apply
        })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);

      console.error("CONSIGNMENT PUSH FAILED", {
        merchantRecordId: merchant.recordId,
        merchantName: merchant.name,
        error: message
      });

      results.push({
        merchantRecordId: merchant.recordId,
        merchantName: merchant.name,
        error: message
      });
    }
  }

  return {
    merchants: wanted.length,
    stockRows: stock.length,
    styleCodes: skus.length,
    apply,
    results
  };
}

/* ------------------------------------------------------------------ *
 * Pictures for products that were born without any
 *
 * A product gets its photographs when it is made and never again, so a pair
 * nobody had a picture of that day keeps a blank card for good - even once a
 * source store starts carrying it. Twenty-seven of UNION's first seven
 * hundred landed that way, and seven of those had a picture within the hour
 * once more stores were asked.
 *
 * Nothing has to look for pictures here: the consignment run asks for all of
 * them every half hour, and the library re-asks whatever nobody had once a
 * day. All that is missing is hanging what was found onto the products that
 * are still empty, which is what this does.
 *
 * Only ever adds. A product with one picture already is left alone, because
 * a second set from another shop beside the first looks worse than either.
 * ------------------------------------------------------------------ */

const BLANK_PRODUCTS = `
  query Blank($cursor: String) {
    products(first: 250, after: $cursor, query: "status:active") {
      nodes {
        id
        title
        media(first: 1) { nodes { id } }
        variants(first: 1) { nodes { sku } }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
`;

const ADD_MEDIA = `
  mutation add($id: ID!, $media: [CreateMediaInput!]!) {
    productCreateMedia(productId: $id, media: $media) {
      media { id }
      mediaUserErrors { field message }
    }
  }
`;

export async function backfillPhotosForMerchant({ merchant, graphql, photos, apply = false }) {
  const fields = merchant.fields || {};

  const base = { merchantRecordId: merchant.recordId, merchantName: merchant.name };

  if (!fields["Consignment Sync?"]) return { ...base, skipped: "Consignment Sync is off" };

  const blank = [];

  let cursor = null;

  for (;;) {
    const data = await graphql(BLANK_PRODUCTS, { cursor });

    for (const product of data?.products?.nodes || []) {
      if (product.media.nodes.length) continue;

      const sku = String(product.variants.nodes[0]?.sku || "").trim().toUpperCase();

      if (sku) blank.push({ id: product.id, sku, title: product.title });
    }

    if (!data?.products?.pageInfo?.hasNextPage) break;

    cursor = data.products.pageInfo.endCursor;
  }

  const fillable = blank
    .map((item) => ({ ...item, urls: photosFor(photos, item.sku) }))
    .filter((item) => item.urls.length);

  if (!apply) {
    return { ...base, dryRun: true, blank: blank.length, fillable: fillable.length };
  }

  let filled = 0;
  const problems = [];

  for (const item of fillable) {
    try {
      const data = await graphql(ADD_MEDIA, {
        id: item.id,
        media: item.urls.map((url) => ({
          originalSource: productImageUrl(url),
          mediaContentType: "IMAGE"
        }))
      });

      const errors = data?.productCreateMedia?.mediaUserErrors || [];

      if (errors.length) {
        problems.push({ sku: item.sku, error: errors.map((e) => e.message).join("; ") });
        continue;
      }

      filled += 1;
    } catch (error) {
      problems.push({ sku: item.sku, error: error instanceof Error ? error.message : String(error) });
    }
  }

  return {
    ...base,
    dryRun: false,
    blank: blank.length,
    fillable: fillable.length,
    filled,
    problems: problems.slice(0, 10)
  };
}

export async function backfillPhotosForAll({
  merchants,
  photoSourceMerchants,
  graphqlFor,
  supabaseUrl,
  supabaseKey,
  apply = false
}) {
  const wanted = merchants.filter((m) => Boolean(m.fields?.["Consignment Sync?"]));

  if (!wanted.length) return { merchants: 0, results: [] };

  const stock = await fetchConsignmentStock({ supabaseUrl, supabaseKey });

  /*
    An empty shelf is never a fact worth acting on. We always hold stock, so
    nothing to list means something went wrong upstream - and acting on it
    would clear every store. Stopping here costs one run; the next one half
    an hour later puts it right.
  */
  if (!stock.length) {
    return { merchants: 0, results: [], message: "Consignment stock came back empty - nothing was touched" };
  }

  const skus = [...new Set(stock.map((row) => String(row.sku || "").trim().toUpperCase()))].filter(Boolean);

  const { photos } = await photosForSkus(skus, { sources: photoSources(photoSourceMerchants) });

  const results = [];

  for (const merchant of wanted) {
    try {
      results.push(
        await backfillPhotosForMerchant({ merchant, graphql: graphqlFor(merchant), photos, apply })
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);

      console.error("PHOTO BACKFILL FAILED", { merchantName: merchant.name, error: message });

      results.push({ merchantName: merchant.name, error: message });
    }
  }

  return { merchants: wanted.length, apply, results };
}
