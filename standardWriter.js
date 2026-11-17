/*
 * The Standard side of a store with "Express Option?" on (delivery.js).
 *
 * Per size the autopricing decided in autoprice_listings, the product page
 * gets a "<size> / Standard" variant at that price, orderable without stock
 * (the pair is sourced after the sale). A size whose autoprice closed loses
 * its Standard variant again. Express variants - our consignors' stock - are
 * never touched here; the consignment run owns those.
 *
 * The first time a page is seen without a delivery option, the option is
 * added and every existing variant becomes Express. That is the migration,
 * and it is the same step for a page made yesterday as for one made today.
 *
 * planStandard decides and writes nothing, so it can be tested with plain
 * objects (standardWriter.test.mjs). writeStandardVariants does what it says.
 */
import { normalizeSize } from "./sizes.js";
import { sortEuSizes } from "./storePricing.js";
import { DELIVERY_OPTION, EXPRESS, STANDARD, deliveryOf, isStandardVariant } from "./delivery.js";

const SIZE_OPTION_NAMES = /^(maat|maten|size|sizes|taille|talla)$/i;
const DELIVERY_NAMES = /^(levering|delivery)$/i;

const PAGE = `
  query standardPage($q: String!) {
    products(first: 5, query: $q) {
      nodes {
        id
        title
        options { id name position optionValues { id name } }
        variants(first: 250) {
          nodes {
            id
            sku
            price
            inventoryPolicy
            selectedOptions { name value }
          }
        }
      }
    }
  }
`;

const ADD_OPTION = `
  mutation addDelivery($productId: ID!, $options: [OptionCreateInput!]!) {
    productOptionsCreate(productId: $productId, options: $options, variantStrategy: LEAVE_AS_IS) {
      product { id }
      userErrors { field message }
    }
  }
`;

const CREATE = `
  mutation createStandard($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkCreate(productId: $productId, variants: $variants) {
      productVariants { id }
      userErrors { field message }
    }
  }
`;

const UPDATE = `
  mutation priceStandard($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkUpdate(productId: $productId, variants: $variants) {
      productVariants { id }
      userErrors { field message }
    }
  }
`;

const REMOVE = `
  mutation removeStandard($productId: ID!, $variantsIds: [ID!]!) {
    productVariantsBulkDelete(productId: $productId, variantsIds: $variantsIds) {
      product { id }
      userErrors { field message }
    }
  }
`;

const REORDER = `
  mutation reorderSizes($productId: ID!, $options: [OptionReorderInput!]!) {
    productOptionsReorder(productId: $productId, options: $options) {
      userErrors { field message }
    }
  }
`;

const sizeOf = (variant) => {
  const options = variant?.selectedOptions || [];
  const named = options.find((o) => SIZE_OPTION_NAMES.test(String(o.name || "").trim()));

  return normalizeSize((named || options[0])?.value || "");
};

// A size option whose values are not in ladder order, e.g. after Standard sizes were appended.
function sizesOutOfOrder(product) {
  const option = (product?.options || []).find((o) => SIZE_OPTION_NAMES.test(String(o.name || "").trim()));

  if (!option) return false;

  const names = option.optionValues.map((v) => v.name);

  return sortEuSizes(names).some((name, i) => name !== names[i]);
}

/*
 * What has to change on one page.
 *
 * product:  the page as PAGE reads it (one product node)
 * wanted:   [{ size, price }] - the open autoprice sizes for this SKU
 */
export function planStandard({ product, wanted }) {
  const sizeOption = (product.options || []).find((o) => SIZE_OPTION_NAMES.test(String(o.name || "").trim()))
    || (product.options || [])[0];
  const deliveryOption = (product.options || []).find((o) => DELIVERY_NAMES.test(String(o.name || "").trim()));

  const variants = product.variants?.nodes || [];
  const standard = new Map(variants.filter(isStandardVariant).map((v) => [sizeOf(v), v]));

  /*
    Only sizes on the page's own ladder - the sizes its Express variants
    were made with. A market can label the same shoe differently: StockX
    Pro gives adidas pairs as 36.5 / 37 / 38.5 where adidas and the page say
    36 2/3 / 37 1/3 / 38. Adding those as new sizes would sell a size that
    does not exist next to the one that does.
  */
  const ladder = new Set(variants.filter((v) => !isStandardVariant(v)).map(sizeOf));
  const want = new Map(
    (wanted || [])
      .filter((w) => Number(w.price) > 0)
      .map((w) => [normalizeSize(w.size), w])
      .filter(([size]) => ladder.has(size))
  );

  const create = [];
  const update = [];
  const remove = [];

  for (const [size, w] of want) {
    const price = Number(w.price).toFixed(2);
    const have = standard.get(size);

    if (!have) create.push({ size, price });
    else if (Number(have.price).toFixed(2) !== price || have.inventoryPolicy !== "CONTINUE") update.push({ id: have.id, size, price });
  }

  for (const [size, v] of standard) {
    if (!want.has(size)) remove.push({ id: v.id, size });
  }

  // The size values a new Standard variant brings that the page does not have yet.
  const known = new Set((sizeOption?.optionValues || []).map((v) => normalizeSize(v.name)));
  const newSizeValues = create.filter((c) => !known.has(c.size)).map((c) => c.size);

  return {
    sizeOptionName: sizeOption?.name || "Maat",
    needsDeliveryOption: !deliveryOption,
    deliveryOptionName: deliveryOption?.name || DELIVERY_OPTION,
    create,
    update,
    remove,
    newSizeValues
  };
}

function errorsOf(payload, key) {
  const errors = payload?.[key]?.userErrors || [];

  return errors.length ? errors.map((e) => `${(e.field || []).join(".")}: ${e.message}`).join("; ") : null;
}

async function findPage(graphql, sku) {
  const data = await graphql(PAGE, { q: `sku:${sku}` });

  return (data?.products?.nodes || []).find((p) =>
    (p.variants?.nodes || []).some((v) => String(v.sku || "").trim().toUpperCase() === sku)
  ) || null;
}

/*
 * Every SKU on the store's shelf, one page at a time.
 *
 * listingsBySku: Map(sku -> [{ size, price }]) of the OPEN autoprice rows.
 */
export async function writeStandardVariants({ graphql, skus, listingsBySku, apply = false, log = console }) {
  const tally = { pages: 0, missingPage: 0, optionAdded: 0, created: 0, updated: 0, removed: 0, failed: 0 };
  const problems = [];

  const run = async (label, query, variables, key) => {
    if (!apply) return { dry: true };

    const data = await graphql(query, variables);
    const failure = errorsOf(data, key);

    if (failure) throw new Error(`${label}: ${failure}`);

    return data[key];
  };

  for (const sku of skus) {
    try {
      let page = await findPage(graphql, sku);

      if (!page) { tally.missingPage += 1; continue; }

      tally.pages += 1;

      let plan = planStandard({ product: page, wanted: listingsBySku.get(sku) || [] });

      if (!plan.create.length && !plan.update.length && !plan.remove.length && !sizesOutOfOrder(page)) continue;

      /*
        The migration. Existing variants all take the first value, Express,
        which is what they are: stock of ours or a sold-out size of it.
      */
      if (plan.needsDeliveryOption) {
        await run(`levering-optie ${sku}`, ADD_OPTION, {
          productId: page.id,
          options: [{ name: DELIVERY_OPTION, values: [{ name: EXPRESS }, { name: STANDARD }] }]
        }, "productOptionsCreate");

        tally.optionAdded += 1;

        if (apply) {
          page = await findPage(graphql, sku);
          plan = planStandard({ product: page, wanted: listingsBySku.get(sku) || [] });
        }
      }

      if (plan.create.length) {
        await run(`standard aanmaken ${sku} (${plan.create.length})`, CREATE, {
          productId: page.id,
          variants: plan.create.map((c) => ({
            optionValues: [
              { optionName: plan.sizeOptionName, name: c.size },
              { optionName: plan.deliveryOptionName, name: STANDARD }
            ],
            price: c.price,
            // Sourced after the sale: always orderable, no stock of anybody's.
            inventoryPolicy: "CONTINUE",
            inventoryItem: { sku, tracked: false }
          }))
        }, "productVariantsBulkCreate");

        tally.created += plan.create.length;
      }

      if (plan.update.length) {
        await run(`standard prijs ${sku} (${plan.update.length})`, UPDATE, {
          productId: page.id,
          variants: plan.update.map((u) => ({ id: u.id, price: u.price, inventoryPolicy: "CONTINUE" }))
        }, "productVariantsBulkUpdate");

        tally.updated += plan.update.length;
      }

      if (plan.remove.length) {
        await run(`standard weg ${sku} (${plan.remove.length})`, REMOVE, {
          productId: page.id,
          variantsIds: plan.remove.map((r) => r.id)
        }, "productVariantsBulkDelete");

        tally.removed += plan.remove.length;
      }

      /*
        A size the page did not have lands at the end of the size option.
        Put the ladder back in order, as the consignment writer does.
      */
      if (apply && (plan.newSizeValues.length || sizesOutOfOrder(page))) {
        const after = await findPage(graphql, sku);

        /*
          Shopify wants every option of the page in a reorder, not only the
          one that moves: sizes sorted, Express before Standard.
        */
        const options = (after?.options || []).map((o) => {
          const byName = new Map(o.optionValues.map((v) => [v.name, v.id]));
          const names = o.name === plan.sizeOptionName
            ? sortEuSizes(o.optionValues.map((v) => v.name))
            : DELIVERY_NAMES.test(o.name)
              ? [EXPRESS, STANDARD].filter((n) => byName.has(n))
              : o.optionValues.map((v) => v.name);

          return { id: o.id, values: names.map((name) => ({ id: byName.get(name) })) };
        });

        if (options.length) {
          await run(`maten ordenen ${sku}`, REORDER, { productId: page.id, options }, "productOptionsReorder")
            .catch((error) => problems.push({ sku, failure: error.message }));
        }
      }
    } catch (error) {
      tally.failed += 1;
      problems.push({ sku, failure: error instanceof Error ? error.message : String(error) });
    }
  }

  log.log("STANDARD " + JSON.stringify({ apply, ...tally, problems: problems.slice(0, 10) }));

  return { apply, ...tally, problems };
}

export { deliveryOf };
