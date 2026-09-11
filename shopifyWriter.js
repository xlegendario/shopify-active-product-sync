/*
 * The writing half. Everything here does exactly what the plan says and
 * decides nothing.
 *
 * Dry by default. A dry run performs no mutation and returns the operations
 * it would have sent, variables and all, so a plan can be read before a shop
 * is touched. Nothing switches that off but an explicit apply.
 *
 * The GraphQL client is handed in rather than built here, so this uses the
 * same connection, retries and error logging as the rest of the service.
 */

const ONLINE_STORE = "Online Store";

/* ------------------------------------------------------------------ *
 * Pictures
 * ------------------------------------------------------------------ */

/*
 * The picture URL as Shopify will be handed it.
 *
 * WARNING - the padding here does nothing, and it was measured rather than
 * read: fit, bg, w and h are imgix parameters, and cdn.shopify.com ignores
 * any parameter it does not know. A 1080x1080 square comes back 1080x1080
 * square however politely we ask for 1200x900 on white.
 *
 * It is kept because it costs nothing and a source that does understand them
 * would be served correctly. What it must NOT be taken for is a guarantee
 * that every picture arrives the same shape. They do not: most are 1.67
 * wide, a few are square and a few are twice as wide as they are tall, and
 * in a card with a fixed frame that is the difference between a shoe sitting
 * neatly in the middle and one filling the whole tile.
 *
 * Shopify's own width/height/crop parameters cannot fix it either. They crop
 * instead of padding and never enlarge, so a small square stays a square.
 * Evening that out belongs in the storefront's theme, where one fixed ratio
 * with the image contained on white settles every product at once.
 */
export function productImageUrl(pictureUrl, { width = 1200, height = 900 } = {}) {
  const raw = String(pictureUrl || "").trim();

  if (!raw) return null;

  try {
    const url = new URL(raw);

    url.searchParams.set("fit", "fill");
    url.searchParams.set("bg", "FFFFFF");
    url.searchParams.set("w", String(width));
    url.searchParams.set("h", String(height));

    return url.toString();
  } catch {
    return raw;
  }
}

/* ------------------------------------------------------------------ *
 * The operations
 * ------------------------------------------------------------------ */

const CREATE_PRODUCT = `
  mutation createProduct($product: ProductCreateInput!, $media: [CreateMediaInput!]) {
    productCreate(product: $product, media: $media) {
      product {
        id
        handle
        options { id name optionValues { id name } }
        variants(first: 100) { nodes { id title sku inventoryItem { id } } }
      }
      userErrors { field message }
    }
  }
`;

const CREATE_VARIANTS = `
  mutation createVariants($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkCreate(productId: $productId, variants: $variants, strategy: REMOVE_STANDALONE_VARIANT) {
      productVariants { id title sku inventoryItem { id } }
      userErrors { field message }
    }
  }
`;

const UPDATE_VARIANTS = `
  mutation updateVariants($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkUpdate(productId: $productId, variants: $variants) {
      productVariants { id price }
      userErrors { field message }
    }
  }
`;

const SET_QUANTITIES = `
  mutation setQuantities($input: InventorySetQuantitiesInput!) {
    inventorySetQuantities(input: $input) {
      inventoryAdjustmentGroup { createdAt }
      userErrors { field message }
    }
  }
`;

/*
 * Stocking an item at our location, and turning tracking on.
 *
 * Needed only for a variant we did not create: the store made the product,
 * so the item has a level at their location and none at ours, and setting a
 * quantity where there is no level is an error rather than a create.
 *
 * The old Make scenario did exactly this with the REST connect endpoint and
 * a PUT setting inventory_management to shopify. Both are one mutation here.
 * Without them a store that carries its own stock silently refuses every
 * quantity we send.
 */
const ACTIVATE = `
  mutation activate($inventoryItemId: ID!, $locationId: ID!) {
    inventoryActivate(inventoryItemId: $inventoryItemId, locationId: $locationId) {
      inventoryLevel { id }
      userErrors { field message }
    }
  }
`;

const TRACK = `
  mutation track($id: ID!) {
    inventoryItemUpdate(id: $id, input: { tracked: true }) {
      inventoryItem { id tracked }
      userErrors { field message }
    }
  }
`;

/*
 * The flag a store's theme reads, per variant.
 *
 * FastCop's is a boolean at custom.kickzcaviar: true while we supply the
 * pair, false the moment we stop. Shopify takes twenty-five at a time.
 */
const SET_METAFIELDS = `
  mutation setFlags($metafields: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $metafields) {
      metafields { id key value }
      userErrors { field message code }
    }
  }
`;

const PUBLISH = `
  mutation publish($id: ID!, $input: [PublicationInput!]!) {
    publishablePublish(id: $id, input: $input) {
      userErrors { field message }
    }
  }
`;

const PUBLICATIONS = `
  query publications {
    publications(first: 25) { nodes { id name } }
  }
`;

const REORDER_OPTION_VALUES = `
  mutation reorderOptionValues($productId: ID!, $option: OptionUpdateInput!, $optionValues: [OptionValueUpdateInput!]) {
    productOptionUpdate(productId: $productId, option: $option, optionValuesToUpdate: $optionValues) {
      product { id options { name optionValues { id name } } }
      userErrors { field message }
    }
  }
`;

/* ------------------------------------------------------------------ *
 * The writer
 * ------------------------------------------------------------------ */

function collectErrors(payload, key) {
  const errors = payload?.[key]?.userErrors || [];

  if (!errors.length) return null;

  return errors.map((e) => `${(e.field || []).join(".")}: ${e.message}`).join("; ");
}

/*
 * What the store has right now, for the style codes about to be touched.
 *
 * Read from Shopify itself and not from store_listings, and that distinction
 * is not academic. store_listings is a copy, refreshed by the inbound sync,
 * and it is empty for a store we have never read. Planning a create against
 * an empty copy makes a second page for every pair that already has one.
 *
 * Which is exactly what happened on the first live try: three attempts, three
 * products, because the plan was handed an empty catalogue every time. Two
 * survived and had to be deleted by hand.
 *
 * Also the live prices, which the margin gate needs when we are not the ones
 * setting them. A price the store changed this morning is not in last night's
 * copy.
 */
export async function readCurrentListings(graphql, skus) {
  const rows = [];

  for (const sku of [...new Set(skus)]) {
    const data = await graphql(
      `query($q: String!) {
         products(first: 5, query: $q) {
           nodes {
             id
             variants(first: 100) {
               nodes {
                 id
                 sku
                 price
                 inventoryQuantity
                 selectedOptions { name value }
                 inventoryItem { id }
               }
             }
           }
         }
       }`,
      { q: `sku:${sku}` }
    );

    for (const product of data?.products?.nodes || []) {
      for (const variant of product.variants?.nodes || []) {
        if (String(variant.sku || "").trim().toUpperCase() !== sku) continue;

        rows.push({
          sku,
          // The size is whichever option carries it, whatever the shop named
          // that option - "Maat" here, "Size" in a store that is not ours.
          size: (variant.selectedOptions || [])[0]?.value || "",
          productId: product.id,
          variantId: variant.id,
          inventoryItemId: variant.inventoryItem?.id || null,
          price: Number(variant.price) || 0,
          quantity: Number(variant.inventoryQuantity) || 0
        });
      }
    }
  }

  return rows;
}

export function createShopifyWriter({ graphql, locationId, apply = false }) {
  const done = [];
  const wouldDo = [];
  const problems = [];

  const locationGid = String(locationId || "").startsWith("gid://")
    ? locationId
    : `gid://shopify/Location/${locationId}`;

  async function run(label, query, variables, resultKey) {
    if (!apply) {
      wouldDo.push({ label, variables });
      return null;
    }

    const data = await graphql(query, variables);
    const failure = collectErrors(data, resultKey);

    if (failure) {
      problems.push({ label, failure });
      return null;
    }

    done.push(label);

    return data?.[resultKey] || null;
  }

  let onlineStoreId = null;

  /*
   * Which publication is the Online Store.
   *
   * Needs read_publications on the app, and a token without it refuses the
   * query outright. That used to take the whole run down at the last step,
   * after the product and all its variants were already made.
   *
   * It is recorded as a problem instead, because the consequence is specific
   * and worth naming: the product exists, is complete, and is visible to
   * nobody. A shop that stays empty while every log line says success is the
   * worst way for this to fail.
   */
  async function onlineStorePublicationId() {
    if (onlineStoreId || !apply) return onlineStoreId;

    try {
      const data = await graphql(PUBLICATIONS, {});
      const nodes = data?.publications?.nodes || [];

      onlineStoreId = nodes.find((n) => n.name === ONLINE_STORE)?.id || null;

      if (!onlineStoreId) {
        problems.push({
          label: "publicatiekanaal zoeken",
          failure: `geen kanaal met de naam "${ONLINE_STORE}" gevonden`
        });
      }
    } catch (err) {
      problems.push({
        label: "publicatiekanaal zoeken",
        failure: `${err.message} - het product staat er wel maar is voor niemand zichtbaar`
      });
    }

    return onlineStoreId;
  }

  /*
   * A product, born complete.
   *
   * The whole size ladder goes in as option values in one call, in order.
   * Shopify sorts variants by the order of those values, and a value added
   * later lands at the end - which is how a page ends up showing 44 before
   * 38. Creating the ladder whole is the only way to avoid that entirely.
   *
   * Sizes we can fill carry stock and the rest are created at zero, so the
   * page shows them sold out. That also gives dropshipping somewhere to land
   * later without touching the ordering again.
   */
  async function createProduct(entry, { photos = [], pictureUrl } = {}) {
    /*
      Real photographs first, several of them, from a store that shoots its
      own stock. The StockX thumbnail only fills in when there are none, and
      then as one image rather than a gallery.
    */
    const images = photos.length ? photos : [productImageUrl(pictureUrl)].filter(Boolean);

    const created = await run(
      `product aanmaken ${entry.sku}`,
      CREATE_PRODUCT,
      {
        product: {
          title: entry.title,
          vendor: entry.brand || undefined,
          status: "ACTIVE",
          productOptions: [{ name: "Maat", values: entry.sizes.map((size) => ({ name: String(size) })) }]
        },
        media: images.length
          ? images.map((src) => ({ originalSource: src, mediaContentType: "IMAGE", alt: entry.title }))
          : undefined
      },
      "productCreate"
    );

    const productId = created?.product?.id || null;

    const filled = new Map(entry.variants.map((v) => [String(v.size), v]));

    const lowestFilledPrice = entry.variants.reduce(
      (lowest, variant) => (lowest === null || variant.price < lowest ? variant.price : lowest),
      null
    ) ?? 0;

    const variants = entry.sizes.map((size) => {
      const held = filled.get(String(size));

      return {
        optionValues: [{ optionName: "Maat", name: String(size) }],
        /*
          A size we cannot fill still needs a price, and it was taking the
          first filled one, which is whichever consignor happened to be first
          in the list. The lowest is at least a rule: the page reads as the
          cheapest we could do it for, and nobody can buy it anyway because
          the policy below refuses it.
        */
        price: String(held ? held.price : lowestFilledPrice),
        inventoryItem: { sku: entry.sku, tracked: true },
        /*
          Sold out has to mean sold out. Most of these shops let a customer
          buy what is not in stock, which is fine for their own goods and
          wrong for consignment: the pair is in somebody else's hands.
        */
        inventoryPolicy: "DENY",
        /*
          FIXED - this said name and quantity, which is the shape
          inventorySetQuantities takes. On a variant the field is an
          InventoryLevelInput and wants availableQuantity, and Shopify
          refused the whole batch over it: the product was created with its
          options and photographs and then stood there with one variant.

          Two shapes for the same idea, one per mutation. Worth the note,
          because reading either one alone gives no hint the other differs.
        */
        inventoryQuantities: [
          { locationId: locationGid, availableQuantity: held ? held.quantity : 0 }
        ]
      };
    });

    await run(
      `maten zetten ${entry.sku} (${variants.length})`,
      CREATE_VARIANTS,
      { productId: productId || "gid://shopify/Product/DRY", variants },
      "productVariantsBulkCreate"
    );

    /*
      A product made through the API is published nowhere at all. Skip this
      and the shop stays empty while everything looks right from our side.
    */
    const publicationId = await onlineStorePublicationId();

    if (publicationId || !apply) {
      await run(
        `publiceren ${entry.sku}`,
        PUBLISH,
        {
          id: productId || "gid://shopify/Product/DRY",
          input: [{ publicationId: publicationId || "gid://shopify/Publication/DRY" }]
        },
        "publishablePublish"
      );
    }

    return productId;
  }

  // The repair path: a size the page did not have yet.
  async function addSizes(entry) {
    await run(
      `maten bijzetten ${entry.sku} (${entry.sizes.length})`,
      CREATE_VARIANTS,
      {
        productId: entry.productId,
        variants: entry.sizes.map((size) => ({
          optionValues: [{ optionName: "Maat", name: String(size) }],
          inventoryItem: { sku: entry.sku, tracked: true },
          inventoryPolicy: "DENY"
        }))
      },
      "productVariantsBulkCreate"
    );

    /*
      And then put the ladder back in order, because the new value was
      appended. Without this the page shows the new size last, whatever its
      number, which is the thing the whole create-it-whole approach avoids.
    */
    await run(
      `maten opnieuw ordenen ${entry.sku}`,
      REORDER_OPTION_VALUES,
      {
        productId: entry.productId,
        option: { name: "Maat" },
        optionValues: entry.reorderTo.map((size, index) => ({ name: String(size), position: index + 1 }))
      },
      "productOptionUpdate"
    );
  }

  /*
   * Make room for our stock on a variant the store already sells.
   *
   * Idempotent by nature: activating an item that is already at the location
   * gives back the level it has. Called only for variants the caller knows
   * were not on our location, so on a dropship store this does nothing at
   * all.
   */
  async function activateAtOurLocation(items) {
    for (const item of items) {
      const gid = String(item.inventoryItemId).startsWith("gid://")
        ? item.inventoryItemId
        : `gid://shopify/InventoryItem/${item.inventoryItemId}`;

      await run(
        `voorraad aanzetten op onze locatie ${item.sku} ${item.size}`,
        TRACK,
        { id: gid },
        "inventoryItemUpdate"
      );

      await run(
        `onze locatie koppelen ${item.sku} ${item.size}`,
        ACTIVATE,
        { inventoryItemId: gid, locationId: locationGid },
        "inventoryActivate"
      );
    }
  }

  /*
   * Quantities, in helpings Shopify will take.
   *
   * inventorySetQuantities accepts 250 at a time. This sent them all in one
   * mutation, which was invisible while the only store was UNION with its
   * twenty-two variants, and would have failed on the first real store: a
   * shop with twelve hundred changes would have had the whole lot rejected,
   * and a rejected batch means nothing moved at all.
   */
  async function setQuantities(changes) {
    if (!changes.length) return;

    for (let i = 0; i < changes.length; i += 250) {
      const batch = changes.slice(i, i + 250);

      await run(
        `aantallen zetten (${batch.length} van ${changes.length})`,
        SET_QUANTITIES,
        {
          input: {
            name: "available",
            reason: "correction",
            ignoreCompareQuantity: true,
            quantities: batch.map((change) => ({
              inventoryItemId: String(change.inventoryItemId).startsWith("gid://")
                ? change.inventoryItemId
                : `gid://shopify/InventoryItem/${change.inventoryItemId}`,
              locationId: locationGid,
              quantity: change.to
            }))
          }
        },
        "inventorySetQuantities"
      );
    }
  }

  /*
   * Set the store's own flag on a list of variants.
   *
   * Each entry says which variant and what it should become. The caller only
   * passes the ones whose value is actually wrong, so a store that is
   * already correct costs nothing.
   */
  async function setFlags(entries, flag) {
    if (!entries.length || !flag) return;

    for (let i = 0; i < entries.length; i += 25) {
      const batch = entries.slice(i, i + 25);

      await run(
        `winkelvlag zetten (${batch.length})`,
        SET_METAFIELDS,
        {
          metafields: batch.map((entry) => ({
            ownerId: String(entry.variantId).startsWith("gid://")
              ? entry.variantId
              : `gid://shopify/ProductVariant/${entry.variantId}`,
            namespace: flag.namespace,
            key: flag.key,
            type: flag.type,
            value: String(entry.value)
          }))
        },
        "metafieldsSet"
      );
    }
  }

  async function setPrices(changes) {
    const byProduct = new Map();

    for (const change of changes) {
      if (!byProduct.has(change.sku)) byProduct.set(change.sku, []);
      byProduct.get(change.sku).push(change);
    }

    for (const [sku, group] of byProduct) {
      await run(
        `prijzen zetten ${sku} (${group.length})`,
        UPDATE_VARIANTS,
        {
          productId: group[0].productId || "gid://shopify/Product/DRY",
          variants: group.map((change) => ({
            id: String(change.variantId).startsWith("gid://")
              ? change.variantId
              : `gid://shopify/ProductVariant/${change.variantId}`,
            price: String(change.to)
          }))
        },
        "productVariantsBulkUpdate"
      );
    }
  }

  return {
    createProduct,
    addSizes,
    activateAtOurLocation,
    setFlags,
    setQuantities,
    setPrices,
    get report() {
      return { apply, done, wouldDo, problems };
    }
  };
}

/*
 * A whole plan, in the order that keeps a page correct at every moment.
 *
 * Products first, so a quantity never arrives before the variant it belongs
 * to. Then the sizes added to pages that already existed. Then prices, then
 * stock, and the clearing last - a pair that moved from one page to another
 * is then never at zero on both at once.
 */
export async function applyPlan(plan, writer, { photosBySku = new Map(), picturesBySku = new Map() } = {}) {
  for (const entry of plan.createProducts) {
    await writer.createProduct(entry, {
      photos: photosBySku.get(entry.sku) || [],
      pictureUrl: picturesBySku.get(entry.sku)
    });
  }

  for (const entry of plan.addSizes) {
    await writer.addSizes(entry);
  }

  /*
    Anything the store made itself has to be stocked at our location before a
    quantity means anything there. The plan says which, because only the
    reader knows what was already on our shelf.
  */
  if (plan.activate?.length) {
    await writer.activateAtOurLocation(plan.activate);
  }

  await writer.setPrices(plan.setPrices);
  await writer.setQuantities(plan.setQuantities);
  await writer.setQuantities(plan.clearQuantities);

  return writer.report;
}
