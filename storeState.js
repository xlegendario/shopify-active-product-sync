/*
 * What a store has right now, seen from our side of it.
 *
 * The runner used to ask the store, per style code, "do you already have
 * this pair?". That answers half a question. It tells us where to add stock
 * and it can never tell us where to remove it, because a pair we sold out of
 * is no longer in the list we are walking. So a consignor could withdraw
 * their shoe and it would stay for sale at the store forever.
 *
 * This asks the other way round, and it asks twice.
 *
 * Once at our own location, which is the exact answer to "what did we put
 * here". Every merchant has a Shopify Location ID pointing at their Lojiq
 * fulfilment location; stock on it is ours by construction and stock on
 * theirs is invisible to us. That makes a tag or a marker on the product
 * unnecessary - the location already says whose it is.
 *
 * And once over the store's active catalogue, which is what stops us
 * creating a second product for a pair the store already sells itself. That
 * one is only needed for stores that carry their own stock.
 *
 * The two are merged so that price comes from the store and quantity comes
 * from our location. Getting that the wrong way round would read the store's
 * own stock as ours and then set it to zero.
 */

/*
 * What is on our location.
 *
 * Built rather than written out, because one store wants a flag on each
 * variant and the others do not. Asking for a metafield takes a namespace
 * and a key that cannot be left empty, so the field is added to the query
 * only when a store has actually asked for one.
 */
function ourLevelsQuery(flag) {
  const metafield = flag
    ? `metafield(namespace: "${flag.namespace}", key: "${flag.key}") { value }`
    : "";

  return `
    query OurStock($id: ID!, $cursor: String) {
      location(id: $id) {
        name
        inventoryLevels(first: 250, after: $cursor) {
          nodes {
            quantities(names: ["available"]) { name quantity }
            item {
              id
              variant {
                id
                price
                inventoryQuantity
                selectedOptions { name value }
                product { id status }
                ${metafield}
              }
              sku
            }
          }
          pageInfo { hasNextPage endCursor }
        }
      }
    }
  `;
}

/*
 * Which flag a store wants on its variants, if any.
 *
 * FastCop's theme reads a boolean at custom.kickzcaviar and shows the pair
 * differently for it. That is one store's arrangement, so it lives in that
 * store's Merchants row rather than in this file, written as
 * "custom.kickzcaviar". Empty means the store wants none, which is every
 * store but that one.
 *
 * Always a boolean. The value we write is true or false and nothing else, so
 * letting the type be configured would only have let somebody ask for a
 * number and get the word "true" in it. A store that genuinely wants
 * something else is a change here, and an honest one.
 */
export function readFlagSetting(merchantFields, fieldName = "Consignment Flag Metafield") {
  const raw = String(merchantFields?.[fieldName] || "").trim();

  if (!raw) return null;

  const [namespace, key, ...rest] = raw.split(".").map((part) => part.trim());

  /*
    Checked here rather than found out at the store. Shopify only takes
    letters, digits, underscores and hyphens, so a typo would come back as a
    userError on every variant in the batch and be read as "the flag is
    broken" instead of "the setting is".
  */
  const allowed = /^[A-Za-z0-9_-]+$/;

  if (!namespace || !key || rest.length || !allowed.test(namespace) || !allowed.test(key)) {
    console.warn("Consignment Flag Metafield is not usable, so no flag is set", { value: raw });

    return null;
  }

  return { namespace, key, type: "boolean" };
}

const CATALOGUE = `
  query Catalogue($cursor: String) {
    products(first: 250, after: $cursor, query: "status:active") {
      nodes {
        id
        variants(first: 250) {
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
      pageInfo { hasNextPage endCursor }
    }
  }
`;

function toLocationGid(locationId) {
  const clean = String(locationId || "").trim();

  if (!clean) return "";

  return clean.startsWith("gid://") ? clean : `gid://shopify/Location/${clean}`;
}

/*
 * The size, whatever the shop called the option.
 *
 * Ours say "Maat", a Spanish store says "Talla", Shopify's own default is
 * "Size". Taking the first option's value rather than matching on a name is
 * what the rest of this branch already does.
 */
function sizeOf(variant) {
  return (variant?.selectedOptions || [])[0]?.value || "";
}

function quantityOf(node) {
  const available = (node?.quantities || []).find((q) => q.name === "available");

  return Number(available?.quantity) || 0;
}

/*
 * Everything sitting on our own location, whatever state the product is in.
 *
 * Deliberately not filtered on active. A store that drafts or archives a
 * product we stocked still holds our consignor's shoe in its inventory, and
 * that has to be able to go back to zero.
 */
export async function readOurLocationStock(graphql, locationId, flag = null) {
  const id = toLocationGid(locationId);

  if (!id) throw new Error("No Shopify Location ID for this merchant");

  const rows = [];

  let cursor = null;

  for (;;) {
    const data = await graphql(ourLevelsQuery(flag), { id, cursor });
    const location = data?.location;

    if (!location) throw new Error(`Location ${id} not found in this store`);

    for (const node of location.inventoryLevels?.nodes || []) {
      const variant = node.item?.variant;

      if (!variant) continue;

      rows.push({
        sku: node.item?.sku || variant.sku || "",
        size: sizeOf(variant),
        productId: variant.product?.id || null,
        variantId: variant.id,
        inventoryItemId: node.item?.id || null,
        price: Number(variant.price) || 0,

        // Ours, at our location. Not the store-wide figure.
        quantity: quantityOf(node),

        /*
          What the store holds of this variant somewhere other than here.
          Above zero means the pair is theirs as well as ours, which is the
          one case where we must leave the price alone.
        */
        otherStock: Math.max(0, (Number(variant.inventoryQuantity) || 0) - quantityOf(node)),

        productStatus: variant.product?.status || "",

        /*
          What the store's flag says today, so only the ones that disagree
          have to be written. Absent means never set, which is not the same
          as false and still needs writing.
        */
        flagValue: flag ? (variant.metafield?.value ?? null) : null
      });
    }

    if (!location.inventoryLevels?.pageInfo?.hasNextPage) break;

    cursor = location.inventoryLevels.pageInfo.endCursor;
  }

  return rows;
}

/*
 * The store's own active catalogue.
 *
 * Only needed to avoid creating a product that already exists, so it is
 * skipped entirely for a store that has no stock of its own - which is every
 * store we run this for today.
 */
export async function readStoreCatalogue(graphql) {
  const rows = [];

  let cursor = null;

  for (;;) {
    const data = await graphql(CATALOGUE, { cursor });
    const products = data?.products;

    for (const product of products?.nodes || []) {
      for (const variant of product.variants?.nodes || []) {
        if (!String(variant.sku || "").trim()) continue;

        rows.push({
          sku: variant.sku,
          size: sizeOf(variant),
          productId: product.id,
          variantId: variant.id,
          inventoryItemId: variant.inventoryItem?.id || null,
          price: Number(variant.price) || 0,

          /*
            Zero, not the store's figure. This row says "the pair exists
            here", never "we have stock here" - the location read is the only
            thing allowed to say that. A store's own five pairs read as ours
            would be set to zero on the first run.
          */
          quantity: 0,

          otherStock: Number(variant.inventoryQuantity) || 0,
          productStatus: "ACTIVE"
        });
      }
    }

    if (!products?.pageInfo?.hasNextPage) break;

    cursor = products.pageInfo.endCursor;
  }

  return rows;
}

/*
 * Does this store keep stock of its own?
 *
 * Answered from its locations rather than from a checkbox somebody has to
 * remember to tick. A store that only dropships what we send has one
 * location, the Lojiq fulfilment one; a store with a shop floor has its own
 * beside it.
 *
 * Not a guarantee that the other location holds anything, so it errs towards
 * doing the extra read. That is the safe direction: the catalogue read only
 * ever stops us creating a duplicate product.
 */
export async function hasOtherLocations(graphql, locationId) {
  const ours = toLocationGid(locationId);

  const data = await graphql(`query { locations(first: 50) { nodes { id isActive } } }`);

  return (data?.locations?.nodes || []).some((node) => node.isActive && node.id !== ours);
}

/*
 * Both reads, in the order the indexer needs them.
 *
 * indexCurrentListings keeps the last row it sees for a size, so our
 * location goes last and wins on quantity. The catalogue rows underneath it
 * supply the product id and the store's price for pairs we have not stocked
 * yet.
 */
export async function readStoreState(graphql, { locationId, includeCatalogue = null, flag = null }) {
  const wantCatalogue =
    includeCatalogue === null ? await hasOtherLocations(graphql, locationId) : includeCatalogue;

  const catalogue = wantCatalogue ? await readStoreCatalogue(graphql) : [];
  const ours = await readOurLocationStock(graphql, locationId, flag);

  return {
    rows: [...catalogue, ...ours],
    readCatalogue: wantCatalogue,

    /*
      Which variants already have a level on our location. Anything we want
      to stock that is not in here has to be connected first, or the quantity
      is refused rather than created.
    */
    ourVariantIds: new Set(ours.map((row) => row.variantId)),

    catalogueRows: catalogue.length,
    ourRows: ours.length,

    /*
      Which variants the store holds stock of elsewhere, so the caller can
      keep its hands off their prices. Keyed by variant id because that is
      what a plan entry carries.
    */
    theirStock: new Map(
      [...catalogue, ...ours]
        .filter((row) => row.otherStock > 0)
        .map((row) => [row.variantId, row.otherStock])
    )
  };
}
