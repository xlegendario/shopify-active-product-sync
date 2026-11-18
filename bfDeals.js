/*
 * Our own pairs on sale, made visible in the shop.
 *
 * Kickz Caviar puts its own pairs on sale in Lojiq Admin, and they reach the
 * pot as own_stock rows (kcOwnStock in sneakerask-consignment). The price is
 * already handled: Price Sync prices the store from the sale ask like any
 * other ask. What a price cannot say is that it is a discount. That is what
 * this does, for the stores that show one (UNION Amsterdam, for its Black
 * Friday ads):
 *
 *   - lojiq.ref_price on the variant: what the store asked for this size
 *     before the sale, captured once on the run that first lists the deal
 *     and then kept while the deal lasts (Dario, 18-11-2026, option B). Never
 *     above what the pair's Ideal would give. The theme's "from" price is that
 *     times the Black Friday mark-up, so a pair dropped from 204 to 173 shows
 *     the drop on top of the 25% every other pair shows - and never a "from"
 *     price the shop never asked.
 *   - a size with no earlier price in the store (a page built for the deal)
 *     gets no ref_price: there is no "before" to show.
 *   - the tag bf-deal on the product, which the "Black Friday Deals"
 *     collection is built on.
 *   - a page that is a draft goes live. UNION's curation drafted most of its
 *     catalogue; a pair we chose to sell cheaply must not sit on a page
 *     nobody can open.
 *
 * Decided from what the store holds after the run, like the store flag, so a
 * pair sold or taken off sale loses its "from" price and its tag on the next
 * run. A page that went live for a sale stays live; taking it down is a shelf
 * decision, made in Your Shelf.
 */
import { normalizeSku, sizeKey } from "./desiredListings.js";

export const DEAL_TAG = "bf-deal";
const NAMESPACE = "lojiq";
const KEY = "ref_price";

/*
 * Which stores show it. By name, because it is a marketing decision about a
 * shop rather than a setting every merchant row needs a column for.
 */
export function showsDeals(merchantName, env = process.env) {
  const names = String(env.BF_DEAL_MERCHANTS ?? "UNION Amsterdam")
    .split(",")
    .map((name) => name.trim().toLowerCase())
    .filter(Boolean);

  return names.includes(String(merchantName || "").trim().toLowerCase());
}

const money = (value) => (Math.round(Number(value) * 100) / 100).toFixed(2);

/*
 * What has to change, from the rows on our location and the listings.
 *
 * A variant is a deal while we hold it (quantity above zero) and the pair we
 * list there is one of ours on sale with a reference price. Everything else
 * that carries a ref_price loses it.
 */
export function dealChanges({ rows, listings, priceBefore = new Map() }) {
  const reference = new Map();

  for (const listing of listings || []) {
    if (!listing.ownStock || !(Number(listing.referenceSellingPrice) > 0)) continue;

    reference.set(`${normalizeSku(listing.sku)}|${sizeKey(listing.size)}`, {
      cap: Number(listing.referenceSellingPrice),
      selling: Number(listing.sellingPrice)
    });
  }

  const setRefs = [];
  const clearRefs = [];
  const products = new Map();

  for (const row of rows || []) {
    if (!row.variantId || !row.productId) continue;

    const deal = row.quantity > 0
      ? reference.get(`${normalizeSku(row.sku)}|${sizeKey(row.size)}`) ?? null
      : null;

    const has = row.refPrice === null || row.refPrice === undefined || row.refPrice === ""
      ? null
      : Number(row.refPrice);

    /*
      Kept once captured: the run after the first one sees the sale price as
      the store's price, and would otherwise move the "from" price down with
      it. Captured from what the store asked when this run began, capped at
      the Ideal's price, and only when that is above what it sells for now.
    */
    const before = Number(priceBefore.get(row.variantId)) || 0;
    const wanted = !deal
      ? null
      : has !== null
        ? has
        : before > deal.selling
          ? Math.min(before, deal.cap)
          : null;

    if (wanted !== null && (has === null || money(has) !== money(wanted))) {
      setRefs.push({ variantId: row.variantId, value: money(wanted) });
    } else if (wanted === null && has !== null) {
      clearRefs.push({ variantId: row.variantId });
    }

    const product = products.get(row.productId) || {
      productId: row.productId,
      sku: row.sku,
      status: row.productStatus,
      tagged: (row.productTags || []).includes(DEAL_TAG),
      deal: false
    };

    if (deal) product.deal = true;

    products.set(row.productId, product);
  }

  const all = [...products.values()];

  return {
    setRefs,
    clearRefs,
    tag: all.filter((p) => p.deal && !p.tagged).map((p) => p.productId),
    untag: all.filter((p) => !p.deal && p.tagged).map((p) => p.productId),
    activate: all.filter((p) => p.deal && p.status === "DRAFT").map((p) => p.productId),
    deals: all.filter((p) => p.deal).length
  };
}

const SET = `
  mutation setRefs($metafields: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $metafields) { userErrors { field message } }
  }
`;

const DELETE = `
  mutation clearRefs($metafields: [MetafieldIdentifierInput!]!) {
    metafieldsDelete(metafields: $metafields) { userErrors { field message } }
  }
`;

const TAG = `
  mutation tag($id: ID!, $tags: [String!]!) {
    tagsAdd(id: $id, tags: $tags) { userErrors { field message } }
  }
`;

const UNTAG = `
  mutation untag($id: ID!, $tags: [String!]!) {
    tagsRemove(id: $id, tags: $tags) { userErrors { field message } }
  }
`;

const ACTIVATE = `
  mutation activate($product: ProductUpdateInput!) {
    productUpdate(product: $product) { userErrors { field message } }
  }
`;

/*
 * Written one step at a time, each step's problems kept rather than thrown:
 * a tag that does not land is a product missing from a collection for half
 * an hour, never a reason to stop the push behind it.
 */
export async function writeDealChanges(graphql, changes) {
  const problems = [];

  const step = async (label, query, variables, field) => {
    try {
      const data = await graphql(query, variables);
      const errors = data?.[field]?.userErrors || [];
      if (errors.length) problems.push({ label, errors });
    } catch (err) {
      problems.push({ label, errors: [{ message: err.message }] });
    }
  };

  for (let i = 0; i < changes.setRefs.length; i += 25) {
    await step("ref_price set", SET, {
      metafields: changes.setRefs.slice(i, i + 25).map((c) => ({
        ownerId: c.variantId,
        namespace: NAMESPACE,
        key: KEY,
        type: "number_decimal",
        value: c.value
      }))
    }, "metafieldsSet");
  }

  for (let i = 0; i < changes.clearRefs.length; i += 25) {
    await step("ref_price clear", DELETE, {
      metafields: changes.clearRefs.slice(i, i + 25).map((c) => ({ ownerId: c.variantId, namespace: NAMESPACE, key: KEY }))
    }, "metafieldsDelete");
  }

  for (const id of changes.tag) await step(`tag ${id}`, TAG, { id, tags: [DEAL_TAG] }, "tagsAdd");
  for (const id of changes.untag) await step(`untag ${id}`, UNTAG, { id, tags: [DEAL_TAG] }, "tagsRemove");
  for (const id of changes.activate) await step(`activate ${id}`, ACTIVATE, { product: { id, status: "ACTIVE" } }, "productUpdate");

  return problems;
}
