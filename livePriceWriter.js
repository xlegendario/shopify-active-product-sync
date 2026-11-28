/*
 * Autopricing prices on a store's own variants.
 *
 * For a store with its own autopricing margins (autoprice_settings) and only
 * for the products it put live in the portal (autoprice_live). Everything
 * else stays in test mode: priced in autoprice_listings, shown in the portal,
 * never written to Shopify.
 *
 * Left alone:
 *   - a size with a Custom Price: the store priced it itself (own stock, or
 *     simply its own choice);
 *   - a size on "Keep my price" (price_mode own), set in the portal: the
 *     store keeps that price, live product or not;
 *   - a closed size (no fresh cost): its price stays as it was;
 *   - a size whose shop price already is the autopricing price.
 *
 * NOT YET - sizes with Lojiq consignment stock in the store. The consignment
 * push prices those too, every 30 minutes, so the two would take turns. Fine
 * for a store without our stock; to be settled (lowest of the two wins)
 * before such a store gets stock.
 */
const UPDATE = `
  mutation autopriceLive($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
    productVariantsBulkUpdate(productId: $productId, variants: $variants) {
      productVariants { id price }
      userErrors { field message }
    }
  }
`;

const clean = (sku) => String(sku || "").trim().toUpperCase().replace(/\s+/g, "-");
const gid = (kind, id) => (String(id).startsWith("gid://") ? String(id) : `gid://shopify/${kind}/${id}`);

/*
 * What would change: one entry per variant whose price moves.
 * listings: store_listings rows; prices: Map "SKU|size" -> price.
 */
export function planLivePrices({ listings, prices }) {
  const changes = [];
  const skipped = { custom: 0, own: 0, same: 0, noPrice: 0 };

  for (const l of listings) {
    if (!l.shopify_variant_id || !l.shopify_product_id) continue;

    const price = prices.get(clean(l.sku) + "|" + String(l.size || "").trim());

    if (!(price > 0)) { skipped.noPrice += 1; continue; }
    if (Number(l.custom_price) > 0 || l.price_mode === "custom") { skipped.custom += 1; continue; }
    if (l.price_mode === "own") { skipped.own += 1; continue; }

    const now = Number(l.store_price ?? l.shopify_price);

    if (Number.isFinite(now) && Math.abs(now - price) < 0.005) { skipped.same += 1; continue; }

    changes.push({
      sku: clean(l.sku), size: l.size, productId: l.shopify_product_id, variantId: l.shopify_variant_id,
      from: Number.isFinite(now) ? now : null, to: price
    });
  }

  return { changes, skipped };
}

export async function writeLivePrices({ db, graphql, merchantId, liveSkus, rows, apply = true, log = console }) {
  if (!liveSkus.size) return { live: 0 };

  const prices = new Map();

  for (const r of rows) {
    if (r.status === "open" && r.price > 0 && liveSkus.has(r.sku)) prices.set(r.sku + "|" + r.size, r.price);
  }

  const list = [...liveSkus];
  const listings = [];

  for (let i = 0; i < list.length; i += 100) {
    // The catalogue keeps the store's own spelling, and a few are lower case.
    const spellings = list.slice(i, i + 100).flatMap((s) => [s, s.toLowerCase()]);
    const inList = "(" + [...new Set(spellings)].map((s) => `"${s}"`).join(",") + ")";
    const products = await db.select(
      "store_catalogue_products",
      "select=shopify_product_id&order=shopify_product_id.asc&merchant_record_id=eq." + encodeURIComponent(merchantId) + "&sku=in." + inList
    );
    const ids = [...new Set(products.map((p) => p.shopify_product_id).filter(Boolean))];

    if (!ids.length) continue;

    /*
      On the product id (idx_store_listings_lookup): store_listings has no
      index on merchant + sku, and a big store has 40k rows.
    */
    listings.push(...await db.select(
      "store_listings",
      "select=sku,size,shopify_product_id,shopify_variant_id,shopify_price,store_price,custom_price,price_mode&order=id.asc" +
        "&merchant_record_id=eq." + encodeURIComponent(merchantId) +
        "&shopify_product_id=in.(" + ids.map((id) => `"${id}"`).join(",") + ")"
    ));
  }

  const { changes, skipped } = planLivePrices({ listings, prices });
  const byProduct = new Map();

  for (const c of changes) {
    if (!byProduct.has(c.productId)) byProduct.set(c.productId, []);
    byProduct.get(c.productId).push(c);
  }

  const problems = [];
  let written = 0;

  if (apply && graphql) {
    for (const [productId, group] of byProduct) {
      try {
        const data = await graphql(UPDATE, {
          productId: gid("Product", productId),
          variants: group.map((c) => ({ id: gid("ProductVariant", c.variantId), price: String(c.to) }))
        });
        const errors = data?.productVariantsBulkUpdate?.userErrors || [];

        if (errors.length) problems.push({ sku: group[0].sku, errors: errors.map((e) => e.message) });
        else written += group.length;
      } catch (error) {
        problems.push({ sku: group[0].sku, errors: [error.message] });
      }
    }
  }

  const report = {
    live: liveSkus.size, variants: listings.length, changes: changes.length, written, skipped, problems,
    sample: changes.slice(0, 5)
  };

  log.log("AUTOPRICE LIVE " + JSON.stringify({ merchant: merchantId, apply, ...report, sample: undefined }));

  return report;
}
