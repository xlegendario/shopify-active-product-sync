/*
 * One autopricing pass for one store.
 *
 * Reads the store's shelf (catalogue choices "add"), refreshes the market
 * prices that are due, decides every size with autoprice.js and writes the
 * outcome to autoprice_listings. A store with "Express Option?" on also gets
 * the open prices written to Shopify as "<size> / Standard" variants
 * (standardWriter.js); for any other store nothing goes to Shopify.
 */
import { priceSize } from "./autoprice.js";
import { refreshMarketPrices, readDewuFee, supabase } from "./marketPrices.js";
import { expressOptionOn } from "./delivery.js";
import { writeStandardVariants } from "./standardWriter.js";
import { createMissingPages } from "./standardPages.js";

const clean = (sku) => String(sku || "").trim().toUpperCase().replace(/\s+/g, "-");

export async function autopriceMerchant({ merchant, env = process.env, refresh = true, onlySkus = null, graphql = null, apply = true, photoSources = [], log = console }) {
  const merchantId = merchant["Record ID"];
  const db = supabase({ supabaseUrl: env.SUPABASE_URL, supabaseKey: env.SUPABASE_SERVICE_ROLE_KEY });

  let skus = onlySkus;

  if (!skus) {
    const choices = await db.select(
      "store_catalogue_choices",
      "select=sku&choice=eq.add&merchant_record_id=eq." + encodeURIComponent(merchantId) + "&order=sku.asc"
    );
    skus = choices.map((c) => c.sku);
  }

  skus = [...new Set(skus.map(clean).filter(Boolean))];

  let market = null;

  if (refresh) {
    const dewuFee = await readDewuFee({ airtableToken: env.AIRTABLE_TOKEN, airtableBaseId: env.AIRTABLE_BASE_ID });
    /*
      GOAT costs Retailed calls (one per product per day), so it is a switch
      of its own: AUTOPRICE_GOAT=true once the Retailed plan carries it.
    */
    const retailedKey = String(env.AUTOPRICE_GOAT || "").toLowerCase() === "true" ? env.RETAILED_API_KEY : "";
    market = await refreshMarketPrices(skus, { db, retailedKey, dewuFee, log });
  }

  const observations = new Map();
  const previous = new Map();

  for (let i = 0; i < skus.length; i += 100) {
    const list = "(" + skus.slice(i, i + 100).map((s) => `"${s}"`).join(",") + ")";

    /*
      Ordered on purpose. select() pages through with Range headers, and
      without an order Postgres may hand back the pages in a different
      order each time - rows skipped on one page and repeated on the next.
      A batch of 100 style codes is ~8000 price rows, so it pages, and the
      skipped ones turned whole products "no fresh cost" (U9060PB, 17-11).
    */
    for (const r of await db.select("market_prices", "select=sku,size,source,cost,observed_at&order=sku,size,source&sku=in." + list)) {
      const key = r.sku + "|" + r.size;
      if (!observations.has(key)) observations.set(key, {});
      observations.get(key)[r.source] = { cost: Number(r.cost), observedAt: r.observed_at };
    }

    for (const r of await db.select(
      "autoprice_listings",
      "select=sku,size,status,price,priced_at&order=sku,size&merchant_record_id=eq." + encodeURIComponent(merchantId) + "&sku=in." + list
    )) {
      previous.set(r.sku + "|" + r.size, { status: r.status, price: Number(r.price) || null, pricedAt: r.priced_at });
    }
  }

  const now = new Date();
  const rows = [];
  const tally = { open: 0, closed: 0, bySource: {} };

  for (const key of new Set([...observations.keys(), ...previous.keys()])) {
    const [sku, size] = key.split("|");
    const d = priceSize({ observations: observations.get(key) || {}, previous: previous.get(key) || null, merchantFields: merchant, now });

    tally[d.status] += 1;
    if (d.source) tally.bySource[d.source] = (tally.bySource[d.source] || 0) + 1;

    rows.push({
      merchant_record_id: merchantId, sku, size, status: d.status, source: d.source, cost: d.cost,
      target_price: d.targetPrice, price: d.price, reason: d.reason,
      priced_at: d.pricedAt ? new Date(d.pricedAt).toISOString() : null, updated_at: now.toISOString()
    });
  }

  await db.upsert("autoprice_listings", rows, "merchant_record_id,sku,size");

  let shopify = null;

  if (graphql && expressOptionOn(merchant)) {
    const listingsBySku = new Map();

    for (const r of rows) {
      if (r.status !== "open" || !(r.price > 0)) continue;
      if (!listingsBySku.has(r.sku)) listingsBySku.set(r.sku, []);
      listingsBySku.get(r.sku).push({ size: r.size, price: r.price });
    }

    shopify = await writeStandardVariants({ graphql, skus, listingsBySku, apply, log });

    /*
      A pair the store chose that has no page yet - nobody ever held it, so
      the consignment run never built one. With Standard it can be sold, so
      the page is made here and then given its Standard sizes.
    */
    const toBuild = (shopify.missing || []).filter((sku) => listingsBySku.has(sku));

    if (toBuild.length) {
      const drafts = new Set(
        (await db.select(
          "store_catalogue_choices",
          "select=sku,photos&photos=eq.own&merchant_record_id=eq." + encodeURIComponent(merchantId)
        ).catch(() => [])).map((c) => clean(c.sku))
      );

      const pages = await createMissingPages({
        graphql, merchant, skus: toBuild, listingsBySku, photoSources, drafts,
        supabaseUrl: env.SUPABASE_URL, supabaseKey: env.SUPABASE_SERVICE_ROLE_KEY, apply, log
      });

      const madeNow = [...pages.created, ...pages.drafts].filter((p) => p.productId);
      const second = apply && madeNow.length
        ? await writeStandardVariants({
          graphql, skus: madeNow.map((p) => p.sku), listingsBySku, apply, log,
          pageIds: new Map(madeNow.map((p) => [p.sku, p.productId]))
        })
        : null;

      shopify = { ...shopify, pages: { created: pages.created.length, drafts: pages.drafts.length, skipped: pages.skipped, problems: pages.problems }, afterPages: second };
    }
  }

  const summary = { merchant: merchant["Client ID"], skus: skus.length, sizes: rows.length, ...tally, market, shopify };
  log.log("AUTOPRICE " + JSON.stringify(summary));

  return summary;
}
