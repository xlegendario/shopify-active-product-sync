/*
 * One autopricing pass for one store.
 *
 * Reads the store's shelf (catalogue choices "add"), refreshes the market
 * prices that are due, decides every size with autoprice.js and writes the
 * outcome to autoprice_listings. Nothing goes to Shopify from here: that is
 * a later step, switched on per store once the numbers have been checked.
 */
import { priceSize } from "./autoprice.js";
import { refreshMarketPrices, readDewuFee, supabase } from "./marketPrices.js";

const clean = (sku) => String(sku || "").trim().toUpperCase().replace(/\s+/g, "-");

export async function autopriceMerchant({ merchant, env = process.env, refresh = true, onlySkus = null, log = console }) {
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
    market = await refreshMarketPrices(skus, { db, retailedKey: env.RETAILED_API_KEY, dewuFee, log });
  }

  const observations = new Map();
  const previous = new Map();

  for (let i = 0; i < skus.length; i += 100) {
    const list = "(" + skus.slice(i, i + 100).map((s) => `"${s}"`).join(",") + ")";

    for (const r of await db.select("market_prices", "select=sku,size,source,cost,observed_at&sku=in." + list)) {
      const key = r.sku + "|" + r.size;
      if (!observations.has(key)) observations.set(key, {});
      observations.get(key)[r.source] = { cost: Number(r.cost), observedAt: r.observed_at };
    }

    for (const r of await db.select(
      "autoprice_listings",
      "select=sku,size,status,price,priced_at&merchant_record_id=eq." + encodeURIComponent(merchantId) + "&sku=in." + list
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

  const summary = { merchant: merchant["Client ID"], skus: skus.length, sizes: rows.length, ...tally, market };
  log.log("AUTOPRICE " + JSON.stringify(summary));

  return summary;
}
