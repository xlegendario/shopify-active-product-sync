/*
 * What a pair costs us on each sourcing platform, read into market_prices.
 *
 * Three readers run from this service, without a browser:
 *
 *   stockx_api  the official API, all sizes in one call. Only a stand-in:
 *               what we really pay comes from the Pro page, which the
 *               autopricing extension posts to /autoprice/stockx-pro.
 *   dewu        tosneaker's Dewu Europe feed, RMB per EU size.
 *   goat        Retailed's GOAT prices for France in dollars, per US size,
 *               converted to what the Dutch page charges (see autoprice.js).
 *
 * Where to find a style on each platform is looked up once and kept in
 * market_products, so a daily read is one or two calls per product.
 */
import { createStockxClient } from "./stockxSizes.js";
import { normalizeSize as sizeKey } from "./sizes.js";
import { safeStockxCost, goatCost, goatEuroCost, dewuCost } from "./autoprice.js";

const STOCKX = "https://api.stockx.com/v2/catalog";
const RETAILED = "https://app.retailed.io/api/v1/scraper/goat";
const DEWU = "https://product.tosneaker.com/api/v1/dewu/europe";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const clean = (sku) => String(sku || "").trim().toUpperCase().replace(/\s+/g, "-");

/* ------------------------------------------------------------------ *
 * Supabase
 * ------------------------------------------------------------------ */

export function supabase({ supabaseUrl, supabaseKey }) {
  const base = String(supabaseUrl).replace(/[/]+$/, "") + "/rest/v1/";
  const headers = { apikey: supabaseKey, Authorization: "Bearer " + supabaseKey, "Content-Type": "application/json" };

  async function select(table, query) {
    const rows = [];
    const page = 1000;

    // Paged: a single read stops at 1000 rows without saying so.
    for (let from = 0; ; from += page) {
      const res = await fetch(base + table + "?" + query, { headers: { ...headers, Range: `${from}-${from + page - 1}` } });

      if (!res.ok) throw new Error(`Supabase ${res.status} reading ${table}: ${await res.text()}`);

      const batch = await res.json();
      rows.push(...batch);

      if (batch.length < page) return rows;
    }
  }

  async function upsert(table, rows, onConflict) {
    for (let i = 0; i < rows.length; i += 500) {
      const res = await fetch(base + table + "?on_conflict=" + onConflict, {
        method: "POST",
        headers: { ...headers, Prefer: "resolution=merge-duplicates,return=minimal" },
        body: JSON.stringify(rows.slice(i, i + 500))
      });

      if (!res.ok) throw new Error(`Supabase ${res.status} writing ${table}: ${await res.text()}`);
    }
  }

  async function remove(table, query) {
    const res = await fetch(base + table + "?" + query, { method: "DELETE", headers: { ...headers, Prefer: "return=minimal" } });

    if (!res.ok) throw new Error(`Supabase ${res.status} deleting from ${table}: ${await res.text()}`);
  }

  return { select, upsert, remove };
}

/* ------------------------------------------------------------------ *
 * Finding a style on each platform
 * ------------------------------------------------------------------ */

async function findStockx(client, sku) {
  // StockX answers an unknown style code with a 404 rather than an empty list.
  const search = await client.call(`${STOCKX}/search?query=${encodeURIComponent(sku)}&pageSize=10`).catch((error) => {
    if (/StockX 404/.test(error.message)) return null;
    throw error;
  });
  const hit = (search?.products || []).find((p) => clean(p.styleId) === sku);

  if (!hit) return null;

  const variants = await client.call(`${STOCKX}/products/${hit.productId}/variants`);

  /*
    Per variant: its EU size (our key everywhere) and the size the product
    is natively sold in. GOAT keys on the native one - US men's for most
    sneakers, US women's for a women's style, EU for Mihara - so this is the
    bridge between the two.
  */
  const sizes = (Array.isArray(variants) ? variants : []).map((v) => {
    const eu = (v.sizeChart?.availableConversions || []).find((c) => String(c.type).toLowerCase() === "eu")?.size;

    return {
      variantId: v.variantId,
      eu: sizeKey(eu || v.variantValue),
      native: String(v.variantValue || "").replace(/[A-Z]+$/i, "").trim()
    };
  });

  return { productId: hit.productId, sizes };
}

async function findGoat(sku, apiKey) {
  const res = await fetch(`${RETAILED}/search?query=${encodeURIComponent(sku)}`, { headers: { "x-api-key": apiKey } });

  if (!res.ok) throw new Error(`Retailed ${res.status} on GOAT search`);

  const body = await res.json();
  const list = Array.isArray(body) ? body : [body];

  // The slug ends in the style code; anything else is a different pair.
  const hit = list.find((x) => String(x?.slug || "").toUpperCase().endsWith(sku));

  return hit ? { id: String(hit.id), slug: hit.slug } : null;
}

async function dewuJson(url) {
  const res = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" } });

  if (!res.ok) throw new Error(`tosneaker ${res.status}`);

  return res.json();
}

async function findDewu(sku) {
  const body = await dewuJson(`${DEWU}/searchProduct?keyword=${encodeURIComponent(sku)}`);
  let list = body?.data?.data || body?.data || [];

  if (!Array.isArray(list)) list = [list];

  const hit = list.find((p) => clean(p.article_number) === sku);

  return hit ? String(hit.spu_id || hit.dw_spu_id) : null;
}

/* ------------------------------------------------------------------ *
 * Reading the prices
 * ------------------------------------------------------------------ */

async function readStockxApi(client, product) {
  const market = await client.call(`${STOCKX}/products/${product.stockx_product_id}/market-data?currencyCode=EUR`);
  const byVariant = new Map((product.stockx_sizes || []).map((s) => [s.variantId, s.eu]));
  const out = [];

  for (const m of Array.isArray(market) ? market : []) {
    const size = byVariant.get(m.variantId);
    const ask = Number(m.standardMarketData?.lowestAsk ?? m.lowestAskAmount) || null;
    const cost = safeStockxCost(ask);

    if (size && cost) out.push({ size, cost, raw: { lowestAsk: ask } });
  }

  return out;
}

async function readGoat(product, apiKey) {
  const res = await fetch(`${RETAILED}/prices?query=${product.goat_id}&country=FR`, { headers: { "x-api-key": apiKey } });

  if (!res.ok) throw new Error(`Retailed ${res.status} on GOAT prices`);

  const rows = await res.json();

  if (!Array.isArray(rows)) throw new Error("Retailed gave no price list");

  const euByNative = new Map((product.stockx_sizes || []).map((s) => [s.native, s.eu]));
  const out = [];

  for (const r of rows) {
    // New, in a good box: what the page sells as "Buy New".
    if (r.shoeCondition !== "new_no_defects" || r.boxCondition !== "good_condition") continue;

    const usd = (r.lowestPriceCents?.amount || 0) / 100;
    const size = euByNative.get(String(r.sizeOption?.value));
    const cost = goatCost(usd);

    if (size && cost) out.push({ size, cost, raw: { frUsd: usd } });
  }

  return out;
}

/*
 * Dewu's size label as an EU size.
 *
 * Usually just "42.5", but New Balance comes as colour, width and size:
 * "灰棕色 D宽 42.5". Only the standard D width is the pair we sell; a wide
 * fit (2E, 4E) is a different shoe and is left out.
 */
export function dewuSize(label) {
  const text = String(label || "").replace(/^EU\s*/i, "").trim();
  const width = text.match(/(\S+)宽/);

  if (width && width[1].toUpperCase() !== "D") return "";

  const size = text.match(/(\d{2}(?:\.5)?(?:\s+[12]\/3)?)\s*$/);

  return size ? sizeKey(size[1]) : "";
}

/*
 * Dewu's half sizes onto an adidas ladder of thirds.
 *
 * Dewu writes adidas as 36, 36.5, 37, 38, 38.5, 39 ... where adidas and
 * StockX write 36, 36 2/3, 37 1/3, 38, 38 2/3, 39 1/3 - the same US sizes
 * (4, 4.5, 5, 5.5, 6, 6.5). Left as they were, Dewu's prices landed on
 * sizes the page does not have and never on the ones it does.
 *
 * Only for a ladder that has thirds; anything else is returned unchanged.
 */
export function dewuOnLadder(size, ladder) {
  const sizes = new Set((ladder || []).map(String));

  if (![...sizes].some((s) => /\d\/3$/.test(s)) || sizes.has(size)) return size;

  const half = String(size).match(/^(\d{2})\.5$/);
  if (half && sizes.has(`${half[1]} 2/3`)) return `${half[1]} 2/3`;

  const whole = String(size).match(/^(\d{2})$/);
  if (whole && sizes.has(`${whole[1]} 1/3`)) return `${whole[1]} 1/3`;

  return "";
}

async function readDewu(product, fee) {
  const body = await dewuJson(`${DEWU}/productPrice?dw_spu_id=${product.dewu_spu_id}`);
  const variants = body?.data?.data || body?.data || [];
  const ladder = (product.stockx_sizes || []).map((s) => s.eu).filter(Boolean);
  const out = [];

  for (const v of Array.isArray(variants) ? variants : []) {
    const normal = Number(v.normal_price) || 0;
    const storage = Number(v.storage_price) || 0;
    const rmb = normal > 0 ? normal : storage;
    const cost = dewuCost(rmb, fee);
    const size = dewuOnLadder(dewuSize(v.size), ladder);

    // The ceiling is applied when pricing, not here: the raw cost stays visible.
    if (size && cost) out.push({ size, cost, raw: { rmb, kind: normal > 0 ? "normal" : "storage" } });
  }

  return out;
}

export async function readDewuFee({ airtableToken, airtableBaseId }) {
  const url = new URL(`https://api.airtable.com/v0/${airtableBaseId}/${encodeURIComponent("Sellers Database")}`);
  url.searchParams.set("filterByFormula", "{Dewu Supplier?} = 1");
  url.searchParams.set("maxRecords", "1");

  const res = await fetch(url, { headers: { Authorization: `Bearer ${airtableToken}` } });
  const fee = Number((await res.json()).records?.[0]?.fields?.["Dewu Fee"]);

  if (!Number.isFinite(fee)) throw new Error("No Dewu Fee in Sellers Database");

  return fee;
}

/* ------------------------------------------------------------------ *
 * One pass over a list of styles
 * ------------------------------------------------------------------ */

const HOURS = 3600000;

/*
 * Refreshes whatever is due for each SKU and writes it to market_prices.
 *
 * every: how many hours between reads, per platform. A platform that fails
 * for one SKU is logged and skipped; the rest carry on.
 */
export async function refreshMarketPrices(skus, {
  db, retailedKey, dewuFee,
  every = { stockx_api: 6, dewu: 24, goat: 24 },
  log = console
} = {}) {
  const client = createStockxClient();
  const wanted = [...new Set(skus.map(clean).filter(Boolean))];
  const known = new Map();

  for (let i = 0; i < wanted.length; i += 100) {
    const chunk = wanted.slice(i, i + 100);
    const rows = await db.select("market_products", "select=*&sku=in.(" + chunk.map((s) => `"${s}"`).join(",") + ")");

    for (const r of rows) known.set(r.sku, r);
  }

  const now = Date.now();
  const due = (at, hours) => !at || now - new Date(at).getTime() >= hours * HOURS;
  const stats = { skus: wanted.length, stockx_api: 0, dewu: 0, goat: 0, errors: 0 };

  for (const sku of wanted) {
    const product = known.get(sku) || { sku };
    const prices = [];
    const note = (source, rows) => {
      /*
        One row per size: two variants can land on the same EU size (a
        women's and a men's label, Dewu's widths), and one write may not
        touch the same row twice. The cheapest is the one we would buy.
      */
      const best = new Map();
      for (const r of rows) if (!best.has(r.size) || r.cost < best.get(r.size).cost) best.set(r.size, r);
      for (const r of best.values()) prices.push({ sku, size: r.size, source, cost: r.cost, raw: r.raw, observed_at: new Date().toISOString() });
      stats[source] += best.size;
    };

    try {
      // Found on a platform once, never searched again; not found is retried weekly.
      if (product.stockx_product_id === undefined || (!product.stockx_product_id && due(product.updated_at, 24 * 7))) {
        const sx = await findStockx(client, sku);
        product.stockx_product_id = sx?.productId || null;
        product.stockx_sizes = sx?.sizes || null;
        if (retailedKey) {
          const goat = await findGoat(sku, retailedKey).catch(() => null);
          product.goat_id = goat?.id || null;
          product.goat_slug = goat?.slug || null;
        }
        product.dewu_spu_id = await findDewu(sku).catch(() => null);
        product.updated_at = new Date().toISOString();
      }

      if (product.stockx_product_id && due(product.api_checked_at, every.stockx_api)) {
        note("stockx_api", await readStockxApi(client, product));
        product.api_checked_at = new Date().toISOString();
      }

      if (product.dewu_spu_id && due(product.dewu_checked_at, every.dewu)) {
        note("dewu", await readDewu(product, dewuFee));
        product.dewu_checked_at = new Date().toISOString();
      }

      if (product.goat_id && retailedKey && product.stockx_sizes && due(product.goat_checked_at, every.goat)) {
        note("goat", await readGoat(product, retailedKey));
        product.goat_checked_at = new Date().toISOString();
      }
    } catch (error) {
      stats.errors += 1;
      log.log(`AUTOPRICE market read failed for ${sku}: ${error.message}`);
    }

    // Pro times are the extension's to write; never put back an old one.
    const { pro_checked_at, ...row } = product;
    try {
      await db.upsert("market_products", [row], "sku");
      if (prices.length) await db.upsert("market_prices", prices, "sku,size,source");
    } catch (error) {
      stats.errors += 1;
      log.log(`AUTOPRICE market write failed for ${sku}: ${error.message}`);
    }

    await sleep(300);
  }

  return stats;
}

/*
 * What the autopricing extension read on the Pro page for one product.
 *
 * sizes: { "42.5": { total, buyNow, asks } } - EU sizes, Total incl. tax.
 */
export async function storeProPrices({ db, sku, sizes }) {
  const at = new Date().toISOString();
  /*
    Only sizes with a Buy Now. Without one the table still shows a Total,
    but that is the total on a bid - nothing anyone is selling - so a size
    like that would be priced on a pair we cannot buy (seen 16-11-2026:
    A01FW702-BLK sizes 49-51 at 74 euros, all without an ask).
  */
  const rows = Object.entries(sizes || {})
    .filter(([, v]) => Number(v?.total) > 0 && Number(v?.buyNow) > 0)
    .map(([size, v]) => ({
      sku: clean(sku), size: sizeKey(size), source: "stockx_pro", cost: Number(v.total),
      depth: Number.isFinite(Number(v.asks)) ? Number(v.asks) : null,
      raw: { buyNow: v.buyNow ?? null }, observed_at: at
    }));

  const unique = [...new Map(rows.sort((x, y) => y.cost - x.cost).map((r) => [r.size, r])).values()];

  /*
    The read replaces the last one for this style: a size whose last ask
    sold since then must not keep its old Pro price for another day and a half.
  */
  if (db.remove) await db.remove("market_prices", `sku=eq.${encodeURIComponent(clean(sku))}&source=eq.stockx_pro`);
  if (unique.length) await db.upsert("market_prices", unique, "sku,size,source");
  await db.upsert("market_products", [{ sku: clean(sku), pro_checked_at: at }], "sku");

  return unique.length;
}

/*
 * What the autopricing extension read on goat.com for one product.
 *
 * sizes: [{ us, cents }] - GOAT's own size value (the product's US scale)
 * and the lowest new ask in a good box, in euro cents. Mapped to EU through
 * StockX's sizes for the same product, whose "native" is that same US scale;
 * a size StockX does not have is left out rather than guessed.
 *
 * The one check is the product: the extension only sends a product whose own
 * sku matches exactly. No price check on top - a GOAT ask far under StockX
 * is usually a real cheap pair in a thin size (EU 47 at 111,58 against
 * 230,37 on StockX, 21-11-2026), which is the point of reading GOAT.
 */
export async function storeGoatPrices({ db, sku, slug = null, sizes = [] }) {
  const at = new Date().toISOString();
  const code = clean(sku);
  const [product] = await db.select("market_products", "select=sku,stockx_sizes&sku=eq." + encodeURIComponent(code));
  const euByUs = new Map((product?.stockx_sizes || []).map((s) => [String(Number(String(s.native).replace(/[^\d.]/g, ""))), s.eu]));

  const rows = [];
  let skipped = 0;

  for (const s of Array.isArray(sizes) ? sizes : []) {
    const eu = euByUs.get(String(Number(s?.us)));
    const cost = goatEuroCost(s?.cents);

    if (!eu || !cost) { skipped += 1; continue; }

    const size = sizeKey(eu);

    rows.push({ sku: code, size, source: "goat", cost, raw: { us: Number(s.us), eurCents: Number(s.cents), slug }, observed_at: at });
  }

  const unique = [...new Map(rows.sort((x, y) => y.cost - x.cost).map((r) => [r.size, r])).values()];

  // Like Pro: this read replaces the last one, so a sold-out size drops out.
  if (db.remove) await db.remove("market_prices", `sku=eq.${encodeURIComponent(code)}&source=eq.goat`);
  if (unique.length) await db.upsert("market_prices", unique, "sku,size,source");
  await db.upsert("market_products", [{ sku: code, goat_slug: slug || null, goat_checked_at: at }], "sku");

  return { stored: unique.length, skipped };
}
