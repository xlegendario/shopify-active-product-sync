/*
 * What sells across the Lojiq network, measured every week (UNION, 18-11-2026).
 *
 * The same measurement the UNION longlist was built with on 14-11-2026
 * (longlist/fetch_sales.py + build.py), now as a job:
 *
 *  - every connected store's Shopify orders, 180 days back (60 when its token
 *    lacks read_all_orders);
 *  - the Unfulfilled Orders Log (marketplaces, and stores without a token),
 *    External Sales and Member WTBs in Airtable.
 *
 * Score per style code:
 *
 *     sum over channels of min(units in that channel, 15)
 *   + 3 x number of channels
 *   + 2 x Member WTBs
 *
 * so one big seller cannot decide the list on its own; selling broadly wins.
 * UNION's own orders and the Test Store are left out.
 *
 * The scores go to storefront_scores, which the nightly storefront job sorts
 * collections by. For each store in TRENDS_CLIENTS, the shoes and clothing
 * that score 30 or more and are not on its shelf yet become suggestions
 * (store_trend_suggestions, shown in Your Shelf > Suggested). A store with
 * "Trend Auto Add?" on in Merchants gets them chosen ('add') straight away.
 *
 * Nothing is ever removed, unpublished or set to draft here: taking things
 * off the shelf stays a person's decision.
 *
 * Everything that decides is pure and tested (trends.test.mjs); the
 * functions that read and write take their clients as arguments.
 */

import { modelFor } from "./storefrontOrder.js";

export const MIN_SCORE = 30;
export const CHANNEL_CAP = 15;

/* ------------------------------------------------------------------ *
 * Style codes and titles
 * ------------------------------------------------------------------ */

/* A style code without its size: DD1391-103_36 and DD1391-103-42 are DD1391-103. */
export function normSku(sku) {
  let s = String(sku || "").toUpperCase().replace(/\s+/g, "");
  s = s.replace(/_[0-9]{2}(\.[05])?$/, "");
  s = s.replace(/^([A-Z0-9]{5,}-[A-Z0-9]{3})-[0-9]{2}(\.5)?$/, "$1");
  return s;
}

/* A title without its size, reduced to letters and digits, for matching rows without a SKU. */
export function titleKey(title) {
  let t = String(title || "").toLowerCase();
  t = t.replace(/\s(eu\s*)?\d{2}(\.5| 1\/3| 2\/3)?\s*$/, "");
  return t.replace(/[^a-z0-9]+/g, "");
}

/* ------------------------------------------------------------------ *
 * Shoes and clothing only
 * ------------------------------------------------------------------ */

/*
 * Not for a sneaker and streetwear shelf. The longlist's list (build.py),
 * widened with what a weekly run will otherwise let through: trading cards
 * and collectibles, accessories, gift cards and services.
 */
export const JUNK = new RegExp(
  [
    // services and packaging (longlist)
    "verzendbescherming", "anti-crease", "cleaning kit", "inlegzolen", "insoles?", "heel ?pads", "\\bcover\\b",
    "stoffen tas", "\\bbulk\\b", "bundles?", "destined packs", "ascended",
    "gift ?card", "cadeau", "insurance", "verzend", "shipping", "shoe ?box", "package", "bescherming",
    // underwear and socks
    "\\bsocks?\\b", "\\bsokken\\b", "boxers?", "boxershorts?", "briefs?", "underwear", "\\btrunks?\\b", "\\bbra\\b",
    // cards and collectibles
    "pok[eé]mon", "\\bcards?\\b", "\\btcg\\b", "booster", "elite trainer", "\\betb\\b", "trading", "one piece card",
    "labubu", "pop ?mart", "\\bfunko\\b", "\\blego\\b", "bearbrick|be@rbrick", "\\bfigures?\\b", "plush", "sonny angel",
    // accessories
    "\\bcaps?\\b", "\\bhats?\\b", "beanie", "bucket hat", "\\bbags?\\b", "backpack", "\\btote\\b", "wallet", "keychain|key ?ring",
    "\\bbelt\\b", "sunglasses", "\\bwatch(es)?\\b", "royal pop", "bracelet", "necklace", "\\bring\\b", "jewel", "\\bscarf\\b",
    "gloves?", "\\bmask\\b", "phone ?case", "airpods", "umbrella", "\\bmug\\b", "bottle", "towel", "sticker", "poster",
    "perfume|parfum|fragrance", "\\blaces?\\b", "\\bveters?\\b", "shoe ?trees?", "crep protect", "cleaner",
    // other
    "custom", "cappuccino", "summer item", "portes"
  ].join("|"),
  "i"
);

/*
 * The brands a UNION shelf carries (longlist BRANDS, minus Swatch, whose
 * watches are accessories), and the streetwear labels it already sells.
 */
export const BRANDS = [
  [/yeezy|yzy/, "Yeezy"], [/jordan/, "Jordan"], [/adidas/, "adidas"], [/new balance/, "New Balance"],
  [/asics/, "ASICS"], [/fear of god|essentials/, "Fear of God Essentials"], [/maison mihara/, "Maison Mihara Yasuhiro"],
  [/\bnike\b/, "Nike"], [/\bugg\b/, "UGG"], [/salomon/, "Salomon"], [/timberland/, "Timberland"], [/crocs/, "Crocs"],
  [/\bvans\b/, "Vans"], [/converse/, "Converse"], [/on running|\bon cloud/, "On"], [/saucony/, "Saucony"],
  [/\bpuma\b/, "Puma"], [/jacquemus/, "Jacquemus"], [/supreme/, "Supreme"], [/denim tears/, "Denim Tears"],
  [/under armour/, "Under Armour"], [/broken planet/, "Broken Planet"], [/corteiz|crtz/, "Corteiz"],
  [/sp5der/, "Sp5der"], [/st[uü]ssy/, "Stüssy"], [/birkenstock/, "Birkenstock"], [/hoka/, "HOKA"]
];

/* The known brand of a product, or "" when it is none of ours. */
export function knownBrand(title, rawBrand = "") {
  const t = `${title || ""} ${rawBrand || ""}`.toLowerCase();
  for (const [rx, name] of BRANDS) if (rx.test(t)) return name;
  return "";
}

/* Shoes or clothing of a brand we know, or of a model we recognise; never junk. */
export function isWanted({ title, brand }) {
  const text = `${title || ""} ${brand || ""}`;
  if (!String(title || "").trim()) return false;
  if (JUNK.test(text)) return false;
  return Boolean(knownBrand(title, brand) || modelFor(title));
}

/* ------------------------------------------------------------------ *
 * Scoring
 * ------------------------------------------------------------------ */

/*
 * rows: [{ src: store|marketplace|external|wtb, channel, sku, title, brand, qty, status }]
 * skip: channel names (lowercase) whose sales are not counted (the store itself, the test store)
 * extraTitles: [{ sku, title }] - stock names, to give rows without a SKU one
 *
 * Returns Map(style code -> { sku, score, units, wtb, channels: Map(channel -> units), title, brand }).
 */
export function scoreSales(rows, { skip = [], extraTitles = [] } = {}) {
  const skipped = new Set(skip.map((s) => String(s).toLowerCase()));

  // Rows without a SKU take the SKU that most often goes with their title.
  const byTitle = new Map();
  const vote = (title, sku) => {
    const k = titleKey(title);
    if (!k || !sku) return;
    const m = byTitle.get(k) || new Map();
    m.set(sku, (m.get(sku) || 0) + 1);
    byTitle.set(k, m);
  };
  for (const r of rows) if (r.sku && r.title) vote(r.title, normSku(r.sku));
  for (const r of extraTitles) vote(r.title, normSku(r.sku));
  const skuForTitle = (title) => {
    const m = byTitle.get(titleKey(title));
    if (!m) return "";
    return [...m.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0][0];
  };

  const agg = new Map();

  for (const r of rows) {
    if (skipped.has(String(r.channel || "").toLowerCase())) continue;

    const sku = normSku(r.sku) || (r.title ? skuForTitle(r.title) : "");
    if (!sku) continue;

    let a = agg.get(sku);
    if (!a) {
      a = { sku, units: 0, wtb: 0, channels: new Map(), titles: new Map(), brands: new Map() };
      agg.set(sku, a);
    }
    if (r.title) a.titles.set(r.title, (a.titles.get(r.title) || 0) + 1);
    if (r.brand) a.brands.set(r.brand, (a.brands.get(r.brand) || 0) + 1);

    if (r.src === "wtb") { a.wtb += 1; continue; }

    const q = Math.max(0, Math.trunc(Number(r.qty ?? 1)) || 0);
    if (!q) continue;
    a.units += q;
    a.channels.set(r.channel || "unknown", (a.channels.get(r.channel || "unknown") || 0) + q);
  }

  const top = (m) => [...m.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0]?.[0] || "";
  const out = new Map();

  for (const [sku, a] of agg) {
    out.set(sku, {
      sku,
      score: scoreOf({ channels: a.channels, wtb: a.wtb }),
      units: a.units,
      wtb: a.wtb,
      channels: a.channels,
      title: top(a.titles),
      brand: top(a.brands)
    });
  }

  return out;
}

/* The longlist's score: capped units per channel, plus breadth, plus demand. */
export function scoreOf({ channels, wtb = 0 }) {
  let s = 0;
  for (const units of channels.values()) s += Math.min(units, CHANNEL_CAP);
  return s + 3 * channels.size + 2 * wtb;
}

/*
 * The suggestions for one store.
 *
 * scored:  scoreSales() result
 * chosen:  Set of style codes with any catalogue choice for this store
 * onShelf: Set of style codes on the store's Shopify
 * dismissed: Set of style codes the store said "not for us" to
 *
 * No maximum: everything that qualifies is listed, best first.
 */
export function pickCandidates({ scored, chosen = new Set(), onShelf = new Set(), dismissed = new Set(), minScore = MIN_SCORE }) {
  const list = [];

  for (const r of scored.values()) {
    if (r.score < minScore) continue;
    if (r.units === 0 && r.wtb < 2) continue;
    if (!isStyleCode(r.sku)) continue;
    if (chosen.has(r.sku) || onShelf.has(r.sku) || dismissed.has(r.sku)) continue;
    if (!isWanted({ title: r.title, brand: r.brand })) continue;
    list.push(r);
  }

  return list.sort((a, b) => b.score - a.score || b.units - a.units || a.sku.localeCompare(b.sku));
}

/*
 * A style code a page can be made from. Not a set of two codes
 * ("125BT212062F/160BT212012F") and not a shop's own slug
 * ("FEAR-OF-GOD-ESSENTIALS-DARK-OATMEAL-SUMMER-SET").
 */
export function isStyleCode(sku) {
  const s = String(sku || "");
  if (!s || s.length > 24 || /[/\s,]/.test(s)) return false;
  if (/^[A-Z]+(-[A-Z]+){2,}$/.test(s)) return false;
  return /\d/.test(s);
}

/* What is listed about a channel mix: "SneakerAsk 12, Woovin 3". */
export function channelText(channels) {
  return [...channels.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([c, n]) => `${c} ${n}`)
    .join(", ");
}

/* ------------------------------------------------------------------ *
 * Reading sales
 * ------------------------------------------------------------------ */

const txt = (v) => {
  if (Array.isArray(v)) v = v[0];
  return v === null || v === undefined ? "" : String(v).trim();
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/*
 * One store's Shopify orders over REST, the way the longlist read them:
 * cancelled orders out, current_quantity so refunded units drop off.
 */
export async function readStoreOrders({ name, domain, token, apiVersion, fetchFn = fetch, now = new Date() }) {
  const headers = { "X-Shopify-Access-Token": token };
  const scopesRes = await fetchFn(`https://${domain}/admin/oauth/access_scopes.json`, { headers });
  if (!scopesRes.ok) return { report: { store: name, status: `no access (${scopesRes.status})` }, rows: [] };

  const scopes = new Set(((await scopesRes.json()).access_scopes || []).map((s) => s.handle));
  if (!scopes.has("read_orders") && !scopes.has("write_orders")) {
    return { report: { store: name, status: "token has no order access" }, rows: [] };
  }

  const days = scopes.has("read_all_orders") ? 180 : 60;
  const since = new Date(now.getTime() - days * 86400000).toISOString().replace(/\.\d+Z$/, "Z");
  let url = `https://${domain}/admin/api/${apiVersion}/orders.json?status=any&limit=250` +
    `&created_at_min=${since}&fields=id,created_at,cancelled_at,line_items`;

  const rows = [];
  let orders = 0;

  while (url) {
    let res;
    for (let attempt = 0; attempt < 5; attempt += 1) {
      res = await fetchFn(url, { headers });
      if (res.status !== 429 && res.status < 500) break;
      await sleep(2000 * (attempt + 1));
    }
    if (!res.ok) return { report: { store: name, status: `orders failed ${res.status}`, days, orders }, rows, failed: true };

    for (const o of (await res.json()).orders || []) {
      if (o.cancelled_at) continue;
      orders += 1;
      for (const li of o.line_items || []) {
        const qty = Math.trunc(Number(li.current_quantity ?? li.quantity ?? 0));
        if (qty <= 0) continue;
        rows.push({ src: "store", channel: name, date: String(o.created_at || "").slice(0, 10),
          sku: txt(li.sku), title: txt(li.title), brand: txt(li.vendor), qty });
      }
    }

    const link = res.headers.get("link") || "";
    const next = link.match(/<([^>]+)>;\s*rel="next"/);
    url = next ? next[1] : null;
    if (url) await sleep(550);
  }

  return { report: { store: name, status: "ok", days, orders, units: rows.reduce((s, r) => s + r.qty, 0) }, rows };
}

/*
 * Every sale and every WTB of the last 180 days.
 *
 * airtable(table, formula, fields) -> records
 */
export async function collectSales({ airtable, apiVersion, fetchFn = fetch, now = new Date(), log = console.log }) {
  const iso180 = new Date(now.getTime() - 180 * 86400000).toISOString().slice(0, 10);
  const rows = [];
  const stores = [];
  const tokenStores = new Set();

  const merchants = await airtable(
    "Merchants",
    "AND({Shopify Token}!='',{Shopify Store URL}!='')",
    ["Store Name", "Shopify Store URL", "Shopify Token"]
  );

  for (const m of merchants) {
    const f = m.fields || {};
    const name = txt(f["Store Name"]);
    const domain = txt(f["Shopify Store URL"]).replace(/^https?:\/\//, "").replace(/\/+$/, "");
    const token = txt(f["Shopify Token"]);
    if (!domain || !token) continue;

    try {
      const got = await readStoreOrders({ name, domain, token, apiVersion, fetchFn, now });
      stores.push(got.report);
      if (got.report.status === "ok") tokenStores.add(name.toLowerCase());
      rows.push(...got.rows);
      log(`TRENDS ${name}: ${got.report.status}` + (got.report.orders !== undefined ? `, ${got.report.orders} orders, ${got.rows.length} lines` : ""));
    } catch (error) {
      stores.push({ store: name, status: "failed: " + error.message });
      log(`TRENDS ${name}: failed ${error.message}`);
    }
  }

  const uol = await airtable(
    "Unfulfilled Orders Log",
    `IS_AFTER({Order Date}, '${iso180}')`,
    ["SKU (Soft)", "SKU", "Product Name", "Brand", "Store Name", "Marketplace", "Fulfillment Status", "Order Date"]
  );
  for (const r of uol) {
    const f = r.fields || {};
    if (txt(f["Fulfillment Status"]).toLowerCase().includes("cancel")) continue;
    const mp = txt(f.Marketplace);
    const store = txt(f["Store Name"]);
    if (!mp && tokenStores.has(store.toLowerCase())) continue; // counted from that store's own Shopify
    rows.push({ src: mp ? "marketplace" : "store", channel: mp || store || "unknown",
      sku: txt(f["SKU (Soft)"]) || txt(f.SKU), title: txt(f["Product Name"]), brand: txt(f.Brand), qty: 1 });
  }

  const ext = await airtable("External Sales Log", `IS_AFTER({Sale Date}, '${iso180}')`, ["SKU", "Sale Date"]);
  for (const r of ext) {
    let skus = (r.fields || {}).SKU || [];
    if (typeof skus === "string") skus = skus.split(",");
    for (const s of skus) rows.push({ src: "external", channel: "External Sales", sku: txt(s), title: "", brand: "", qty: 1 });
  }

  const wtb = await airtable("Member WTBs", `IS_AFTER(CREATED_TIME(), '${iso180}')`, ["SKU", "Product Name", "Brand", "Purchase Status"]);
  for (const r of wtb) {
    const f = r.fields || {};
    rows.push({ src: "wtb", channel: "Member WTB", sku: txt(f.SKU), title: txt(f["Product Name"]), brand: txt(f.Brand), qty: 1 });
  }

  log(`TRENDS airtable: ${uol.length} UOL, ${ext.length} external, ${wtb.length} WTBs; ${rows.length} rows in all`);

  return { rows, stores };
}

/* ------------------------------------------------------------------ *
 * Supabase (PostgREST), read in pages of 1000
 * ------------------------------------------------------------------ */

export function supabaseRest({ url, key, fetchFn = fetch }) {
  const base = String(url).replace(/[/]+$/, "") + "/rest/v1/";
  const headers = { apikey: key, Authorization: `Bearer ${key}`, "Content-Type": "application/json" };

  return {
    async all(path) {
      const out = [];
      for (let from = 0; ; from += 1000) {
        const res = await fetchFn(base + path, { headers: { ...headers, Range: `${from}-${from + 999}` } });
        if (!res.ok) throw new Error(`Supabase ${path.split("?")[0]}: ${res.status} ${(await res.text()).slice(0, 200)}`);
        const page = await res.json();
        out.push(...page);
        if (page.length < 1000) return out;
      }
    },
    async send(path, method, body, prefer = "return=minimal") {
      const res = await fetchFn(base + path, { method, headers: { ...headers, Prefer: prefer }, body: body === undefined ? undefined : JSON.stringify(body) });
      if (!res.ok) throw new Error(`Supabase ${method} ${path.split("?")[0]}: ${res.status} ${(await res.text()).slice(0, 200)}`);
    }
  };
}

/* Stock we hold per style: 48h sizes (lead time <= 2 days), lowest price, a picture, the name. */
export function summariseStock(rows) {
  const out = new Map();
  for (const r of rows) {
    const sku = normSku(r.sku);
    if (!sku || !(Number(r.quantity) > 0)) continue;
    const s = out.get(sku) || { fast: new Set(), price: null, picture: "", name: "" };
    if (r.lead_time_days === null || r.lead_time_days === undefined || Number(r.lead_time_days) <= 2) s.fast.add(String(r.size || "").trim());
    // selling_price_suggested is the one filled (the incl. VAT column is empty, 18-11-2026).
    const p = Number(r.selling_price_suggested ?? r.suggested_selling_price_incl_vat);
    if (p > 0 && (s.price === null || p < s.price)) s.price = p;
    if (!s.picture && r.image_url) s.picture = r.image_url;
    if (!s.name && r.product_name) s.name = r.product_name;
    out.set(sku, s);
  }
  return out;
}

const sizeOrder = (a, b) => (parseFloat(a) || 999) - (parseFloat(b) || 999) || a.localeCompare(b);

const ACTIVE_SKUS = `
  query trendsShelf($after: String) {
    products(first: 100, after: $after, query: "status:active") {
      pageInfo { hasNextPage endCursor }
      nodes { variants(first: 5) { nodes { sku } } }
    }
  }
`;

/* The style codes on a store's active Shopify products. */
export async function activeShelfSkus(graphql) {
  const skus = new Set();
  let after = null;
  do {
    const page = (await graphql(ACTIVE_SKUS, { after })).products;
    for (const p of page.nodes) for (const v of p.variants.nodes) if (v.sku) skus.add(normSku(v.sku));
    after = page.pageInfo.hasNextPage ? page.pageInfo.endCursor : null;
  } while (after);
  return skus;
}

/* ------------------------------------------------------------------ *
 * The run
 * ------------------------------------------------------------------ */

/*
 * stores: [{ recordId, name, autoAdd, graphql }] - the stores that get suggestions
 * skipChannels: channel names whose sales are not counted
 * apply: false = measure and decide everything, write nothing
 */
export async function runTrends({ airtable, db, stores, skipChannels, apply = false, apiVersion, fetchFn = fetch, now = new Date(), log = console.log, notify = null, full = false }) {
  const { rows, stores: storeReport } = await collectSales({ airtable, apiVersion, fetchFn, now, log });

  const stock = await db.all(
    "consignment_inventory?select=sku,size,quantity,lead_time_days,selling_price_suggested,image_url,product_name&quantity=gt.0&order=id.asc"
  );
  const held = summariseStock(stock);
  const extraTitles = [...held.entries()].filter(([, s]) => s.name).map(([sku, s]) => ({ sku, title: s.name }));

  const scored = scoreSales(rows, { skip: skipChannels, extraTitles });

  // Fill in names and brands from our stock where the sales rows had none (External Sales).
  for (const r of scored.values()) {
    if (!r.title && held.get(r.sku)?.name) r.title = held.get(r.sku).name;
  }

  const kept = [...scored.values()].filter((r) => r.score > 0 && !JUNK.test(`${r.title} ${r.brand}`));
  const source = "trends-" + now.toISOString().slice(0, 10);
  log(`TRENDS scored ${scored.size} styles, ${kept.length} kept for storefront_scores`);

  if (apply) {
    for (let i = 0; i < kept.length; i += 500) {
      await db.send("storefront_scores?on_conflict=sku", "POST",
        kept.slice(i, i + 500).map((r) => ({ sku: r.sku, score: r.score, source, updated_at: now.toISOString() })),
        "resolution=merge-duplicates,return=minimal");
    }
  }

  const market = new Map();
  const reports = [];

  for (const store of stores) {
    const choices = await db.all(`store_catalogue_choices?select=sku&merchant_record_id=eq.${encodeURIComponent(store.recordId)}&order=sku.asc`);
    const products = await db.all(`store_catalogue_products?select=sku&merchant_record_id=eq.${encodeURIComponent(store.recordId)}&order=shopify_product_id.asc`);
    const chosen = new Set(choices.map((c) => normSku(c.sku)));
    const dismissedRows = await db.all(`store_trend_suggestions?select=sku&merchant_record_id=eq.${encodeURIComponent(store.recordId)}&dismissed_at=not.is.null&order=sku.asc`);
    const dismissed = new Set(dismissedRows.map((d) => normSku(d.sku)));
    const onShelf = await activeShelfSkus(store.graphql);
    for (const p of products) if (p.sku) onShelf.add(normSku(p.sku));

    const picks = pickCandidates({ scored, chosen, onShelf, dismissed });

    // Prices the sourcing platforms gave for these styles, if we ever read them.
    const unknown = picks.map((p) => p.sku).filter((s) => !market.has(s));
    for (let i = 0; i < unknown.length; i += 100) {
      const list = unknown.slice(i, i + 100).map((s) => `"${s.replace(/"/g, "")}"`).join(",");
      for (const r of await db.all(`market_prices?select=sku,cost&sku=in.(${encodeURIComponent(list)})&order=sku.asc`)) {
        const c = Number(r.cost);
        if (c > 0 && (!market.has(r.sku) || c < market.get(r.sku))) market.set(r.sku, c);
      }
    }

    const suggestions = picks.map((p) => {
      const s = held.get(p.sku);
      return {
        merchant_record_id: store.recordId,
        sku: p.sku,
        name: p.title || s?.name || "",
        brand: knownBrand(p.title, p.brand) || p.brand || "",
        model: modelFor(p.title),
        score: p.score,
        units: p.units,
        channels: channelText(p.channels),
        wtb: p.wtb,
        fast_sizes: s ? [...s.fast].filter(Boolean).sort(sizeOrder).join(" ") : "",
        lojiq_price: s?.price ?? null,
        market_price: market.get(p.sku) ?? null,
        picture_url: s?.picture || null,
        suggested_at: now.toISOString()
      };
    });

    const auto = Boolean(store.autoAdd);

    if (apply) {
      // The list is derived: rebuilt each week. What the store dismissed stays, so it stays away.
      await db.send(`store_trend_suggestions?merchant_record_id=eq.${encodeURIComponent(store.recordId)}&dismissed_at=is.null`, "DELETE");
      for (let i = 0; i < suggestions.length; i += 500) {
        await db.send("store_trend_suggestions", "POST", suggestions.slice(i, i + 500));
      }

      if (auto && suggestions.length) {
        // Only styles without any choice yet; ignore-duplicates never overwrites a store's own decision.
        await db.send("store_catalogue_choices?on_conflict=merchant_record_id,sku", "POST",
          suggestions.map((s) => ({ merchant_record_id: store.recordId, sku: s.sku, choice: "add", photos: "lojiq", status: "pending" })),
          "resolution=ignore-duplicates,return=minimal");
      }
    }

    log(`TRENDS ${store.name}: ${suggestions.length} suggestions, ${dismissed.size} dismissed earlier` +
      (auto ? (apply ? ", all chosen (Trend Auto Add? on)" : ", would all be chosen (Trend Auto Add? on)") : "") +
      (apply ? "" : " (dry)"));

    const report = { store: store.name, suggestions: suggestions.length, autoAdded: auto && apply ? suggestions.length : 0, top: suggestions.slice(0, 15) };
    if (full) report.list = suggestions;
    reports.push(report);
    if (notify && apply) await notify(store, suggestions, auto).catch((e) => log("TRENDS notify failed: " + e.message));
  }

  return { source, rows: rows.length, stores: storeReport, scored: scored.size, kept: kept.length, reports };
}
