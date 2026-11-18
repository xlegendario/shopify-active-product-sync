import test from "node:test";
import assert from "node:assert/strict";
import {
  normSku, scoreSales, scoreOf, pickCandidates, isWanted, isStyleCode, backInTrend, knownBrand, summariseStock, channelText, runTrends, MIN_SCORE
} from "./trends.js";

test("normSku drops the size from a style code", () => {
  assert.equal(normSku("dd1391-103_36"), "DD1391-103");
  assert.equal(normSku("DD1391-103-42"), "DD1391-103");
  assert.equal(normSku("DD1391-103-42.5"), "DD1391-103");
  assert.equal(normSku(" U9060 BLK "), "U9060BLK");
  assert.equal(normSku(""), "");
});

test("score: capped units per channel, plus 3 per channel, plus 2 per WTB", () => {
  assert.equal(scoreOf({ channels: new Map([["SneakerAsk", 40], ["Woovin", 2]]), wtb: 3 }), 15 + 2 + 3 * 2 + 2 * 3);
  assert.equal(scoreOf({ channels: new Map(), wtb: 1 }), 2);
});

test("scoreSales counts per channel, leaves out UNION and the test store, and WTBs only as demand", () => {
  const rows = [
    ...Array.from({ length: 20 }, () => ({ src: "store", channel: "SneakerAsk", sku: "DD1391-103-42", title: "Nike Dunk Low Panda", qty: 1 })),
    { src: "marketplace", channel: "Woovin", sku: "DD1391-103", title: "Nike Dunk Low Panda", qty: 2 },
    { src: "store", channel: "UNION Amsterdam", sku: "DD1391-103", title: "Nike Dunk Low Panda", qty: 50 },
    { src: "store", channel: "Test Store Auto-Supply", sku: "DD1391-103", title: "Nike Dunk Low Panda", qty: 50 },
    { src: "wtb", channel: "Member WTB", sku: "DD1391-103", title: "Nike Dunk Low Panda" },
    { src: "store", channel: "SOSU", sku: "", title: "Nike Dunk Low Panda 42", qty: 1 }
  ];
  const s = scoreSales(rows, { skip: ["UNION Amsterdam", "Test Store Auto-Supply"] }).get("DD1391-103");

  assert.equal(s.units, 23); // 20 + 2 + the row matched on its title
  assert.equal(s.wtb, 1);
  assert.equal(s.channels.size, 3);
  assert.equal(s.score, 15 + 2 + 1 + 3 * 3 + 2);
});

test("a row without a SKU takes it from our stock's name", () => {
  const scored = scoreSales(
    [{ src: "store", channel: "ALC", sku: "", title: "New Balance 9060 Sea Salt 44", qty: 1 }],
    { extraTitles: [{ sku: "U9060WHT", title: "New Balance 9060 Sea Salt" }] }
  );
  assert.equal(scored.get("U9060WHT").units, 1);
});

test("isWanted keeps shoes and clothing of our brands or models, nothing else", () => {
  assert.equal(isWanted({ title: "Nike Dunk Low Panda" }), true);
  assert.equal(isWanted({ title: "Fear of God Essentials Hoodie Black" }), true);
  assert.equal(isWanted({ title: "Broken Planet Market Hoodie" }), true);
  assert.equal(isWanted({ title: "Supreme Box Logo Hoodie Black" }), true);
  assert.equal(isWanted({ title: "Samba OG White", brand: "" }), true); // model known, brand not in title

  for (const junk of [
    "Pokémon TCG Prismatic Evolutions Elite Trainer Box",
    "Pokemon 151 Booster Bundle",
    "Pop Mart Labubu Have a Seat",
    "Nike Everyday Cushioned Socks (3 Pack)",
    "Calvin Klein Boxershorts 3-pack",
    "Supreme Box Logo Cap",
    "Nike Heritage Backpack",
    "Gift Card €50",
    "Verzendbescherming",
    "Crep Protect Cleaning Kit",
    "Swatch x Audemars Piguet Royal Pop",
    "Jordan Jumpman Keychain",
    "One Piece Card Game OP-09"
  ]) assert.equal(isWanted({ title: junk }), false, junk);

  assert.equal(isWanted({ title: "Zara Linen Shirt" }), false); // a brand we do not carry
  assert.equal(isWanted({ title: "" }), false);
});

test("knownBrand reads the brand from title or vendor", () => {
  assert.equal(knownBrand("Air Jordan 4 Retro Bred"), "Jordan");
  assert.equal(knownBrand("9060 Sea Salt", "New Balance"), "New Balance");
  assert.equal(knownBrand("Hoodie", "Corteiz"), "Corteiz");
  assert.equal(knownBrand("Plain tee"), "");
});

test("pickCandidates: score 30 or more, not chosen, not on the shelf, wanted; no maximum", () => {
  const ch = (n) => new Map([["A", n], ["B", n]]);
  const scored = new Map();
  const add = (sku, title, channels, wtb = 0) => scored.set(sku, {
    sku, title, brand: "", units: [...channels.values()].reduce((a, b) => a + b, 0), wtb, channels, score: scoreOf({ channels, wtb })
  });
  add("HIGH1", "Nike Dunk Low Panda", ch(15));          // 36
  add("EDGE1", "Air Jordan 1 Low Wolf Grey", ch(12));    // 30
  add("LOW1", "Nike Air Max 1 Patta", ch(11));          // 28
  add("CHOSEN1", "Nike Vomero 5 Black", ch(15));
  add("SHELF1", "New Balance 9060 Sea Salt", ch(15));
  add("JUNK1", "Pokemon Booster Box", ch(15));
  for (let i = 0; i < 200; i += 1) add("MANY" + i, "adidas Samba OG " + i, ch(14));

  const picks = pickCandidates({ scored, chosen: new Set(["CHOSEN1"]), onShelf: new Set(["SHELF1"]) });
  const skus = picks.map((p) => p.sku);

  assert.equal(MIN_SCORE, 30);
  assert.equal(skus[0], "HIGH1");
  assert.ok(skus.includes("EDGE1"));
  for (const s of ["LOW1", "CHOSEN1", "SHELF1", "JUNK1"]) assert.ok(!skus.includes(s), s);
  assert.equal(picks.length, 202);
});

test("pickCandidates leaves out pure demand with a single WTB", () => {
  const scored = new Map([["DD1391-100", { sku: "DD1391-100", title: "Nike Dunk Low", brand: "", units: 0, wtb: 1, channels: new Map(), score: 40 }]]);
  assert.equal(pickCandidates({ scored }).length, 0);
});

test("summariseStock: 48h sizes, lowest price, Pearl's five days are not 48h", () => {
  const s = summariseStock([
    { sku: "DD1391-103", size: "42", quantity: 1, lead_time_days: null, selling_price_suggested: 130 },
    { sku: "DD1391-103", size: "43", quantity: 2, lead_time_days: 2, selling_price_suggested: 120 },
    { sku: "DD1391-103", size: "44", quantity: 1, lead_time_days: 5, selling_price_suggested: 99 },
    { sku: "DD1391-103", size: "45", quantity: 0, lead_time_days: null, selling_price_suggested: 50 }
  ]).get("DD1391-103");
  assert.deepEqual([...s.fast].sort(), ["42", "43"]);
  assert.equal(s.price, 99);
});

test("channelText lists the biggest channel first", () => {
  assert.equal(channelText(new Map([["Woovin", 2], ["SneakerAsk", 12]])), "SneakerAsk 12, Woovin 2");
});

/* A whole run against fakes: what is written, and that a dry run writes nothing. */
function fakes({ autoAdd }) {
  const writes = [];
  const sales = Array.from({ length: 15 }, () => ({ id: 1, created_at: "2026-11-01T10:00:00Z", line_items: [{ sku: "HQ4199-100", title: "Nike Air Max 1 Patta Waves", vendor: "Nike", current_quantity: 1 }] }));
  const fetchFn = async (url) => {
    if (url.includes("access_scopes")) return new Response(JSON.stringify({ access_scopes: [{ handle: "read_orders" }, { handle: "read_all_orders" }] }));
    if (url.includes("orders.json")) {
      const store = url.includes("a.myshopify") ? 0 : 1;
      return new Response(JSON.stringify({ orders: store === 0 ? sales : sales.concat(sales) }));
    }
    throw new Error("unexpected " + url);
  };
  const airtable = async (table) => table === "Merchants"
    ? [{ fields: { "Store Name": "A", "Shopify Store URL": "a.myshopify.com", "Shopify Token": "t" } },
       { fields: { "Store Name": "B", "Shopify Store URL": "b.myshopify.com", "Shopify Token": "t" } },
       { fields: { "Store Name": "UNION Amsterdam", "Shopify Store URL": "u.myshopify.com", "Shopify Token": "t" } }]
    : table === "Member WTBs"
      ? [{ fields: { SKU: "IH0296-400", "Product Name": "Pokemon Booster Box" } }]
      : [];
  const db = {
    all: async (path) => (path.startsWith("store_catalogue_choices") ? [{ sku: "OTHER" }] : []),
    send: async (path, method, body) => { writes.push({ path, method, body }); }
  };
  const graphql = async () => ({ products: { nodes: [], pageInfo: { hasNextPage: false } } });
  return { writes, fetchFn, airtable, db, stores: [{ recordId: "recU", name: "UNION Amsterdam", autoAdd, graphql }] };
}

test("runTrends dry: decides, writes nothing", async () => {
  const f = fakes({ autoAdd: true });
  const out = await runTrends({ ...f, skipChannels: ["UNION Amsterdam"], apply: false, apiVersion: "x", log: () => {} });
  assert.equal(f.writes.length, 0);
  assert.equal(out.reports[0].suggestions, 1);
  assert.equal(out.reports[0].top[0].sku, "HQ4199-100");
  assert.equal(out.reports[0].top[0].score, 15 + 15 + 6); // A and B capped at 15 each; UNION's own orders left out
});

test("runTrends apply: scores, a rebuilt list, and choices only with Trend Auto Add? on", async () => {
  const off = fakes({ autoAdd: false });
  await runTrends({ ...off, skipChannels: ["UNION Amsterdam"], apply: true, apiVersion: "x", log: () => {} });
  assert.deepEqual(off.writes.map((w) => `${w.method} ${w.path.split("?")[0]}`), [
    "POST storefront_scores", "DELETE store_trend_suggestions", "POST store_trend_suggestions"
  ]);
  assert.ok(!off.writes.some((w) => w.path.startsWith("store_catalogue_choices")));

  const on = fakes({ autoAdd: true });
  await runTrends({ ...on, skipChannels: ["UNION Amsterdam"], apply: true, apiVersion: "x", log: () => {} });
  const choice = on.writes.find((w) => w.path.startsWith("store_catalogue_choices"));
  assert.equal(choice.method, "POST");
  assert.deepEqual(choice.body, [{ merchant_record_id: "recU", sku: "HQ4199-100", choice: "add", photos: "lojiq", status: "pending" }]);
  // Never a delete, a draft or an overwrite of a store's own choice.
  assert.ok(!on.writes.some((w) => w.path.startsWith("store_catalogue_choices") && w.method !== "POST"));
});

test("isStyleCode: no sets of two codes and no shop slugs", () => {
  for (const ok of ["DD1391-103", "192HO246258F", "KH6729", "1016422-CHE", "U9060BLK", "6005284-100"]) assert.equal(isStyleCode(ok), true, ok);
  for (const bad of ["125BT212062F/160BT212012F", "FEAR-OF-GOD-ESSENTIALS-DARK-OATMEAL-SUMMER-SET", "", "SUMMER SET", "ABC"]) assert.equal(isStyleCode(bad), false, bad);
});

test("backInTrend: at least 10 points above the score it was set aside at, and 30 or more", () => {
  assert.equal(backInTrend(30, 40), true);   // Dario's example
  assert.equal(backInTrend(30, 39), false);
  assert.equal(backInTrend(80, 90), true);
  assert.equal(backInTrend(80, 89), false);
  assert.equal(backInTrend(12, 29), false);  // +17 but below 30
  assert.equal(backInTrend(12, 30), true);
  assert.equal(backInTrend(null, 500), false); // no known score: stays away
});

test("a dismissed style stays away until it is back in trend, then returns marked", () => {
  const scored = new Map([["DD1391-104", { sku: "DD1391-104", title: "Nike Dunk Low Court Purple", brand: "", units: 30, wtb: 0, channels: new Map([["A", 15], ["B", 15]]), score: 36 }]]);
  assert.equal(pickCandidates({ scored }).length, 1);
  assert.equal(pickCandidates({ scored, dismissed: new Map([["DD1391-104", 27]]) }).length, 0);
  const back = pickCandidates({ scored, dismissed: new Map([["DD1391-104", 26]]) });
  assert.equal(back.length, 1);
  assert.equal(back[0].returnedFrom, 26);
});

test("weekly rebuild keeps a dismissed style away, and brings one back in trend (auto-added when the switch is on)", async () => {
  // HQ4199-100 scores 36 in the fakes.
  const stay = fakes({ autoAdd: true });
  const all = stay.db.all;
  stay.db.all = async (path) => (path.includes("dismissed_at=not.is.null") ? [{ sku: "HQ4199-100", score: 30, dismissed_score: 27 }] : all(path));
  const out = await runTrends({ ...stay, skipChannels: ["UNION Amsterdam"], apply: true, apiVersion: "x", log: () => {} });
  assert.equal(out.reports[0].suggestions, 0);
  assert.ok(!stay.writes.some((w) => w.path.startsWith("store_catalogue_choices")));
  assert.match(stay.writes.find((w) => w.method === "DELETE").path, /dismissed_at=is\.null/);

  const back = fakes({ autoAdd: true });
  const all2 = back.db.all;
  back.db.all = async (path) => (path.includes("dismissed_at=not.is.null") ? [{ sku: "HQ4199-100", score: 26, dismissed_score: 26 }] : all2(path));
  const out2 = await runTrends({ ...back, skipChannels: ["UNION Amsterdam"], apply: true, apiVersion: "x", log: () => {} });
  assert.equal(out2.reports[0].backInTrend, 1);
  const list = back.writes.find((w) => w.method === "POST" && w.path.startsWith("store_trend_suggestions"));
  assert.match(list.path, /on_conflict=merchant_record_id,sku/);
  assert.equal(list.body[0].returned_from_score, 26);
  assert.equal(list.body[0].dismissed_at, null);
  assert.equal(back.writes.find((w) => w.path.startsWith("store_catalogue_choices")).body[0].sku, "HQ4199-100");
});
