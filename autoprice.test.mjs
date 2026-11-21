/*
 * Run with: node --test autoprice.test.mjs
 */
import test from "node:test";
import assert from "node:assert/strict";

import { priceSize, usableCosts, safeStockxCost, goatCost, goatEuroCost, dewuCost, allowedSources, dewuMarkup } from "./autoprice.js";
import { minimumSellingPrice, maximumBuyingPrice } from "./storePricing.js";

// UNION's Firm Range bands (CL-00031), as in Merchants on 16-11-2026.
const UNION = {
  "Margin Method": "Firm Range",
  "Base Costs": 0,
  "VAT Rate": 0.21,
  "FR <75 Min Margin": 35,
  "FR 75–99 Min Margin": 40,
  "FR 100–124 Min Margin": 40,
  "FR 125–149 Min Margin": 45,
  "FR 150–174 Min Margin": 50,
  "FR 175–199 Min Margin": 50,
  "FR 200–224 Min Margin": 55,
  "FR 225–249 Min Margin": 60,
  "FR 250–274 Min Margin": 60,
  "FR 275–299 Min Margin": 65,
  "FR 300–324 Min Margin": 65,
  "FR 325–349 Min Margin": 70,
  "FR 350–374 Min Margin": 75,
  "FR 375–399 Min Margin": 75,
  "FR 400–424 Min Margin": 75,
  "FR 425–449 Min Margin": 75,
  "FR 450–474 Min Margin": 85,
  "FR 475–499 Min Margin": 85,
  "FR 500–524 Min Margin": 90,
  "FR 525–549 Min Margin": 90,
  "FR 550–574 Min Margin": 90,
  "FR 575–599 Min Margin": 90,
  "FR 600–624 Min Margin": 95,
  "FR 625–649 Min Margin": 95,
  "FR 650–674 Min Margin": 95,
  "FR 675–699 Min Margin": 95,
  "FR 700–724 Min Margin": 100,
  "FR 725–749 Min Margin": 100,
  "FR 750–774 Min Margin": 100,
  "FR 775–799 Min Margin": 100,
  "FR 800–824 Min Margin": 110,
  "FR 825–849 Min Margin": 110,
  "FR 850–874 Min Margin": 110,
  "FR 875–899 Min Margin": 110,
  "FR 900–924 Min Margin": 120,
  "FR 925–949 Min Margin": 120,
  "FR 950–974 Min Margin": 120,
  "FR 974> Min Margin": 130
};

const NOW = new Date("2026-11-16T12:00:00Z");
const hoursAgo = (h) => new Date(NOW - h * 3600000).toISOString();
const priceOf = (cost) => minimumSellingPrice({ buyingPrice: cost, vatType: "Margin", merchantFields: UNION });
// A StockX cost is without VAT, so it is priced with 21% on top.
const stockxPriceOf = (cost) => minimumSellingPrice({ buyingPrice: cost, vatType: "VAT21", merchantFields: UNION });

test("the cheapest fresh source wins", () => {
  const r = priceSize({
    observations: {
      stockx_pro: { cost: 180, observedAt: hoursAgo(2) },
      dewu: { cost: 170, observedAt: hoursAgo(2) },
      goat: { cost: 160, observedAt: hoursAgo(2) }
    },
    merchantFields: UNION, now: NOW
  });

  assert.equal(r.status, "open");
  assert.equal(r.source, "goat");
  assert.equal(r.price, priceOf(160));
});

test("Dewu above 200 is a pair we would never get", () => {
  const costs = usableCosts({
    stockx_pro: { cost: 260, observedAt: hoursAgo(1) },
    dewu: { cost: 205, observedAt: hoursAgo(1) }
  }, { now: NOW });

  assert.deepEqual(costs.map((c) => c.source), ["stockx_pro"]);
});

test("Dewu at exactly 200 still counts", () => {
  const costs = usableCosts({ dewu: { cost: 200, observedAt: hoursAgo(1) } }, { now: NOW });

  assert.equal(costs[0].source, "dewu");
});

test("a store's sources leave the others out; empty is all of them", () => {
  const observations = {
    stockx_pro: { cost: 180, observedAt: hoursAgo(1) },
    dewu: { cost: 120, observedAt: hoursAgo(1) },
    goat: { cost: 170, observedAt: hoursAgo(1) }
  };

  assert.equal(allowedSources({}), null);
  assert.deepEqual(usableCosts(observations, { now: NOW, sources: allowedSources({ "Autoprice Sources": ["StockX", "GOAT"] }) }).map((c) => c.source), ["goat", "stockx_pro"]);

  const r = priceSize({ observations, merchantFields: { ...UNION, "Autoprice Sources": ["StockX"] }, now: NOW });
  assert.equal(r.source, "stockx_pro");
});

test("the Dewu markup lifts the price, and the store's Max lands that far above Dewu", () => {
  const observations = { dewu: { cost: 140, observedAt: hoursAgo(1) } };

  const plain = priceSize({ observations, merchantFields: UNION, now: NOW });
  const marked = priceSize({ observations, merchantFields: { ...UNION, "Autoprice Dewu Markup": 12.5 }, now: NOW });

  assert.equal(dewuMarkup({}), 0);
  assert.equal(plain.price, minimumSellingPrice({ buyingPrice: 140, vatType: "Margin", merchantFields: UNION }));
  assert.equal(marked.price, minimumSellingPrice({ buyingPrice: 152.5, vatType: "Margin", merchantFields: UNION }));
  // What we pay stays the partner's price; only the shop price carries the markup.
  assert.equal(marked.cost, 140);

  // The order's Max (the Airtable formula, margin goods: no VAT type on a
  // Standard order) gives back at least Dewu + markup - the spread the
  // Outsource Engine asks for.
  const max = maximumBuyingPrice({ sellingPrice: marked.price, vatType: "Margin", merchantFields: UNION });
  assert.ok(max >= 152.5 && max < 152.5 + 15, `max ${max}`);
});

test("the API stands in only when Pro is stale", () => {
  const fresh = usableCosts({
    stockx_pro: { cost: 200, observedAt: hoursAgo(5) },
    stockx_api: { cost: 150, observedAt: hoursAgo(1) }
  }, { now: NOW });

  assert.deepEqual(fresh.map((c) => c.source), ["stockx_pro"]);

  const stale = usableCosts({
    stockx_pro: { cost: 200, observedAt: hoursAgo(40) },
    stockx_api: { cost: 230, observedAt: hoursAgo(1) }
  }, { now: NOW });

  assert.deepEqual(stale.map((c) => c.source), ["stockx_api"]);
});

test("no fresh cost closes the size", () => {
  const r = priceSize({
    observations: { goat: { cost: 150, observedAt: hoursAgo(60) } },
    previous: { status: "open", price: 200, pricedAt: hoursAgo(24) },
    merchantFields: UNION, now: NOW
  });

  assert.equal(r.status, "closed");
  assert.equal(r.price, null);
});

test("a rising cost moves the price up at once", () => {
  const r = priceSize({
    observations: { stockx_pro: { cost: 250, observedAt: hoursAgo(1) } },
    previous: { status: "open", price: 250, pricedAt: hoursAgo(1) },
    merchantFields: UNION, now: NOW
  });

  assert.equal(r.price, stockxPriceOf(250));
  assert.ok(r.price > 250 * 1.21);
});

test("a falling cost is followed at most 5% a day", () => {
  const r = priceSize({
    observations: { stockx_pro: { cost: 100, observedAt: hoursAgo(1) } },
    previous: { status: "open", price: 300, pricedAt: hoursAgo(24) },
    merchantFields: UNION, now: NOW
  });

  assert.equal(r.price, 285);
  assert.equal(r.reason, "coming down slowly");
});

test("hourly runs still let the price come down", () => {
  let previous = { status: "open", price: 300, pricedAt: NOW.toISOString() };

  for (let hour = 1; hour <= 24; hour += 1) {
    const now = new Date(NOW.getTime() + hour * 3600000);
    const r = priceSize({
      observations: { stockx_pro: { cost: 100, observedAt: now.toISOString() } },
      previous, merchantFields: UNION, now
    });

    previous = { status: "open", price: r.price, pricedAt: new Date(r.pricedAt).toISOString() };
  }

  assert.ok(previous.price <= 290, `still at ${previous.price} after a day of hourly runs`);
});

test("the slow way down never goes below the margin price", () => {
  const r = priceSize({
    observations: { stockx_pro: { cost: 190, observedAt: hoursAgo(1) } },
    previous: { status: "open", price: stockxPriceOf(190) + 40, pricedAt: hoursAgo(24 * 10) },
    merchantFields: UNION, now: NOW
  });

  assert.equal(r.price, stockxPriceOf(190));
  assert.equal(r.reason, "down");
});

test("a closed size reopens at its margin price, not slowly", () => {
  const r = priceSize({
    observations: { stockx_pro: { cost: 150, observedAt: hoursAgo(1) } },
    previous: { status: "closed", price: null, pricedAt: null },
    merchantFields: UNION, now: NOW
  });

  assert.equal(r.price, stockxPriceOf(150));
});

test("StockX is priced with VAT on top, so a dearer Dewu cost can still win", () => {
  // HF3713-002 42 on 21-11-2026: Pro 117,28 without VAT, Dewu 132,88 margin goods.
  const r = priceSize({
    observations: {
      stockx_pro: { cost: 117.28, observedAt: hoursAgo(1) },
      dewu: { cost: 132.88, observedAt: hoursAgo(1) }
    },
    merchantFields: UNION, now: NOW
  });

  assert.equal(r.source, "dewu");
  assert.equal(r.cost, 132.88);
  assert.equal(r.price, priceOf(132.88));
  assert.ok(priceOf(132.88) < stockxPriceOf(117.28));
});

test("the cost conversions", () => {
  // goat.com NL: 119,45 for a new pair + 15 shipping (HF3713-002 US 8.5, 21-11-2026)
  assert.equal(goatEuroCost(11945), 134.45);
  assert.equal(goatEuroCost(0), null);
  assert.equal(goatCost(199.48), 199.52);
  assert.equal(dewuCost(1500, 35), 197.5);
  // 200 ask: 194 buy now + 15.52 fee + 10.95
  assert.equal(safeStockxCost(200), 220.47);
  assert.equal(safeStockxCost(0), null);
});

test("with a Price Ending the slow way down stays on the ending", () => {
  const store = { ...UNION, "Price Ending": 0.95 };
  const target = minimumSellingPrice({ buyingPrice: 150, vatType: "Margin", merchantFields: store });
  const r = priceSize({
    observations: { stockx_pro: { cost: 150, observedAt: hoursAgo(1) } },
    previous: { status: "open", price: 300, pricedAt: hoursAgo(24) },
    merchantFields: store, now: NOW
  });

  assert.equal(Math.round((target % 1) * 100), 95);
  assert.equal(r.price, 285.95);
  assert.equal(r.reason, "coming down slowly");
});
