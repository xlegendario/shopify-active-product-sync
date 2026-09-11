/*
 * The rules that must not drift.
 *
 * Run with: node --test storePricing.test.mjs
 *
 * These check the arithmetic against hand-worked numbers and, more
 * importantly, that the two directions are each other's inverse. A price we
 * set from a cost has to survive the gate that judges it; anything else is a
 * pair listed below the margin the store agreed to.
 */
import test from "node:test";
import assert from "node:assert/strict";

import {
  maximumBuyingPrice,
  targetBuyingPrice,
  minimumSellingPrice,
  passesMarginGate,
  readMarginBands,
  parseEuSize,
  sortEuSizes
} from "./storePricing.js";

// CL-00004: the one client with both a percentage and a flat amount.
const PERCENTAGE_STORE = {
  "Margin Method": "Percentage",
  "Minimum Margin (%)": 0.25,
  "Target Margin (%)": 0.25,
  "Min Margin Amount": 60,
  "Base Costs": 15,
  "VAT Rate": 0.21
};

// CL-00001: percentage only, no flat amount, no base costs.
const SIMPLE_STORE = {
  "Margin Method": "Percentage",
  "Minimum Margin (%)": 0.1,
  "Target Margin (%)": 0.15,
  "Min Margin Amount": 0,
  "Base Costs": 0,
  "VAT Rate": 0.21
};

const FIRM_RANGE_STORE = {
  "Margin Method": "Firm Range",
  "Base Costs": 12,
  "VAT Rate": 0.21,
  "FR <75 Min Margin": 35,
  "FR 75–99 Min Margin": 40,
  "FR 100–124 Min Margin": 45,
  "FR 125-149 Min Margin": 45,
  "FR 150-174 Min Margin": 50,
  "FR 175–199 Min Margin": 55,
  "FR 200–224 Min Margin": 55,
  "FR 974> Min Margin": 145
};

test("the flat amount and the percentage both bind, whichever is stricter", () => {
  // 200 - 60 = 140 against 200 x 0.75 = 150, so the amount wins, minus costs.
  assert.equal(
    maximumBuyingPrice({ sellingPrice: 200, vatType: "Margin", merchantFields: PERCENTAGE_STORE }),
    125
  );

  // At 300 the percentage is the stricter of the two: 225 against 240.
  assert.equal(
    maximumBuyingPrice({ sellingPrice: 300, vatType: "Margin", merchantFields: PERCENTAGE_STORE }),
    210
  );
});

test("the window where the old threshold let the margin dip is closed", () => {
  // Between 185 and 240 the percentage alone asked for less than the amount.
  for (const sellingPrice of [185, 200, 220, 239]) {
    const allowed = maximumBuyingPrice({ sellingPrice, vatType: "Margin", merchantFields: PERCENTAGE_STORE });
    const margin = sellingPrice - allowed - PERCENTAGE_STORE["Base Costs"];

    assert.ok(
      margin >= 60 - 2.5,
      `at ${sellingPrice} the margin was ${margin}, under the amount it must guarantee`
    );
  }
});

test("a VAT21 pair leaves room for the VAT the store owes", () => {
  const margin = maximumBuyingPrice({ sellingPrice: 300, vatType: "Margin", merchantFields: SIMPLE_STORE });
  const vat21 = maximumBuyingPrice({ sellingPrice: 300, vatType: "VAT21", merchantFields: SIMPLE_STORE });

  assert.ok(vat21 < margin, "a VAT21 ask has to be lower, the tax comes on top of it");
  assert.equal(vat21, Math.ceil((300 * 0.9) / 1.21 / 2.5) * 2.5);
});

test("target never allows more than maximum", () => {
  for (const sellingPrice of [80, 150, 240, 399, 1200]) {
    const max = maximumBuyingPrice({ sellingPrice, vatType: "Margin", merchantFields: SIMPLE_STORE });
    const target = targetBuyingPrice({ sellingPrice, vatType: "Margin", merchantFields: SIMPLE_STORE });

    assert.ok(target <= max, `at ${sellingPrice} the target ${target} sat above the maximum ${max}`);
  }
});

test("firm range picks the band the selling price falls in", () => {
  const bands = readMarginBands(FIRM_RANGE_STORE, "min");

  assert.equal(bands[0].upTo, 75);
  assert.equal(bands[bands.length - 1].upTo, Infinity);

  // 180 sits in the 175-199 band, so 55 plus the base costs comes off.
  assert.equal(
    maximumBuyingPrice({ sellingPrice: 180, vatType: "Margin", merchantFields: FIRM_RANGE_STORE }),
    Math.ceil((180 - 55 - 12) / 2.5) * 2.5
  );
});

test("a price set from a cost always survives the gate that judges it", () => {
  const stores = [PERCENTAGE_STORE, SIMPLE_STORE, FIRM_RANGE_STORE];
  const vatTypes = ["Margin", "VAT21", "VAT0"];

  for (const merchantFields of stores) {
    for (const vatType of vatTypes) {
      for (let cost = 20; cost <= 900; cost += 7.5) {
        const selling = minimumSellingPrice({ buyingPrice: cost, vatType, merchantFields });

        assert.ok(selling !== null, "no selling price came back");

        assert.ok(
          passesMarginGate({ buyingPrice: cost, sellingPrice: selling, vatType, merchantFields }),
          `cost ${cost} on ${vatType} produced ${selling}, which the gate refuses`
        );
      }
    }
  }
});

test("and it is the LEAST such price, not just any", () => {
  const stores = [PERCENTAGE_STORE, SIMPLE_STORE, FIRM_RANGE_STORE];

  for (const merchantFields of stores) {
    for (let cost = 20; cost <= 400; cost += 11) {
      const selling = minimumSellingPrice({ buyingPrice: cost, vatType: "Margin", merchantFields });

      assert.ok(
        !passesMarginGate({ buyingPrice: cost, sellingPrice: selling - 1, vatType: "Margin", merchantFields }),
        `cost ${cost} came back at ${selling}, but a euro less also passes`
      );
    }
  }
});

test("the gate refuses a pair that costs more than the room allows", () => {
  const allowed = maximumBuyingPrice({ sellingPrice: 200, vatType: "Margin", merchantFields: PERCENTAGE_STORE });

  assert.equal(passesMarginGate({ buyingPrice: allowed, sellingPrice: 200, vatType: "Margin", merchantFields: PERCENTAGE_STORE }), true);
  assert.equal(passesMarginGate({ buyingPrice: allowed + 0.01, sellingPrice: 200, vatType: "Margin", merchantFields: PERCENTAGE_STORE }), false);
});

test("a store with no method priced nothing, rather than guessing", () => {
  assert.equal(maximumBuyingPrice({ sellingPrice: 200, vatType: "Margin", merchantFields: {} }), null);
  assert.equal(minimumSellingPrice({ buyingPrice: 100, vatType: "Margin", merchantFields: {} }), null);
  assert.equal(passesMarginGate({ buyingPrice: 100, sellingPrice: 200, vatType: "Margin", merchantFields: {} }), false);
});

test("sizes read as numbers, halves and thirds alike", () => {
  assert.equal(parseEuSize("EU 44"), 44);
  assert.equal(parseEuSize("EU 44.5"), 44.5);
  assert.equal(parseEuSize("44,5"), 44.5);
  assert.equal(Math.round(parseEuSize("EU 41 1/3") * 1000) / 1000, 41.333);
  assert.equal(Math.round(parseEuSize("EU 34 2/3") * 1000) / 1000, 34.667);
  assert.equal(parseEuSize(""), null);
});

test("the adidas ladder of thirds comes out in order", () => {
  const asStockxGivesThem = [
    "EU 34 2/3", "EU 35 1/3", "EU 36", "EU 36 2/3", "EU 37 1/3", "EU 38",
    "EU 38 2/3", "EU 39 1/3", "EU 40", "EU 40 2/3", "EU 41 1/3", "EU 42",
    "EU 42 2/3", "EU 43 1/3", "EU 44"
  ];

  const shuffled = [...asStockxGivesThem].reverse();

  assert.deepEqual(sortEuSizes(shuffled), asStockxGivesThem);
});

/*
  Where text sorting actually breaks.

  Not on the adidas ladder: every one of those is two digits, so letter by
  letter happens to land in the right order. It breaks the moment the sizes
  arrive bare, the way they sit in our own stock, and a single digit shows up
  next to a double.
*/
test("bare sizes sort as numbers, which text sorting gets wrong", () => {
  const sizes = ["40", "5", "38.5", "44", "9", "36"];

  assert.deepEqual(sortEuSizes(sizes), ["5", "9", "36", "38.5", "40", "44"]);
  assert.deepEqual([...sizes].sort(), ["36", "38.5", "40", "44", "5", "9"]);
});

test("half sizes sort too", () => {
  assert.deepEqual(
    sortEuSizes(["EU 45", "EU 37.5", "EU 36", "EU 44.5", "EU 40"]),
    ["EU 36", "EU 37.5", "EU 40", "EU 44.5", "EU 45"]
  );
});
