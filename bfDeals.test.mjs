import test from "node:test";
import assert from "node:assert/strict";
import { dealChanges, showsDeals } from "./bfDeals.js";
import { buildDesiredListings } from "./desiredListings.js";

const union = {
  "Offer Method": "Firm Range",
  "Offer Margin": 0,
  "Margin Method": "Percentage",
  "Minimum Margin %": 0.2
};

test("a sale pair of ours beats a custom price and carries its normal store price", () => {
  const inventoryRows = [{
    id: "x", sku: "DZ2628-102", size: "42", vat_type: "Margin", selling_price_suggested: 100,
    quantity: 1, seller_id: "SE-00001", seller_record_id: "rec1", own_stock: true, reference_price: 160
  }];

  const { listings } = buildDesiredListings({
    inventoryRows,
    merchantFields: union,
    customPrices: new Map([["DZ2628-102|42", 999]]),
    ownPriced: new Set(["DZ2628-102|42"]),
    priceSync: true
  });

  assert.equal(listings.length, 1);
  assert.notEqual(listings[0].sellingPrice, 999);
  assert.ok(listings[0].referenceSellingPrice > listings[0].sellingPrice);
});

test("deal changes: set, clear, tag, untag, activate", () => {
  const listings = [{ sku: "A", size: "42", ownStock: true, onSale: true, referenceSellingPrice: 200, sellingPrice: 150 }];
  const rows = [
    { sku: "A", size: "42", variantId: "v1", productId: "p1", quantity: 1, refPrice: null, productTags: [], productStatus: "DRAFT" },
    { sku: "B", size: "40", variantId: "v2", productId: "p2", quantity: 0, refPrice: "180.00", productTags: ["bf-deal"], productStatus: "ACTIVE" }
  ];

  const c = dealChanges({ rows, listings, priceBefore: new Map([["v1", 210]]) });

  assert.deepEqual(c.setRefs, [{ variantId: "v1", value: "200.00" }]);
  assert.deepEqual(c.clearRefs, [{ variantId: "v2" }]);
  assert.deepEqual(c.tag, ["p1"]);
  assert.deepEqual(c.untag, ["p2"]);
  assert.deepEqual(c.activate, ["p1"]);
  assert.equal(showsDeals("UNION Amsterdam", {}), true);
  assert.equal(showsDeals("FastCop", {}), false);
});

test("option B: the from-price is the price before the sale, captured once, never above the Ideal's", () => {
  const listings = [{ sku: "A", size: "42", ownStock: true, onSale: true, referenceSellingPrice: 313, sellingPrice: 173 }];
  const row = { sku: "A", size: "42", variantId: "v1", productId: "p1", quantity: 1, productTags: [], productStatus: "ACTIVE" };

  // First run: the store asked 204 before; captured.
  assert.deepEqual(dealChanges({ rows: [{ ...row, refPrice: null }], listings, priceBefore: new Map([["v1", 204]]) }).setRefs, [{ variantId: "v1", value: "204.00" }]);

  // Next run the store price is the sale price already; the captured 204 stays.
  const next = dealChanges({ rows: [{ ...row, refPrice: "204.00" }], listings, priceBefore: new Map([["v1", 173]]) });
  assert.deepEqual(next.setRefs, []);
  assert.deepEqual(next.clearRefs, []);

  // A page built for the deal has no before: no from-price, still tagged.
  const fresh = dealChanges({ rows: [{ ...row, refPrice: null }], listings, priceBefore: new Map() });
  assert.deepEqual(fresh.setRefs, []);
  assert.deepEqual(fresh.tag, ["p1"]);

  // Before above the Ideal's price: capped.
  assert.deepEqual(dealChanges({ rows: [{ ...row, refPrice: null }], listings, priceBefore: new Map([["v1", 400]]) }).setRefs, [{ variantId: "v1", value: "313.00" }]);
});

test("a partner row with a reference above its ask is a deal too", async () => {
  const { buildDesiredListings } = await import("./desiredListings.js");
  const union = { "Offer Method": "Firm Range", "Offer Margin": 0, "Margin Method": "Percentage", "Minimum Margin %": 0.2 };
  const { listings } = buildDesiredListings({
    inventoryRows: [{ id: "p", sku: "205759-610", size: "42-43", vat_type: "Margin", selling_price_suggested: 68, reference_price: 80, quantity: 5, seller_id: "SE-00781", seller_record_id: "r" }],
    merchantFields: union,
    priceSync: true
  });
  assert.equal(listings[0].onSale, true);
  assert.ok(listings[0].referenceSellingPrice > listings[0].sellingPrice);
});

test("a shop's own price under its normal one is a deal in that shop", async () => {
  const { buildDesiredListings } = await import("./desiredListings.js");
  const union = { "Offer Method": "Firm Range", "Offer Margin": 0, "Margin Method": "Percentage", "Minimum Margin (%)": 0.3, "Base Costs": 10 };
  const row = { id: "p", sku: "205759-610", size: "42-43", vat_type: "Margin", selling_price_suggested: 80, quantity: 5, seller_id: "SE-00781", seller_record_id: "r" };
  const normal = buildDesiredListings({ inventoryRows: [row], merchantFields: union, priceSync: true }).listings[0].sellingPrice;
  const { listings } = buildDesiredListings({ inventoryRows: [row], merchantFields: union, priceSync: true, customPrices: new Map([["205759-610|42-43", normal - 5]]) });
  assert.equal(listings[0].sellingPrice, normal - 5);
  assert.equal(listings[0].onSale, true);
  assert.equal(listings[0].referenceSellingPrice, normal);
});
