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
  const listings = [{ sku: "A", size: "42", ownStock: true, referenceSellingPrice: 200 }];
  const rows = [
    { sku: "A", size: "42", variantId: "v1", productId: "p1", quantity: 1, refPrice: null, productTags: [], productStatus: "DRAFT" },
    { sku: "B", size: "40", variantId: "v2", productId: "p2", quantity: 0, refPrice: "180.00", productTags: ["bf-deal"], productStatus: "ACTIVE" }
  ];

  const c = dealChanges({ rows, listings });

  assert.deepEqual(c.setRefs, [{ variantId: "v1", value: "200.00" }]);
  assert.deepEqual(c.clearRefs, [{ variantId: "v2" }]);
  assert.deepEqual(c.tag, ["p1"]);
  assert.deepEqual(c.untag, ["p2"]);
  assert.deepEqual(c.activate, ["p1"]);
  assert.equal(showsDeals("UNION Amsterdam", {}), true);
  assert.equal(showsDeals("FastCop", {}), false);
});
