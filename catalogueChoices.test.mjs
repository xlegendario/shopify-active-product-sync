/*
 * What a store's catalogue choices do to the plan, and how they are marked
 * afterwards (07-10-2026).
 */
import test from "node:test";
import assert from "node:assert/strict";

import { planListings, indexCurrentListings } from "./listingPlan.js";
import { choiceOutcomes } from "./consignmentRun.js";

const listing = (sku, size, quantity = 1) => ({ sku, size, quantity, sellingPrice: 150, cost: 100, productName: sku });

const current = indexCurrentListings([
  { sku: "HAVE1", size: "42", productId: "gid://shopify/Product/1", variantId: "v1", quantity: 1, price: 150 },
  { sku: "UNLINK1", size: "42", productId: "gid://shopify/Product/2", variantId: "v2", quantity: 1, price: 150 },
  { sku: "OFF1", size: "42", productId: "gid://shopify/Product/3", variantId: "v3", quantity: 1, price: 150 }
]);

const desired = [
  listing("HAVE1", "42"), listing("HAVE1", "43"),
  listing("NEW1", "42"), listing("NEWOWN", "42"), listing("NOTCHOSEN", "42"),
  listing("UNLINK1", "42"), listing("OFF1", "42")
];

const choices = new Map([
  ["NEW1", { choice: "add", photos: "lojiq", status: "pending" }],
  ["NEWOWN", { choice: "add", photos: "own", status: "pending" }],
  ["UNLINK1", { choice: "unlinked", photos: null, status: "pending" }],
  ["OFF1", { choice: "deactivated", photos: null, status: "pending" }]
]);

test("only added styles get a page, own photos as a draft", () => {
  const plan = planListings({ desired, current, choices });

  assert.deepEqual(plan.createProducts.map((p) => [p.sku, p.draft]), [["NEW1", false], ["NEWOWN", true]]);
  assert.ok(plan.skipped.some((s) => s.sku === "NOTCHOSEN" && s.reason === "product_not_in_store"));
});

test("a page the store did not add gets no new sizes", () => {
  const plan = planListings({ desired, current, choices });

  assert.equal(plan.addSizes.length, 0);
  assert.ok(plan.skipped.some((s) => s.sku === "HAVE1" && s.reason === "size_not_in_store"));

  const withAdd = planListings({ desired, current, choices: new Map([["HAVE1", { choice: "add", status: "done" }]]) });
  assert.deepEqual(withAdd.addSizes.map((a) => [a.sku, a.sizes]), [["HAVE1", ["43"]]]);
});

test("unlinked and switched off lose our stock, switched off goes to draft once", () => {
  const plan = planListings({ desired, current, choices });

  assert.deepEqual(plan.clearQuantities.map((c) => c.sku).sort(), ["OFF1", "UNLINK1"]);
  assert.deepEqual(plan.deactivateProducts, [{ sku: "OFF1", productId: "gid://shopify/Product/3" }]);

  const later = planListings({ desired, current, choices: new Map([["OFF1", { choice: "deactivated", status: "done" }]]) });
  assert.equal(later.deactivateProducts.length, 0);
  assert.deepEqual(later.clearQuantities.map((c) => c.sku), ["OFF1"]);
});

test("choices are marked from what actually happened", () => {
  const plan = planListings({ desired, current, choices });
  const outcomes = new Map([
    ["NEW1", { ok: true, productId: "gid://shopify/Product/9" }],
    ["NEWOWN", { ok: false, error: "Title can't be blank" }],
    ["OFF1", { ok: true }]
  ]);

  const all = new Map([...choices, ["HAVE1", { choice: "add", status: "pending" }], ["GONE", { choice: "add", status: "pending" }]]);
  const marks = Object.fromEntries(choiceOutcomes({ choices: all, plan, current, outcomes }).map((m) => [m.sku, m.status]));

  assert.deepEqual(marks, { NEW1: "done", NEWOWN: "failed", UNLINK1: "done", OFF1: "done", HAVE1: "done" });
});
