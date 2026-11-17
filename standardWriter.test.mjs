/*
 * Run with: node --test standardWriter.test.mjs
 */
import test from "node:test";
import assert from "node:assert/strict";

import { planStandard } from "./standardWriter.js";
import { normalizeSize } from "./sizes.js";

const v = (id, size, delivery, price, policy = delivery === "Standard" ? "CONTINUE" : "DENY") => ({
  id, price: String(price), inventoryPolicy: policy,
  selectedOptions: [{ name: "Maat", value: size }, ...(delivery ? [{ name: "Levering", value: delivery }] : [])]
});

const page = (variants, withDelivery = true) => ({
  id: "p1",
  options: [
    { id: "o1", name: "Maat", optionValues: [...new Set(variants.map((x) => x.selectedOptions[0].value))].map((name) => ({ id: name, name })) },
    ...(withDelivery ? [{ id: "o2", name: "Levering", optionValues: [{ id: "e", name: "Express" }, { id: "s", name: "Standard" }] }] : [])
  ],
  variants: { nodes: variants }
});

test("the delivery choice in a variant title is not part of the size", () => {
  assert.equal(normalizeSize("42 / Express"), "42");
  assert.equal(normalizeSize("44,5 / Standard"), "44.5");
  assert.equal(normalizeSize("38 2/3"), "38 2/3");
});

test("a page without the option gets it, and every wanted size a Standard variant", () => {
  const plan = planStandard({ product: page([v("a", "42", null, 200), v("b", "43", null, 210)], false), wanted: [{ size: "42", price: 180 }, { size: "44", price: 190 }] });

  assert.equal(plan.needsDeliveryOption, true);
  assert.deepEqual(plan.create.map((c) => [c.size, c.price]), [["42", "180.00"], ["44", "190.00"]]);
  assert.deepEqual(plan.newSizeValues, ["44"]);
});

test("Express variants are never touched; Standard follows the autoprice", () => {
  const plan = planStandard({
    product: page([v("e42", "42", "Express", 300), v("s42", "42", "Standard", 250), v("s43", "43", "Standard", 260)]),
    wanted: [{ size: "42", price: 240 }]
  });

  assert.equal(plan.needsDeliveryOption, false);
  assert.deepEqual(plan.create, []);
  assert.deepEqual(plan.update.map((u) => [u.id, u.price]), [["s42", "240.00"]]);
  assert.deepEqual(plan.remove.map((r) => r.id), ["s43"]);
});

test("an unchanged price is left alone", () => {
  const plan = planStandard({ product: page([v("s42", "42", "Standard", 240)]), wanted: [{ size: "42", price: 240 }] });

  assert.deepEqual([plan.create.length, plan.update.length, plan.remove.length], [0, 0, 0]);
});
