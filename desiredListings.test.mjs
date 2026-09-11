/*
 * Matching a store's size label against ours.
 *
 * Written after the real thing went wrong: Mentastore labels every variant
 * "EU 42" and our consignment stock says "42", so not one pair matched and
 * a run would have set all 1.404 variants on our own location to zero.
 *
 * The two halves that matter are both here. A European label has to reduce
 * to the bare number, and a label on some other scale has to stay exactly as
 * it is - because a US 9 matching an EU 9 would put the wrong shoe in a box,
 * which is worse than matching nothing.
 */
import test from "node:test";
import assert from "node:assert/strict";

import { sizeKey, normalizeSku } from "./desiredListings.js";

test("a bare size is left alone", () => {
  assert.equal(sizeKey("42"), "42");
  assert.equal(sizeKey("37.5"), "37.5");
});

test("half sizes written as fractions survive", () => {
  assert.equal(sizeKey("42 1/2"), "42 1/2");
  assert.equal(sizeKey("38 2/3"), "38 2/3");
});

test("a comma is a decimal point", () => {
  assert.equal(sizeKey("42,5"), "42.5");
  assert.equal(sizeKey("EU 42,5"), "42.5");
});

test("European labels reduce to the number", () => {
  for (const label of ["EU 42", "EU42", "eu 42", "EUR 42", "Maat 42", "Size 42", "Talla 42", "Taglia 42"]) {
    assert.equal(sizeKey(label), "42", `${label} should read as 42`);
  }
});

test("another scale is never made to match ours", () => {
  assert.equal(sizeKey("US 9"), "US 9");
  assert.equal(sizeKey("UK 8"), "UK 8");
});

test("clothing sizes pass through", () => {
  assert.equal(sizeKey("XL"), "XL");
  assert.equal(sizeKey("M"), "M");
});

test("a label that is only the word keeps it rather than becoming empty", () => {
  // Two variants called "Size" and "Maat" must not collapse into one key.
  assert.equal(sizeKey("Size"), "Size");
  assert.equal(sizeKey("Maat"), "Maat");
});

test("nothing in, nothing out", () => {
  assert.equal(sizeKey(""), "");
  assert.equal(sizeKey(null), "");
  assert.equal(sizeKey(undefined), "");
});

test("style codes lose their spaces and their case", () => {
  assert.equal(normalizeSku(" dd1503 101 "), "DD1503101");
  assert.equal(normalizeSku("m2002rdb"), "M2002RDB");
});
