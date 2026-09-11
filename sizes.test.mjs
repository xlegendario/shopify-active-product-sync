/*
 * Every shape a store has actually used, and the one it must never become.
 *
 * Written from real labels, not invented ones: Mentastore writes "EU 42",
 * FastCop writes "38 2/3", and stores that serve two markets write both
 * scales in one line.
 */
import test from "node:test";
import assert from "node:assert/strict";

import { normalizeSize } from "./sizes.js";

test("a bare size is left alone", () => {
  assert.equal(normalizeSize("42"), "42");
  assert.equal(normalizeSize("37.5"), "37.5");
  assert.equal(normalizeSize(" 44 "), "44");
});

test("a comma is a decimal point", () => {
  assert.equal(normalizeSize("42,5"), "42.5");
  assert.equal(normalizeSize("EU 42,5"), "42.5");
});

test("thirds survive whole", () => {
  assert.equal(normalizeSize("38 2/3"), "38 2/3");
  assert.equal(normalizeSize("42 1/2"), "42 1/2");
  assert.equal(normalizeSize("40 1/3"), "40 1/3");
});

test("the European word in front is dropped", () => {
  for (const label of ["EU 42", "EU42", "eu 42", "EUR 42", "EURO 42", "Maat 42", "Size 42", "Talla 42", "Taglia 42"]) {
    assert.equal(normalizeSize(label), "42", `${label} should read as 42`);
  }
});

test("the European word behind is dropped too", () => {
  assert.equal(normalizeSize("42 EU"), "42");
  assert.equal(normalizeSize("38 2/3 EU"), "38 2/3");
  assert.equal(normalizeSize("42.5 EUR"), "42.5");
});

test("both ways round, with a third written out", () => {
  assert.equal(normalizeSize("EU 38 2/3"), "38 2/3");
  assert.equal(normalizeSize("38 2/3 EU"), "38 2/3");
  assert.equal(normalizeSize("EU38 2/3"), "38 2/3");
});

test("two scales on one line: the European one wins", () => {
  assert.equal(normalizeSize("38 2/3 EU - 6 US"), "38 2/3");
  assert.equal(normalizeSize("EU 42 - US 8.5"), "42");
  assert.equal(normalizeSize("42 EU | 8 UK"), "42");
});

test("another scale on its own is never turned into a number", () => {
  assert.equal(normalizeSize("US 9"), "US 9");
  assert.equal(normalizeSize("UK 8"), "UK 8");
  assert.equal(normalizeSize("27 CM"), "27 CM");
});

test("clothing sizes pass through", () => {
  assert.equal(normalizeSize("XL"), "XL");
  assert.equal(normalizeSize("M"), "M");
  assert.equal(normalizeSize("S"), "S");
});

test("a label that is only the word keeps it rather than becoming empty", () => {
  assert.equal(normalizeSize("Size"), "Size");
  assert.equal(normalizeSize("Maat"), "Maat");
});

test("nothing in, nothing out", () => {
  assert.equal(normalizeSize(""), "");
  assert.equal(normalizeSize(null), "");
  assert.equal(normalizeSize(undefined), "");
});

test("a real 38 and a 38 2/3 stay two different sizes", () => {
  // The whole point. These collapsed into one key, so the stock of one
  // could be written onto the other.
  assert.notEqual(normalizeSize("38"), normalizeSize("38 2/3"));
  assert.notEqual(normalizeSize("EU 38"), normalizeSize("38 2/3 EU"));
});
