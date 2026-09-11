/*
 * What this module decides for itself.
 *
 * The size rule moved to sizes.js and is tested there, in one place, because
 * having it in two was the bug: store_listings was filled by one rule and
 * matched with another. sizeKey is re-exported here, so all this has to
 * check is that it really is the same function.
 */
import test from "node:test";
import assert from "node:assert/strict";

import { sizeKey, normalizeSku } from "./desiredListings.js";
import { normalizeSize } from "./sizes.js";

test("sizeKey is the shared rule, not a second one", () => {
  assert.equal(sizeKey, normalizeSize);
});

test("style codes lose their spaces and their case", () => {
  assert.equal(normalizeSku(" dd1503 101 "), "DD1503101");
  assert.equal(normalizeSku("m2002rdb"), "M2002RDB");
  assert.equal(normalizeSku(null), "");
});
