/*
 * Run with: node --test livePriceWriter.test.mjs
 */
import test from "node:test";
import assert from "node:assert/strict";

import { planLivePrices } from "./livePriceWriter.js";

test("only moved prices are written; Custom Price and unpriced sizes are left alone", () => {
  const row = (size, extra) => ({ sku: "dd1391-100", size, shopify_product_id: "1", shopify_variant_id: "v" + size, ...extra });
  const prices = new Map([["DD1391-100|42", 170.5], ["DD1391-100|43", 180.5], ["DD1391-100|44", 190.5]]);
  const { changes, skipped } = planLivePrices({
    prices,
    listings: [
      row("42", { store_price: 199.5 }),
      row("43", { store_price: 180.5 }),
      row("44", { store_price: 210, custom_price: 210 }),
      row("45", { store_price: 220 }),
      row("44", { store_price: 230, price_mode: "own", shopify_variant_id: "v44b" })
    ]
  });

  assert.deepEqual(changes.map((c) => [c.size, c.from, c.to]), [["42", 199.5, 170.5]]);
  assert.deepEqual(skipped, { custom: 1, own: 1, same: 1, noPrice: 1 });
});
