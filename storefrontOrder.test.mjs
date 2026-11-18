import test from "node:test";
import assert from "node:assert/strict";
import { modelFor, planOrder } from "./storefrontOrder.js";

test("modelFor reads the model the longlist rules gave", () => {
  assert.equal(modelFor("ASICS Gel-Kayano 14 Sprite"), "Gel-Kayano 14");
  assert.equal(modelFor("New Balance 9060 Black Castlerock"), "New Balance 9060");
  assert.equal(modelFor("New Balance 2002R Olive Brown"), "New Balance 2002R");
  assert.equal(modelFor("UGG Classic Ultra Mini Boot Black (Women's)"), "UGG Classic Ultra Mini");
  assert.equal(modelFor("On Running Cloudtilt Black Eclipse"), "On Cloudtilt");
  assert.equal(modelFor("adidas Vento XLG Deluxe Black"), "XLG Runner Deluxe");
  assert.equal(modelFor("adidas Yeezy Boost 350 V2 Onyx"), "Yeezy 350");
  assert.equal(modelFor("Salomon XT-6 Coffee French Roast"), "Salomon XT-6");
  assert.equal(modelFor("Fear of God Essentials Fleece (FW24) Black - Full Set"), "Essentials Full Set");
  assert.equal(modelFor("Fear of God Essentials Fleece Essential Sweatpant (FW24) Black"), "Essentials Sweatpants");
  assert.equal(modelFor("Maison Mihara Yasuhiro Peterson OG Sole Canvas Low Black"), "Maison Mihara Peterson");
  assert.equal(modelFor("Nike Mind 001 Slide Geode Teal"), "Nike Mind 001");
  assert.equal(modelFor("adidas Focus Olive adidas Chinese New Year Jacket Olive"), "");
});

test("every Travis Scott pair is Travis Scott, whatever the silhouette", () => {
  assert.equal(modelFor("Jordan 1 Retro Low OG SP Travis Scott Shy Pink"), "Travis Scott");
  assert.equal(modelFor("Jordan Jumpman Jack TR Travis Scott Dark Mocha"), "Travis Scott");
  assert.equal(modelFor("Nike Air Max 1 Travis Scott Cactus Jack Baroque Brown"), "Travis Scott");
});

test("planOrder: live, full sets, then score; children last; 48h breaks a tie", () => {
  const scores = new Map([["A", 50], ["B", 80], ["C", 50], ["K", 99]]);
  const order = planOrder([
    { id: "a", title: "Shoe A", status: "ACTIVE", sku: "A", express: false },
    { id: "b", title: "Shoe B", status: "ACTIVE", sku: "b", express: false },
    { id: "c", title: "Shoe C", status: "ACTIVE", sku: "C", express: true },
    { id: "k", title: "Shoe K (GS)", status: "ACTIVE", sku: "K", express: true },
    { id: "d", title: "Shoe D", status: "DRAFT", sku: "B", express: true },
    { id: "s", title: "Essentials - Full Set", status: "ACTIVE", sku: "S", express: false }
  ], scores);

  assert.deepEqual(order, ["s", "b", "c", "a", "k", "d"]);
});
