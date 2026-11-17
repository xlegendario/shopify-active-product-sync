/*
 * The format checks for automatic descriptions.
 *
 * Run with: node --test productDescriptions.test.mjs
 */
import test from "node:test";
import assert from "node:assert/strict";
import { renderDescription, textProblems, fromTemplate, normalizeStyle } from "./productDescriptions.js";

const good = {
  body: [
    "The Air Jordan 4 arrived in 1989 as Tinker Hatfield's follow-up to the Jordan 3, with mesh side panels, plastic wings for the laces and a visible Air unit in the heel. It has been part of every serious collection since, and the Black Cat is one of its most loved colourways.",
    "This 2025 pair is black from top to bottom: nubuck upper, black wings and eyelets, and Light Graphite details on the tongue and heel. Easy to wear with black denim or a tracksuit."
  ],
  fit: "True to size for most people.",
  details: [
    { label: "Style code", value: "FV5029-010" },
    { label: "Colorway", value: "Black/Black/Light Graphite" },
    { label: "Release", value: "November 2025" },
    { label: "Upper", value: "" }
  ],
  seo: "Jordan 4 Retro Black Cat (2025) | UNION Amsterdam",
  meta: "Shop the Jordan 4 Retro Black Cat (2025) at UNION Amsterdam. All-black nubuck with Light Graphite details. 100% authentic."
};

test("a text in the approved format passes", () => {
  assert.deepEqual(textProblems(good, "UNION Amsterdam"), []);
});

test("shipping, prices and long fields are caught", () => {
  const bad = { ...good, meta: good.meta + " Ships in 48 hours from $220.", seo: "x".repeat(61) };
  const problems = textProblems(bad, "UNION Amsterdam");
  assert.ok(problems.some((p) => p.includes("SEO title")));
  assert.ok(problems.some((p) => p.includes("price, shipping")));
});

test("empty detail rows are left out and HTML is escaped", () => {
  const html = renderDescription({ ...good, body: ["A & B <i>", good.body[1]] });
  assert.ok(html.startsWith("<p>A &amp; B &lt;i&gt;</p>"));
  assert.ok(html.includes("<p><strong>Fit:</strong> True to size for most people.</p>"));
  assert.ok(!html.includes("Upper"));
});

test("the store name goes in, and shortens when the title would run over", () => {
  assert.equal(fromTemplate("Shop it at {store}.", "UNION Amsterdam", 155), "Shop it at UNION Amsterdam.");
  const long = "Jordan 4 Retro SB Navy (2025) DR5415-103 | {store}";
  assert.equal(fromTemplate(long, "UNION Amsterdam", 52), "Jordan 4 Retro SB Navy (2025) DR5415-103 | UNION");
});

test("style codes are compared without case or spaces", () => {
  assert.equal(normalizeStyle(" fv5029-010 "), "FV5029-010");
});
