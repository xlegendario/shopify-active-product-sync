/*
 * Do the code and the Airtable formulas agree?
 *
 * Target Buying Price and Maximum Buying Price are computed by Airtable on
 * every order and decide what we pay. storePricing.js computes the same thing
 * for the Shopify branch. Two places, one rule, which is exactly the shape of
 * bug that has cost us most this year: they agree until one of them is edited.
 *
 * So this reads real orders and compares, per order, both directions. It needs
 * no test data and no fixtures - the base is full of the real thing.
 *
 * Run with: node verify-against-airtable.mjs
 */
import { fileURLToPath } from "node:url";
import path from "node:path";
import fs from "node:fs";

import { maximumBuyingPrice, targetBuyingPrice, minimumSellingPrice, passesMarginGate } from "./storePricing.js";

/*
  Credentials come from this service's own .env when it has one, and otherwise
  from the portal next to it, which is where they live on a laptop. Airtable is
  read over plain REST for the same reason server.js does: no dependency worth
  adding for a check that runs by hand.
*/
const here = path.dirname(fileURLToPath(import.meta.url));

for (const candidate of [path.join(here, ".env"), path.join(here, "..", "kickz-caviar-portal-main", ".env")]) {
  if (!fs.existsSync(candidate)) continue;

  for (const line of fs.readFileSync(candidate, "utf8").split("\n")) {
    const match = line.match(/^([A-Z0-9_]+)=(.*)$/);
    if (match && !process.env[match[1]]) process.env[match[1]] = match[2];
  }

  break;
}

const TOKEN = process.env.AIRTABLE_TOKEN;
const BASE_ID = process.env.AIRTABLE_BASE_ID;

if (!TOKEN || !BASE_ID) {
  console.error("Geen Airtable-sleutels gevonden.");
  process.exit(1);
}

async function fetchAll(table, formula) {
  const rows = [];
  let offset;

  do {
    const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(table)}`);
    url.searchParams.set("pageSize", "100");
    if (formula) url.searchParams.set("filterByFormula", formula);
    if (offset) url.searchParams.set("offset", offset);

    const res = await fetch(url, { headers: { Authorization: `Bearer ${TOKEN}` } });
    const data = await res.json();

    if (!res.ok) throw new Error(`Airtable: ${res.status} ${JSON.stringify(data).slice(0, 200)}`);

    rows.push(...data.records);
    offset = data.offset;
  } while (offset);

  return rows;
}

const first = (v) => (Array.isArray(v) ? v[0] : v);
const num = (v) => { const n = Number(first(v)); return Number.isFinite(n) ? n : null; };

const orders = await fetchAll(
  "Unfulfilled Orders Log",
  `AND({Client Margin Method} != '', {Final Selling Price} > 0)`
);

console.log("orders met een margemethode:", orders.length);

let checked = 0;
let differences = 0;
const perMethod = new Map();

for (const order of orders) {
  const f = order.fields;
  const sellingPrice = num(f["Final Selling Price"]);
  const vatType = first(f["VAT Type"]);
  const method = first(f["Client Margin Method"]);

  const airtableMax = num(f["Maximum Buying Price"]);
  const airtableTarget = num(f["Target Buying Price"]);

  if (airtableMax === null && airtableTarget === null) continue;

  const mine = {
    max: maximumBuyingPrice({ sellingPrice, vatType, merchantFields: f }),
    target: targetBuyingPrice({ sellingPrice, vatType, merchantFields: f })
  };

  checked += 1;
  perMethod.set(method, (perMethod.get(method) || 0) + 1);

  const off = [];

  if (airtableMax !== null && Math.abs(mine.max - airtableMax) > 0.001) {
    off.push(`Maximum: Airtable ${airtableMax}, code ${mine.max}`);
  }

  if (airtableTarget !== null && Math.abs(mine.target - airtableTarget) > 0.001) {
    off.push(`Target: Airtable ${airtableTarget}, code ${mine.target}`);
  }

  // And the direction the Shopify branch needs: a price built from what we pay
  // has to survive the gate that Airtable's own number describes.
  if (airtableMax !== null && airtableMax > 0) {
    const selling = minimumSellingPrice({ buyingPrice: airtableMax, vatType, merchantFields: f });

    if (selling === null || !passesMarginGate({ buyingPrice: airtableMax, sellingPrice: selling, vatType, merchantFields: f })) {
      off.push(`omkering: ${airtableMax} gaf ${selling}, en dat komt niet door de poort`);
    }
  }

  if (off.length) {
    differences += 1;

    if (differences <= 10) {
      console.log(`\n${first(f["Order ID"]) || order.id}  verkoop ${sellingPrice}  ${vatType || "?"}  ${method}`);
      off.forEach((line) => console.log("   " + line));
    }
  }
}

console.log("\nvergeleken:", checked);
[...perMethod.entries()].forEach(([method, count]) => console.log("  " + String(method).padEnd(14) + count));
console.log(differences ? `\n${differences} orders waar het verschilt.` : "\nGeen enkel verschil.");
