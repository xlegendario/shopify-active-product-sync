/*
 * Fill the storefront library for every style code we hold stock of.
 *
 * A library rather than a step in the product sync. UNION is the first thing
 * that needs it, a kiosk in a shop is the next, and neither should have to
 * ask Shopify again for a photograph somebody already found. One row per
 * style code, in storefront_library.
 *
 * Asked per style code through GraphQL, which can filter on SKU where the
 * REST endpoint cannot. Sources are tried in the configured order and the
 * first hit wins, so the shelf keeps one house style.
 *
 * Safe to stop and start. What is answered is written immediately, and a
 * second run skips everything already known - including the ones nobody had,
 * which are only re-asked after a month.
 *
 * Run with: node fill-photo-library.mjs [--limit=200]
 */
import { fileURLToPath } from "node:url";
import path from "node:path";
import fs from "node:fs";

import { photoSources, photosForSkus, readKnownPhotos } from "./storePhotos.js";

const here = path.dirname(fileURLToPath(import.meta.url));

for (const candidate of [path.join(here, ".env"), path.join(here, "..", "kickz-caviar-portal-main", ".env")]) {
  if (!fs.existsSync(candidate)) continue;

  for (const line of fs.readFileSync(candidate, "utf8").split("\n")) {
    const match = line.match(/^([A-Z0-9_]+)=(.*)$/);
    if (match && !process.env[match[1]]) process.env[match[1]] = match[2].trim();
  }

  break;
}

const LIMIT = Number((process.argv.find((a) => a.startsWith("--limit=")) || "").split("=")[1]) || 0;

async function airtable(table, formula) {
  const url = new URL(`https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${encodeURIComponent(table)}`);

  if (formula) url.searchParams.set("filterByFormula", formula);

  const res = await fetch(url, { headers: { Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}` } });
  const data = await res.json();

  if (!res.ok) throw new Error(JSON.stringify(data).slice(0, 200));

  return data.records;
}

// Every style code we hold, paged because Supabase stops at a thousand.
const codes = new Set();

for (let from = 0; ; from += 1000) {
  const page = await fetch(
    `${process.env.SUPABASE_URL}/rest/v1/consignment_inventory?select=sku&quantity=gt.0`,
    {
      headers: {
        apikey: process.env.SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`,
        Range: `${from}-${from + 999}`
      }
    }
  ).then((r) => r.json());

  if (!Array.isArray(page) || !page.length) break;

  page.forEach((row) => codes.add(String(row.sku || "").trim().toUpperCase()));

  if (page.length < 1000) break;
}

const all = [...codes].filter(Boolean).sort();
const known = await readKnownPhotos(all);

const missing = all.filter((sku) => {
  const entry = known.get(sku);

  return !entry || (!entry.images.length && !entry.checkedAt);
});

console.log(`stijlcodes met voorraad : ${all.length}`);
console.log(`al in de bibliotheek    : ${all.length - missing.length}`);
console.log(`nog op te halen         : ${missing.length}`);

const merchants = (await airtable("Merchants")).map((r) => ({
  name: r.fields["Store Name"],
  storeUrl: r.fields["Shopify Store URL"],
  token: r.fields["Shopify Token"]
}));

const sources = photoSources(merchants);

console.log(`bronwinkels             : ${sources.map((s) => s.name).join(" > ") || "geen"}\n`);

if (!sources.length) {
  console.log("Geen bronwinkel met een URL en token. Niets te doen.");
  process.exit(0);
}

const target = LIMIT ? all.slice(0, LIMIT) : all;
const started = Date.now();

const byStore = new Map();
let withPhotos = 0;
let without = 0;

const { asked, known: alreadyKnown } = await photosForSkus(target, {
  sources,
  onProgress: (step) => {
    if (step.error) {
      console.log(`  ${step.sku}: ${step.error}`);
      return;
    }

    if (step.images) {
      withPhotos += 1;
      byStore.set(step.source, (byStore.get(step.source) || 0) + 1);
    } else {
      without += 1;
    }

    if (step.asked % 25 === 0) {
      const perSecond = step.asked / ((Date.now() - started) / 1000);

      console.log(
        `  ${step.asked}/${step.total} opgevraagd, ${withPhotos} met foto's, ` +
          `${perSecond.toFixed(1)} per seconde`
      );
    }
  }
});

const seconds = (Date.now() - started) / 1000;

console.log(`\nklaar in ${seconds.toFixed(0)} seconden`);
console.log(`  opgevraagd     : ${asked}`);
console.log(`  al bekend      : ${alreadyKnown}`);
console.log(`  met foto's     : ${withPhotos}`);
console.log(`  zonder         : ${without}`);

[...byStore.entries()].forEach(([store, count]) => console.log(`  uit ${store}: ${count}`));
