/*
 * The whole shelf, for one store.
 *
 * Same chain as create-one-product, run over every style code we hold: the
 * cheapest consignor per size, our fee, the store's margin gate, the size
 * ladder, the photographs, and then the writing.
 *
 * Three things make it safe to run more than once.
 *
 * It reads the store live before planning, per style code, so a pair that is
 * already there is never made twice. That is not theoretical - the first live
 * attempt made three copies of one shoe because it was handed an empty
 * catalogue each time.
 *
 * It respects the merchant's own switches. Consignment Sync off means it
 * refuses to touch the store at all, and Product Sync off means it fills
 * sizes the store already sells and creates nothing. Those boxes exist to be
 * the answer, not to be worked around.
 *
 * And it is dry unless told otherwise, with --limit to do a handful first.
 *
 * Run with: node create-all-products.mjs CL-00031 [--limit=25] [--apply]
 */
import { fileURLToPath } from "node:url";
import path from "node:path";
import fs from "node:fs";

import { buildDesiredListings } from "./desiredListings.js";
import { planListings, indexCurrentListings } from "./listingPlan.js";
import { ladderFor } from "./sizeLadders.js";
import { createStockxClient } from "./stockxSizes.js";
import { photoSources, photosForSkus, photosFor } from "./storePhotos.js";
import { createShopifyWriter, applyPlan, readCurrentListings } from "./shopifyWriter.js";

const here = path.dirname(fileURLToPath(import.meta.url));

for (const candidate of [path.join(here, ".env"), path.join(here, "..", "kickz-caviar-portal-main", ".env")]) {
  if (!fs.existsSync(candidate)) continue;

  for (const line of fs.readFileSync(candidate, "utf8").split("\n")) {
    const match = line.match(/^([A-Z0-9_]+)=(.*)$/);
    if (match && !process.env[match[1]]) process.env[match[1]] = match[2].trim();
  }

  break;
}

const CLIENT = process.argv[2] || "CL-00031";
const APPLY = process.argv.includes("--apply");
const LIMIT = Number((process.argv.find((a) => a.startsWith("--limit=")) || "").split("=")[1]) || 0;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function airtable(table, formula) {
  const url = new URL(`https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${encodeURIComponent(table)}`);

  if (formula) url.searchParams.set("filterByFormula", formula);

  const res = await fetch(url, { headers: { Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}` } });
  const data = await res.json();

  if (!res.ok) throw new Error(JSON.stringify(data).slice(0, 200));

  return data.records;
}

const [merchantRecord] = await airtable("Merchants", `{Client ID} = '${CLIENT}'`);

if (!merchantRecord) {
  console.log(`Geen merchant ${CLIENT}`);
  process.exit(1);
}

const m = merchantRecord.fields;
const host = String(m["Shopify Store URL"] || "").replace(/^https?:\/\//, "").replace(/\/$/, "");

const consignmentSync = Boolean(m["Consignment Sync?"]);
const priceSync = Boolean(m["Price Sync?"]);
const productSync = Boolean(m["Product Sync?"]);

console.log(`${CLIENT}  ${m["Store Name"]}  (${host})`);
console.log(`  Consignment Sync : ${consignmentSync ? "aan" : "uit"}`);
console.log(`  Price Sync       : ${priceSync ? "aan" : "uit"}`);
console.log(`  Product Sync     : ${productSync ? "aan" : "uit"}`);

if (!consignmentSync) {
  console.log("\nConsignment Sync staat uit voor deze winkel, dus er gebeurt niets.");
  console.log("Zet het vinkje aan in Merchants als je dit wil laten lopen.");
  process.exit(0);
}

/*
  Shopify's GraphQL runs on a leaky bucket rather than a request count, and a
  product create is one of the expensive calls. Every answer says how much is
  left, so the pace follows what the shop actually allows instead of a guess.
*/
let throttleWait = 0;

async function graphql(query, variables) {
  if (throttleWait) {
    await sleep(throttleWait);
    throttleWait = 0;
  }

  const res = await fetch(`https://${host}/admin/api/${process.env.SHOPIFY_API_VERSION || "2026-01"}/graphql.json`, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": m["Shopify Token"],
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ query, variables })
  });

  const body = await res.json();

  const status = body?.extensions?.cost?.throttleStatus;

  if (status && status.currentlyAvailable < 300) {
    // Enough to get back to a comfortable margin, rounded up to a whole second.
    throttleWait = Math.ceil(((400 - status.currentlyAvailable) / status.restoreRate) * 1000);
  }

  if (!res.ok) throw new Error(`Shopify ${res.status}: ${JSON.stringify(body).slice(0, 200)}`);

  if (body.errors) {
    const throttled = body.errors.some((e) => String(e.message).toLowerCase().includes("throttle"));

    if (throttled) {
      await sleep(2000);
      return graphql(query, variables);
    }

    throw new Error(`Shopify: ${JSON.stringify(body.errors).slice(0, 200)}`);
  }

  return body.data;
}

// Our stock, paged.
const stock = [];

for (let from = 0; ; from += 1000) {
  const page = await fetch(
    `${process.env.SUPABASE_URL}/rest/v1/consignment_inventory` +
      `?select=id,sku,size,vat_type,selling_price_suggested,quantity,seller_id,seller_record_id,brand,product_name&quantity=gt.0`,
    {
      headers: {
        apikey: process.env.SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`,
        Range: `${from}-${from + 999}`
      }
    }
  ).then((r) => r.json());

  if (!Array.isArray(page) || !page.length) break;

  stock.push(...page);

  if (page.length < 1000) break;
}

const { listings, rejected } = buildDesiredListings({
  inventoryRows: stock,
  merchantFields: m,
  priceSync,
  excludeSellerRecordId: (m["Seller ID"] || [])[0] || null
});

const allSkus = [...new Set(listings.map((l) => l.sku))].sort();
const skus = LIMIT ? allSkus.slice(0, LIMIT) : allSkus;

console.log(`\nvoorraadregels : ${stock.length}`);
console.log(`hoort op de plank : ${listings.length} paren over ${allSkus.length} stijlcodes`);
console.log(`afgevallen     : ${rejected.length}`);
console.log(`deze ronde     : ${skus.length} stijlcodes`);
console.log(APPLY ? "\nECHT SCHRIJVEN\n" : "\ndroog, er wordt niets geschreven\n");

const { ladders } = await ladderFor(skus, { stockx: createStockxClient() });

const merchants = (await airtable("Merchants")).map((r) => ({
  name: r.fields["Store Name"],
  storeUrl: r.fields["Shopify Store URL"],
  token: r.fields["Shopify Token"]
}));

const { photos } = await photosForSkus(skus, { sources: photoSources(merchants) });

const started = Date.now();
const totals = { created: 0, skipped: 0, quantities: 0, prices: 0, failed: 0 };
const failures = [];

for (const [index, sku] of skus.entries()) {
  const forThisSku = listings.filter((l) => l.sku === sku);

  let current;

  try {
    current = indexCurrentListings(await readCurrentListings(graphql, [sku]));
  } catch (err) {
    totals.failed += 1;
    failures.push({ sku, step: "winkel lezen", error: err.message });
    continue;
  }

  const plan = planListings({
    desired: forThisSku,
    current,
    sizeLadders: ladders,
    productSync,
    priceSync
  });

  if (!plan.createProducts.length && !plan.setQuantities.length && !plan.setPrices.length) {
    totals.skipped += 1;
    continue;
  }

  const writer = createShopifyWriter({ graphql, locationId: m["Shopify Location ID"], apply: APPLY });

  try {
    const report = await applyPlan(plan, writer, {
      photosBySku: new Map([[sku, photosFor(photos, sku)]])
    });

    if (report.problems.length) {
      totals.failed += 1;
      failures.push({ sku, step: report.problems[0].label, error: report.problems[0].failure });
    } else {
      totals.created += plan.createProducts.length;
      totals.quantities += plan.setQuantities.length;
      totals.prices += plan.setPrices.length;
    }
  } catch (err) {
    totals.failed += 1;
    failures.push({ sku, step: "schrijven", error: err.message });
  }

  if ((index + 1) % 10 === 0 || index + 1 === skus.length) {
    const perMinute = ((index + 1) / ((Date.now() - started) / 60000)).toFixed(1);

    console.log(
      `  ${index + 1}/${skus.length}  aangemaakt ${totals.created}, ongewijzigd ${totals.skipped}, ` +
        `mislukt ${totals.failed}  (${perMinute} per minuut)`
    );
  }
}

console.log(`\nklaar in ${((Date.now() - started) / 60000).toFixed(1)} minuten`);
console.log(`  producten aangemaakt : ${totals.created}`);
console.log(`  al goed              : ${totals.skipped}`);
console.log(`  aantallen bijgewerkt : ${totals.quantities}`);
console.log(`  prijzen bijgewerkt   : ${totals.prices}`);
console.log(`  mislukt              : ${totals.failed}`);

failures.slice(0, 15).forEach((f) => console.log(`    ${f.sku}  ${f.step}: ${String(f.error).slice(0, 120)}`));
