/*
 * One product, for real, so the page can be looked at before thousands follow.
 *
 * Everything else in this branch decides without writing. This is the first
 * thing that writes, and it writes exactly one product: the whole size ladder
 * in order, the photographs from the library, our stock on our own location,
 * published to the Online Store.
 *
 * Dry unless --apply is given, and it prints the product's admin link when it
 * is done so the result can be judged rather than assumed.
 *
 * Run with: node create-one-product.mjs CL-00031 M2002RDB [--apply]
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

const CLIENT = process.argv[2];
const SKU = String(process.argv[3] || "").toUpperCase();
const APPLY = process.argv.includes("--apply");

if (!CLIENT || !SKU) {
  console.log("Gebruik: node create-one-product.mjs CL-00031 M2002RDB [--apply]");
  process.exit(1);
}

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

console.log(`${CLIENT}  ${m["Store Name"]}  (${host})`);
console.log(`locatie: ${m["Shopify Location ID"]}`);
console.log(APPLY ? "\nECHT SCHRIJVEN\n" : "\ndroog, er wordt niets geschreven\n");

// The stock for this one style code.
const stock = await fetch(
  `${process.env.SUPABASE_URL}/rest/v1/consignment_inventory` +
    `?select=id,sku,size,vat_type,selling_price_suggested,quantity,seller_id,seller_record_id,brand,product_name` +
    `&quantity=gt.0&sku=eq.${encodeURIComponent(SKU)}`,
  {
    headers: {
      apikey: process.env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`
    }
  }
).then((r) => r.json());

if (!Array.isArray(stock) || !stock.length) {
  console.log(`Geen voorraad voor ${SKU}`);
  process.exit(1);
}

const { listings, rejected } = buildDesiredListings({
  inventoryRows: stock,
  merchantFields: m,
  priceSync: true,
  excludeSellerRecordId: (m["Seller ID"] || [])[0] || null
});

if (!listings.length) {
  console.log("Niets dat door de poort komt:", rejected.map((r) => r.reason).join(", "));
  process.exit(1);
}

const { ladders } = await ladderFor([SKU], { stockx: createStockxClient() });

const merchants = (await airtable("Merchants")).map((r) => ({
  name: r.fields["Store Name"],
  storeUrl: r.fields["Shopify Store URL"],
  token: r.fields["Shopify Token"]
}));

const { photos } = await photosForSkus([SKU], { sources: photoSources(merchants) });
const photosBySku = new Map([[SKU, photosFor(photos, SKU)]]);

/*
  What the store already has, read live.

  This said indexCurrentListings([]) - an empty catalogue - and so every run
  planned a fresh product. Three runs made three products. The planner was
  right all along; it was being told the shop was empty.
*/
const current = indexCurrentListings(await readCurrentListings(graphql, [SKU]));

if (current.size) {
  const existing = current.get(SKU);
  console.log(`
de winkel heeft dit paar al: ${existing.variants.size} maten`);
}

const plan = planListings({
  desired: listings,
  current,
  sizeLadders: ladders,
  productSync: true,
  priceSync: true
});

/*
  What the plan says, whatever it says.

  This printed the product it was about to create and nothing else, which was
  fine while it always created one. Now that it looks at the shop first, "the
  pair is already there and only the stock moves" is the normal answer.
*/
const entry = plan.createProducts[0];

if (entry) {
  console.log(`
${entry.title}  (nieuw)`);
  console.log(`  ladder    : ${entry.sizes.length} maten`);
  console.log(`  te vullen : ${entry.variants.map((v) => `${v.size} (${v.quantity}x à ${v.price})`).join(", ")}`);
  console.log(`  foto's    : ${photosBySku.get(SKU).length}`);
} else {
  console.log("\nniets aan te maken, het paar staat er al");
}

console.log("\nplan:");
console.log(`  producten aanmaken : ${plan.createProducts.length}`);
console.log(`  maten bijzetten    : ${plan.addSizes.length}`);
console.log(`  aantallen zetten   : ${plan.setQuantities.length}`);
console.log(`  prijzen zetten     : ${plan.setPrices.length}`);
console.log(`  op nul zetten      : ${plan.clearQuantities.length}`);

plan.setPrices.slice(0, 5).forEach((c) => console.log(`    prijs ${c.size}: ${c.from} -> ${c.to}`));
plan.setQuantities.slice(0, 5).forEach((c) => console.log(`    aantal ${c.size}: ${c.from} -> ${c.to}`));

/*
  The client. Kept here rather than imported so this script can be run on its
  own, and so a mistake in it cannot reach the rest of the service.
*/
async function graphql(query, variables) {
  const res = await fetch(`https://${host}/admin/api/${process.env.SHOPIFY_API_VERSION || "2026-01"}/graphql.json`, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": m["Shopify Token"],
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ query, variables })
  });

  const body = await res.json();

  if (!res.ok) throw new Error(`Shopify ${res.status}: ${JSON.stringify(body).slice(0, 300)}`);
  if (body.errors) throw new Error(`Shopify: ${JSON.stringify(body.errors).slice(0, 300)}`);

  return body.data;
}

const writer = createShopifyWriter({
  graphql,
  locationId: m["Shopify Location ID"],
  apply: APPLY
});

const report = await applyPlan(plan, writer, { photosBySku });

console.log("\n--- resultaat ---");

if (!APPLY) {
  console.log(`${report.wouldDo.length} bewerkingen zouden verstuurd worden:`);
  report.wouldDo.forEach((op) => console.log("  " + op.label));
  console.log("\nDraai opnieuw met --apply om het echt te doen.");
  process.exit(0);
}

report.done.forEach((label) => console.log("  gedaan: " + label));
report.problems.forEach((p) => console.log("  MISLUKT: " + p.label + " -> " + p.failure));

// And what it actually became, read back rather than assumed.
const check = await graphql(
  `query($q: String!) {
     products(first: 1, query: $q) {
       nodes {
         id
         title
         handle
         status
         onlineStoreUrl
         media(first: 10) { nodes { id } }
         options { name optionValues { name } }
         variants(first: 60) {
           nodes { title price inventoryPolicy inventoryQuantity sku }
         }
       }
     }
   }`,
  { q: `sku:${SKU}` }
);

const product = check?.products?.nodes?.[0];

if (!product) {
  console.log("\nNiets teruggevonden op die SKU.");
  process.exit(1);
}

const numericId = String(product.id).split("/").pop();

console.log(`\n${product.title}`);
console.log(`  status        : ${product.status}`);
console.log(`  online        : ${product.onlineStoreUrl || "(nog niet zichtbaar)"}`);
console.log(`  foto's        : ${product.media.nodes.length}`);
console.log(`  maten         : ${product.options[0]?.optionValues.length}`);
console.log(`  volgorde      : ${product.options[0]?.optionValues.map((v) => v.name).join(" ")}`);
console.log(`  met voorraad  : ${product.variants.nodes.filter((v) => v.inventoryQuantity > 0).map((v) => `${v.title} (${v.inventoryQuantity})`).join(", ") || "geen"}`);
console.log(`  doorverkopen  : ${[...new Set(product.variants.nodes.map((v) => v.inventoryPolicy))].join(", ")}`);
console.log(`\n  beheer: https://${host}/admin/products/${numericId}`);
