/*
 * The whole chain, end to end, without writing a thing.
 *
 * Stock -> cheapest consignor -> our fee -> the store's margin gate -> the
 * difference with what the store already has -> what would have to happen.
 *
 * Run with: node preview-plan.mjs CL-00031 [--limit 20]
 */
import { fileURLToPath } from "node:url";
import path from "node:path";
import fs from "node:fs";

import { buildDesiredListings } from "./desiredListings.js";
import { planListings, indexCurrentListings } from "./listingPlan.js";
import { createStockxClient } from "./stockxSizes.js";
import { ladderFor } from "./sizeLadders.js";
import { createShopifyWriter, applyPlan } from "./shopifyWriter.js";
import { buildImageLibrary, photosFor } from "./storePhotos.js";

const here = path.dirname(fileURLToPath(import.meta.url));

for (const candidate of [path.join(here, ".env"), path.join(here, "..", "kickz-caviar-portal-main", ".env")]) {
  if (!fs.existsSync(candidate)) continue;
  for (const line of fs.readFileSync(candidate, "utf8").split("\n")) {
    const m = line.match(/^([A-Z0-9_]+)=(.*)$/);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2].trim();
  }
  break;
}

const CLIENT = process.argv[2] || "CL-00031";
const LIMIT = Number((process.argv.find((a) => a.startsWith("--limit=")) || "").split("=")[1]) || 15;

const airtable = async (table, formula) => {
  const url = new URL(`https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${encodeURIComponent(table)}`);
  if (formula) url.searchParams.set("filterByFormula", formula);
  const res = await fetch(url, { headers: { Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}` } });
  const data = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(data).slice(0, 200));
  return data.records;
};

const [merchant] = await airtable("Merchants", `{Client ID} = '${CLIENT}'`);
const m = merchant.fields;

console.log(`${CLIENT}  ${m["Store Name"]}`);
console.log("  vinkjes:",
  "Consignment", m["Consignment Sync?"] ? "aan" : "uit",
  "| Price", m["Price Sync?"] ? "aan" : "uit",
  "| Product", m["Product Sync?"] ? "aan" : "uit");

// Our stock.
const stock = [];
for (let from = 0; ; from += 1000) {
  const page = await fetch(
    `${process.env.SUPABASE_URL}/rest/v1/consignment_inventory?select=id,sku,size,vat_type,selling_price_suggested,quantity,seller_id,seller_record_id,brand,product_name&quantity=gt.0`,
    { headers: { apikey: process.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`, Range: `${from}-${from + 999}` } }
  ).then((r) => r.json());
  if (!Array.isArray(page) || !page.length) break;
  stock.push(...page);
  if (page.length < 1000) break;
}

// What the store already has, out of our copy of its catalogue.
const listingsRows = await fetch(
  `${process.env.SUPABASE_URL}/rest/v1/store_listings?select=sku,size,price,quantity&merchant_record_id=eq.${merchant.id}&is_active=eq.true`,
  { headers: { apikey: process.env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}` } }
).then((r) => r.json()).catch(() => []);

const current = indexCurrentListings(Array.isArray(listingsRows) ? listingsRows : []);

console.log("\nvoorraadregels:", stock.length, "| producten die de winkel al heeft:", current.size);

const { listings, rejected } = buildDesiredListings({
  inventoryRows: stock,
  merchantFields: m,
  priceSync: process.argv.includes("--price-sync") || Boolean(m["Price Sync?"]),
  excludeSellerRecordId: (m["Seller ID"] || [])[0] || null
});

console.log("hoort op de plank:", listings.length, "| afgevallen:", rejected.length);

// The size ladders, for a handful of SKUs so this stays a preview.
const skus = [...new Set(listings.map((l) => l.sku))].slice(0, LIMIT);

const { ladders, asked, known } = await ladderFor(skus, {
  stockx: createStockxClient(),
  onProgress: ({ sku, sizes, error, asked: n, total }) =>
    console.log(`  ${String(n).padStart(3)}/${total}  ${sku.padEnd(16)} ${error ? "mislukt: " + error : sizes + " maten"}`)
});

const found = [...ladders.values()].filter((v) => v.length).length;
console.log(`matenladders: ${found} met maten, ${asked} nieuw opgehaald, ${known} al bekend`);

const plan = planListings({
  desired: listings.filter((l) => skus.includes(l.sku)),
  current,
  sizeLadders: ladders,
  productSync: process.argv.includes("--product-sync") || Boolean(m["Product Sync?"]),
  priceSync: process.argv.includes("--price-sync") || Boolean(m["Price Sync?"]),
  sellWithoutStock: true
});

console.log("\nwat er zou gebeuren, voor deze", skus.length, "SKU's:");
console.log("  producten aanmaken :", plan.createProducts.length);
console.log("  maten bijzetten    :", plan.addSizes.length);
console.log("  aantallen zetten   :", plan.setQuantities.length);
console.log("  prijzen zetten     :", plan.setPrices.length);
console.log("  op nul zetten      :", plan.clearQuantities.length);
console.log("  overgeslagen       :", plan.skipped.length);

plan.createProducts.slice(0, 3).forEach((p) => {
  const held = new Map(p.variants.map((v) => [v.size, v]));
  console.log(`\n  ${p.sku}  ${p.title}`);
  console.log(`    ladder (${p.sizes.length}): ` + p.sizes.map((s) => held.has(s) ? `[${s}]` : s).join(" "));
  console.log("    tussen haakjes = maat die we kunnen vullen, de rest komt uitverkocht op de pagina");
});


/*
  And what would be sent to Shopify, without sending any of it.

  The client throws if anything reaches it, so a dry run that quietly wrote
  something is not possible: it would fail loudly instead.
*/
const skuList = skus.map((sku) => "{SKU} = '" + sku + "'").join(",");

const pictureRows = await fetch(
  "https://api.airtable.com/v0/" + process.env.AIRTABLE_BASE_ID + "/" +
    encodeURIComponent(process.env.AIRTABLE_SKU_MASTER_TABLE || "SKU Master") +
    "?filterByFormula=" + encodeURIComponent("OR(" + skuList + ",FALSE())") +
    "&fields%5B0%5D=SKU&fields%5B1%5D=Picture%20URL",
  { headers: { Authorization: "Bearer " + process.env.AIRTABLE_TOKEN } }
).then((r) => r.json());

const picturesBySku = new Map(
  (pictureRows.records || []).map((r) => [String(r.fields.SKU || "").toUpperCase(), r.fields["Picture URL"] || ""])
);

console.log("\nfoto's gevonden: " + skus.filter((s) => picturesBySku.get(s)).length + " van " + skus.length);

const writer = createShopifyWriter({
  graphql: async () => { throw new Error("droog: er wordt niets geschreven"); },
  locationId: m["Shopify Location ID"],
  apply: process.argv.includes("--apply")
});

const report = await applyPlan(plan, writer, { picturesBySku });

console.log("\nbewerkingen die verstuurd zouden worden: " + report.wouldDo.length);
report.wouldDo.slice(0, 6).forEach((op) => console.log("  " + op.label));

const sample = report.wouldDo.find((op) => op.label.startsWith("product aanmaken"));

if (sample) {
  console.log("\neerste bewerking, volledig:");
  console.log(JSON.stringify(sample.variables, null, 1).slice(0, 800));
}

const variantSample = report.wouldDo.find((op) => op.label.startsWith("maten zetten"));

if (variantSample) {
  const v = variantSample.variables.variants;
  console.log("\neerste twee varianten van " + variantSample.label + ":");
  console.log(JSON.stringify(v.slice(0, 2), null, 1));
}


/*
  The real photographs, from the stores that shoot their own stock.

  Reading three whole catalogues takes a few minutes, so it is behind a flag.
  Without it the run shows what it would do; with it, it shows what the pages
  would actually look like.
*/
if (process.argv.includes("--photos")) {
  const merchantRows = await airtable("Merchants");

  const merchants = merchantRows.map((r) => ({
    name: r.fields["Store Name"],
    storeUrl: r.fields["Shopify Store URL"],
    token: r.fields["Shopify Token"]
  }));

  console.log("\nfotobibliotheek opbouwen...");

  const { library, sources } = await buildImageLibrary({
    merchants,
    onPage: ({ storeName, pages, products, codes }) => {
      if (pages % 10 === 0) console.log(`    ${storeName}: ${pages} pagina's, ${products} producten, ${codes} stijlcodes`);
    }
  });

  sources.forEach((s) =>
    console.log("  " + String(s.name).padEnd(24) +
      (s.skipped ? "overgeslagen: " + s.skipped : `${s.products} producten, ${s.added} stijlcodes toegevoegd, ${s.alreadyCovered} had de vorige winkel al`))
  );

  console.log("  stijlcodes met foto's:", library.size);

  const withPhotos = skus.filter((sku) => photosFor(library, sku).length);

  console.log(`\nvan de ${skus.length} SKU's op de plank hebben er ${withPhotos.length} echte foto's`);

  withPhotos.slice(0, 5).forEach((sku) => {
    const photos = photosFor(library, sku);
    console.log("  " + sku.padEnd(16) + photos.length + " foto's, eerste: " + photos[0].slice(0, 70));
  });

  const zonder = skus.filter((sku) => !photosFor(library, sku).length);

  if (zonder.length) console.log("\nzonder foto's, die vallen terug op StockX: " + zonder.join(", "));
}
