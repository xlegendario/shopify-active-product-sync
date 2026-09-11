/*
 * What would land in a store, without writing a thing.
 *
 * Reads the real consignment stock and the real merchant settings, runs the
 * list through the same rules the sync will use, and prints both sides: what
 * makes it and what does not, with the reason.
 *
 * Run with: node preview-union.mjs [Client ID]
 */
import { fileURLToPath } from "node:url";
import path from "node:path";
import fs from "node:fs";

import { buildDesiredListings, storeCost } from "./desiredListings.js";

const here = path.dirname(fileURLToPath(import.meta.url));

for (const candidate of [path.join(here, ".env"), path.join(here, "..", "kickz-caviar-portal-main", ".env")]) {
  if (!fs.existsSync(candidate)) continue;
  for (const line of fs.readFileSync(candidate, "utf8").split("\n")) {
    const m = line.match(/^([A-Z0-9_]+)=(.*)$/);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2].trim();
  }
  break;
}

const CLIENT = process.argv[2] || "CL-00001";

const airtable = async (table, formula) => {
  const url = new URL(`https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${encodeURIComponent(table)}`);
  if (formula) url.searchParams.set("filterByFormula", formula);
  const res = await fetch(url, { headers: { Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}` } });
  const data = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(data).slice(0, 200));
  return data.records;
};

const [merchant] = await airtable("Merchants", `{Client ID} = '${CLIENT}'`);

if (!merchant) {
  console.log(`Geen merchant ${CLIENT}`);
  process.exit(0);
}

const m = merchant.fields;

console.log(`${CLIENT}  ${m["Store Name"] || ""}`);
console.log("  onze marge  :", m["Offer Method"] || "-", m["Offer Percentage"] ?? "", "cap", m["Margin Cap"] ?? "-");
console.log("  hun marge   :", m["Margin Method"] || "-", m["Minimum Margin (%)"] ?? "", "bedrag", m["Min Margin Amount"] ?? "-", "basiskosten", m["Base Costs"] ?? "-");
console.log("  vinkjes     :",
  "Consignment", m["Consignment Sync?"] ? "aan" : "uit",
  "| Price", m["Price Sync?"] ? "aan" : "uit",
  "| Product", m["Product Sync?"] ? "aan" : "uit");

/*
  Paged, because Supabase hands back a thousand rows and stops. Reading only
  the first page would quietly hide most of the stock and the list would look
  right while being a fraction of it.
*/
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

console.log("\nconsignmentregels met voorraad:", stock.length);

const { listings, rejected } = buildDesiredListings({
  inventoryRows: stock,
  merchantFields: m,
  priceSync: process.argv.includes("--price-sync") || Boolean(m["Price Sync?"]),
  excludeSellerRecordId: (m["Seller ID"] || [])[0] || null
});

console.log("op de plank:", listings.length, "| afgevallen:", rejected.length);

const reasons = new Map();
rejected.forEach((r) => reasons.set(r.reason, (reasons.get(r.reason) || 0) + 1));
[...reasons.entries()].forEach(([reason, count]) => console.log("   " + reason.padEnd(28) + count));

console.log("\neerste tien die het halen:");
listings.slice(0, 10).forEach((l) =>
  console.log(
    "  " + l.sku.padEnd(16) + String(l.size).padEnd(7) +
    l.vatType.padEnd(8) +
    "vraag " + String(l.ask).padStart(6) +
    " -> kost " + String(l.cost).padStart(7) + " (factuur " + String(l.invoiced).padStart(6) + ")" +
    " -> verkoop " + String(l.sellingPrice).padStart(7) +
    (l.priceSetByUs ? "  (door ons gezet)" : "")
  )
);
