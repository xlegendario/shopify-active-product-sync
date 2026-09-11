/*
 * Photographs for products that were created without any.
 *
 * A product gets its pictures when it is made, and never again. So a pair
 * whose photograph nobody had that day keeps a blank card for good, even
 * once a source store starts carrying it. Nine of UNION's first five hundred
 * landed that way: a Gucci Gazelle, an Off-White cap, an On Running.
 *
 * This walks a store, finds every product with no media at all, and gives it
 * what the library holds now. It asks the sources again for anything still
 * missing, so widening PHOTO_SOURCE_MERCHANTS takes effect here too.
 *
 * Only ever adds. A product that already has one picture is left alone,
 * because a second set from another shop next to the first is worse than
 * either on its own.
 *
 * Dry unless --apply is given.
 *
 * Run with: node backfill-photos.mjs CL-00031 [--apply]
 */
import { fileURLToPath } from "node:url";
import path from "node:path";
import fs from "node:fs";

import { photoSources, photosForSkus, photosFor } from "./storePhotos.js";
import { productImageUrl } from "./shopifyWriter.js";

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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function airtable(table, formula) {
  const url = new URL(`https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${encodeURIComponent(table)}`);

  if (formula) url.searchParams.set("filterByFormula", formula);

  url.searchParams.set("pageSize", "100");

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
console.log(APPLY ? "\nECHT SCHRIJVEN\n" : "\ndroog, er wordt niets geschreven\n");

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
    throttleWait = Math.ceil(((400 - status.currentlyAvailable) / status.restoreRate) * 1000);
  }

  if (!res.ok) throw new Error(`Shopify ${res.status}: ${JSON.stringify(body).slice(0, 200)}`);
  if (body.errors) throw new Error(`Shopify: ${JSON.stringify(body.errors).slice(0, 200)}`);

  return body.data;
}

/*
  Every active product with no media at all.

  Asked of the store rather than of our own copy, because store_listings does
  not record whether a product has pictures.
*/
const blank = [];

let cursor = null;

for (;;) {
  const data = await graphql(
    `query($cursor: String) {
       products(first: 250, after: $cursor, query: "status:active") {
         nodes {
           id
           title
           media(first: 1) { nodes { id } }
           variants(first: 1) { nodes { sku } }
         }
         pageInfo { hasNextPage endCursor }
       }
     }`,
    { cursor }
  );

  for (const product of data.products.nodes) {
    if (product.media.nodes.length) continue;

    const sku = String(product.variants.nodes[0]?.sku || "").trim().toUpperCase();

    if (sku) blank.push({ id: product.id, title: product.title, sku });
  }

  if (!data.products.pageInfo.hasNextPage) break;

  cursor = data.products.pageInfo.endCursor;
}

console.log(`producten zonder enige foto : ${blank.length}`);

if (!blank.length) {
  console.log("Niets te doen.");
  process.exit(0);
}

const merchants = (await airtable("Merchants")).map((r) => ({
  name: r.fields["Store Name"],
  storeUrl: r.fields["Shopify Store URL"],
  token: r.fields["Shopify Token"]
}));

const sources = photoSources(merchants);

console.log(`bronwinkels                 : ${sources.map((s) => s.name).join(" > ") || "geen"}`);

const { photos } = await photosForSkus(
  blank.map((b) => b.sku),
  { sources }
);

const teVullen = blank
  .map((b) => ({ ...b, urls: photosFor(photos, b.sku) }))
  .filter((b) => b.urls.length);

console.log(`waarvan nu een foto gevonden: ${teVullen.length}\n`);

teVullen.slice(0, 12).forEach((b) => console.log(`  ${b.sku.padEnd(16)}${b.urls.length} foto's  ${b.title.slice(0, 48)}`));

if (!APPLY) {
  console.log(`\n${blank.length - teVullen.length} blijven leeg, die heeft niemand.`);
  console.log("Draai opnieuw met --apply om ze te vullen.");
  process.exit(0);
}

let gevuld = 0;
const problemen = [];

for (const item of teVullen) {
  try {
    const data = await graphql(
      `mutation add($id: ID!, $media: [CreateMediaInput!]!) {
         productCreateMedia(productId: $id, media: $media) {
           media { id }
           mediaUserErrors { field message }
         }
       }`,
      {
        id: item.id,
        media: item.urls.map((url) => ({
          originalSource: productImageUrl(url),
          mediaContentType: "IMAGE"
        }))
      }
    );

    const errors = data?.productCreateMedia?.mediaUserErrors || [];

    if (errors.length) {
      problemen.push({ sku: item.sku, error: errors.map((e) => e.message).join("; ") });
      continue;
    }

    gevuld += 1;
  } catch (err) {
    problemen.push({ sku: item.sku, error: err.message });
  }
}

console.log(`\ngevuld  : ${gevuld}`);
console.log(`mislukt : ${problemen.length}`);

problemen.slice(0, 10).forEach((p) => console.log(`  ${p.sku}: ${String(p.error).slice(0, 120)}`));
