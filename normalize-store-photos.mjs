/*
 * The photographs already in a store, brought into the one look.
 *
 * New pages get normalised photographs as they are made (photoNormalize.js);
 * this does the pages that were there before. Per product: every image is
 * normalised, the new ones are added, and only once Shopify has processed
 * all of them are the old ones removed. A product where anything fails keeps
 * its old photographs untouched, so a page never ends up half old and half
 * new, or empty.
 *
 * Done products carry the metafield lojiq.photo_style with the style they
 * were given, so a second run skips them and a new style redoes everything.
 *
 * Dry unless --apply. Active products only, unless --all.
 *
 * Run with: node normalize-store-photos.mjs CL-00031 [--apply] [--all] [--limit 5] [--sku FZ5808-009]
 */
import { fileURLToPath } from "node:url";
import path from "node:path";
import fs from "node:fs";

import { STYLE, normalizedPhotoUrls } from "./photoNormalize.js";

const here = path.dirname(fileURLToPath(import.meta.url));

for (const candidate of [path.join(here, ".env"), path.join(here, "..", "kickz-caviar-portal-main", ".env")]) {
  if (!fs.existsSync(candidate)) continue;

  for (const line of fs.readFileSync(candidate, "utf8").split("\n")) {
    const match = line.match(/^([A-Z0-9_]+)=(.*)$/);
    if (match && !process.env[match[1]]) process.env[match[1]] = match[2].trim().replace(/^"|"$/g, "");
  }

  break;
}

const args = process.argv.slice(2);
const CLIENT = args[0];
const APPLY = args.includes("--apply");
const ALL = args.includes("--all");
const LIMIT = args.includes("--limit") ? Number(args[args.indexOf("--limit") + 1]) : Infinity;
const ONLY_SKU = args.includes("--sku") ? String(args[args.indexOf("--sku") + 1]).toUpperCase() : null;
const API_VERSION = process.env.SHOPIFY_API_VERSION || "2026-01";

if (!CLIENT) {
  console.log("Gebruik: node normalize-store-photos.mjs CL-00031 [--apply] [--all] [--limit 5] [--sku SKU]");
  process.exit(1);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function airtable(table, formula) {
  const url = new URL(`https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${encodeURIComponent(table)}`);
  if (formula) url.searchParams.set("filterByFormula", formula);

  const res = await fetch(url, { headers: { Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}` } });
  const data = await res.json();

  if (!res.ok) throw new Error(JSON.stringify(data).slice(0, 200));
  return data.records;
}

const [merchant] = await airtable("Merchants", `{Client ID} = '${CLIENT}'`);

if (!merchant) {
  console.log(`Geen merchant ${CLIENT}`);
  process.exit(1);
}

const host = String(merchant.fields["Shopify Store URL"] || "").replace(/^https?:\/\//, "").replace(/\/$/, "");
const token = merchant.fields["Shopify Token"];

async function graphql(query, variables = {}) {
  for (let attempt = 0; ; attempt += 1) {
    const res = await fetch(`https://${host}/admin/api/${API_VERSION}/graphql.json`, {
      method: "POST",
      headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
      body: JSON.stringify({ query, variables })
    });

    const data = await res.json().catch(() => ({}));
    const throttled = res.status === 429 || (data.errors || []).some((e) => e.extensions?.code === "THROTTLED");

    if (throttled && attempt < 6) {
      await sleep(2000 * (attempt + 1));
      continue;
    }

    if (!res.ok || data.errors) throw new Error(`Shopify ${res.status}: ${JSON.stringify(data.errors || data).slice(0, 200)}`);
    return data.data;
  }
}

const PRODUCTS = `
  query products($cursor: String, $q: String) {
    products(first: 50, after: $cursor, query: $q) {
      nodes {
        id
        title
        variants(first: 1) { nodes { sku } }
        metafield(namespace: "lojiq", key: "photo_style") { value }
        media(first: 30) {
          nodes { id mediaContentType ... on MediaImage { image { url } } }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
`;

const ADD = `
  mutation add($productId: ID!, $media: [CreateMediaInput!]!) {
    productCreateMedia(productId: $productId, media: $media) {
      media { id }
      mediaUserErrors { message }
    }
  }
`;

const STATUS = `
  query status($id: ID!) {
    product(id: $id) { media(first: 60) { nodes { id status } } }
  }
`;

const REMOVE = `
  mutation remove($productId: ID!, $mediaIds: [ID!]!) {
    productDeleteMedia(productId: $productId, mediaIds: $mediaIds) {
      deletedMediaIds
      mediaUserErrors { message }
    }
  }
`;

const MARK = `
  mutation mark($metafields: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $metafields) { userErrors { message } }
  }
`;

// Wait until Shopify has fetched and processed every new image.
async function settled(productId, ids) {
  for (let i = 0; i < 20; i += 1) {
    await sleep(1500);

    const nodes = (await graphql(STATUS, { id: productId }))?.product?.media?.nodes || [];
    const mine = nodes.filter((node) => ids.includes(node.id));

    if (mine.some((node) => node.status === "FAILED")) return "FAILED";
    if (mine.length === ids.length && mine.every((node) => node.status === "READY")) return "READY";
  }

  return "TIMEOUT";
}

const todo = [];
let cursor = null;

for (;;) {
  const data = await graphql(PRODUCTS, { cursor, q: ALL ? null : "status:active" });

  for (const product of data.products.nodes) {
    const sku = String(product.variants.nodes[0]?.sku || "").toUpperCase();
    if (ONLY_SKU && sku !== ONLY_SKU) continue;

    const images = product.media.nodes.filter((node) => node.mediaContentType === "IMAGE" && node.image?.url);
    if (!images.length) continue;
    if (product.metafield?.value === STYLE) continue;

    todo.push({ id: product.id, title: product.title, sku, images, restyle: Boolean(product.metafield?.value) });
  }

  if (!data.products.pageInfo.hasNextPage) break;
  cursor = data.products.pageInfo.endCursor;
}

console.log(`${merchant.fields["Store Name"]} (${host}), stijl ${STYLE}`);
console.log(`${todo.length} producten te doen, ${todo.reduce((n, p) => n + p.images.length, 0)} foto's`);

if (!APPLY) {
  console.log("droog, er wordt niets geschreven. Voeg --apply toe.");
  process.exit(0);
}

const supabase = { supabaseUrl: process.env.SUPABASE_URL, supabaseKey: process.env.SUPABASE_SERVICE_ROLE_KEY };

/*
  A product normalised before in an older style no longer holds its original
  photographs - those were replaced. Working on the replacements would stack
  one look on another, so its sources come from the photo library instead.
*/
async function librarySources(sku) {
  if (!sku) return [];

  const res = await fetch(
    `${supabase.supabaseUrl}/rest/v1/storefront_library?select=images&style_code=eq.${encodeURIComponent(sku)}`,
    { headers: { apikey: supabase.supabaseKey, Authorization: `Bearer ${supabase.supabaseKey}` } }
  );
  const rows = res.ok ? await res.json() : [];
  const images = rows[0]?.images || [];

  return images.map((image) => (typeof image === "string" ? image : image?.url)).filter(Boolean);
}
const result = { done: 0, failed: [] };

for (const product of todo.slice(0, LIMIT)) {
  const label = `${product.sku || "?"} ${product.title}`;

  try {
    const sources = product.restyle
      ? await librarySources(product.sku)
      : product.images.map((image) => image.image.url);

    if (!sources.length) throw new Error("geen originele foto's in de bibliotheek");

    const urls = await normalizedPhotoUrls(
      sources,
      { ...supabase, all: true, log: { warn: (msg, info) => console.log("  ", msg, info?.error || "") } }
    );

    if (!urls) throw new Error("niet elke foto lukte, oude foto's blijven staan");

    const added = await graphql(ADD, {
      productId: product.id,
      media: urls.map((url) => ({ originalSource: url, mediaContentType: "IMAGE", alt: product.title }))
    });

    const errors = added.productCreateMedia.mediaUserErrors;
    if (errors.length) throw new Error(errors.map((e) => e.message).join("; "));

    const newIds = added.productCreateMedia.media.map((media) => media.id);
    const state = await settled(product.id, newIds);

    if (state !== "READY") {
      await graphql(REMOVE, { productId: product.id, mediaIds: newIds });
      throw new Error(`Shopify verwerkte de nieuwe foto's niet (${state}), teruggedraaid`);
    }

    const removed = await graphql(REMOVE, { productId: product.id, mediaIds: product.images.map((image) => image.id) });
    const removeErrors = removed.productDeleteMedia.mediaUserErrors;
    if (removeErrors.length) throw new Error("oude foto's: " + removeErrors.map((e) => e.message).join("; "));

    const marked = await graphql(MARK, {
      metafields: [{ ownerId: product.id, namespace: "lojiq", key: "photo_style", type: "single_line_text_field", value: STYLE }]
    });
    if (marked.metafieldsSet.userErrors.length) throw new Error(marked.metafieldsSet.userErrors[0].message);

    result.done += 1;
    console.log(`ok   ${label} (${urls.length})`);
  } catch (error) {
    result.failed.push({ sku: product.sku, title: product.title, error: error.message });
    console.log(`FOUT ${label}: ${error.message}`);
  }
}

console.log(`\nklaar: ${result.done} gedaan, ${result.failed.length} mislukt`);
fs.writeFileSync(path.join(here, `normalize-${CLIENT}-result.json`), JSON.stringify(result, null, 2));
