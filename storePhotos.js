/*
 * Product photographs, borrowed from the merchants who shoot their own stock.
 *
 * A page in our own shop needs real photographs, several of them, from more
 * than one angle. SKU Master holds a StockX thumbnail: a hundred and forty
 * pixels, three kilobytes, and asking for it larger makes it bigger rather
 * than sharper.
 *
 * We do not photograph consignment stock, because it never physically passes
 * through us. Several merchants do, on Shopify stores this service already
 * holds credentials for, and their catalogues overlap ours almost completely.
 *
 * CHANGED - this read whole catalogues and indexed them by style code.
 *
 * The reason was that Shopify's REST products endpoint cannot search on SKU,
 * so one pass of everything beat a query per pair. It works, and it is
 * miserable: SOSU alone runs past fifteen thousand products and ALC past
 * twelve, and a pass takes minutes before a single photograph is used.
 *
 * GraphQL can filter on SKU, which REST cannot, and answers one style code in
 * about two hundred milliseconds. So the catalogue pass is gone and this asks
 * per style code, in order of preference, stopping at the first store that
 * has it.
 *
 * What it finds is kept in storefront_library. Not because the create path needs
 * it - a product is created once and Shopify copies the image into the
 * store's own files, so the source is never needed again - but because a
 * first run over thousands of pairs will be interrupted at some point, and
 * starting over should not mean asking eight thousand times again.
 *
 * Deliberately NOT in store_listings. That table mirrors what a store has,
 * and this is ours; and keyed per variant it would repeat the same handful of
 * URLs on every size of every shoe.
 */

const API_VERSION = process.env.SHOPIFY_API_VERSION || "2026-01";

/*
 * One row per style code, holding everything a storefront has to say about
 * the shoe. Photographs today; a description belongs in the same row, which
 * is why the table is not called after the pictures.
 */
const LIBRARY_TABLE = "storefront_library";

/*
 * How long "asked and nobody had it" stands before it is worth asking again.
 *
 * CHANGED from a month, which was a guess made before there were any numbers.
 * The numbers say a day: of 724 style codes with stock, 27 came back without
 * a photograph, and asking 27 again costs seven seconds. A month meant a pair
 * that one of the source stores started carrying yesterday stayed blank for
 * weeks.
 *
 * It lengthens again if that pile grows. Five hundred unknowns asked daily is
 * two minutes a run, and then a week is the better trade.
 */
const RETRY_UNKNOWN_AFTER_DAYS = Number(process.env.PHOTO_RETRY_DAYS || 1);

// Shopify takes far more, but eight is plenty and keeps a page quick.
const MAX_PHOTOS = Number(process.env.PHOTO_MAX || 8);

const BY_SKU = `
  query photosBySku($q: String!) {
    products(first: 3, query: $q) {
      nodes {
        title
        images(first: 20) { nodes { url width height } }
        variants(first: 1) { nodes { sku } }
      }
    }
  }
`;

export function normalizeSku(sku) {
  return String(sku || "").trim().toUpperCase().replace(/\s+/g, "");
}

/*
 * The stores to ask, in the order to ask them.
 *
 * The order IS the preference, and it is strict: the first store that has the
 * pair keeps it, however many photographs a later one has. Every shop shoots
 * on its own background, at its own angle, with its own crop, so a shelf
 * filled from whoever had the most pictures looks like a jumble sale.
 *
 * A setting rather than a decision in code. SOSU first because they are the
 * most consistent; ALC carries files plainly named STOCKX, which is the thing
 * we are trying to get away from.
 */
export function photoSourceNames() {
  return String(
    process.env.PHOTO_SOURCE_MERCHANTS || "SOSU KICKS S.R.L.,ALC SELECT STORE SL,SneakerAsk"
  )
    .split(",")
    .map((name) => name.trim())
    .filter(Boolean);
}

export function photoSources(merchants) {
  return photoSourceNames()
    .map((name) => {
      const merchant = (merchants || []).find(
        (entry) => String(entry.name || "").trim().toLowerCase() === name.toLowerCase()
      );

      if (!merchant?.storeUrl || !merchant?.token) return null;

      return {
        name,
        host: String(merchant.storeUrl).replace(/^https?:\/\//, "").replace(/\/$/, ""),
        token: merchant.token
      };
    })
    .filter(Boolean);
}

async function askStore(source, sku) {
  const res = await fetch(`https://${source.host}/admin/api/${API_VERSION}/graphql.json`, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": source.token,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ query: BY_SKU, variables: { q: `sku:${sku}` } })
  });

  if (!res.ok) throw new Error(`${source.name}: ${res.status}`);

  const data = await res.json();

  if (data.errors) throw new Error(`${source.name}: ${JSON.stringify(data.errors).slice(0, 120)}`);

  const nodes = data?.data?.products?.nodes || [];

  /*
    The style code has to match, not merely be found. Shopify's search is
    forgiving, and a partial hit on another pair would put the wrong shoe on
    the page - the one mistake here nobody catches by eye.
  */
  const hit = nodes.find((product) =>
    (product.variants?.nodes || []).some((variant) => normalizeSku(variant.sku) === sku)
  );

  if (!hit) return null;

  const images = (hit.images?.nodes || [])
    .map((image) => String(image.url || ""))
    .filter(Boolean)
    .slice(0, MAX_PHOTOS);

  return images.length ? { images, title: hit.title || "", source: source.name } : null;
}

/* ------------------------------------------------------------------ *
 * What we already know
 * ------------------------------------------------------------------ */

function supabaseUrl(pathPart, params = {}) {
  const url = new URL(`${process.env.SUPABASE_URL}/rest/v1/${pathPart}`);

  Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, value));

  return url;
}

const supabaseHeaders = () => ({
  apikey: process.env.SUPABASE_SERVICE_ROLE_KEY,
  Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`,
  "Content-Type": "application/json"
});

export async function readKnownPhotos(skus) {
  const wanted = [...new Set((skus || []).map(normalizeSku))].filter(Boolean);
  const known = new Map();

  // In batches, because a URL has a length and thousands of style codes do
  // not fit in one.
  for (let i = 0; i < wanted.length; i += 200) {
    const batch = wanted.slice(i, i + 200);

    const res = await fetch(
      supabaseUrl(LIBRARY_TABLE, {
        select: "style_code,images,source_store,checked_at",
        style_code: `in.(${batch.map((code) => `"${code}"`).join(",")})`
      }),
      { headers: supabaseHeaders() }
    );

    const rows = await res.json();

    if (!Array.isArray(rows)) {
      throw new Error(`storefront_library lezen: ${JSON.stringify(rows).slice(0, 150)}`);
    }

    for (const row of rows) {
      known.set(row.style_code, {
        images: Array.isArray(row.images) ? row.images : [],
        source: row.source_store || null,
        checkedAt: row.checked_at
      });
    }
  }

  return known;
}

async function remember(sku, found) {
  await fetch(supabaseUrl(LIBRARY_TABLE, { on_conflict: "style_code" }), {
    method: "POST",
    headers: { ...supabaseHeaders(), Prefer: "resolution=merge-duplicates" },
    body: JSON.stringify([
      {
        style_code: sku,
        images: found?.images || [],
        source_store: found?.source || null,
        checked_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      }
    ])
  });
}

function worthAsking(entry) {
  if (!entry) return true;
  if (entry.images.length) return false;
  if (!entry.checkedAt) return true;

  const age = Date.now() - new Date(entry.checkedAt).getTime();

  return age > RETRY_UNKNOWN_AFTER_DAYS * 24 * 60 * 60 * 1000;
}

/*
 * Photographs for a list of style codes, asking only for what is missing.
 *
 * The sources are tried in order and the first hit wins, so a pair SOSU
 * carries never gets ALC's version of it.
 */
export async function photosForSkus(skus, { sources = [], onProgress = () => {} } = {}) {
  const wanted = [...new Set((skus || []).map(normalizeSku))].filter(Boolean);
  const known = await readKnownPhotos(wanted);

  const photos = new Map();

  let asked = 0;

  for (const sku of wanted) {
    const entry = known.get(sku);

    if (!worthAsking(entry)) {
      photos.set(sku, entry.images);
      continue;
    }

    if (!sources.length) {
      photos.set(sku, entry?.images || []);
      continue;
    }

    let found = null;

    for (const source of sources) {
      try {
        found = await askStore(source, sku);
      } catch (err) {
        onProgress({ sku, error: `${source.name}: ${err.message}` });
        continue;
      }

      if (found) break;
    }

    photos.set(sku, found?.images || []);
    await remember(sku, found);

    asked += 1;

    onProgress({
      sku,
      images: found?.images.length || 0,
      source: found?.source || null,
      asked,
      total: wanted.length
    });
  }

  return { photos, asked, known: wanted.length - asked };
}

export function photosFor(photos, sku) {
  return photos.get(normalizeSku(sku)) || [];
}
