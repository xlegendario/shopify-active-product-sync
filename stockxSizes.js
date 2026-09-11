/*
 * Which European sizes a pair exists in.
 *
 * Only for building the ladder a product page is created with. Prices,
 * stock and everything else come from our own records; this answers one
 * question and no other.
 *
 * StockX rather than Retailed. Retailed's search gives the product but not
 * its variants, and it is being wound back to what it is good at, which is
 * pictures.
 *
 * The token lives in Airtable, shared with the portal, because both refresh
 * the same one and StockX is happy to have several alive at once. It expires
 * within the day, so a 401 is expected rather than exceptional: refresh once,
 * try again, and only then give up.
 */

const SEARCH = "https://api.stockx.com/v2/catalog/search";
const PRODUCTS = "https://api.stockx.com/v2/catalog/products";
const TOKEN_URL = "https://accounts.stockx.com/oauth/token";

function airtableUrl(table, params = {}) {
  const url = new URL(
    `https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${encodeURIComponent(table)}`
  );

  Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, value));

  return url;
}

const TOKEN_TABLE = process.env.AIRTABLE_STOCKX_ACCESS_TOKEN_TABLE || "StockX Access Token";

async function readStoredToken() {
  const res = await fetch(airtableUrl(TOKEN_TABLE, { maxRecords: "1" }), {
    headers: { Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}` }
  });

  const data = await res.json();
  const record = data?.records?.[0];

  if (!record) throw new Error("Geen StockX Access Token in Airtable");

  return { recordId: record.id, token: String(record.fields?.["Access Token"] || "").trim() };
}

async function refreshToken(recordId) {
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    client_id: process.env.STOCKX_CLIENT_ID,
    client_secret: process.env.STOCKX_CLIENT_SECRET,
    audience: "gateway.stockx.com",
    refresh_token: process.env.STOCKX_REFRESH_TOKEN
  });

  const res = await fetch(TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body
  });

  const data = await res.json().catch(() => ({}));

  if (!res.ok || !data.access_token) {
    throw new Error(`StockX gaf geen nieuw token: ${res.status}`);
  }

  // Written back so the portal does not have to discover the same expiry.
  await fetch(airtableUrl(TOKEN_TABLE) + `/${recordId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      fields: { "Access Token": data.access_token, "Refreshed At": new Date().toISOString() }
    })
  }).catch(() => {});

  return data.access_token;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/*
 * StockX allows a burst and then says no.
 *
 * Measured: about eight requests in a row and the ninth comes back 429. Since
 * a ladder costs two requests, that is four pairs before it stops - and the
 * first run for a store this size wants a thousand.
 *
 * Two things keep it civil. A gap between requests, so the burst never
 * builds. And a wait when it happens anyway, because another process shares
 * this quota and no gap of ours can account for that.
 */
const MIN_GAP_MS = Number(process.env.STOCKX_MIN_GAP_MS || 350);
const RETRY_WAITS_MS = [2000, 5000, 15000];

export function createStockxClient() {
  let stored = null;
  let lastCallAt = 0;

  async function call(url) {
    if (!stored) stored = await readStoredToken();

    const since = Date.now() - lastCallAt;

    if (since < MIN_GAP_MS) await sleep(MIN_GAP_MS - since);

    lastCallAt = Date.now();

    const attempt = (token) =>
      fetch(url, {
        headers: {
          Authorization: `Bearer ${token}`,
          "x-api-key": process.env.STOCKX_API_KEY,
          Accept: "application/json"
        }
      });

    let res = await attempt(stored.token);

    /*
      A 401 here almost always means the token aged out, not that anything is
      wrong with the account. StockX words it as an authorisation denial,
      which reads alarming and sent me looking in the wrong place once - so
      the refresh comes first and the alarm only if that fails too.
    */
    if (res.status === 401 || res.status === 403) {
      stored.token = await refreshToken(stored.recordId);
      res = await attempt(stored.token);
    }

    // Backing off rather than failing: a 429 is a "later", not a "no".
    for (const wait of RETRY_WAITS_MS) {
      if (res.status !== 429) break;

      await sleep(wait);
      lastCallAt = Date.now();
      res = await attempt(stored.token);
    }

    if (!res.ok) {
      throw new Error(`StockX ${res.status} op ${url}`);
    }

    return res.json();
  }

  return { call };
}

const euOf = (variant) =>
  (variant?.sizeChart?.availableConversions || [])
    .find((entry) => String(entry.type).toLowerCase() === "eu")?.size || null;

/*
 * The ladder for one style code, as bare sizes.
 *
 * "EU 44 2/3" comes back as "44 2/3", because that is how our own stock
 * writes it and the two have to key against each other.
 *
 * Nothing found is not an error. A pair StockX has never seen still gets a
 * page, built from the sizes we actually hold.
 */
export async function euSizesForSku(client, sku) {
  const clean = String(sku || "").trim().toUpperCase();

  if (!clean) return [];

  const search = await client.call(`${SEARCH}?query=${encodeURIComponent(clean)}`);
  const products = search?.products || [];

  const hit =
    products.find((product) => String(product.styleId || "").toUpperCase() === clean) || null;

  if (!hit) return [];

  const variants = await client.call(`${PRODUCTS}/${hit.productId}/variants`);

  if (!Array.isArray(variants)) return [];

  return variants
    .map(euOf)
    .filter(Boolean)
    .map((size) => size.replace(/^EU\s*/i, "").trim());
}
