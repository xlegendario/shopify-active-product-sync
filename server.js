import express from "express";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;
const SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN; // bv. jouwshop.myshopify.com
const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const SHOPIFY_API_VERSION = process.env.SHOPIFY_API_VERSION || "2026-01";
const MAKE_WEBHOOK_URL = process.env.MAKE_WEBHOOK_URL;
const BATCH_SIZE = Number(process.env.BATCH_SIZE || 100);

function assertEnv() {
  const missing = [];
  if (!SHOPIFY_STORE_DOMAIN) missing.push("SHOPIFY_STORE_DOMAIN");
  if (!SHOPIFY_ACCESS_TOKEN) missing.push("SHOPIFY_ACCESS_TOKEN");
  if (!MAKE_WEBHOOK_URL) missing.push("MAKE_WEBHOOK_URL");
  if (missing.length) {
    throw new Error(`Missing env vars: ${missing.join(", ")}`);
  }
}

async function shopifyGraphQL(query, variables = {}) {
  const url = `https://${SHOPIFY_STORE_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
    },
    body: JSON.stringify({ query, variables })
  });

  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`Shopify returned non-JSON (${res.status}): ${text.slice(0, 500)}`);
  }

  if (!res.ok) {
    throw new Error(`Shopify HTTP ${res.status}: ${JSON.stringify(json).slice(0, 1000)}`);
  }

  if (json.errors?.length) {
    throw new Error(`Shopify GraphQL errors: ${JSON.stringify(json.errors)}`);
  }

  return json;
}

async function postBatchToMake(productIds, meta = {}) {
  const payload = {
    source: "render-shopify-sync",
    sentAt: new Date().toISOString(),
    batchSize: productIds.length,
    ...meta,
    products: productIds.map((id) => ({ id }))
  };

  const res = await fetch(MAKE_WEBHOOK_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  const body = await res.text();
  if (!res.ok) {
    throw new Error(`Make webhook failed (${res.status}): ${body.slice(0, 1000)}`);
  }

  return body;
}

async function* fetchActiveProductIds() {
  const query = `
    query GetProducts($cursor: String) {
      products(first: 250, after: $cursor) {
        edges {
          node {
            id
            status
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
  `;

  let cursor = null;
  let page = 0;

  while (true) {
    page += 1;
    const json = await shopifyGraphQL(query, { cursor });
    const products = json.data?.products;

    if (!products) {
      throw new Error(`Unexpected Shopify response: ${JSON.stringify(json).slice(0, 1000)}`);
    }

    const activeIds = products.edges
      .map((edge) => edge?.node)
      .filter((node) => node && node.status === "ACTIVE")
      .map((node) => node.id);

    yield {
      page,
      activeIds,
      hasNextPage: products.pageInfo.hasNextPage,
      endCursor: products.pageInfo.endCursor
    };

    if (!products.pageInfo.hasNextPage) break;
    cursor = products.pageInfo.endCursor;
  }
}

function chunk(array, size) {
  const out = [];
  for (let i = 0; i < array.length; i += size) {
    out.push(array.slice(i, i + size));
  }
  return out;
}

async function runSync() {
  assertEnv();

  let totalActive = 0;
  let totalSent = 0;
  let pages = 0;
  let batches = 0;

  for await (const pageResult of fetchActiveProductIds()) {
    pages += 1;
    totalActive += pageResult.activeIds.length;

    const chunks = chunk(pageResult.activeIds, BATCH_SIZE);

    for (let i = 0; i < chunks.length; i += 1) {
      const ids = chunks[i];
      if (!ids.length) continue;

      batches += 1;
      await postBatchToMake(ids, {
        page: pageResult.page,
        batchInPage: i + 1,
        hasNextPage: pageResult.hasNextPage
      });
      totalSent += ids.length;
    }
  }

  return {
    ok: true,
    pages,
    batches,
    totalActive,
    totalSent
  };
}

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    service: "shopify-active-product-sync"
  });
});

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.post("/run", async (_req, res) => {
  try {
    const result = await runSync();
    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).json({
      ok: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

app.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
});
