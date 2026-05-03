import express from "express";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";
const AIRTABLE_MERCHANTS_TABLE_NAME =
  process.env.AIRTABLE_MERCHANTS_TABLE_NAME || "Merchants";

const AIRTABLE_MERCHANT_ACTIVE_FIELD =
  process.env.AIRTABLE_MERCHANT_ACTIVE_FIELD || "Active?";

const AIRTABLE_SHOPIFY_URL_FIELD =
  process.env.AIRTABLE_SHOPIFY_URL_FIELD || "Shopify URL";

const AIRTABLE_SHOPIFY_TOKEN_FIELD =
  process.env.AIRTABLE_SHOPIFY_TOKEN_FIELD || "Shopify Token";

const SHOPIFY_API_VERSION = process.env.SHOPIFY_API_VERSION || "2026-01";
const MAKE_WEBHOOK_URL = process.env.MAKE_WEBHOOK_URL || "";
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "100", 10);

function assertEnv() {
  const missing = [];

  if (!AIRTABLE_TOKEN) missing.push("AIRTABLE_TOKEN");
  if (!AIRTABLE_BASE_ID) missing.push("AIRTABLE_BASE_ID");
  if (!MAKE_WEBHOOK_URL) missing.push("MAKE_WEBHOOK_URL");

  if (missing.length > 0) {
    throw new Error(`Missing environment variables: ${missing.join(", ")}`);
  }
}

function createSyncId() {
  return `sync_${new Date().toISOString()}`;
}

function normalizeShopifyDomain(value) {
  return String(value || "")
    .trim()
    .replace(/^https?:\/\//, "")
    .replace(/\/$/, "");
}

function getNumericProductId(gid) {
  return String(gid).split("/").pop();
}

function chunkArray(array, size) {
  const chunks = [];

  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }

  return chunks;
}

async function sendToMake(payload) {
  console.log("Sending to Make:", {
    event: payload.event,
    syncId: payload.syncId,
    merchantRecordId: payload.merchantRecordId,
    shopifyToken: merchant.shopifyToken,
    batchSize: payload.products?.length || 0
  });

  const response = await fetch(MAKE_WEBHOOK_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  const responseText = await response.text();

  console.log("Make response:", {
    status: response.status,
    body: responseText.slice(0, 500)
  });

  if (!response.ok) {
    throw new Error(`Make webhook failed: ${response.status} ${responseText}`);
  }
}

async function fetchActiveMerchants() {
  const table = encodeURIComponent(AIRTABLE_MERCHANTS_TABLE_NAME);
  const formula = encodeURIComponent(`{${AIRTABLE_MERCHANT_ACTIVE_FIELD}} = 1`);

  let url =
    `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${table}` +
    `?filterByFormula=${formula}`;

  const merchants = [];

  while (url) {
    const response = await fetch(url, {
      headers: {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`
      }
    });

    const data = await response.json();

    if (!response.ok) {
      throw new Error(`Airtable error: ${JSON.stringify(data)}`);
    }

    for (const record of data.records || []) {
      const fields = record.fields || {};

      const rawShopifyUrl = fields[AIRTABLE_SHOPIFY_URL_FIELD];
      const shopifyToken = fields[AIRTABLE_SHOPIFY_TOKEN_FIELD];

      const shopifyDomain = normalizeShopifyDomain(rawShopifyUrl);

      if (!shopifyDomain || !shopifyToken) {
        console.warn("Skipping merchant missing Shopify URL or token:", {
          recordId: record.id,
          rawShopifyUrl,
          hasToken: Boolean(shopifyToken)
        });
        continue;
      }

      merchants.push({
        recordId: record.id,
        name: fields.Name || fields["Name"] || record.id,
        shopifyDomain,
        shopifyToken
      });
    }

    if (data.offset) {
      url =
        `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${table}` +
        `?filterByFormula=${formula}&offset=${encodeURIComponent(data.offset)}`;
    } else {
      url = null;
    }
  }

  return merchants;
}

async function shopifyGraphQL({ shopifyDomain, shopifyToken }, query, variables = {}) {
  const url = `https://${shopifyDomain}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": shopifyToken
    },
    body: JSON.stringify({ query, variables })
  });

  const text = await response.text();

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`Shopify non-JSON response from ${shopifyDomain}: ${text.slice(0, 500)}`);
  }

  if (!response.ok || data.errors) {
    throw new Error(
      `Shopify error for ${shopifyDomain}: ${JSON.stringify(data.errors || data)}`
    );
  }

  return data;
}

async function syncMerchant(merchant, runId) {
  const syncId = `${runId}_${merchant.recordId}`;

  await sendToMake({
    event: "sync_started",
    syncId,
    runId,
    sentAt: new Date().toISOString(),
    merchantRecordId: merchant.recordId,
    merchantName: merchant.name,
    shopifyDomain: merchant.shopifyDomain,
    shopifyToken: merchant.shopifyToken
  });

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
  let hasNextPage = true;
  let page = 0;
  let totalProductsSeen = 0;
  let totalActiveProducts = 0;
  let totalBatchesSent = 0;

  while (hasNextPage) {
    page += 1;

    const result = await shopifyGraphQL(merchant, query, { cursor });
    const connection = result.data.products;

    const activeProducts = connection.edges
      .map((edge) => edge.node)
      .filter((product) => product.status === "ACTIVE")
      .map((product) => ({
        id: product.id,
        shopifyProductId: getNumericProductId(product.id)
      }));

    totalProductsSeen += connection.edges.length;
    totalActiveProducts += activeProducts.length;

    const batches = chunkArray(activeProducts, BATCH_SIZE);

    for (let i = 0; i < batches.length; i += 1) {
      totalBatchesSent += 1;

      await sendToMake({
        event: "product_batch",
        syncId,
        runId,
        sentAt: new Date().toISOString(),
        merchantRecordId: merchant.recordId,
        merchantName: merchant.name,
        shopifyDomain: merchant.shopifyDomain,
        shopifyToken: merchant.shopifyToken,
        page,
        batchInPage: i + 1,
        batchesInPage: batches.length,
        products: batches[i]
      });
    }

    hasNextPage = connection.pageInfo.hasNextPage;
    cursor = connection.pageInfo.endCursor;
  }

  await sendToMake({
    event: "sync_completed",
    syncId,
    runId,
    sentAt: new Date().toISOString(),
    merchantRecordId: merchant.recordId,
    merchantName: merchant.name,
    shopifyDomain: merchant.shopifyDomain,
    shopifyToken: merchant.shopifyToken,
    totalProductsSeen,
    totalActiveProducts,
    totalBatchesSent
  });

  return {
    merchantRecordId: merchant.recordId,
    merchantName: merchant.name,
    shopifyDomain: merchant.shopifyDomain,
    syncId,
    totalProductsSeen,
    totalActiveProducts,
    totalBatchesSent
  };
}

async function syncAllMerchants() {
  assertEnv();

  const runId = createSyncId();
  const merchants = await fetchActiveMerchants();

  console.log(`Found ${merchants.length} active merchants`);

  const results = [];

  for (const merchant of merchants) {
    console.log("Syncing merchant:", {
      recordId: merchant.recordId,
      name: merchant.name,
      shopifyDomain: merchant.shopifyDomain
    });

    const result = await syncMerchant(merchant, runId);
    results.push(result);
  }

  return {
    runId,
    merchantsSynced: results.length,
    results
  };
}

app.get("/", (_req, res) => {
  res.json({
    success: true,
    message: "Multi-merchant Shopify Active Product Sync is running"
  });
});

app.get("/health", (_req, res) => {
  res.json({
    success: true,
    status: "ok"
  });
});

app.get("/test-merchants", async (_req, res) => {
  try {
    assertEnv();

    const merchants = await fetchActiveMerchants();

    res.json({
      success: true,
      count: merchants.length,
      merchants: merchants.map((merchant) => ({
        recordId: merchant.recordId,
        name: merchant.name,
        shopifyDomain: merchant.shopifyDomain,
        hasToken: Boolean(merchant.shopifyToken)
      }))
    });
  } catch (error) {
    console.error("Test merchants error:", error);

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.get("/run-test-full", async (_req, res) => {
  try {
    assertEnv();

    const merchants = await fetchActiveMerchants();

    if (!merchants.length) {
      return res.json({
        success: false,
        message: "No active merchants found"
      });
    }

    const merchant = merchants[0];
    const runId = createSyncId();
    const syncId = `${runId}_${merchant.recordId}`;

    await sendToMake({
      event: "sync_started",
      syncId,
      runId,
      sentAt: new Date().toISOString(),
      test: true,
      merchantRecordId: merchant.recordId,
      merchantName: merchant.name,
      shopifyDomain: merchant.shopifyDomain
    });

    const query = `
      query GetProducts {
        products(first: 5) {
          edges {
            node {
              id
              status
            }
          }
        }
      }
    `;

    const result = await shopifyGraphQL(merchant, query);

    const activeProducts = result.data.products.edges
      .map((edge) => edge.node)
      .filter((product) => product.status === "ACTIVE")
      .map((product) => ({
        id: product.id,
        shopifyProductId: getNumericProductId(product.id)
      }));

    await sendToMake({
      event: "product_batch",
      syncId,
      runId,
      sentAt: new Date().toISOString(),
      test: true,
      merchantRecordId: merchant.recordId,
      merchantName: merchant.name,
      shopifyDomain: merchant.shopifyDomain,
      page: 1,
      batchInPage: 1,
      batchesInPage: 1,
      products: activeProducts
    });

    await sendToMake({
      event: "sync_completed",
      syncId,
      runId,
      sentAt: new Date().toISOString(),
      test: true,
      merchantRecordId: merchant.recordId,
      merchantName: merchant.name,
      shopifyDomain: merchant.shopifyDomain,
      totalProductsSeen: result.data.products.edges.length,
      totalActiveProducts: activeProducts.length,
      totalBatchesSent: 1
    });

    res.json({
      success: true,
      message: "Mini full sync for first active merchant sent to Make",
      merchantRecordId: merchant.recordId,
      merchantName: merchant.name,
      syncId,
      count: activeProducts.length
    });
  } catch (error) {
    console.error("Mini full sync error:", error);

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.get("/run", async (_req, res) => {
  try {
    const result = await syncAllMerchants();

    res.json({
      success: true,
      message: "Multi-merchant sync completed successfully",
      ...result
    });
  } catch (error) {
    console.error("Sync error:", error);

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.post("/run", async (_req, res) => {
  try {
    const result = await syncAllMerchants();

    res.json({
      success: true,
      message: "Multi-merchant sync completed successfully",
      ...result
    });
  } catch (error) {
    console.error("Sync error:", error);

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
