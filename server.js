import express from "express";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";

const AIRTABLE_MERCHANTS_TABLE_NAME =
  process.env.AIRTABLE_MERCHANTS_TABLE_NAME || "Merchants";

const AIRTABLE_STORE_LISTINGS_TABLE_NAME =
  process.env.AIRTABLE_STORE_LISTINGS_TABLE_NAME || "Store Listings";

const AIRTABLE_MERCHANT_ACTIVE_FIELD =
  process.env.AIRTABLE_MERCHANT_ACTIVE_FIELD || "Test Active?";

const AIRTABLE_SHOPIFY_URL_FIELD =
  process.env.AIRTABLE_SHOPIFY_URL_FIELD || "Shopify Store URL";

const AIRTABLE_SHOPIFY_TOKEN_FIELD =
  process.env.AIRTABLE_SHOPIFY_TOKEN_FIELD || "Shopify Token";

const SHOPIFY_API_VERSION = process.env.SHOPIFY_API_VERSION || "2026-01";
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "100", 10);

const RETAILED_API_BASE =
  process.env.RETAILED_API_BASE || "https://app.retailed.io/api/v1/scraper/stockx/search";

const RETAILED_API_KEY = process.env.RETAILED_API_KEY || "";
const HTTP_TIMEOUT_MS = parseInt(process.env.HTTP_TIMEOUT_MS || "15000", 10);

function assertEnv() {
  const missing = [];

  if (!AIRTABLE_TOKEN) missing.push("AIRTABLE_TOKEN");
  if (!AIRTABLE_BASE_ID) missing.push("AIRTABLE_BASE_ID");

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

function getNumericId(gid) {
  return String(gid || "").split("/").pop();
}

function airtableEscape(value) {
  return String(value || "").replace(/'/g, "\\'");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(url, options = {}, retries = 2) {
  let lastError;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), HTTP_TIMEOUT_MS);

    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal
      });

      clearTimeout(timeout);

      if (
        response.status === 429 ||
        response.status >= 500
      ) {
        const text = await response.text();
        throw new Error(`HTTP ${response.status}: ${text.slice(0, 500)}`);
      }

      return response;
    } catch (error) {
      clearTimeout(timeout);
      lastError = error;

      if (attempt < retries) {
        const delay = [1000, 3000, 7000][attempt] || 7000;
        console.warn(`Retrying request in ${delay}ms`, {
          url,
          attempt: attempt + 1,
          error: error.message
        });
        await sleep(delay);
      }
    }
  }

  throw lastError;
}

async function airtableRequest(path, options = {}) {
  const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${path}`;

  const response = await fetchWithRetry(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  }, 3);

  const data = await response.json();

  if (!response.ok) {
    throw new Error(`Airtable error: ${JSON.stringify(data)}`);
  }

  return data;
}

async function fetchAllAirtableRecords(tableName, filterByFormula = "") {
  const table = encodeURIComponent(tableName);
  const records = [];

  let urlPath = table;
  const params = new URLSearchParams();

  if (filterByFormula) {
    params.set("filterByFormula", filterByFormula);
  }

  let offset = null;

  do {
    const pageParams = new URLSearchParams(params);

    if (offset) {
      pageParams.set("offset", offset);
    }

    const path = `${urlPath}?${pageParams.toString()}`;
    const data = await airtableRequest(path);

    records.push(...(data.records || []));
    offset = data.offset || null;
  } while (offset);

  return records;
}

async function updateAirtableRecord(tableName, recordId, fields) {
  const table = encodeURIComponent(tableName);

  return airtableRequest(`${table}/${recordId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields })
  });
}

async function createAirtableRecord(tableName, fields) {
  const table = encodeURIComponent(tableName);

  return airtableRequest(table, {
    method: "POST",
    body: JSON.stringify({ fields })
  });
}

async function fetchActiveMerchants() {
  const formula = `{${AIRTABLE_MERCHANT_ACTIVE_FIELD}} = 1`;
  const records = await fetchAllAirtableRecords(AIRTABLE_MERCHANTS_TABLE_NAME, formula);

  const merchants = [];

  for (const record of records) {
    const fields = record.fields || {};

    const rawShopifyUrl = fields[AIRTABLE_SHOPIFY_URL_FIELD];
    const shopifyToken = fields[AIRTABLE_SHOPIFY_TOKEN_FIELD];
    const shopifyDomain = normalizeShopifyDomain(rawShopifyUrl);

    if (!shopifyDomain || !shopifyToken) {
      console.warn("Skipping merchant missing Shopify URL/token", {
        recordId: record.id,
        rawShopifyUrl,
        hasToken: Boolean(shopifyToken)
      });
      continue;
    }

    merchants.push({
      recordId: record.id,
      name: fields.Name || record.id,
      shopifyDomain,
      shopifyToken
    });
  }

  return merchants;
}

async function shopifyGraphQL(merchant, query, variables = {}) {
  const url = `https://${merchant.shopifyDomain}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

  const response = await fetchWithRetry(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": merchant.shopifyToken
    },
    body: JSON.stringify({ query, variables })
  }, 3);

  const text = await response.text();

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`Shopify non-JSON response from ${merchant.shopifyDomain}: ${text.slice(0, 500)}`);
  }

  if (!response.ok || data.errors) {
    throw new Error(`Shopify error for ${merchant.shopifyDomain}: ${JSON.stringify(data.errors || data)}`);
  }

  return data;
}

async function fetchActiveProducts(merchant) {
  const query = `
    query GetProducts($cursor: String) {
      products(first: 250, after: $cursor) {
        edges {
          node {
            id
            legacyResourceId
            title
            handle
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
  const products = [];

  while (hasNextPage) {
    const result = await shopifyGraphQL(merchant, query, { cursor });
    const connection = result.data.products;

    for (const edge of connection.edges) {
      if (edge.node.status === "ACTIVE") {
        products.push(edge.node);
      }
    }

    hasNextPage = connection.pageInfo.hasNextPage;
    cursor = connection.pageInfo.endCursor;
  }

  return products;
}

async function fetchProductVariants(merchant, productGid) {
  const query = `
    query GetProductVariants($id: ID!) {
      product(id: $id) {
        id
        legacyResourceId
        title
        handle
        status
        variants(first: 250) {
          edges {
            node {
              id
              legacyResourceId
              title
              sku
              price
              inventoryQuantity
              inventoryItem {
                id
                legacyResourceId
              }
              selectedOptions {
                name
                value
              }
            }
          }
        }
      }
    }
  `;

  const result = await shopifyGraphQL(merchant, query, { id: productGid });
  return result.data.product;
}

async function searchRetailed(query) {
  if (!query) return null;

  const url = `${RETAILED_API_BASE}?query=${encodeURIComponent(query)}`;

  try {
    const headers = {
      "x-api-key": RETAILED_API_KEY
    };

    console.log("Retailed lookup:", { query, url, hasApiKey: Boolean(RETAILED_API_KEY) });

    const response = await fetchWithRetry(url, {
      method: "GET",
      headers
    }, 2);

    const data = await response.json();

    console.log("Retailed response preview:", JSON.stringify(data).slice(0, 500));

    if (!response.ok) {
      throw new Error(`Retailed error: ${JSON.stringify(data)}`);
    }

    const first =
      (Array.isArray(data) && data[0]) ||
      data?.data?.[0] ||
      data?.results?.[0] ||
      data?.products?.[0] ||
      null;

    if (!first) return null;

    return {
      name: first.name || "",
      colorway: first.colorway || "",
      brand: first.brand || "",
      image: first.image || ""
    };
  } catch (error) {
    console.warn("Retailed lookup failed, continuing without Retailed data", {
      query,
      error: error.message
    });

    return null;
  }
}

function buildStockxName(retailed) {
  if (!retailed) return "";

  return [retailed.name, retailed.colorway]
    .filter(Boolean)
    .join(" ")
    .trim();
}

async function findStoreListing({ merchantRecordId, productId, variantId }) {
  const formula = `AND(
    {Merchant Record ID} = '${airtableEscape(merchantRecordId)}',
    {Shopify Product ID} = '${airtableEscape(productId)}',
    {Shopify Variant ID} = '${airtableEscape(variantId)}'
  )`;

  const records = await fetchAllAirtableRecords(AIRTABLE_STORE_LISTINGS_TABLE_NAME, formula);
  return records[0] || null;
}

async function upsertStoreListing({ merchant, syncId, product, variant, retailed, retailedStatus, productSku }) {
  const now = new Date().toISOString();

  const productId = String(product.legacyResourceId || getNumericId(product.id));
  const variantId = String(variant.legacyResourceId || getNumericId(variant.id));
  const inventoryItemId = String(
    variant.inventoryItem?.legacyResourceId || getNumericId(variant.inventoryItem?.id)
  );

  const stockxProductName = buildStockxName(retailed);

  const fields = {
    "Client": [merchant.recordId],
    "Merchant Record ID": merchant.recordId,
  
    "Shopify Product ID": productId,
    "Shopify Variant ID": variantId,
    "Shopify Inventory Item ID": inventoryItemId,
  
    "StockX Product Name": stockxProductName,
    "Shopify Product Name": product.title || "",
    "Size": variant.title || "",
    "SKU": productSku || "",
  
    "Brand": retailed?.brand || "",
    "Retailed Status": retailedStatus,
    "Last Seen Sync ID": syncId,
    "Last Shopify Sync At": now,
    "Status": "active"
  };

  if (retailed?.image) {
    fields.Picture = [
      {
        url: retailed.image,
        filename: `${stockxProductName || product.title || variantId}.webp`
      }
    ];
  }

  const existing = await findStoreListing({
    merchantRecordId: merchant.recordId,
    productId,
    variantId
  });

  if (existing) {
    await updateAirtableRecord(AIRTABLE_STORE_LISTINGS_TABLE_NAME, existing.id, fields);
    return { action: "updated", recordId: existing.id };
  }

  const created = await createAirtableRecord(AIRTABLE_STORE_LISTINGS_TABLE_NAME, fields);
  return { action: "created", recordId: created.id };
}

async function deactivateOldListings(merchant, syncId) {
  const formula = `AND(
    {Merchant Record ID} = '${airtableEscape(merchant.recordId)}',
    {Status} = 'active',
    OR(
      {Last Seen Sync ID} = BLANK(),
      {Last Seen Sync ID} != '${airtableEscape(syncId)}'
    )
  )`;

  const records = await fetchAllAirtableRecords(AIRTABLE_STORE_LISTINGS_TABLE_NAME, formula);

  for (const record of records) {
    await updateAirtableRecord(AIRTABLE_STORE_LISTINGS_TABLE_NAME, record.id, {
      "Status": "inactive"
    });

    await sleep(220);
  }

  return records.length;
}

async function syncMerchant(merchant, runId) {
  const syncId = `${runId}_${merchant.recordId}`;

  console.log("Syncing merchant", {
    merchantRecordId: merchant.recordId,
    merchantName: merchant.name,
    shopifyDomain: merchant.shopifyDomain,
    syncId
  });

  const products = await fetchActiveProducts(merchant);

  let productsProcessed = 0;
  let variantsProcessed = 0;
  let created = 0;
  let updated = 0;
  let retailedMisses = 0;

  for (const product of products) {
    productsProcessed += 1;

    const fullProduct = await fetchProductVariants(merchant, product.id);
    const variants = fullProduct.variants.edges.map((edge) => edge.node);

    const firstVariantSku = variants[0]?.sku || "";
    const retailedQuery = firstVariantSku || fullProduct.title;

    const retailed = await searchRetailed(retailedQuery);

    let retailedStatus = "ok";
    
    if (!retailedQuery) {
      retailedStatus = "not_found";
      retailedMisses += 1;
    } else if (!retailed) {
      retailedStatus = "failed";
      retailedMisses += 1;
    }

    for (const variant of variants) {
      variantsProcessed += 1;

      const result = await upsertStoreListing({
        merchant,
        syncId,
        product: fullProduct,
        variant,
        retailed,
        retailedStatus,
        productSku: firstVariantSku
      });

      if (result.action === "created") created += 1;
      if (result.action === "updated") updated += 1;

      await sleep(220);
    }
  }

  const deactivated = await deactivateOldListings(merchant, syncId);

  await updateAirtableRecord(AIRTABLE_MERCHANTS_TABLE_NAME, merchant.recordId, {
    "Last Sync ID": syncId,
    "Last Shopify Sync At": new Date().toISOString()
  });

  return {
    merchantRecordId: merchant.recordId,
    merchantName: merchant.name,
    syncId,
    productsProcessed,
    variantsProcessed,
    created,
    updated,
    deactivated,
    retailedMisses
  };
}

async function syncAllMerchants() {
  assertEnv();

  const runId = createSyncId();
  const merchants = await fetchActiveMerchants();

  const results = [];

  for (const merchant of merchants) {
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
    message: "Shopify Store Listings Sync is running"
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
      message: "Sync completed successfully",
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

app.get("/run-test", async (_req, res) => {
  try {
    assertEnv();

    const merchants = await fetchActiveMerchants();

    if (!merchants.length) {
      return res.json({
        success: false,
        message: "No active merchants"
      });
    }

    const merchant = merchants[0];
    const runId = createSyncId();
    const syncId = `${runId}_${merchant.recordId}`;

    console.log("TEST RUN", { merchant: merchant.name, syncId });

    const products = await fetchActiveProducts(merchant);

    // 👇 LIMIT TO 5 PRODUCTS
    const testProducts = products.slice(0, 5);

    let variantsProcessed = 0;

    for (const product of testProducts) {
      const fullProduct = await fetchProductVariants(merchant, product.id);
      const variants = fullProduct.variants.edges.map((e) => e.node);

      const firstVariantSku = variants[0]?.sku || "";
      const retailedQuery = firstVariantSku || fullProduct.title;

      const retailed = await searchRetailed(retailedQuery);

      let retailedStatus = "ok";

      if (!retailedQuery) retailedStatus = "not_found";
      else if (!retailed) retailedStatus = "failed";

      for (const variant of variants) {
        variantsProcessed += 1;

        await upsertStoreListing({
          merchant,
          syncId,
          product: fullProduct,
          variant,
          retailed,
          retailedStatus,
          productSku: firstVariantSku
        });

        await sleep(200);
      }
    }

    // ❗ BELANGRIJK: GEEN deactivate in test
    // await deactivateOldListings(merchant, syncId);

    await updateAirtableRecord(AIRTABLE_MERCHANTS_TABLE_NAME, merchant.recordId, {
      "Last Sync ID": syncId,
      "Last Shopify Sync At": new Date().toISOString()
    });

    res.json({
      success: true,
      message: "Test run completed",
      merchant: merchant.name,
      productsProcessed: testProducts.length,
      variantsProcessed
    });

  } catch (error) {
    console.error("Test run error:", error);

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
