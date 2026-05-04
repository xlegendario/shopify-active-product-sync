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

const AIRTABLE_SYNC_ERRORS_TABLE_NAME =
  process.env.AIRTABLE_SYNC_ERRORS_TABLE_NAME || "Sync Errors";

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

let isSyncRunning = false;
let activeSyncStartedAt = null;

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

function chunkArray(array, size) {
  const chunks = [];

  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }

  return chunks;
}

async function batchCreateAirtableRecords(tableName, records) {
  if (!records.length) return [];

  const table = encodeURIComponent(tableName);
  const created = [];

  for (const chunk of chunkArray(records, 10)) {
    const result = await airtableRequest(table, {
      method: "POST",
      body: JSON.stringify({
        records: chunk.map((fields) => ({ fields }))
      })
    });

    created.push(...(result.records || []));
    await sleep(220);
  }

  return created;
}

async function batchUpdateAirtableRecords(tableName, records) {
  if (!records.length) return [];

  const table = encodeURIComponent(tableName);
  const updated = [];

  for (const chunk of chunkArray(records, 10)) {
    const result = await airtableRequest(table, {
      method: "PATCH",
      body: JSON.stringify({
        records: chunk.map((record) => ({
          id: record.id,
          fields: record.fields
        }))
      })
    });

    updated.push(...(result.records || []));
    await sleep(220);
  }

  return updated;
}

async function batchDeleteAirtableRecords(tableName, recordIds) {
  if (!recordIds.length) return [];

  const table = encodeURIComponent(tableName);
  const deleted = [];

  for (const chunk of chunkArray(recordIds, 10)) {
    const params = new URLSearchParams();

    for (const id of chunk) {
      params.append("records[]", id);
    }

    const result = await airtableRequest(`${table}?${params.toString()}`, {
      method: "DELETE"
    });

    deleted.push(...(result.records || []));
    await sleep(220);
  }

  return deleted;
}

async function logSyncError({ merchant, syncId, product, error }) {
  try {
    await createAirtableRecord(AIRTABLE_SYNC_ERRORS_TABLE_NAME, {
      "Merchant Record ID": merchant.recordId,
      "Sync ID": syncId,
      "Shopify Product ID": String(product?.legacyResourceId || getNumericId(product?.id)),
      "Shopify Product Name": product?.title || "",
      "Error Message": error instanceof Error ? error.message : String(error),
      "Created At": new Date().toISOString(),
      "Resolved?": false
    });
  } catch (logError) {
    console.error("Failed to log sync error:", logError);
  }
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
      products(first: 250, after: $cursor, query: "status:active") {
        edges {
          node {
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
      products.push(edge.node);
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

async function buildVariantRecordsMap({ merchant, product, variants }) {
  const productId = String(product.legacyResourceId || getNumericId(product.id));

  const variantIds = [...new Set(
    variants.map((variant) =>
      String(variant.legacyResourceId || getNumericId(variant.id))
    )
  )];

  if (!variantIds.length) return new Map();

  const variantConditions = variantIds
    .map((variantId) => `{Shopify Variant ID} = '${airtableEscape(variantId)}'`)
    .join(",");

  const formula = `AND(
    {Merchant Record ID} = '${airtableEscape(merchant.recordId)}',
    {Shopify Product ID} = '${airtableEscape(productId)}',
    OR(${variantConditions})
  )`;

  const records = await fetchAllAirtableRecords(
    AIRTABLE_STORE_LISTINGS_TABLE_NAME,
    formula
  );

  const recordsByVariantId = new Map();

  for (const variantId of variantIds) {
    recordsByVariantId.set(variantId, {
      primary: null,
      duplicates: []
    });
  }

  for (const record of records) {
    const variantId = String(record.fields?.["Shopify Variant ID"] || "");
    if (!variantId) continue;

    const group = recordsByVariantId.get(variantId) || {
      primary: null,
      duplicates: []
    };

    if (!group.primary) {
      group.primary = record;
    } else {
      group.duplicates.push(record);
    }

    recordsByVariantId.set(variantId, group);
  }

  return recordsByVariantId;
}

async function deleteDuplicateStoreListings(existingByVariantId) {
  const duplicateIds = [];

  for (const [, group] of existingByVariantId.entries()) {
    for (const duplicate of group.duplicates || []) {
      duplicateIds.push(duplicate.id);
    }
  }

  if (!duplicateIds.length) return 0;

  console.warn("Deleting duplicate Store Listings", {
    count: duplicateIds.length
  });

  const deleted = await batchDeleteAirtableRecords(
    AIRTABLE_STORE_LISTINGS_TABLE_NAME,
    duplicateIds
  );

  return deleted.length;
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

function extractSize(value) {
  const text = String(value || "");

  const match = text.match(/\d+(?:[.,]\d+)?/);

  if (!match) return "";

  return match[0].replace(",", ".");
}

function buildStoreListingMutation({
  merchant,
  syncId,
  product,
  variant,
  retailed,
  retailedStatus,
  productSku,
  existingRecord = null,
  enrich = false
}) {
  const now = new Date().toISOString();

  const productId = String(product.legacyResourceId || getNumericId(product.id));
  const variantId = String(variant.legacyResourceId || getNumericId(variant.id));
  const inventoryItemId = String(
    variant.inventoryItem?.legacyResourceId || getNumericId(variant.inventoryItem?.id)
  );

  const fields = {
    "Client": [merchant.recordId],
    "Merchant Record ID": merchant.recordId,

    "Shopify Product ID": productId,
    "Shopify Variant ID": variantId,
    "Shopify Inventory Item ID": inventoryItemId,

    "Shopify Product Name": product.title || "",
    "Size": extractSize(variant.title),
    "SKU": productSku || "",

    "Last Seen Sync ID": syncId,
    "Last Shopify Sync At": now,
    "Status": "active"
  };

  const shouldEnrich =
    enrich ||
    !existingRecord ||
    ["failed", "not_found"].includes(existingRecord?.fields?.["Retailed Status"]);

  if (shouldEnrich) {
    const stockxProductName = buildStockxName(retailed);

    fields["StockX Product Name"] = stockxProductName;
    fields["Brand"] = retailed?.brand || "";
    fields["Retailed Status"] = retailedStatus;

    if (retailed?.image) {
      fields.Picture = [
        {
          url: retailed.image,
          filename: `${stockxProductName || product.title || variantId}.webp`
        }
      ];
    }
  }

  if (existingRecord) {
    return {
      action: "update",
      id: existingRecord.id,
      fields
    };
  }

  return {
    action: "create",
    fields
  };
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

  let failedProducts = 0;

  for (const product of products) {
    try {
      productsProcessed += 1;
  
      const fullProduct = product;
      const variants = fullProduct.variants.edges.map((edge) => edge.node);
      
      const firstVariantSku = variants[0]?.sku || "";
      const retailedQuery = firstVariantSku || fullProduct.title;

      const existingByVariantId = await buildVariantRecordsMap({
        merchant,
        product: fullProduct,
        variants
      });
        
      const duplicatesDeleted = await deleteDuplicateStoreListings(existingByVariantId);

      if (duplicatesDeleted > 0) {
        console.warn("Deleted duplicate Store Listings for product", {
          productId: fullProduct.legacyResourceId,
          productTitle: fullProduct.title,
          duplicatesDeleted
        });
      }
  
      const allVariantsExist = variants.every((variant) => {
        const variantId = String(variant.legacyResourceId || getNumericId(variant.id));
        return Boolean(existingByVariantId.get(variantId)?.primary);
      });
  
      const needsRetailedRetry = variants.some((variant) => {
        const variantId = String(variant.legacyResourceId || getNumericId(variant.id));
        const existing = existingByVariantId.get(variantId)?.primary;
        return ["failed", "not_found"].includes(existing?.fields?.["Retailed Status"]);
      });
  
      let retailed = null;
      let retailedStatus = "ok";
  
      if (!allVariantsExist || needsRetailedRetry) {
        retailed = await searchRetailed(retailedQuery);
  
        if (!retailedQuery) {
          retailedStatus = "not_found";
          retailedMisses += 1;
        } else if (!retailed) {
          retailedStatus = "failed";
          retailedMisses += 1;
        }
      }
  
      const recordsToCreate = [];
      const recordsToUpdate = [];
      
      for (const variant of variants) {
        variantsProcessed += 1;
      
        const variantId = String(variant.legacyResourceId || getNumericId(variant.id));
        const existingRecord = existingByVariantId.get(variantId)?.primary || null;
      
        const mutation = buildStoreListingMutation({
          merchant,
          syncId,
          product: fullProduct,
          variant,
          retailed,
          retailedStatus,
          productSku: firstVariantSku,
          existingRecord,
          enrich: !existingRecord || needsRetailedRetry
        });
      
        if (mutation.action === "update") {
          recordsToUpdate.push({
            id: mutation.id,
            fields: mutation.fields
          });
        } else {
          recordsToCreate.push(mutation.fields);
        }
      }
      
      const updatedRecords = await batchUpdateAirtableRecords(
        AIRTABLE_STORE_LISTINGS_TABLE_NAME,
        recordsToUpdate
      );
      
      const createdRecords = await batchCreateAirtableRecords(
        AIRTABLE_STORE_LISTINGS_TABLE_NAME,
        recordsToCreate
      );
      
      updated += updatedRecords.length;
      created += createdRecords.length;
    } catch (error) {
      failedProducts += 1;
  
      console.error("Product sync failed:", {
        merchantRecordId: merchant.recordId,
        productId: product.id,
        productTitle: product.title,
        error: error.message
      });
  
      await logSyncError({
        merchant,
        syncId,
        product,
        error
      });
    }
  }

  let deactivated = 0;

  if (failedProducts === 0) {
    deactivated = await deactivateOldListings(merchant, syncId);
  } else {
    console.warn("Skipping inactive cleanup because products failed", {
      merchantRecordId: merchant.recordId,
      syncId,
      failedProducts
    });
  }

  console.log("SYNC FINISHED", {
    merchant: merchant.name,
    syncId,
    productsProcessed,
    variantsProcessed,
    created,
    updated,
    deactivated,
    retailedMisses,
    failedProducts
  });

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
    retailedMisses,
    failedProducts
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
  if (isSyncRunning) {
    return res.status(409).json({
      success: false,
      message: "Sync is already running",
      activeSyncStartedAt
    });
  }

  isSyncRunning = true;
  activeSyncStartedAt = new Date().toISOString();

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
  } finally {
    isSyncRunning = false;
    activeSyncStartedAt = null;
  }
});

app.get("/run-test", async (_req, res) => {
  if (isSyncRunning) {
    return res.status(409).json({
      success: false,
      message: "Sync is already running",
      activeSyncStartedAt
    });
  }

  isSyncRunning = true;
  activeSyncStartedAt = new Date().toISOString();

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
    const testProducts = products.slice(0, 5);

    let variantsProcessed = 0;
    let created = 0;
    let updated = 0;
    let duplicatesDeletedTotal = 0;

    for (const product of testProducts) {
      const fullProduct = product;
      const variants = fullProduct.variants.edges.map((e) => e.node);

      const firstVariantSku = variants[0]?.sku || "";
      const retailedQuery = firstVariantSku || fullProduct.title;

      const existingByVariantId = await buildVariantRecordsMap({
        merchant,
        product: fullProduct,
        variants
      });

      const duplicatesDeleted = await deleteDuplicateStoreListings(existingByVariantId);
      duplicatesDeletedTotal += duplicatesDeleted;

      const allVariantsExist = variants.every((variant) => {
        const variantId = String(variant.legacyResourceId || getNumericId(variant.id));
        return Boolean(existingByVariantId.get(variantId)?.primary);
      });

      const needsRetailedRetry = variants.some((variant) => {
        const variantId = String(variant.legacyResourceId || getNumericId(variant.id));
        const existing = existingByVariantId.get(variantId)?.primary;
        return ["failed", "not_found"].includes(existing?.fields?.["Retailed Status"]);
      });

      let retailed = null;
      let retailedStatus = "ok";

      if (!allVariantsExist || needsRetailedRetry) {
        retailed = await searchRetailed(retailedQuery);

        if (!retailedQuery) retailedStatus = "not_found";
        else if (!retailed) retailedStatus = "failed";
      }

      const recordsToCreate = [];
      const recordsToUpdate = [];

      for (const variant of variants) {
        variantsProcessed += 1;

        const variantId = String(variant.legacyResourceId || getNumericId(variant.id));
        const existingRecord = existingByVariantId.get(variantId)?.primary || null;

        const mutation = buildStoreListingMutation({
          merchant,
          syncId,
          product: fullProduct,
          variant,
          retailed,
          retailedStatus,
          productSku: firstVariantSku,
          existingRecord,
          enrich: !existingRecord || needsRetailedRetry
        });

        console.log("TEST mutation:", {
          product: fullProduct.title,
          variant: variant.title,
          sku: firstVariantSku,
          action: mutation.action,
          recordId: mutation.id || null
        });

        if (mutation.action === "update") {
          recordsToUpdate.push({
            id: mutation.id,
            fields: mutation.fields
          });
        } else {
          recordsToCreate.push(mutation.fields);
        }
      }

      const updatedRecords = await batchUpdateAirtableRecords(
        AIRTABLE_STORE_LISTINGS_TABLE_NAME,
        recordsToUpdate
      );

      const createdRecords = await batchCreateAirtableRecords(
        AIRTABLE_STORE_LISTINGS_TABLE_NAME,
        recordsToCreate
      );

      updated += updatedRecords.length;
      created += createdRecords.length;

      console.log("TEST batch result:", {
        product: fullProduct.title,
        updated: updatedRecords.length,
        created: createdRecords.length,
        duplicatesDeleted
      });
    }

    await updateAirtableRecord(AIRTABLE_MERCHANTS_TABLE_NAME, merchant.recordId, {
      "Last Sync ID": syncId,
      "Last Shopify Sync At": new Date().toISOString()
    });

    res.json({
      success: true,
      message: "Test run completed",
      merchant: merchant.name,
      productsProcessed: testProducts.length,
      variantsProcessed,
      created,
      updated,
      duplicatesDeleted: duplicatesDeletedTotal
    });

  } catch (error) {
    console.error("Test run error:", error);

    res.status(500).json({
      success: false,
      error: error.message
    });
  } finally {
    isSyncRunning = false;
    activeSyncStartedAt = null;
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
