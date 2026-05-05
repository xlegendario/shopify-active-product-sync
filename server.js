import express from "express";
import { createClient } from "@supabase/supabase-js";

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

const AIRTABLE_RISKY_PRODUCT_MATCHES_TABLE_NAME =
  process.env.AIRTABLE_RISKY_PRODUCT_MATCHES_TABLE_NAME || "Risky Product Matches";

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

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || "";

console.log("SUPABASE CONFIG", {
  url: SUPABASE_URL,
  hasKey: Boolean(SUPABASE_SERVICE_ROLE_KEY),
  keyPrefix: SUPABASE_SERVICE_ROLE_KEY.slice(0, 10)
});

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

let isSyncRunning = false;
let activeSyncStartedAt = null;

function assertEnv() {
  const missing = [];

  if (!AIRTABLE_TOKEN) missing.push("AIRTABLE_TOKEN");
  if (!AIRTABLE_BASE_ID) missing.push("AIRTABLE_BASE_ID");
  if (!SUPABASE_URL) missing.push("SUPABASE_URL");
  if (!SUPABASE_SERVICE_ROLE_KEY) missing.push("SUPABASE_SERVICE_ROLE_KEY");

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

async function logSyncError({ merchant, syncId, product, error }) {
  try {
    await createAirtableRecord(AIRTABLE_SYNC_ERRORS_TABLE_NAME, {
      "Merchant Record ID": merchant.recordId,
      "Sync ID": syncId,
      "Shopify Product ID": String(product?.legacyResourceId || getNumericId(product?.id)),
      "Shopify Product Name": product?.title || "",
      "Error Message": error instanceof Error ? error.message : String(error),
      "Resolved?": false
    });
  } catch (logError) {
    console.error("Failed to log sync error:", logError);
  }
}

function getRiskIssue({ sku, retailedStatus, matchRiskLevel, pictureUrl }) {
  const issueTypes = [];
  const issueNotes = [];

  if (!sku || !String(sku).trim()) {
    issueTypes.push("Missing SKU");
    issueNotes.push("SKU is missing.");
  }

  if (retailedStatus !== "ok") {
    issueTypes.push(
      retailedStatus === "not_found" ? "Retailed Not Found" : "Retailed Failed"
    );
    issueNotes.push(`Retailed status is ${retailedStatus}.`);
  }

  if (matchRiskLevel !== "Low") {
    issueTypes.push(`${matchRiskLevel} Risk Match`);
    issueNotes.push(`Match risk level is ${matchRiskLevel}.`);
  }

  if (!pictureUrl) {
    issueTypes.push("Missing Image");
    issueNotes.push("Retailed image is missing.");
  }

  return {
    isRisky: issueTypes.length > 0,
    issueTypes,
    issueNotes: issueNotes.join(" ")
  };
}

async function findRiskyProductMatch({ merchantRecordId, productId }) {
  const formula = `AND(
    {Merchant Record ID} = '${airtableEscape(merchantRecordId)}',
    {Shopify Product ID} = '${airtableEscape(productId)}'
  )`;

  const records = await fetchAllAirtableRecords(
    AIRTABLE_RISKY_PRODUCT_MATCHES_TABLE_NAME,
    formula
  );

  return records[0] || null;
}

async function upsertRiskyProductMatch({
  merchant,
  product,
  productSku,
  retailed,
  retailedStatus,
  matchRiskLevel,
  riskyMap
}) {
  const productId = String(product.legacyResourceId || getNumericId(product.id));
  const stockxProductName = buildStockxName(retailed);
  const pictureUrl = retailed?.image || "";

  const riskIssue = getRiskIssue({
    sku: productSku,
    retailedStatus,
    matchRiskLevel,
    pictureUrl
  });

  if (!riskIssue.isRisky) {
    return { action: "skipped" };
  }

  const fields = {
    "Merchant Record ID": merchant.recordId,
    "Shopify Product ID": productId,
    "Shopify Product Name": product.title || "",
    "StockX Product Name": stockxProductName,
    "Brand": retailed?.brand || "",
    "SKU": productSku || "",
    "Match Risk Level": matchRiskLevel,
    "Retailed Status": retailedStatus,
    "Issue Type": riskIssue.issueTypes,
    "Issue Notes": riskIssue.issueNotes,
    "Correction Synced?": false
  };

  if (pictureUrl) {
    fields.Picture = [
      {
        url: pictureUrl,
        filename: `${stockxProductName || product.title || productId}.webp`
      }
    ];
  }

  const existing = riskyMap.get(productId);

  if (existing) {
    await updateAirtableRecord(
      AIRTABLE_RISKY_PRODUCT_MATCHES_TABLE_NAME,
      existing.id,
      fields
    );
  
    riskyMap.set(productId, { id: existing.id, fields });
  
    return { action: "updated", recordId: existing.id };
  }
  
  const created = await createAirtableRecord(
    AIRTABLE_RISKY_PRODUCT_MATCHES_TABLE_NAME,
    fields
  );
  
  riskyMap.set(productId, { id: created.id, fields });
  
  return { action: "created", recordId: created.id };
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
      name: fields["Store Name"] || record.id,
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

function calculateMatchRisk({ sku, shopifyProductName, stockxProductName }) {
  function clean(str) {
    return String(str || "")
      .toLowerCase()
      .replace(/[\(\)\[\]'"]/g, "")
      .replace(/\bwomens\b/g, "w")
      .replace(/\bmen's\b/g, "m")
      .replace(/\s+/g, " ")
      .trim();
  }

  if (!sku || !String(sku).trim()) return "High";

  const productA = clean(shopifyProductName);
  const productB = clean(stockxProductName);

  if (!productA || !productB) return "High";

  function levenshtein(a, b) {
    const m = a.length;
    const n = b.length;
    const dp = Array.from({ length: m + 1 }, () => Array(n + 1).fill(0));

    for (let i = 0; i <= m; i += 1) dp[i][0] = i;
    for (let j = 0; j <= n; j += 1) dp[0][j] = j;

    for (let i = 1; i <= m; i += 1) {
      for (let j = 1; j <= n; j += 1) {
        dp[i][j] = Math.min(
          dp[i - 1][j] + 1,
          dp[i][j - 1] + 1,
          dp[i - 1][j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1)
        );
      }
    }

    return dp[m][n];
  }

  const distance = levenshtein(productA, productB);

  if (distance <= 5) return "Low";
  if (distance <= 10) return "Medium";
  return "High";
}

function mapToSupabaseStoreListing({
  merchant,
  syncId,
  product,
  variant,
  retailed,
  retailedStatus,
  productSku
}) {
  const stockxProductName = buildStockxName(retailed);

  const matchRiskLevel = calculateMatchRisk({
    sku: productSku,
    shopifyProductName: product.title || "",
    stockxProductName
  });

  return {
    merchant_record_id: merchant.recordId,
    merchant_name: merchant.name,

    shopify_product_id: String(product.legacyResourceId || getNumericId(product.id)),
    shopify_variant_id: String(variant.legacyResourceId || getNumericId(variant.id)),
    shopify_inventory_item_id: String(
      variant.inventoryItem?.legacyResourceId || getNumericId(variant.inventoryItem?.id)
    ),

    shopify_product_name: product.title || "",
    size: extractSize(variant.title),
    sku: productSku || null,

    stockx_product_name: stockxProductName || null,
    brand: retailed?.brand || null,
    picture_url: retailed?.image || null,

    retailed_status: retailedStatus,
    match_risk_level: matchRiskLevel,

    status: "active",
    last_seen_sync_id: syncId,
    last_shopify_sync_at: new Date().toISOString(),
    updated_at: new Date().toISOString()
  };
}

async function upsertStoreListingsSupabase(rows) {
  if (!rows.length) return [];

  const upserted = [];

  for (const chunk of chunkArray(rows, 500)) {
    const { data, error } = await supabase
      .from("store_listings")
      .upsert(chunk, {
        onConflict: "merchant_record_id,shopify_product_id,shopify_variant_id"
      })
      .select("id");

    if (error) {
      throw new Error(`Supabase upsert error: ${error.message}`);
    }

    upserted.push(...(data || []));
  }

  return upserted;
}

async function deactivateOldListingsSupabase(merchant, syncId) {
  const { data, error } = await supabase
    .from("store_listings")
    .update({
      status: "inactive",
      updated_at: new Date().toISOString()
    })
    .eq("merchant_record_id", merchant.recordId)
    .eq("status", "active")
    .neq("last_seen_sync_id", syncId)
    .select("id");

  if (error) {
    throw new Error(`Supabase inactive cleanup error: ${error.message}`);
  }

  return data?.length || 0;
}



function extractSize(value) {
  const text = String(value || "");

  const match = text.match(/\d+(?:[.,]\d+)?/);

  if (!match) return "";

  return match[0].replace(",", ".");
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
  const existingRiskyRecords = await fetchAllAirtableRecords(
    AIRTABLE_RISKY_PRODUCT_MATCHES_TABLE_NAME,
    `{Merchant Record ID} = '${airtableEscape(merchant.recordId)}'`
  );
  
  const riskyMap = new Map();
  
  for (const record of existingRiskyRecords) {
    const productId = record.fields["Shopify Product ID"];
    if (productId) {
      riskyMap.set(productId, record);
    }
  }

  let productsProcessed = 0;
  let variantsProcessed = 0;
  let created = 0;
  let updated = 0;
  let retailedMisses = 0;
  let riskyCreated = 0;
  let riskyUpdated = 0;
  
  let failedProducts = 0;

  for (const product of products) {
    try {
      productsProcessed += 1;
  
      const fullProduct = product;
      const variants = fullProduct.variants.edges.map((edge) => edge.node);
  
      const firstVariantSku = variants[0]?.sku || "";
      const retailedQuery = firstVariantSku || fullProduct.title;
  
      let retailed = null;
      let retailedStatus = "ok";
  
      retailed = await searchRetailed(retailedQuery);
  
      if (!retailedQuery) {
        retailedStatus = "not_found";
        retailedMisses += 1;
      } else if (!retailed) {
        retailedStatus = "failed";
        retailedMisses += 1;
      }
  
      const stockxProductName = buildStockxName(retailed);
  
      const productMatchRiskLevel = calculateMatchRisk({
        sku: firstVariantSku,
        shopifyProductName: fullProduct.title || "",
        stockxProductName
      });
  
      const riskyResult = await upsertRiskyProductMatch({
        merchant,
        product: fullProduct,
        productSku: firstVariantSku,
        retailed,
        retailedStatus,
        matchRiskLevel: productMatchRiskLevel,
        riskyMap
      });
  
      if (riskyResult.action === "created") riskyCreated += 1;
      if (riskyResult.action === "updated") riskyUpdated += 1;
  
      const supabaseRows = [];
  
      for (const variant of variants) {
        variantsProcessed += 1;
  
        supabaseRows.push(
          mapToSupabaseStoreListing({
            merchant,
            syncId,
            product: fullProduct,
            variant,
            retailed,
            retailedStatus,
            productSku: firstVariantSku
          })
        );
      }
  
      const supabaseRecords = await upsertStoreListingsSupabase(supabaseRows);
  
      updated += supabaseRecords.length;
  
      console.log("Supabase upsert completed", {
        product: fullProduct.title,
        rows: supabaseRecords.length,
        retailedStatus,
        matchRiskLevel: productMatchRiskLevel
      });
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
    deactivated = await deactivateOldListingsSupabase(merchant, syncId);
  } else {
    console.warn("Skipping Supabase inactive cleanup because products failed", {
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
    riskyCreated,
    riskyUpdated,
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
    riskyCreated,
    riskyUpdated,
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

    const existingRiskyRecords = await fetchAllAirtableRecords(
      AIRTABLE_RISKY_PRODUCT_MATCHES_TABLE_NAME,
      `{Merchant Record ID} = '${airtableEscape(merchant.recordId)}'`
    );
    
    const riskyMap = new Map();
    
    for (const record of existingRiskyRecords) {
      const productId = record.fields["Shopify Product ID"];
      if (productId) {
        riskyMap.set(String(productId), record);
      }
    }

    let variantsProcessed = 0;
    let created = 0;
    let updated = 0;
    let duplicatesDeletedTotal = 0;

    let riskyCreated = 0;
    let riskyUpdated = 0;
    
    for (const product of testProducts) {
      const fullProduct = product;
      const variants = fullProduct.variants.edges.map((e) => e.node);
    
      const firstVariantSku = variants[0]?.sku || "";
      const retailedQuery = firstVariantSku || fullProduct.title;
    
      let retailed = null;
      let retailedStatus = "ok";
    
      retailed = await searchRetailed(retailedQuery);
    
      if (!retailedQuery) {
        retailedStatus = "not_found";
      } else if (!retailed) {
        retailedStatus = "failed";
      }
    
      const stockxProductName = buildStockxName(retailed);
    
      const productMatchRiskLevel = calculateMatchRisk({
        sku: firstVariantSku,
        shopifyProductName: fullProduct.title || "",
        stockxProductName
      });
    
      const riskyResult = await upsertRiskyProductMatch({
        merchant,
        product: fullProduct,
        productSku: firstVariantSku,
        retailed,
        retailedStatus,
        matchRiskLevel: productMatchRiskLevel
      });
    
      if (riskyResult.action === "created") riskyCreated += 1;
      if (riskyResult.action === "updated") riskyUpdated += 1;
    
      const supabaseRows = [];
    
      for (const variant of variants) {
        variantsProcessed += 1;
    
        supabaseRows.push(
          mapToSupabaseStoreListing({
            merchant,
            syncId,
            product: fullProduct,
            variant,
            retailed,
            retailedStatus,
            productSku: firstVariantSku
          })
        );
      }
    
      const supabaseRecords = await upsertStoreListingsSupabase(supabaseRows);
    
      updated += supabaseRecords.length;
    
      console.log("TEST Supabase product completed", {
        product: fullProduct.title,
        rows: supabaseRecords.length,
        retailedStatus,
        matchRiskLevel: productMatchRiskLevel,
        riskyAction: riskyResult.action
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
      duplicatesDeleted: duplicatesDeletedTotal,
      riskyCreated,
      riskyUpdated
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
