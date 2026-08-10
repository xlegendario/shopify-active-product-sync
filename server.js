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

const AIRTABLE_SHOPIFY_LOCATION_ID_FIELD =
  process.env.AIRTABLE_SHOPIFY_LOCATION_ID_FIELD || "Shopify Location ID";

const AIRTABLE_STOCK_SYNC_FIELD =
  process.env.AIRTABLE_STOCK_SYNC_FIELD || "Sync Stock?";

const AIRTABLE_STOCK_LEVELS_TABLE_NAME =
  process.env.AIRTABLE_STOCK_LEVELS_TABLE_NAME || "Stock Levels";

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

  const hasSku = Boolean(sku && String(sku).trim());

  if (!hasSku) {
    issueTypes.push("Missing SKU");
    issueNotes.push("SKU is missing.");

    if (matchRiskLevel !== "Low") {
      issueTypes.push(`${matchRiskLevel} Risk Match`);
      issueNotes.push(`Match risk level is ${matchRiskLevel}.`);
    }
  }

  if (retailedStatus !== "ok") {
    issueTypes.push(
      retailedStatus === "not_found" ? "Retailed Not Found" : "Retailed Failed"
    );
    issueNotes.push(`Retailed status is ${retailedStatus}.`);
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
    const existing = riskyMap?.get(productId) || null;
  
    if (existing) {
      await airtableRequest(
        `${encodeURIComponent(AIRTABLE_RISKY_PRODUCT_MATCHES_TABLE_NAME)}/${existing.id}`,
        {
          method: "DELETE"
        }
      );
  
      riskyMap.delete(productId);
  
      return { action: "deleted" };
    }
  
    return { action: "skipped" };
  }

  const fields = {
    "Client": [merchant.recordId],
    "Merchant Record ID": merchant.recordId,
    "Shopify Product ID": productId,
    "Shopify Product Name": product.title || "",
    "StockX Product Name": stockxProductName,
    "Brand": retailed?.brand || "",
    "SKU": productSku || "",
    "SKU (Soft)": !productSku && retailed?.sku ? retailed.sku : "",
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

  const existing = riskyMap?.get(productId) || null;

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

function toShopifyGid(type, id) {
  const cleanId = String(id || "").trim();

  if (!cleanId) return "";

  if (cleanId.startsWith("gid://shopify/")) {
    return cleanId;
  }

  return `gid://shopify/${type}/${cleanId}`;
}

function normalizeStockKey(sku, size) {
  return `${String(sku || "").trim().toUpperCase()}|||${String(size || "").trim().toUpperCase()}`;
}

function numberFromAirtable(value) {
  const raw = Array.isArray(value) ? value[0] : value;
  const n = Number(raw);
  return Number.isFinite(n) ? n : 0;
}

async function fetchStockSyncMerchants() {
  const formula = `{${AIRTABLE_STOCK_SYNC_FIELD}} = 1`;

  const records = await fetchAllAirtableRecords(
    AIRTABLE_MERCHANTS_TABLE_NAME,
    formula
  );

  const merchants = [];

  for (const record of records) {
    const fields = record.fields || {};

    const rawShopifyUrl = fields[AIRTABLE_SHOPIFY_URL_FIELD];
    const shopifyToken = fields[AIRTABLE_SHOPIFY_TOKEN_FIELD];
    const shopifyDomain = normalizeShopifyDomain(rawShopifyUrl);
    const locationId = String(fields[AIRTABLE_SHOPIFY_LOCATION_ID_FIELD] || "").trim();
    const storeName = String(fields["Store Name"] || "").trim();

    if (!shopifyDomain || !shopifyToken || !locationId || !storeName) {
      console.warn("Skipping stock sync merchant missing required fields", {
        recordId: record.id,
        storeName,
        hasShopifyDomain: Boolean(shopifyDomain),
        hasToken: Boolean(shopifyToken),
        hasLocationId: Boolean(locationId)
      });
      continue;
    }

    merchants.push({
      recordId: record.id,
      name: storeName,
      shopifyDomain,
      shopifyToken,
      locationId,
      locationGid: toShopifyGid("Location", locationId)
    });
  }

  return merchants;
}

async function fetchStockLevelRows() {
  const records = await fetchAllAirtableRecords(
    AIRTABLE_STOCK_LEVELS_TABLE_NAME
  );

  return records
    .map((record) => {
      const fields = record.fields || {};

      return {
        recordId: record.id,
        sku: String(fields["SKU"] || "").trim().toUpperCase(),
        size: String(fields["Size"] || "").trim(),
        stockLevel: numberFromAirtable(fields["Stock Level"])
      };
    })
    .filter((row) => row.sku && row.size);
}

async function fetchAllStoreListingsForMerchant(merchantName) {
  const allRows = [];
  const pageSize = 1000;

  for (let from = 0; ; from += pageSize) {
    const to = from + pageSize - 1;

    const { data, error } = await supabase
      .from("store_listings")
      .select(`
        id,
        merchant_name,
        sku,
        size,
        shopify_variant_id,
        shopify_inventory_item_id,
        status
      `)
      .eq("merchant_name", merchantName)
      .eq("status", "active")
      .not("sku", "is", null)
      .not("shopify_inventory_item_id", "is", null)
      .range(from, to);

    if (error) {
      throw new Error(`Supabase store_listings lookup error: ${error.message}`);
    }

    allRows.push(...(data || []));

    if (!data || data.length < pageSize) {
      break;
    }
  }

  return allRows;
}

async function shopifyInventoryItemUpdateTracked(merchant, inventoryItemGid) {
  const mutation = `
    mutation InventoryItemUpdate($id: ID!) {
      inventoryItemUpdate(
        id: $id,
        input: {
          tracked: true
        }
      ) {
        inventoryItem {
          id
          tracked
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const result = await shopifyGraphQL(merchant, mutation, {
    id: inventoryItemGid
  });

  const errors =
    result.data?.inventoryItemUpdate?.userErrors || [];

  if (errors.length) {
    throw new Error(`inventoryItemUpdate errors: ${JSON.stringify(errors)}`);
  }

  return result.data.inventoryItemUpdate.inventoryItem;
}

async function shopifyInventoryActivate(merchant, inventoryItemGid, available) {
  const mutation = `
    mutation ActivateInventoryItem(
      $inventoryItemId: ID!,
      $locationId: ID!,
      $available: Int
    ) {
      inventoryActivate(
        inventoryItemId: $inventoryItemId,
        locationId: $locationId,
        available: $available
      ) {
        inventoryLevel {
          id
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const result = await shopifyGraphQL(merchant, mutation, {
    inventoryItemId: inventoryItemGid,
    locationId: merchant.locationGid,
    available
  });

  const errors =
    result.data?.inventoryActivate?.userErrors || [];

  if (errors.length) {
    const message = JSON.stringify(errors);

    if (
      message.toLowerCase().includes("already") ||
      message.toLowerCase().includes("active")
    ) {
      return {
        alreadyActive: true
      };
    }

    throw new Error(`inventoryActivate errors: ${message}`);
  }

  return {
    alreadyActive: false
  };
}

async function shopifyInventorySetQuantities(merchant, quantities) {
  if (!quantities.length) {
    return null;
  }

  const mutation = `
    mutation InventorySetQuantities($input: InventorySetQuantitiesInput!) {
      inventorySetQuantities(input: $input) {
        inventoryAdjustmentGroup {
          createdAt
          reason
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  const result = await shopifyGraphQL(merchant, mutation, {
    input: {
      name: "available",
      reason: "correction",
      ignoreCompareQuantity: true,
      quantities
    }
  });

  const errors =
    result.data?.inventorySetQuantities?.userErrors || [];

  if (errors.length) {
    throw new Error(`inventorySetQuantities errors: ${JSON.stringify(errors)}`);
  }

  return result.data.inventorySetQuantities.inventoryAdjustmentGroup;
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
      sku: first.sku || "",
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

function normalizeMerchantVariantSku(merchant, sku, variant) {
  const rawSku = String(sku || "").trim();

  if (!rawSku) return "";

  // Alleen voor deze specifieke store
  if (merchant.name !== "LetzKick") {
    return rawSku;
  }

  const sizeOption = variant?.selectedOptions?.find((option) =>
    ["size", "maat"].includes(
      String(option.name || "").trim().toLowerCase()
    )
  );

  const size = String(
    sizeOption?.value || variant?.title || ""
  ).trim();

  if (!size) return rawSku;

  const possibleSizes = new Set([
    size,
    size.replace(",", "."),
    size.replace(".", ",")
  ]);

  for (const possibleSize of possibleSizes) {
    const suffix = `-${possibleSize}`;

    if (rawSku.endsWith(suffix)) {
      return rawSku.slice(0, -suffix.length).trim();
    }
  }

  return rawSku;
}

function calculateMatchRisk({ sku }) {
  const hasSku = Boolean(sku && String(sku).trim());

  if (!hasSku) {
    return "High";
  }

  return "Low";
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
    stockxProductName,
    brand: retailed?.brand || ""
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

  let affected = 0;

  for (const chunk of chunkArray(rows, 500)) {
    const { error } = await supabase.rpc(
      "upsert_store_listings_keep_sku",
      { rows: chunk }
    );

    if (error) {
      throw new Error(`Supabase upsert error: ${error.message}`);
    }

    affected += chunk.length;
  }

  return Array.from({ length: affected }, (_, i) => ({ id: i }));
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

async function fetchExistingSupabaseProduct({ merchant, product }) {
  const productId = String(product.legacyResourceId || getNumericId(product.id));

  const { data, error } = await supabase
    .from("store_listings")
    .select("stockx_product_name, brand, picture_url, retailed_status, match_risk_level")
    .eq("merchant_record_id", merchant.recordId)
    .eq("shopify_product_id", productId)
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Supabase existing product lookup error: ${error.message}`);
  }

  return data || null;
}

async function fetchExistingSupabaseSkuMaster(productSku) {
  const sku = String(productSku || "").trim();

  if (!sku) return null;

  const { data, error } = await supabase
    .from("store_listings")
    .select("stockx_product_name, brand, picture_url, retailed_status")
    .eq("sku", sku)
    .eq("retailed_status", "ok")
    .not("stockx_product_name", "is", null)
    .not("picture_url", "is", null)
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Supabase SKU master lookup error: ${error.message}`);
  }

  return data || null;
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
  
      const firstVariant = variants[0];

      const firstVariantSku = normalizeMerchantVariantSku(
        merchant,
        firstVariant?.sku || "",
        firstVariant
      );
      
      const retailedQuery = firstVariantSku || fullProduct.title;
  
      let retailed = null;
      let retailedStatus = "ok";
      
      const existingSupabaseProduct = await fetchExistingSupabaseProduct({
        merchant,
        product: fullProduct
      });
      
      const canSkipRetailedFromProduct =
        existingSupabaseProduct &&
        existingSupabaseProduct.retailed_status === "ok" &&
        existingSupabaseProduct.stockx_product_name &&
        existingSupabaseProduct.picture_url;
      
      if (canSkipRetailedFromProduct) {
        retailed = {
          name: existingSupabaseProduct.stockx_product_name,
          colorway: "",
          brand: existingSupabaseProduct.brand || "",
          image: existingSupabaseProduct.picture_url || ""
        };
      
        retailedStatus = "ok";
      
        console.log("Skipping Retailed lookup, using same-store Supabase cache", {
          product: fullProduct.title
        });
      } else {
        const existingSkuMaster = await fetchExistingSupabaseSkuMaster(firstVariantSku);
      
        if (existingSkuMaster) {
          retailed = {
            name: existingSkuMaster.stockx_product_name,
            colorway: "",
            brand: existingSkuMaster.brand || "",
            image: existingSkuMaster.picture_url || ""
          };
      
          retailedStatus = "ok";
      
          console.log("Skipping Retailed lookup, using SKU master cache", {
            product: fullProduct.title,
            sku: firstVariantSku
          });
        } else {
          retailed = await searchRetailed(retailedQuery);
      
          if (!retailedQuery) {
            retailedStatus = "not_found";
            retailedMisses += 1;
          } else if (!retailed) {
            retailedStatus = "failed";
            retailedMisses += 1;
          }
        }
      }
  
      const stockxProductName = buildStockxName(retailed);
  
      const productMatchRiskLevel = calculateMatchRisk({
        sku: firstVariantSku,
        shopifyProductName: fullProduct.title || "",
        stockxProductName,
        brand: retailed?.brand || ""
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

async function pushStockLevelsToShopify({
  dryRun = false,
  activateInventory = true,
  setTracked = true
} = {}) {
  assertEnv();

  const runId = `stock_push_${new Date().toISOString()}`;
  const merchants = await fetchStockSyncMerchants();
  const stockLevelRows = await fetchStockLevelRows();

  const stockMap = new Map();

  for (const stock of stockLevelRows) {
    stockMap.set(
      normalizeStockKey(stock.sku, stock.size),
      stock
    );
  }

  const results = [];

  for (const merchant of merchants) {
    console.log("Starting stock level push merchant", {
      merchant: merchant.name,
      recordId: merchant.recordId,
      dryRun
    });

    const listings = await fetchAllStoreListingsForMerchant(merchant.name);

    const updates = [];

    for (const listing of listings) {
      const key = normalizeStockKey(listing.sku, listing.size);
      const stock = stockMap.get(key);

      if (!stock) continue;

      const inventoryItemId = String(listing.shopify_inventory_item_id || "").trim();

      if (!inventoryItemId) continue;

      updates.push({
        listing_id: listing.id,
        sku: listing.sku,
        size: listing.size,
        shopify_variant_id: listing.shopify_variant_id,
        shopify_inventory_item_id: inventoryItemId,
        inventoryItemGid: toShopifyGid("InventoryItem", inventoryItemId),
        available: Math.max(0, Math.floor(Number(stock.stockLevel || 0)))
      });
    }

    if (dryRun) {
      results.push({
        merchantRecordId: merchant.recordId,
        merchantName: merchant.name,
        listingsScanned: listings.length,
        updatesFound: updates.length,
        sample: updates.slice(0, 10).map((item) => ({
          sku: item.sku,
          size: item.size,
          available: item.available,
          shopify_inventory_item_id: item.shopify_inventory_item_id
        }))
      });

      continue;
    }

    let trackedUpdated = 0;
    let activated = 0;
    let setQuantity = 0;
    let failed = 0;
    const errors = [];

    const uniqueByInventoryItem = new Map();

    for (const update of updates) {
      uniqueByInventoryItem.set(update.shopify_inventory_item_id, update);
    }

    if (setTracked || activateInventory) {
      console.log("Starting activate/tracked phase", {
        merchant: merchant.name,
        count: uniqueByInventoryItem.size
      });
    
      for (const update of uniqueByInventoryItem.values()) {
        console.log("Activate/tracked item", {
          sku: update.sku,
          size: update.size,
          inventoryItemId: update.shopify_inventory_item_id
        });
        try {
          if (setTracked) {
            await shopifyInventoryItemUpdateTracked(
              merchant,
              update.inventoryItemGid
            );

            trackedUpdated += 1;
          }

          if (activateInventory) {
            await shopifyInventoryActivate(
              merchant,
              update.inventoryItemGid,
              update.available
            );

            activated += 1;
          }
        } catch (error) {
          failed += 1;

          errors.push({
            sku: update.sku,
            size: update.size,
            inventory_item_id: update.shopify_inventory_item_id,
            stage: "activate_or_track",
            error: error.message
          });

          console.error("Stock push activate/track failed", {
            merchant: merchant.name,
            update,
            error: error.message
          });
        }

        await sleep(150);
      }
    }

    const quantityInputs = updates.map((update) => ({
      inventoryItemId: update.inventoryItemGid,
      locationId: merchant.locationGid,
      quantity: update.available
    }));

    console.log("Starting quantity phase", {
      merchant: merchant.name,
      count: quantityInputs.length
    });
    
    for (const chunk of chunkArray(quantityInputs, 100)) {
      console.log("Sending quantity batch", {
        merchant: merchant.name,
        batchSize: chunk.length
      });
      try {
        await shopifyInventorySetQuantities(merchant, chunk);

        setQuantity += chunk.length;
        console.log("Quantity batch success", {
          merchant: merchant.name,
          batchSize: chunk.length,
          totalSetQuantity: setQuantity
        });
      } catch (error) {
        failed += chunk.length;

        errors.push({
          stage: "set_quantities",
          count: chunk.length,
          error: error.message
        });

        console.error("Stock push set quantities failed", {
          merchant: merchant.name,
          count: chunk.length,
          error: error.message
        });
      }

      await sleep(500);
    }

    results.push({
      merchantRecordId: merchant.recordId,
      merchantName: merchant.name,
      listingsScanned: listings.length,
      updatesFound: updates.length,
      uniqueInventoryItems: uniqueByInventoryItem.size,
      trackedUpdated,
      activated,
      setQuantity,
      failed,
      errors: errors.slice(0, 25)
    });
  }

  return {
    runId,
    dryRun,
    merchantsProcessed: results.length,
    stockLevelsScanned: stockLevelRows.length,
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
    let retailedMisses = 0;
    
    for (const product of testProducts) {
      const fullProduct = product;
      const variants = fullProduct.variants.edges.map((e) => e.node);
    
      const firstVariant = variants[0];

      const firstVariantSku = normalizeMerchantVariantSku(
        merchant,
        firstVariant?.sku || "",
        firstVariant
      );
      
      const retailedQuery = firstVariantSku || fullProduct.title;
    
      let retailed = null;
      let retailedStatus = "ok";
      
      const existingSupabaseProduct = await fetchExistingSupabaseProduct({
        merchant,
        product: fullProduct
      });
      
      const canSkipRetailedFromProduct =
        existingSupabaseProduct &&
        existingSupabaseProduct.retailed_status === "ok" &&
        existingSupabaseProduct.stockx_product_name &&
        existingSupabaseProduct.picture_url;
      
      if (canSkipRetailedFromProduct) {
        retailed = {
          name: existingSupabaseProduct.stockx_product_name,
          colorway: "",
          brand: existingSupabaseProduct.brand || "",
          image: existingSupabaseProduct.picture_url || ""
        };
      
        retailedStatus = "ok";
      
        console.log("Skipping Retailed lookup, using same-store Supabase cache", {
          product: fullProduct.title
        });
      } else {
        const existingSkuMaster = await fetchExistingSupabaseSkuMaster(firstVariantSku);
      
        if (existingSkuMaster) {
          retailed = {
            name: existingSkuMaster.stockx_product_name,
            colorway: "",
            brand: existingSkuMaster.brand || "",
            image: existingSkuMaster.picture_url || ""
          };
      
          retailedStatus = "ok";
      
          console.log("Skipping Retailed lookup, using SKU master cache", {
            product: fullProduct.title,
            sku: firstVariantSku
          });
        } else {
          retailed = await searchRetailed(retailedQuery);
      
          if (!retailedQuery) {
            retailedStatus = "not_found";
            retailedMisses += 1;
          } else if (!retailed) {
            retailedStatus = "failed";
            retailedMisses += 1;
          }
        }
      }
    
      const stockxProductName = buildStockxName(retailed);
    
      const productMatchRiskLevel = calculateMatchRisk({
        sku: firstVariantSku,
        shopifyProductName: fullProduct.title || "",
        stockxProductName,
        brand: retailed?.brand || ""
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

async function applyPendingRiskyCorrections() {
  const formula = `AND(
    {Match Risk Level} = 'Low',
    OR(
      {Correction Synced?} = FALSE(),
      {Correction Synced?} = BLANK()
    )
  )`;

  const records = await fetchAllAirtableRecords(
    AIRTABLE_RISKY_PRODUCT_MATCHES_TABLE_NAME,
    formula
  );

  console.log("Pending risky corrections:", records.length);

  let updatedRows = 0;

  for (const record of records) {
    const fields = record.fields || {};

    const merchantRecordId = fields["Merchant Record ID"];
    const shopifyProductId = String(fields["Shopify Product ID"] || "");

    if (!merchantRecordId || !shopifyProductId) continue;

    const pictureUrl =
      Array.isArray(fields.Picture) && fields.Picture[0]
        ? fields.Picture[0].url
        : "";

    const updateFields = {
      match_risk_level: "Low",
      retailed_status: "ok",
      updated_at: new Date().toISOString()
    };

    if (fields["StockX Product Name"]) {
      updateFields.stockx_product_name = fields["StockX Product Name"];
    }

    if (fields.Brand) {
      updateFields.brand = fields.Brand;
    }

    if (fields.SKU) {
      updateFields.sku = fields.SKU;
    } else if (fields["SKU (Soft)"]) {
      updateFields.sku = fields["SKU (Soft)"];
    }

    if (pictureUrl) {
      updateFields.picture_url = pictureUrl;
    }

    const { data, error } = await supabase
      .from("store_listings")
      .update(updateFields)
      .eq("merchant_record_id", merchantRecordId)
      .eq("shopify_product_id", shopifyProductId)
      .select("id");

    if (error) {
      throw new Error(`Supabase correction update error: ${error.message}`);
    }

    updatedRows += data?.length || 0;

    await updateAirtableRecord(
      AIRTABLE_RISKY_PRODUCT_MATCHES_TABLE_NAME,
      record.id,
      {
        "Correction Synced?": true,
        "Last Sent To Supabase At": new Date().toISOString()
      }
    );

    console.log("Applied risky correction", {
      productId: shopifyProductId,
      updated: data?.length || 0
    });
  }

  return updatedRows;
}

app.get("/apply-risky-corrections", async (_req, res) => {
  try {
    assertEnv();

    const correctedRows = await applyPendingRiskyCorrections();

    res.json({
      success: true,
      message: "Pending risky corrections applied",
      correctedRows
    });
  } catch (error) {
    console.error("Apply risky corrections error:", error);

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.get("/run-stock-level-push", async (req, res) => {
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
    const dryRun =
      String(req.query.dry_run || "").toLowerCase() === "true" ||
      String(req.query.dry_run || "") === "1";

    const activateInventory =
      String(req.query.activate || "true").toLowerCase() !== "false";

    const setTracked =
      String(req.query.tracked || "true").toLowerCase() !== "false";

    const result = await pushStockLevelsToShopify({
      dryRun,
      activateInventory,
      setTracked
    });

    res.json({
      success: true,
      message: dryRun
        ? "Stock level push dry run completed"
        : "Stock levels pushed to Shopify",
      ...result
    });
  } catch (error) {
    console.error("Stock level push error:", error);

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
