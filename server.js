import express from "express";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;

const SHOPIFY_STORE_DOMAIN = (process.env.SHOPIFY_STORE_DOMAIN || "")
  .replace(/^https?:\/\//, "")
  .replace(/\/$/, "");

const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN || "";
const SHOPIFY_API_VERSION = process.env.SHOPIFY_API_VERSION || "2026-01";
const MAKE_WEBHOOK_URL = process.env.MAKE_WEBHOOK_URL || "";
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "100", 10);

function assertEnv() {
  const missing = [];

  if (!SHOPIFY_STORE_DOMAIN) missing.push("SHOPIFY_STORE_DOMAIN");
  if (!SHOPIFY_ACCESS_TOKEN) missing.push("SHOPIFY_ACCESS_TOKEN");
  if (!MAKE_WEBHOOK_URL) missing.push("MAKE_WEBHOOK_URL");

  if (missing.length > 0) {
    throw new Error(`Missing environment variables: ${missing.join(", ")}`);
  }
}

function getShopifyGraphqlUrl() {
  return `https://${SHOPIFY_STORE_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
}

async function shopifyGraphQL(query, variables = {}) {
  const url = getShopifyGraphqlUrl();

  console.log("Shopify URL:", url);

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
    },
    body: JSON.stringify({ query, variables })
  });

  const rawText = await response.text();

  let data;
  try {
    data = JSON.parse(rawText);
  } catch (error) {
    throw new Error(
      `Shopify returned non-JSON response. HTTP ${response.status}. Body: ${rawText.slice(0, 1000)}`
    );
  }

  if (!response.ok) {
    throw new Error(
      `Shopify HTTP ${response.status}: ${JSON.stringify(data).slice(0, 2000)}`
    );
  }

  if (data.errors && data.errors.length > 0) {
    throw new Error(`Shopify GraphQL errors: ${JSON.stringify(data.errors)}`);
  }

  return data;
}

async function sendToMake(productIds, meta = {}) {
  const payload = {
    source: "render-shopify-sync",
    sentAt: new Date().toISOString(),
    batchSize: productIds.length,
    ...meta,
    products: productIds.map((id) => ({ id }))
  };

  console.log("Sending batch to Make", {
    webhookConfigured: !!MAKE_WEBHOOK_URL,
    batchSize: payload.batchSize,
    meta,
    firstThreeIds: productIds.slice(0, 3)
  });

  const response = await fetch(MAKE_WEBHOOK_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  const responseText = await response.text();

  console.log("Make webhook response", {
    status: response.status,
    bodyPreview: responseText.slice(0, 1000)
  });

  if (!response.ok) {
    throw new Error(`Make webhook failed with status ${response.status}: ${responseText}`);
  }

  return {
    status: response.status,
    body: responseText
  };
}

function chunkArray(array, chunkSize) {
  const chunks = [];

  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }

  return chunks;
}

async function syncActiveProducts() {
  assertEnv();

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
  let totalActiveProducts = 0;
  let totalProductsSeen = 0;
  let page = 0;
  let totalBatchesSent = 0;

  while (hasNextPage) {
    page += 1;

    console.log(`Fetching Shopify page ${page} with cursor:`, cursor);

    const result = await shopifyGraphQL(query, { cursor });
    const productsConnection = result?.data?.products;

    if (!productsConnection) {
      throw new Error(`Unexpected Shopify response shape: ${JSON.stringify(result).slice(0, 2000)}`);
    }

    const nodes = productsConnection.edges
      .map((edge) => edge?.node)
      .filter(Boolean);

    totalProductsSeen += nodes.length;

    const activeIds = nodes
      .filter((node) => node.status === "ACTIVE")
      .map((node) => node.id);

    totalActiveProducts += activeIds.length;

    console.log(`Page ${page} fetched`, {
      productsOnPage: nodes.length,
      activeOnPage: activeIds.length,
      hasNextPage: productsConnection.pageInfo.hasNextPage
    });

    const batches = chunkArray(activeIds, BATCH_SIZE);

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex += 1) {
      const batch = batches[batchIndex];
      totalBatchesSent += 1;

      console.log(`Sending Make batch ${batchIndex + 1}/${batches.length} for Shopify page ${page}`);

      await sendToMake(batch, {
        page,
        batchInPage: batchIndex + 1,
        batchesInPage: batches.length,
        hasNextPage: productsConnection.pageInfo.hasNextPage
      });
    }

    hasNextPage = productsConnection.pageInfo.hasNextPage;
    cursor = productsConnection.pageInfo.endCursor || null;
  }

  return {
    totalProductsSeen,
    totalActiveProducts,
    totalBatchesSent,
    batchSize: BATCH_SIZE
  };
}

app.get("/", (_req, res) => {
  res.json({
    success: true,
    message: "Shopify Active Product Sync is running"
  });
});

app.get("/health", (_req, res) => {
  res.json({
    success: true,
    status: "ok"
  });
});

app.get("/test-make", async (_req, res) => {
  try {
    assertEnv();

    const testIds = [
      "gid://shopify/Product/1234567890",
      "gid://shopify/Product/2345678901",
      "gid://shopify/Product/3456789012"
    ];

    const makeResult = await sendToMake(testIds, {
      test: true,
      page: 1,
      batchInPage: 1,
      batchesInPage: 1,
      hasNextPage: false
    });

    res.json({
      success: true,
      message: "Test payload sent to Make",
      makeResult
    });
  } catch (error) {
    console.error("Test Make error:", error);

    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

app.get("/run", async (_req, res) => {
  try {
    const result = await syncActiveProducts();

    res.json({
      success: true,
      message: "Sync completed successfully",
      ...result
    });
  } catch (error) {
    console.error("Sync error:", error);

    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

app.post("/run", async (_req, res) => {
  try {
    const result = await syncActiveProducts();

    res.json({
      success: true,
      message: "Sync completed successfully",
      ...result
    });
  } catch (error) {
    console.error("Sync error:", error);

    res.status(500).json({
      success: false,
      error: error instanceof Error ? error.message : String(error)
    });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
