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

function createSyncId() {
  return `sync_${new Date().toISOString()}`;
}

function getShopifyGraphqlUrl() {
  return `https://${SHOPIFY_STORE_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
}

async function shopifyGraphQL(query, variables = {}) {
  const response = await fetch(getShopifyGraphqlUrl(), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
    },
    body: JSON.stringify({ query, variables })
  });

  const text = await response.text();
  const data = JSON.parse(text);

  if (!response.ok || data.errors) {
    throw new Error(JSON.stringify(data.errors || data));
  }

  return data;
}

async function sendToMake(payload) {
  console.log("Sending to Make:", {
    event: payload.event,
    syncId: payload.syncId,
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

function chunkArray(array, size) {
  const chunks = [];

  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }

  return chunks;
}

async function syncActiveProducts() {
  assertEnv();

  const syncId = createSyncId();

  await sendToMake({
    event: "sync_started",
    syncId,
    sentAt: new Date().toISOString()
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

    const result = await shopifyGraphQL(query, { cursor });
    const connection = result.data.products;

    const activeIds = connection.edges
      .map((edge) => edge.node)
      .filter((product) => product.status === "ACTIVE")
      .map((product) => product.id);

    totalProductsSeen += connection.edges.length;
    totalActiveProducts += activeIds.length;

    const batches = chunkArray(activeIds, BATCH_SIZE);

    for (let i = 0; i < batches.length; i += 1) {
      totalBatchesSent += 1;

      await sendToMake({
        event: "product_batch",
        syncId,
        sentAt: new Date().toISOString(),
        page,
        batchInPage: i + 1,
        batchesInPage: batches.length,
        products: batches[i].map((id) => ({ id }))
      });
    }

    hasNextPage = connection.pageInfo.hasNextPage;
    cursor = connection.pageInfo.endCursor;
  }

  await sendToMake({
    event: "sync_completed",
    syncId,
    sentAt: new Date().toISOString(),
    totalProductsSeen,
    totalActiveProducts,
    totalBatchesSent
  });

  return {
    syncId,
    totalProductsSeen,
    totalActiveProducts,
    totalBatchesSent
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

    const syncId = createSyncId();

    await sendToMake({
      event: "product_batch",
      syncId,
      sentAt: new Date().toISOString(),
      test: true,
      products: [
        { id: "gid://shopify/Product/8651421024523" },
        { id: "gid://shopify/Product/8651451924747" }
      ]
    });

    res.json({
      success: true,
      message: "Test batch sent to Make",
      syncId
    });
  } catch (error) {
    console.error("Test Make error:", error);

    res.status(500).json({
      success: false,
      error: error.message
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
      error: error.message
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
      error: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
