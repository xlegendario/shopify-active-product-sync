import express from "express";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 10000;

const SHOPIFY_STORE_DOMAIN = process.env.SHOPIFY_STORE_DOMAIN;
const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const SHOPIFY_API_VERSION = process.env.SHOPIFY_API_VERSION || "2026-01";
const MAKE_WEBHOOK_URL = process.env.MAKE_WEBHOOK_URL;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "100", 10);

// Shopify GraphQL call
async function shopifyGraphQL(query, variables = {}) {
  const url = `https://${SHOPIFY_STORE_DOMAIN}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
    },
    body: JSON.stringify({ query, variables })
  });

  const data = await response.json();

  if (!response.ok || data.errors) {
    throw new Error(JSON.stringify(data.errors || data));
  }

  return data;
}

// Send batch to Make webhook
async function sendToMake(productIds, meta = {}) {
  await fetch(MAKE_WEBHOOK_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      source: "render-shopify-sync",
      sentAt: new Date().toISOString(),
      ...meta,
      products: productIds.map(id => ({ id }))
    })
  });
}

// Fetch active product IDs
async function syncActiveProducts() {
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
  let total = 0;
  let page = 0;

  while (hasNextPage) {
    const res = await shopifyGraphQL(query, { cursor });
    const products = res.data.products;

    page++;

    const activeIds = products.edges
      .map(edge => edge.node)
      .filter(p => p.status === "ACTIVE")
      .map(p => p.id);

    total += activeIds.length;

    // Send in batches to Make
    for (let i = 0; i < activeIds.length; i += BATCH_SIZE) {
      const batch = activeIds.slice(i, i + BATCH_SIZE);
      await sendToMake(batch, { page });
    }

    hasNextPage = products.pageInfo.hasNextPage;
    cursor = products.pageInfo.endCursor;
  }

  return { total };
}

// Health check
app.get("/", (_, res) => {
  res.send("Shopify Active Product Sync is running.");
});

app.get("/health", (_, res) => {
  res.json({ status: "ok" });
});

// Trigger sync
app.post("/run", async (_, res) => {
  try {
    const result = await syncActiveProducts();
    res.json({
      success: true,
      totalActiveProducts: result.total
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
