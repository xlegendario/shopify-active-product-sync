/*
 * Product descriptions for the pages in a store that have none.
 *
 * The 300 active UNION Amsterdam products were written by hand-checked batch
 * in November 2026; this keeps new ones from going up blank. It only ever
 * fills an EMPTY description and an EMPTY SEO field: a store's own text is
 * never overwritten.
 *
 * Per product, cheapest first:
 *   1. storefront_library already has a text for the style code: use it.
 *      Same shoe in another store, or a page that was deleted and made
 *      again, costs nothing.
 *   2. Facts from the official StockX API (free with our key): name, brand,
 *      colorway, release date, season, gender.
 *   3. Retailed for what StockX's API leaves out: StockX's own product copy
 *      (materials, colour blocking) by the urlKey we already have, so one
 *      call; and GOAT for apparel, where StockX knows very little.
 *   4. Claude writes the text from those facts in the approved format.
 *
 * A style code nobody knows anything about is left blank and asked again
 * after a week, rather than letting the model write from nothing.
 */

import Anthropic from "@anthropic-ai/sdk";
import { createStockxClient } from "./stockxSizes.js";

const LIBRARY_TABLE = "storefront_library";
const STOCKX_SEARCH = "https://api.stockx.com/v2/catalog/search";
const RETAILED = "https://app.retailed.io/api/v1/scraper";

const MODEL = process.env.DESCRIPTION_MODEL || "claude-opus-5-5";
const MAX_PER_RUN = Number(process.env.DESCRIPTION_MAX_PER_RUN || 30);
const RETRY_NO_FACTS_DAYS = Number(process.env.DESCRIPTION_RETRY_DAYS || 7);
// Retailed adds StockX's product copy and GOAT for apparel. Off = StockX API only.
const USE_RETAILED = String(process.env.DESCRIPTION_RETAILED || "true") !== "false";

// Opus 5.5 list prices per million tokens, for the run report only.
const PRICE_IN = 4;
const PRICE_OUT = 20;

/* ------------------------------------------------------------------ *
 * The format
 * ------------------------------------------------------------------ */

const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

export function renderDescription(text) {
  return (
    text.body.map((p) => `<p>${esc(p)}</p>`).join("") +
    (text.fit ? `<p><strong>Fit:</strong> ${esc(text.fit)}</p>` : "") +
    `<ul>${text.details
      .filter((d) => d.value)
      .map((d) => `<li><strong>${esc(d.label)}:</strong> ${esc(d.value)}</li>`)
      .join("")}</ul>`
  );
}

/*
 * The same checks the hand-written batch had to pass. A text that fails is
 * asked for once more with the problems named, and then left out.
 */
export function textProblems(text, storeName) {
  const out = [];
  const words = text.body.join(" ").split(/\s+/).filter(Boolean).length;

  if (text.body.length !== 2) out.push(`${text.body.length} paragraphs instead of 2`);
  if (words < 70 || words > 120) out.push(`${words} words, needs 70-120`);
  if (text.seo.length > 60) out.push(`SEO title is ${text.seo.length} characters, max 60`);
  if (text.meta.length > 155) out.push(`meta description is ${text.meta.length} characters, max 155`);
  if (!text.meta.includes(storeName)) out.push(`meta description must name ${storeName}`);
  if (/\$|\bUSD\b|\bship(s|ped|ping)?\b|delivery|limited stock/i.test(text.body.join(" ") + " " + text.meta)) {
    out.push("mentions a price, shipping or urgency");
  }
  if (!text.details.some((d) => d.label === "Style code")) out.push("no Style code row");

  return out;
}

// The shop's name is kept as {store} in the library, so a text can serve any store.
const toTemplate = (s, storeName) => String(s || "").split(storeName).join("{store}");

export function fromTemplate(s, storeName, max) {
  const full = String(s || "").split("{store}").join(storeName);
  if (!max || full.length <= max) return full;
  // "| UNION Amsterdam" too long for this title: the first word of the name.
  return String(s).split("{store}").join(storeName.split(/\s+/)[0]);
}

/* ------------------------------------------------------------------ *
 * The library
 * ------------------------------------------------------------------ */

function supabaseHeaders(extra = {}) {
  return {
    apikey: process.env.SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`,
    "Content-Type": "application/json",
    ...extra
  };
}

export const normalizeStyle = (sku) => String(sku || "").trim().toUpperCase().replace(/\s+/g, "");

async function readLibrary(styles) {
  const known = new Map();
  const wanted = [...new Set(styles)].filter(Boolean);

  for (let i = 0; i < wanted.length; i += 200) {
    const url = new URL(`${process.env.SUPABASE_URL}/rest/v1/${LIBRARY_TABLE}`);
    url.searchParams.set("select", "style_code,description,seo_title,seo_description,description_checked_at");
    url.searchParams.set("style_code", `in.(${wanted.slice(i, i + 200).map((c) => `"${c}"`).join(",")})`);

    const rows = await (await fetch(url, { headers: supabaseHeaders() })).json();
    if (!Array.isArray(rows)) throw new Error(`storefront_library lezen: ${JSON.stringify(rows).slice(0, 150)}`);

    for (const row of rows) known.set(row.style_code, row);
  }

  return known;
}

async function saveLibrary(row) {
  const res = await fetch(`${process.env.SUPABASE_URL}/rest/v1/${LIBRARY_TABLE}?on_conflict=style_code`, {
    method: "POST",
    headers: supabaseHeaders({ Prefer: "resolution=merge-duplicates,return=minimal" }),
    body: JSON.stringify([{ ...row, updated_at: new Date().toISOString() }])
  });

  if (!res.ok) throw new Error(`storefront_library schrijven ${res.status}: ${(await res.text()).slice(0, 150)}`);
}

/* ------------------------------------------------------------------ *
 * Facts
 * ------------------------------------------------------------------ */

const sameCode = (styleId, sku) =>
  String(styleId || "")
    .split("/")
    .map(normalizeStyle)
    .some((code) => code.replace(/[^A-Z0-9]/g, "") === sku.replace(/[^A-Z0-9]/g, ""));

async function stockxFacts(client, sku) {
  const search = await client
    .call(`${STOCKX_SEARCH}?query=${encodeURIComponent(sku)}&pageSize=10`)
    .catch((error) => {
      if (/StockX 404/.test(error.message)) return null;
      throw error;
    });

  // Only an exact style code: a near miss is a different shoe with confident facts.
  const hit = (search?.products || []).find((p) => sameCode(p.styleId, sku));
  if (!hit) return null;

  return {
    name: hit.title,
    brand: hit.brand,
    productType: hit.productType,
    styleId: hit.styleId,
    urlKey: hit.urlKey,
    ...hit.productAttributes
  };
}

async function retailed(path) {
  const res = await fetch(`${RETAILED}/${path}`, {
    headers: { "x-api-key": process.env.RETAILED_API_KEY },
    signal: AbortSignal.timeout(30000)
  });
  if (!res.ok) throw new Error(`Retailed ${res.status} on ${path.split("?")[0]}`);
  return res.json();
}

async function stockxCopy(urlKey) {
  const product = await retailed(`stockx/product?query=${encodeURIComponent(urlKey)}`);
  const traits = Object.fromEntries((product.traits || []).map((t) => [t.name, t.value]));

  return { description: product.description || "", model: product.model || "", traits };
}

async function goatFacts(sku) {
  const search = await retailed(`goat/search?query=${encodeURIComponent(sku)}`);
  const hits = Array.isArray(search) ? search : search.data || [];
  const hit = hits.find((h) => sameCode(h.sku || String(h.slug || "").split("-").pop(), sku)) || null;
  if (!hit) return null;

  const product = await retailed(`goat/product?query=${encodeURIComponent(hit.slug)}`);
  if (!sameCode(product.sku, sku)) return null;

  return {
    name: product.name,
    brand: product.brand_name,
    season: product.season,
    color: product.color,
    colorway: product.details,
    story: product.story,
    releaseDate: product.release_date || null
  };
}

export async function gatherFacts(client, sku) {
  const facts = { sku };

  facts.stockx = await stockxFacts(client, sku).catch((error) => ({ error: error.message }));
  if (facts.stockx?.error) throw new Error(`StockX: ${facts.stockx.error}`);

  if (USE_RETAILED && process.env.RETAILED_API_KEY) {
    if (facts.stockx?.urlKey) {
      facts.stockxCopy = await stockxCopy(facts.stockx.urlKey).catch(() => null);
    }

    // StockX knows a hoodie's name and little else; GOAT has the material and season.
    if (!facts.stockx || facts.stockx.productType !== "sneakers") {
      facts.goat = await goatFacts(sku).catch(() => null);
    }
  }

  return facts.stockx || facts.goat ? facts : null;
}

/* ------------------------------------------------------------------ *
 * Writing
 * ------------------------------------------------------------------ */

const SYSTEM = `You write product descriptions for {store}, a sneaker and streetwear shop in the Netherlands that sells authentic pairs. English only.

Format, exactly:
- body: two short paragraphs, 85-100 words in total (hard limits 70-120). Paragraph 1: the model's history or what the silhouette is. Paragraph 2: this colorway, its materials and tech, and how to wear it. Plain and confident with a bit of personality. No hype clichés ("elevate", "game-changer", "must-have"; "iconic" at most once), no emojis.
- fit: one short sentence, only when the sizing of this model is well known (Jordan 1 true to size, Maison Mihara runs large, Yeezy Slide runs small, Samba narrow). Women's, grade school and kids pairs ALWAYS get a fit line, starting with "Women's sizing," / "Grade school sizing," / "Kids sizing," followed by what you know about the fit (e.g. "Women's sizing, true to size."). Fear of God Essentials is always exactly: "Relaxed, oversized fit. Take your usual size for the intended look, or one size down for a closer fit." When not sure, return an empty string.
- details, in this order: Style code (the product's style code as given), Colorway, Release (as "Month YYYY"; for apparel "Season" with the season, e.g. "Fall/Winter 2024"), Upper (main material, short, e.g. "Leather", "Suede and mesh"; "Material" for apparel). Leave out any row you do not know.
- seo: at most 60 characters: "<product name> <style code> | {store}". Too long: drop the style code; still too long: shorten the name sensibly. Never cut a word.
- meta: at most 155 characters: "Shop the <product name> at {store}. <one fact that sets this pair apart>. 100% authentic."

Rules:
- Use only facts from the input or well-established knowledge of the model (launch year, designer, cushioning tech). If unsure, leave it out. Never invent collaborations, materials or release details.
- Text marked "copy" in the input is another shop's description: use it only as a source of facts, never reuse its sentences or distinctive phrasing.
- No prices, no shipping or delivery promises, no urgency.
- Collaborations: name the collaborator.`;

const TEXT_SCHEMA = {
  type: "object",
  properties: {
    body: { type: "array", items: { type: "string" } },
    fit: { type: "string" },
    details: {
      type: "array",
      items: {
        type: "object",
        properties: { label: { type: "string" }, value: { type: "string" } },
        required: ["label", "value"],
        additionalProperties: false
      }
    },
    seo: { type: "string" },
    meta: { type: "string" }
  },
  required: ["body", "fit", "details", "seo", "meta"],
  additionalProperties: false
};

function factSheet(product, facts) {
  const sx = facts.stockx || {};
  const goat = facts.goat || {};
  const lines = [
    `Shop title: ${product.title}`,
    `Style code: ${facts.sku}`,
    sx.name && `StockX name: ${sx.name}`,
    (sx.brand || goat.brand || product.vendor) && `Brand: ${sx.brand || goat.brand || product.vendor}`,
    sx.productType && `Type: ${sx.productType}`,
    sx.gender && `Gender: ${sx.gender}`,
    (sx.colorway || sx.color) && `Colorway: ${sx.colorway || sx.color}`,
    sx.releaseDate && `Release date: ${sx.releaseDate}`,
    (sx.season || goat.season) && `Season: ${sx.season || goat.season}`,
    facts.stockxCopy?.model && `Model: ${facts.stockxCopy.model}`,
    facts.stockxCopy?.description && `StockX copy: ${facts.stockxCopy.description.replace(/<[^>]+>/g, " ").trim()}`,
    goat.name && `GOAT name: ${goat.name}`,
    goat.colorway && `GOAT colorway: ${goat.colorway}`,
    goat.story && `GOAT copy: ${goat.story}`
  ];

  return lines.filter(Boolean).join("\n");
}

export function createDescriptionWriter({ apiKey = process.env.ANTHROPIC_API_KEY } = {}) {
  const client = new Anthropic({ apiKey });
  const usage = { input: 0, output: 0, calls: 0 };

  async function write(product, facts, storeName, problems = []) {
    const response = await client.beta.messages.create({
      model: MODEL,
      max_tokens: 16000,
      betas: ["server-side-fallback-2026-07-01"],
      fallbacks: "default",
      output_config: { effort: "medium", format: { type: "json_schema", schema: TEXT_SCHEMA } },
      system: SYSTEM.split("{store}").join(storeName),
      messages: [
        {
          role: "user",
          content:
            factSheet(product, facts) +
            (problems.length ? `\n\nYour previous version had these problems, fix them: ${problems.join("; ")}` : "")
        }
      ]
    });

    usage.calls += 1;
    usage.input += response.usage?.input_tokens || 0;
    usage.output += response.usage?.output_tokens || 0;

    if (response.stop_reason === "refusal") throw new Error("Claude declined");
    if (response.stop_reason === "max_tokens") throw new Error("Claude ran out of tokens");

    const text = response.content.filter((b) => b.type === "text").map((b) => b.text).join("");
    const parsed = JSON.parse(text);

    return { ...parsed, fit: parsed.fit?.trim() || null };
  }

  return {
    async describe(product, facts, storeName) {
      let text = await write(product, facts, storeName);
      let problems = textProblems(text, storeName);

      if (problems.length) {
        text = await write(product, facts, storeName, problems);
        problems = textProblems(text, storeName);
      }

      return { text, problems };
    },
    cost: () => ({
      ...usage,
      usd: Math.round(((usage.input * PRICE_IN + usage.output * PRICE_OUT) / 1e6) * 100) / 100
    })
  };
}

/* ------------------------------------------------------------------ *
 * One store
 * ------------------------------------------------------------------ */

const ACTIVE_PRODUCTS = `
  query activeProducts($after: String) {
    products(first: 100, after: $after, query: "status:active") {
      pageInfo { hasNextPage endCursor }
      nodes {
        id title handle vendor
        description(truncateAt: 20)
        seo { title description }
        variants(first: 1) { nodes { sku } }
      }
    }
  }
`;

const UPDATE = `
  mutation describe($product: ProductUpdateInput!) {
    productUpdate(product: $product) { product { id } userErrors { field message } }
  }
`;

/*
 * graphql(query, variables) answers with the response's `data`.
 * Dry unless apply: decides and writes the texts, but changes nothing in
 * Shopify or the library.
 */
export async function describeStore({ graphql, storeName, apply = false, max = MAX_PER_RUN, writer, stockx }) {
  const blank = [];

  for (let after = null; ; ) {
    const data = await graphql(ACTIVE_PRODUCTS, { after });
    for (const p of data.products.nodes) if (!String(p.description || "").trim()) blank.push(p);
    if (!data.products.pageInfo.hasNextPage) break;
    after = data.products.pageInfo.endCursor;
  }

  const report = { storeName, apply, blank: blank.length, fromLibrary: [], written: [], skipped: [], failed: [] };
  if (!blank.length) return report;

  const library = await readLibrary(blank.map((p) => normalizeStyle(p.variants.nodes[0]?.sku)));
  const stockxClient = stockx || createStockxClient();
  const days = 24 * 60 * 60 * 1000;
  let generated = 0;

  for (const product of blank) {
    const sku = normalizeStyle(product.variants.nodes[0]?.sku);
    const known = library.get(sku);

    try {
      if (!sku) {
        report.skipped.push({ handle: product.handle, reason: "no SKU" });
        continue;
      }

      let html;
      let seo;
      let meta;
      let fresh = null;

      if (known?.description) {
        html = known.description;
        seo = known.seo_title ? fromTemplate(known.seo_title, storeName, 60) : null;
        meta = known.seo_description ? fromTemplate(known.seo_description, storeName, 155) : null;
        report.fromLibrary.push({ handle: product.handle, sku });
      } else {
        if (known?.description_checked_at && Date.now() - new Date(known.description_checked_at) < RETRY_NO_FACTS_DAYS * days) {
          report.skipped.push({ handle: product.handle, sku, reason: "no facts last time" });
          continue;
        }

        if (!writer) {
          report.skipped.push({ handle: product.handle, sku, reason: "no ANTHROPIC_API_KEY" });
          continue;
        }

        if (generated >= max) {
          report.skipped.push({ handle: product.handle, sku, reason: `run limit of ${max}` });
          continue;
        }

        const facts = await gatherFacts(stockxClient, sku);

        if (!facts) {
          report.skipped.push({ handle: product.handle, sku, reason: "no StockX or GOAT match" });
          if (apply) await saveLibrary({ style_code: sku, description_checked_at: new Date().toISOString() });
          continue;
        }

        generated += 1;
        const { text, problems } = await writer.describe(product, facts, storeName);

        if (problems.length) {
          report.failed.push({ handle: product.handle, sku, reason: problems.join("; ") });
          continue;
        }

        html = renderDescription(text);
        seo = text.seo;
        meta = text.meta;
        fresh = {
          style_code: sku,
          description: html,
          description_source: facts.stockx ? "auto-stockx" : "auto-goat",
          seo_title: toTemplate(seo, storeName),
          seo_description: toTemplate(meta, storeName)
        };
        report.written.push({ handle: product.handle, sku, source: fresh.description_source, seo, meta, body: text.body, fit: text.fit });
      }

      if (!apply) continue;

      // The store's own SEO text stays; ours only fills what is empty.
      const seoInput = {};
      if (seo && !product.seo?.title) seoInput.title = seo;
      if (meta && !product.seo?.description) seoInput.description = meta;

      const result = await graphql(UPDATE, {
        product: { id: product.id, descriptionHtml: html, ...(Object.keys(seoInput).length ? { seo: seoInput } : {}) }
      });
      const errors = result.productUpdate.userErrors;
      if (errors.length) throw new Error(errors.map((e) => e.message).join("; "));

      if (fresh) await saveLibrary(fresh);
    } catch (error) {
      report.failed.push({ handle: product.handle, sku, reason: error.message });
    }
  }

  report.cost = writer ? writer.cost() : null;
  return report;
}
