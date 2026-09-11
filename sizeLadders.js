/*
 * The size ladder for a style code, asked once and kept.
 *
 * The sizes a shoe exists in never change, so asking StockX every run is
 * paying the same toll forever. It also does not survive contact with reality:
 * StockX allows a burst of about eight requests and a ladder costs two of
 * them, so a first run over a thousand style codes spends most of its time
 * waiting to be allowed to ask again.
 *
 * SKU Master is where it lives, next to the picture and the product name that
 * came from the same place. Two fields:
 *
 *   EU Sizes            the ladder, in order, comma separated
 *   EU Sizes Checked At when it was last asked
 *
 * A date with an empty ladder is an answer too: StockX does not know this
 * pair. Worth remembering, or every run asks again about the same unknowns.
 */
import { euSizesForSku } from "./stockxSizes.js";
import { sortEuSizes } from "./storePricing.js";

const TABLE = process.env.AIRTABLE_SKU_MASTER_TABLE || "SKU Master";

// How long an "asked and not found" stands before it is worth asking again.
const RETRY_UNKNOWN_AFTER_DAYS = Number(process.env.SIZE_LADDER_RETRY_DAYS || 30);

function airtableUrl(pathPart = "", params = {}) {
  const url = new URL(
    `https://api.airtable.com/v0/${process.env.AIRTABLE_BASE_ID}/${encodeURIComponent(TABLE)}${pathPart}`
  );

  Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, value));

  return url;
}

const headers = () => ({
  Authorization: `Bearer ${process.env.AIRTABLE_TOKEN}`,
  "Content-Type": "application/json"
});

function escapeFormulaValue(value) {
  return String(value ?? "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

/*
 * What we already know, for a batch of style codes.
 *
 * Asked in one query per hundred rather than one per code: this runs over
 * every SKU in a store and Airtable pages at a hundred either way.
 */
export async function readKnownLadders(skus) {
  const wanted = [...new Set((skus || []).map((s) => String(s || "").trim().toUpperCase()))].filter(Boolean);

  const known = new Map();

  for (let i = 0; i < wanted.length; i += 100) {
    const batch = wanted.slice(i, i + 100);

    const formula = `OR(${batch.map((sku) => `{SKU} = '${escapeFormulaValue(sku)}'`).join(",")},FALSE())`;

    let offset;

    do {
      const params = { filterByFormula: formula, pageSize: "100" };

      ["SKU", "EU Sizes", "EU Sizes Checked At"].forEach((field, index) => {
        params[`fields[${index}]`] = field;
      });

      if (offset) params.offset = offset;

      const res = await fetch(airtableUrl("", params), { headers: headers() });
      const data = await res.json();

      if (!res.ok) throw new Error(`SKU Master lezen: ${res.status}`);

      for (const record of data.records || []) {
        const sku = String(record.fields?.["SKU"] || "").trim().toUpperCase();

        if (!sku) continue;

        known.set(sku, {
          recordId: record.id,
          sizes: String(record.fields?.["EU Sizes"] || "")
            .split(",")
            .map((size) => size.trim())
            .filter(Boolean),
          checkedAt: record.fields?.["EU Sizes Checked At"] || null
        });
      }

      offset = data.offset;
    } while (offset);
  }

  return known;
}

function worthAsking(entry) {
  if (!entry) return true;
  if (entry.sizes.length) return false;
  if (!entry.checkedAt) return true;

  const age = Date.now() - new Date(entry.checkedAt).getTime();

  return age > RETRY_UNKNOWN_AFTER_DAYS * 24 * 60 * 60 * 1000;
}

async function remember(entry, sku, sizes) {
  const fields = {
    "EU Sizes": sizes.join(", "),
    "EU Sizes Checked At": new Date().toISOString()
  };

  if (entry?.recordId) {
    await fetch(airtableUrl(`/${entry.recordId}`), {
      method: "PATCH",
      headers: headers(),
      body: JSON.stringify({ fields })
    });

    return;
  }

  /*
    A style code we have stock of but no master record for. Creating one is
    right: SKU Master is meant to hold every code we touch, and the picture
    enrichment picks it up from there afterwards.
  */
  await fetch(airtableUrl(), {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ fields: { SKU: sku, ...fields } })
  });
}

/*
 * Ladders for a list of style codes, asking StockX only for what is missing.
 *
 * onProgress exists because the first run is slow by design - the throttle in
 * the StockX client is what keeps it inside the quota - and a run that prints
 * nothing for ten minutes looks broken.
 */
export async function ladderFor(skus, { stockx, onProgress = () => {} } = {}) {
  const known = await readKnownLadders(skus);
  const ladders = new Map();

  const wanted = [...new Set((skus || []).map((s) => String(s || "").trim().toUpperCase()))].filter(Boolean);

  let asked = 0;

  for (const sku of wanted) {
    const entry = known.get(sku);

    if (!worthAsking(entry)) {
      ladders.set(sku, entry.sizes);
      continue;
    }

    if (!stockx) {
      ladders.set(sku, entry?.sizes || []);
      continue;
    }

    try {
      const sizes = sortEuSizes(await euSizesForSku(stockx, sku));

      ladders.set(sku, sizes);
      await remember(entry, sku, sizes);

      asked += 1;
      onProgress({ sku, sizes: sizes.length, asked, total: wanted.length });
    } catch (err) {
      // One unreachable ladder must not stop the rest. The pair still gets a
      // page, built from the sizes we hold.
      ladders.set(sku, entry?.sizes || []);
      onProgress({ sku, error: err.message, asked, total: wanted.length });
    }
  }

  return { ladders, asked, known: wanted.length - asked };
}
