/*
 * Pages for pairs a store chose but nobody holds.
 *
 * The consignment run only builds a page for a pair with stock behind it. On
 * a store with "Express Option?" a pair without stock can still be sold, as
 * Standard at the autopricing price, so the page has to exist anyway. This
 * builds it the way the consignment run would - StockX name and size ladder,
 * the photo library, the store's own photo treatment - with every Express
 * size sold out. standardWriter.js then adds the Standard sizes.
 *
 * A pair with no photo anywhere is made as a draft: a page without a
 * picture is worse than no page, and the store can finish it by hand.
 */
import { createShopifyWriter } from "./shopifyWriter.js";
import { createStockxClient } from "./stockxSizes.js";
import { ladderFor } from "./sizeLadders.js";
import { photosForSkus, photosFor } from "./storePhotos.js";
import { wantsNormalizedPhotos, normalizedPhotoUrls } from "./photoNormalize.js";
import { mirrorPages } from "./consignmentRun.js";
import { sortEuSizes } from "./storePricing.js";
import { normalizeSize } from "./sizes.js";

const SEARCH = "https://api.stockx.com/v2/catalog/search";

async function stockxProduct(stockx, sku) {
  const search = await stockx.call(`${SEARCH}?query=${encodeURIComponent(sku)}`);

  return (search?.products || []).find((p) => String(p.styleId || "").toUpperCase() === sku) || null;
}

/*
 * skus:          the style codes that have open autoprice sizes but no page
 * listingsBySku: Map(sku -> [{ size, price }]), the open autoprice rows
 * merchant:      Merchants fields plus "Record ID" (and "Store Name")
 * photoSources:  the stores we take photographs from (storePhotos.photoSources)
 */
export async function createMissingPages({
  graphql, merchant, skus, listingsBySku, photoSources = [], drafts = new Set(),
  supabaseUrl, supabaseKey, apply = false, log = console
}) {
  const result = { created: [], drafts: [], skipped: [], problems: [] };

  if (!skus.length) return result;

  const locationId = merchant["Shopify Location ID"];
  const writer = createShopifyWriter({ graphql, locationId, apply, expressOption: true });
  const stockx = createStockxClient();

  const { ladders } = await ladderFor(skus, { stockx });
  const { photos } = await photosForSkus(skus, { sources: photoSources });
  const ref = { recordId: merchant["Record ID"], name: merchant["Store Name"] || "" };
  const normalize = wantsNormalizedPhotos(ref);

  for (const sku of skus) {
    try {
      const prices = listingsBySku.get(sku) || [];

      if (!prices.length) { result.skipped.push({ sku, reason: "no open price" }); continue; }

      const hit = await stockxProduct(stockx, sku).catch(() => null);

      if (!hit) { result.skipped.push({ sku, reason: "not found on StockX" }); continue; }

      const ladder = sortEuSizes(
        (ladders.get(sku)?.length ? ladders.get(sku) : prices.map((p) => p.size)).map(normalizeSize)
      );

      const urls = photosFor(photos, sku);
      const images = normalize && urls.length ? await normalizedPhotoUrls(urls, { supabaseUrl, supabaseKey }) : urls;
      const draft = drafts.has(sku) || !images?.length;

      /*
        Express at the dearest Standard price and sold out. It cannot be
        bought, and a sold-out size at nought would read as "from 0" on a
        card that takes the lowest price of the page.
      */
      const placeholder = Math.max(...prices.map((p) => Number(p.price) || 0));

      const productId = await writer.createProduct(
        {
          sku,
          title: hit.title || sku,
          brand: hit.brand || "",
          draft,
          sizes: ladder,
          variants: [{ size: ladder[0], price: placeholder, quantity: 0 }]
        },
        { photos: draft ? [] : images }
      );

      (draft ? result.drafts : result.created).push({ sku, productId, title: hit.title, photos: images?.length || 0 });
    } catch (error) {
      result.problems.push({ sku, failure: error instanceof Error ? error.message : String(error) });
    }
  }

  result.problems.push(...(writer.report.problems || []));

  const made = [...result.created, ...result.drafts].map((r) => r.sku);

  if (apply && made.length && supabaseUrl) {
    await mirrorPages({
      graphql,
      merchant: ref,
      skus: made, supabaseUrl, supabaseKey
    }).catch((error) => result.problems.push({ sku: "-", failure: "mirror: " + error.message }));
  }

  log.log("STANDARD PAGES " + JSON.stringify({
    apply, created: result.created.length, drafts: result.drafts.length,
    skipped: result.skipped.length, problems: result.problems.slice(0, 10)
  }));

  return result;
}
