/*
 * The difference between what a store has and what it should have.
 *
 * Still nothing written. This takes the desired list, the store's current
 * catalogue and the size ladder for each SKU, and says what would have to
 * happen. The writing step then does only what this says, which keeps the
 * deciding testable and the writing dumb.
 *
 * Five kinds of work come out, and they are deliberately separate because
 * they carry different risk:
 *
 *   createProducts   a SKU the store does not have at all
 *   addSizes         a SKU it has, missing a size - the awkward one, see below
 *   setQuantities    stock we supply, on our own location
 *   setPrices        only when we are allowed to set them
 *   clearQuantities  what the store still lists and we no longer supply
 *
 * addSizes is awkward on purpose. Shopify orders variants by the order of the
 * option's values, and a variant added later lands at the end of that list -
 * which is how a product page ends up showing 44 before 38. So a product is
 * created with its whole ladder at once, and anything added afterwards has to
 * put the option values back in order. The plan says so rather than leaving
 * the writer to remember.
 */
import { sortEuSizes, parseEuSize } from "./storePricing.js";
import { normalizeSku, sizeKey } from "./desiredListings.js";

/*
 * What the store has today, in one shape.
 *
 * Whether it came from Shopify just now or from our copy of its catalogue,
 * the planner should not care. It does care that the keys match, so both
 * sides go through the same SKU and size normalising as the desired list.
 */
export function indexCurrentListings(rows) {
  const bySku = new Map();

  for (const row of rows || []) {
    const sku = normalizeSku(row.sku);
    const size = sizeKey(row.size);

    if (!sku) continue;

    if (!bySku.has(sku)) {
      bySku.set(sku, { sku, productId: row.productId || null, variants: new Map() });
    }

    const product = bySku.get(sku);

    if (!product.productId && row.productId) product.productId = row.productId;

    if (!size) continue;

    product.variants.set(size, {
      size,
      variantId: row.variantId || null,
      inventoryItemId: row.inventoryItemId || null,
      price: Number(row.price) || 0,
      quantity: Number(row.quantity) || 0
    });
  }

  return bySku;
}

/*
 * The ladder a product is created with.
 *
 * Every size the pair exists in, not only the ones we can fill. A size that
 * is on the page but sold out can be filled later without touching the
 * ordering, and it is what the store needs the day it starts dropshipping.
 * Sizes we hold but the ladder does not mention are added anyway - a real
 * pair in a real box beats a catalogue.
 */
export function ladderFor({ sku, knownSizes, heldSizes }) {
  const all = new Set();

  for (const size of knownSizes || []) {
    const clean = sizeKey(size);
    if (clean && parseEuSize(clean) !== null) all.add(clean);
  }

  for (const size of heldSizes || []) {
    const clean = sizeKey(size);
    if (clean) all.add(clean);
  }

  return sortEuSizes([...all]);
}

/*
 * What has to happen, per store.
 *
 * `productSync` off means we never create anything: a store's product pages
 * are its own, and we only fill sizes it already sells. That is the setting
 * every store but our own runs with.
 */
export function planListings({
  desired,
  current,
  sizeLadders = new Map(),
  productSync = false,
  priceSync = false,
  sellWithoutStock = false
}) {
  const createProducts = [];
  const addSizes = [];
  const setQuantities = [];
  const setPrices = [];
  const clearQuantities = [];
  const skipped = [];

  const wantedBySku = new Map();

  for (const listing of desired || []) {
    if (!wantedBySku.has(listing.sku)) wantedBySku.set(listing.sku, []);
    wantedBySku.get(listing.sku).push(listing);
  }

  for (const [sku, listings] of wantedBySku) {
    const product = current.get(sku);
    const heldSizes = listings.map((l) => l.size);

    if (!product) {
      if (!productSync) {
        listings.forEach((l) => skipped.push({ ...l, reason: "product_not_in_store" }));
        continue;
      }

      const first = listings[0];

      createProducts.push({
        sku,
        title: first.productName || sku,
        brand: first.brand || "",
        /*
          The whole ladder, in order, in one go. Adding sizes later is what
          shuffles a product page, so a page is born complete: the sizes we
          can fill carry stock, the rest are there and sold out.
        */
        sizes: ladderFor({ sku, knownSizes: sizeLadders.get(sku), heldSizes }),
        variants: listings.map((l) => ({
          size: l.size,
          price: l.sellingPrice,
          quantity: l.quantity,
          cost: l.cost
        })),
        /*
          Sold out has to mean sold out on a page we create. Most of these
          stores let a customer buy what is not in stock, which is fine for
          their own goods and wrong for consignment: the pair is in somebody
          else's hands and may be gone tomorrow.
        */
        continueSellingWhenOutOfStock: false
      });

      continue;
    }

    const missing = listings.filter((l) => !product.variants.has(l.size));

    if (missing.length) {
      if (!productSync) {
        missing.forEach((l) => skipped.push({ ...l, reason: "size_not_in_store" }));
      } else {
        addSizes.push({
          sku,
          productId: product.productId,
          sizes: missing.map((l) => l.size),
          /*
            After adding, the option's values have to be put back in order or
            the new size sits at the end of the list on the page. This is the
            whole reason a product is created with its full ladder.
          */
          reorderTo: ladderFor({
            sku,
            knownSizes: sizeLadders.get(sku),
            heldSizes: [...product.variants.keys(), ...heldSizes]
          })
        });
      }
    }

    for (const listing of listings) {
      const variant = product.variants.get(listing.size);

      if (!variant) continue;

      if (variant.quantity !== listing.quantity) {
        setQuantities.push({
          sku,
          size: listing.size,
          variantId: variant.variantId,
          inventoryItemId: variant.inventoryItemId,
          from: variant.quantity,
          to: listing.quantity
        });
      }

      if (priceSync && Number(variant.price) !== Number(listing.sellingPrice)) {
        setPrices.push({
          sku,
          size: listing.size,
          /*
            FIXED - missing, so the writer sent its "DRY" placeholder as the
            product and Shopify refused every price update. The refusal is
            kept as a problem rather than thrown, so nothing looked wrong:
            no price was ever changed by this push.
          */
          productId: product.productId,
          variantId: variant.variantId,
          from: variant.price,
          to: listing.sellingPrice
        });
      }
    }
  }

  /*
   * And what the store still has from us that we no longer supply.
   *
   * Stock goes to zero, the page stays. Archiving loses the handle and
   * whatever the page has earned in search, and the same pair comes back next
   * week through another consignor. Tidying up is a separate job, and one
   * that should only ever touch pages we made ourselves.
   */
  for (const [sku, product] of current) {
    const wanted = wantedBySku.get(sku);
    const wantedSizes = new Set((wanted || []).map((l) => l.size));

    for (const [size, variant] of product.variants) {
      if (wantedSizes.has(size)) continue;
      if (!variant.quantity) continue;

      clearQuantities.push({
        sku,
        size,
        variantId: variant.variantId,
        inventoryItemId: variant.inventoryItemId,
        from: variant.quantity,
        to: 0,
        // Worth knowing when reading a plan: on a store that oversells, zero
        // does not stop a sale. Nothing here can fix that from our side.
        stillSellableAtZero: sellWithoutStock
      });
    }
  }

  return { createProducts, addSizes, setQuantities, setPrices, clearQuantities, skipped };
}
