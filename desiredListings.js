/*
 * What a store's shelves should hold, before anything is written to Shopify.
 *
 * The whole branch is easier to trust if the deciding is separated from the
 * writing, so this answers one question and touches nothing: given a store,
 * which pairs belong in it, at what price, and in which sizes.
 *
 * Four steps, and each one can be wrong on its own:
 *
 *   1. every consignment pair we can see
 *   2. the cheapest holder per SKU and size, on one comparable scale
 *   3. the store's own stock dropped, because a store may not be sold its own
 *   4. our margin on top, then the store's margin gate
 *
 * Nothing here talks to Shopify. See storePricing.js for the arithmetic in
 * step four, which is shared with the Airtable formulas that decide what we
 * pay on every order.
 */
import { maximumBuyingPrice, minimumSellingPrice, passesMarginGate, storeVatRate } from "./storePricing.js";
import { normalizeSize as sizeKey } from "./sizes.js";

const GRID = 2.5;

/* ------------------------------------------------------------------ *
 * Reading the stock
 * ------------------------------------------------------------------ */

function num(value) {
  const parsed = Number(Array.isArray(value) ? value[0] : value);

  return Number.isFinite(parsed) ? parsed : 0;
}

// The same, but keeping the difference between "zero" and "not filled in".
function maybeNum(value) {
  const raw = Array.isArray(value) ? value[0] : value;

  if (raw === null || raw === undefined || raw === "") return null;

  const parsed = Number(raw);

  return Number.isFinite(parsed) ? parsed : null;
}

function text(value) {
  return String((Array.isArray(value) ? value[0] : value) ?? "").trim();
}

/*
 * One SKU, written the same way everywhere.
 *
 * store_listings holds them upper case, and a search that normalises to
 * anything else silently misses rows. So everything that is going to be
 * compared passes through here first.
 */
export function normalizeSku(value) {
  return text(value).toUpperCase().replace(/\s+/g, "");
}

/*
 * A size label, however the store wrote it.
 *
 * Kept as a re-export rather than a second implementation, because this rule
 * also decides what goes INTO store_listings, and the two drifting apart is
 * exactly what broke: the table was filled by a regex that took the first
 * number, so "38 2/3" was stored as "38".
 */
export { sizeKey };

/*
 * Two asks on one scale, so the cheapest is really the cheapest.
 *
 * A VAT0 ask is quoted without tax and a margin-scheme ask includes it, so
 * comparing them as they stand hands the deal to whoever happens to be on the
 * cheaper-looking regime. This is the same conversion the marketplace side
 * uses to pick a consignor, and it exists only for comparing - never for
 * paying anyone.
 */
export function comparableAsk(price, vatType) {
  const amount = num(price);
  const type = text(vatType).toUpperCase().replace(/\s/g, "");

  return type === "VAT0" ? amount * 1.21 : amount;
}

/* ------------------------------------------------------------------ *
 * Our margin
 * ------------------------------------------------------------------ */

/*
 * What the store pays us for a consignment pair.
 *
 * Lifted from getStoreConsignmentShopPrice in the portal, deliberately
 * unchanged, so a pair costs a store the same here as it does in the Lojiq
 * shop. A store that finds one price on its own shelf and another in the shop
 * stops trusting both.
 *
 * The order matters. The ask first, bare. Then our mark-up, capped. Then, on
 * margin goods, that mark-up grossed up by 1.21, because there is no VAT to
 * reclaim on them and a flat ten would otherwise earn us less than the same
 * ten on a VAT21 pair. Then the same 2.50 grid the offer side rounds to, so
 * the shop can never quietly undercut our own offer. Only then VAT, and only
 * for the types that carry it.
 *
 * TWO numbers come back, and mixing them up is the trap this branch is most
 * likely to fall into. `net` is what the pair costs before VAT, which is the
 * scale the margin gate works in - the Airtable formula divides the room by
 * one plus the rate for exactly this reason. `gross` is what the invoice
 * says, VAT and all.
 *
 * Feeding `gross` into the gate looks harmless and is not: on a VAT21 pair it
 * counts the tax twice and asks the store for a fifth more than it needs to,
 * which is a pair that quietly never gets listed. I did it that way first.
 * Margin goods carry no VAT, so there the two are the same number.
 */
export function storeCost({ ask, vatType, merchantFields, inventoryType = "all" }) {
  const bare = num(ask);

  if (bare <= 0) return null;

  const type = text(vatType).toUpperCase().replace(/\s/g, "");
  const base = type === "VAT21" ? bare / 1.21 : bare;

  const method = text(merchantFields?.["Offer Method"]);
  const pct = num(merchantFields?.["Offer Percentage"]);
  const flat = maybeNum(merchantFields?.["Offer Margin"]);
  const cap = num(merchantFields?.["Margin Cap"]);

  let withMargin;

  if (method === "Firm Range") {
    /*
      FIXED - a mark-up of zero was read as no mark-up at all.

      The portal checks whether the number exists; this copy checked whether
      it was truthy, and nothing tells those apart until a store sets it to
      zero on purpose. UNION Amsterdam is our own shop, so Lojiq takes nothing
      on it, and every pair fell out of the list as "no margin configured".

      An empty field still refuses. A zero means zero: the store pays what the
      consignor asks.
    */
    if (flat === null) return null;

    withMargin = base + flat;
  } else if (pct > 0) {
    withMargin = Math.max(base + 10, base * (1 + pct) + 5);
  } else if (flat !== null && flat > 0) {
    withMargin = base + flat;
  } else {
    return null;
  }

  if (cap > 0) withMargin = Math.min(withMargin, base + cap);

  const earned = type === "MARGIN" ? base + (withMargin - base) * 1.21 : withMargin;
  const onGrid = Math.round(earned / GRID) * GRID;

  const net = Math.ceil(onGrid);

  const showsVat = inventoryType === "all" && type !== "MARGIN";

  if (!showsVat) return { net, gross: net };

  /*
    No rate, no invoice. The gate needs it too, so guessing here would only
    move the wrong answer one step further along. Margin goods never reach
    this line, which is why a store without a rate can still be sold those.
  */
  const rate = storeVatRate(merchantFields);

  if (rate === null) return null;

  return { net, gross: Math.ceil(onGrid * (1 + rate)) };
}

/* ------------------------------------------------------------------ *
 * The list
 * ------------------------------------------------------------------ */

/*
 * The cheapest holder of each SKU and size, with this store's own stock left
 * out.
 *
 * A store that consigns with us would otherwise be offered its own pairs
 * back, priced with our margin on top. Nobody has both roles today, which is
 * exactly why it goes in now: the day somebody does, nobody will remember.
 * The same rule already keeps a buyer's own pairs out of the Lojiq shop.
 */
export function cheapestPerPair(inventoryRows, { excludeSellerRecordId } = {}) {
  const best = new Map();

  for (const row of inventoryRows || []) {
    if (num(row.quantity) <= 0) continue;

    if (excludeSellerRecordId && text(row.seller_record_id) === text(excludeSellerRecordId)) {
      continue;
    }

    const sku = normalizeSku(row.sku);
    const size = sizeKey(row.size);

    if (!sku || !size) continue;

    const ask = num(row.selling_price_suggested);

    if (ask <= 0) continue;

    const key = `${sku}|${size}`;
    const comparable = comparableAsk(ask, row.vat_type);
    const held = best.get(key);

    if (!held || comparable < held.comparable) {
      best.set(key, {
        sku,
        size,
        ask,
        comparable,
        vatType: text(row.vat_type),
        sellerId: text(row.seller_id),
        sellerRecordId: text(row.seller_record_id),
        productName: text(row.product_name),
        brand: text(row.brand),
        quantity: num(row.quantity),
        inventoryId: text(row.id)
      });
    }
  }

  return [...best.values()];
}

/*
 * What belongs on the shelf, and what does not, with the reason.
 *
 * Every pair comes back either way. A silent list of what made it through
 * says nothing about the ones that did not, and "why is this pair not in my
 * shop" is the question this branch will be asked most.
 *
 * currentPrices maps "SKU|size" to what the store asks for it today, read
 * live from Shopify rather than from our copy of its catalogue. With Price
 * Sync on it is not needed: we set the price, so we set it to whatever clears
 * the gate.
 */
export function buildDesiredListings({
  inventoryRows,
  merchantFields,
  currentPrices = new Map(),
  priceSync = false,
  excludeSellerRecordId = null,
  inventoryType = "all"
}) {
  const pairs = cheapestPerPair(inventoryRows, { excludeSellerRecordId });

  const listings = [];
  const rejected = [];

  for (const pair of pairs) {
    const cost = storeCost({
      ask: pair.ask,
      vatType: pair.vatType,
      merchantFields,
      inventoryType
    });

    if (cost === null) {
      /*
        Two ways to get here, and they are worth telling apart: the store has
        no mark-up rule at all, or it has no VAT rate and this pair carries
        tax. The second is almost always a field nobody filled in.
      */
      const needsRate = !["MARGIN"].includes(text(pair.vatType).toUpperCase().replace(/\s/g, ""));

      rejected.push({
        ...pair,
        reason: needsRate && storeVatRate(merchantFields) === null
          ? "no_vat_rate_for_store"
          : "no_store_margin_configured"
      });

      continue;
    }

    if (priceSync) {
      const selling = minimumSellingPrice({
        buyingPrice: cost.net,
        vatType: pair.vatType,
        merchantFields
      });

      if (selling === null) {
        rejected.push({ ...pair, cost: cost.net, reason: "no_margin_method_configured" });
        continue;
      }

      listings.push({ ...pair, cost: cost.net, invoiced: cost.gross, sellingPrice: selling, priceSetByUs: true });
      continue;
    }

    const current = num(currentPrices.get(`${pair.sku}|${pair.size}`));

    if (current <= 0) {
      /*
        No price of its own means the store does not sell this pair yet. With
        Price Sync off we do not invent one, because the gate has nothing to
        judge and listing it would put stock behind a price nobody set.
      */
      rejected.push({ ...pair, cost: cost.net, reason: "no_price_in_store" });
      continue;
    }

    const allowed = maximumBuyingPrice({
      sellingPrice: current,
      vatType: pair.vatType,
      merchantFields
    });

    if (!passesMarginGate({ buyingPrice: cost.net, sellingPrice: current, vatType: pair.vatType, merchantFields })) {
      rejected.push({
        ...pair,
        cost: cost.net,
        sellingPrice: current,
        allowed,
        shortBy: allowed === null ? null : Math.round((cost.net - allowed) * 100) / 100,
        reason: "margin_too_thin"
      });

      continue;
    }

    listings.push({ ...pair, cost: cost.net, invoiced: cost.gross, sellingPrice: current, priceSetByUs: false });
  }

  return { listings, rejected };
}
