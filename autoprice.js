/*
 * Autopricing: a price for a size the store does not hold itself.
 *
 * The pair is sourced after the sale, so the price is our cheapest way to
 * buy it plus the store's own margin from Merchants - the same arithmetic as
 * a consignment pair, only with a sourcing platform as the seller.
 *
 * This file only decides. It reads nothing and writes nothing, so every rule
 * below can be tested with plain numbers (autoprice.test.mjs).
 *
 * The rules, in the order they apply:
 *
 *   1. Only fresh costs count. A platform read that is too old is ignored,
 *      and a size with no fresh cost left is closed rather than sold on a
 *      number we can no longer stand behind.
 *   2. StockX Pro is what we really pay. When Pro has not been read in time
 *      (the browser was logged out, say) the public API stands in, converted
 *      to a cost that was never below Pro in the calibration - so we are a
 *      little dear for a while, never under water.
 *   3. Dewu only up to its ceiling: the supplier does not accept offers above
 *      it, so a dearer Dewu price is a pair we would never actually get.
 *   4. Only the sources the store allows (Merchants "Autoprice Sources";
 *      empty is all of them), and a Dewu cost carries the store's Dewu
 *      markup (Merchants "Autoprice Dewu Markup") before its margin.
 *   5. The cheapest remaining cost, with the store's margin on top.
 *   6. Up at once, down slowly. A rising cost moves the price straight away,
 *      because selling below it loses money. A falling cost is followed only
 *      a few percent a day, so one seller dumping a single pair for a day
 *      does not drag the whole shelf down with it.
 */
import { downToEnding, minimumSellingPrice, priceEnding, upToEnding } from "./storePricing.js";

export const SETTINGS = {
  // How old a read may be before it stops counting, in hours.
  maxAgeHours: { stockx_pro: 36, stockx_api: 36, dewu: 48, goat: 48 },
  /*
    The Dewu supplier never accepts an offer above this all-in cost.

    CHANGED - 210, while the Outsource Engine and the partner flow stop at
    200: a size priced on a Dewu cost of 205 could sell and then never be
    offered to the partner (21-11-2026).
  */
  dewuMaxCost: 200,
  // How far a price may come down per day, as a share of the shown price.
  dropPerDay: 0.05,
  /*
    How each source's cost is taxed, and so how it is priced.

    CHANGED - everything used to be priced as margin goods. StockX's cost,
    from Pro and from the API alike, is quoted without VAT: a pair bought
    there comes with a VAT invoice and is sold with 21% VAT on the whole
    price. Priced as margin goods it came out about a fifth too cheap
    (HF3713-002 42 sold at 165,95 on a 117,28 cost, 21-11-2026). Dewu and
    GOAT are bought without a VAT invoice and stay margin goods.
  */
  vatTypeBySource: { stockx_pro: "VAT21", stockx_api: "VAT21", dewu: "Margin", goat: "Margin" }
};

const vatTypeOf = (source, settings) => settings.vatTypeBySource?.[source] || "Margin";

// Merchants "Autoprice Sources" names a platform; a cost row names a feed.
const PLATFORM = { stockx_pro: "stockx", stockx_api: "stockx", goat: "goat", dewu: "dewu" };

/*
 * The platforms a store wants priced from. Empty is all of them - how every
 * store priced before the field existed.
 */
export function allowedSources(merchantFields) {
  const raw = merchantFields?.["Autoprice Sources"];
  const names = (Array.isArray(raw) ? raw : String(raw || "").split(","))
    .map((name) => String(name?.name ?? name).trim().toLowerCase())
    .filter(Boolean);

  return names.length ? new Set(names) : null;
}

/*
 * What a store adds to a Dewu cost before its margin, in euros.
 *
 * Lojiq's own cut on an Asia pair. It lifts the shop price, and with it the
 * store's Maximum Buying Price, to about this much above Dewu - the room the
 * Outsource Engine asks for (Merchants "Asia Min Spread") before it offers
 * the order to the partner. Kept apart from that threshold on purpose: Dewu
 * moves between pricing and the order. Empty is none, as for UNION.
 */
export function dewuMarkup(merchantFields) {
  const amount = Number(merchantFields?.["Autoprice Dewu Markup"]);

  return Number.isFinite(amount) && amount > 0 ? amount : 0;
}

/*
 * StockX's public lowest ask, made into a cost we can safely sell against.
 *
 * Measured on 818 sizes (16-11-2026): the Buy Now we get on Pro was never
 * above 97% of the API ask. The fee is at most 8%, capped, plus shipping.
 */
export function safeStockxCost(lowestAsk) {
  const ask = Number(lowestAsk);

  if (!Number.isFinite(ask) || ask <= 0) return null;

  const buyNow = ask * 0.97;

  return Math.round((buyNow + Math.min(buyNow * 0.08, 90.7) + 10.95) * 100) / 100;
}

/*
 * GOAT's French dollar price as what we pay in the Netherlands.
 *
 * The NL page was 0.913-0.930 times the FR dollar price on 235 sizes over
 * eleven products, tax included. Shipping is a flat 15 euros on top.
 */
export const GOAT_FX = 0.925;
export const GOAT_SHIPPING = 15;

export function goatCost(frUsd) {
  const usd = Number(frUsd);

  if (!Number.isFinite(usd) || usd <= 0) return null;

  return Math.round((usd * GOAT_FX + GOAT_SHIPPING) * 100) / 100;
}

/*
 * goat.com's own Dutch price, read by the autopricing extension: the lowest
 * new ask in a good box, in euro cents, costs and taxes included as the page
 * shows it. Plus the same flat shipping, as the Outsource Engine counts it.
 */
export function goatEuroCost(cents) {
  const amount = Number(cents);

  if (!Number.isFinite(amount) || amount <= 0) return null;

  return Math.round((Math.round(amount) / 100 + GOAT_SHIPPING) * 100) / 100;
}

// Dewu's RMB price as our all-in cost, the same formula the Outsource Engine uses.
export function dewuCost(rmb, fee) {
  const yuan = Number(rmb);

  if (!Number.isFinite(yuan) || yuan <= 0) return null;

  return Math.round(((yuan - 200) / 8 + Number(fee || 0)) * 100) / 100;
}

const hoursBetween = (from, to) => (new Date(to) - new Date(from)) / 3600000;

/*
 * Which costs may be used right now, cheapest first.
 *
 * observations: { stockx_pro: {cost, observedAt}, stockx_api: {...}, dewu: {...}, goat: {...} }
 */
export function usableCosts(observations, { now = new Date(), settings = SETTINGS, sources = null } = {}) {
  const fresh = (source) => {
    if (sources && !sources.has(PLATFORM[source])) return null;

    const row = observations?.[source];

    if (!row || !(Number(row.cost) > 0) || !row.observedAt) return null;

    return hoursBetween(row.observedAt, now) <= settings.maxAgeHours[source] ? Number(row.cost) : null;
  };

  const out = [];

  // Pro when we have it; the API only stands in when Pro is missing or stale.
  const pro = fresh("stockx_pro");
  const api = fresh("stockx_api");

  if (pro) out.push({ source: "stockx_pro", cost: pro });
  else if (api) out.push({ source: "stockx_api", cost: api });

  const dewu = fresh("dewu");

  if (dewu && dewu <= settings.dewuMaxCost) out.push({ source: "dewu", cost: dewu });

  const goat = fresh("goat");

  if (goat) out.push({ source: "goat", cost: goat });

  return out.sort((a, b) => a.cost - b.cost);
}

/*
 * The decision for one size.
 *
 * previous: what was decided last time ({ price, status, pricedAt }) or null.
 * pricedAt is when the price last CHANGED, not when it was last looked at:
 * the slow way down counts from there, so a run every hour still lets the
 * price fall instead of rounding each tiny step back up to where it was.
 *
 * Returns { status, source, cost, targetPrice, price, pricedAt, reason }.
 */
export function priceSize({ observations, previous = null, merchantFields, now = new Date(), settings = SETTINGS }) {
  const costs = usableCosts(observations, { now, settings, sources: allowedSources(merchantFields) });
  const markup = dewuMarkup(merchantFields);

  if (!costs.length) {
    return { status: "closed", source: null, cost: null, targetPrice: null, price: null, pricedAt: null, reason: "no fresh cost" };
  }

  /*
    The cheapest cost is not always the cheapest price: a StockX cost carries
    VAT on top and a Dewu cost does not. So every source is priced, and the
    lowest selling price wins.
  */
  const priced = costs
    .map((c) => ({
      ...c,
      // The ceiling above is on what the partner is paid; the markup is ours on top.
      target: minimumSellingPrice({
        buyingPrice: c.source === "dewu" ? c.cost + markup : c.cost,
        vatType: vatTypeOf(c.source, settings),
        merchantFields
      })
    }))
    .filter((c) => c.target);
  const best = priced.sort((a, b) => a.target - b.target)[0] || costs[0];
  const targetPrice = best.target || null;

  if (!targetPrice) {
    return { status: "closed", source: best.source, cost: best.cost, targetPrice: null, price: null, pricedAt: null, reason: "no margin rule" };
  }

  const shown = previous?.status === "open" ? Number(previous.price) : null;

  // Nothing to slow down from, or the cost went up: the target as it is.
  if (!shown || targetPrice >= shown) {
    return {
      status: "open", source: best.source, cost: best.cost, targetPrice, price: targetPrice,
      pricedAt: targetPrice === shown ? previous.pricedAt || now : now,
      reason: shown ? "up or equal" : "new"
    };
  }

  // Coming down: no further than the daily step allows since the last price.
  const days = Math.max(hoursBetween(previous.pricedAt || now, now) / 24, 0);
  // On the store's own price ending, if it has one; a price from before
  // the ending was set steps down onto it.
  const ending = priceEnding(merchantFields);
  const floor = upToEnding(shown * (1 - settings.dropPerDay * days), ending);
  const price = Math.max(targetPrice, Math.min(downToEnding(shown, ending), floor));

  return {
    status: "open",
    source: best.source,
    cost: best.cost,
    targetPrice,
    price,
    pricedAt: price === shown ? previous.pricedAt || now : now,
    reason: price > targetPrice ? "coming down slowly" : "down"
  };
}
