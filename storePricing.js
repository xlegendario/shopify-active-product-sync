/*
 * What a store may pay, what it must ask, and whether a pair is worth listing.
 *
 * One module because it is one rule. The same arithmetic already lives in two
 * Airtable formulas, Target Buying Price and Maximum Buying Price, and those
 * decide what we pay on every order today. If this file and those formulas
 * ever disagree, a pair gets listed that earns the store less than it agreed
 * to, and nothing on screen would say so. verify-against-airtable.mjs exists
 * to make that impossible to miss: it runs both over real orders and compares.
 *
 * Three directions, all from the same numbers:
 *
 *   maximumBuyingPrice   selling price -> the most we may pay
 *   targetBuyingPrice    selling price -> what we aim to pay
 *   minimumSellingPrice  what we pay   -> the least it may be sold for
 *
 * The first is the gate when the store sets its own prices. The last is what
 * we need when Price Sync is on and we set them.
 */

const STEP = 2.5;

/* ------------------------------------------------------------------ *
 * Reading a merchant
 * ------------------------------------------------------------------ */

function num(value) {
  const first = Array.isArray(value) ? value[0] : value;
  const parsed = Number(first);

  return Number.isFinite(parsed) ? parsed : 0;
}

function text(value) {
  const first = Array.isArray(value) ? value[0] : value;

  return String(first ?? "").trim();
}

/*
 * The Firm Range ladder, read out of the merchant record once.
 *
 * In Airtable this is thirty-eight nested IFs, because a formula has no other
 * way to say it. Here it is a list, sorted, and a band is found by walking it.
 *
 * The field names are not consistent - some carry an en dash and some a plain
 * hyphen, and not even the same way between the Min and Target sets - so a
 * band is matched on its numbers rather than on its name.
 */
export function readMarginBands(merchantFields, kind) {
  const suffix = kind === "target" ? " Target Margin" : " Min Margin";

  const bands = [];

  for (const [name, value] of Object.entries(merchantFields || {})) {
    if (!name.startsWith("FR ") || !name.endsWith(suffix)) continue;

    const label = name.slice(3, name.length - suffix.length);
    const margin = num(value);

    if (label.includes("<")) {
      bands.push({ upTo: 75, margin });
      continue;
    }

    if (label.includes(">")) {
      bands.push({ upTo: Infinity, margin });
      continue;
    }

    const digits = label.match(/\d+/g) || [];

    if (digits.length !== 2) continue;

    // The band ends where the next one starts: "200–224" covers up to 225.
    bands.push({ upTo: Number(digits[1]) + 1, margin });
  }

  return bands.sort((a, b) => a.upTo - b.upTo);
}

function bandMarginFor(sellingPrice, bands) {
  const band = bands.find((entry) => sellingPrice < entry.upTo);

  return band ? band.margin : 0;
}

/*
 * The store's own VAT rate, or nothing.
 *
 * CHANGED - this used to fall back to 21 percent when the field was empty.
 * That reads as harmless and is not: the rates in the base run from 19 to 25,
 * so a guess is up to four points off, on a margin that is often ten. And a
 * store genuinely on zero looks exactly like a store nobody filled in.
 *
 * So it says nothing rather than guessing, and the caller refuses the pair.
 * A blank field then shows up as a shop that stays empty with a reason, which
 * is findable, instead of a margin quietly measured against the wrong number
 * for as long as nobody checks.
 */
export function storeVatRate(merchantFields) {
  const rate = num(merchantFields?.["VAT Rate"] ?? merchantFields?.["Client VAT Rate"]);

  return rate > 0 ? rate : null;
}

/*
 * VAT0 and VAT21 asks are quoted without the buyer's VAT, so the room for a
 * margin has to be divided back out of a VAT-inclusive selling price. Margin
 * goods carry no VAT at all and are left as they stand - and need no rate,
 * which is why a store without one can still be sold those.
 */
function vatDivisor(vatType, merchantFields) {
  const type = text(vatType).toUpperCase().replace(/\s/g, "");

  if (type !== "VAT0" && type !== "VAT21") return 1;

  const rate = storeVatRate(merchantFields);

  return rate === null ? null : 1 + rate;
}

/* ------------------------------------------------------------------ *
 * Selling price -> buying price
 * ------------------------------------------------------------------ */

/*
 * What is left for us once the store has its margin, before VAT and rounding.
 *
 * The percentage branch takes the LOWER of the two, which is the stricter of
 * them: at least the flat amount, and at least the percentage. That replaced a
 * threshold field that decided which of the two applied, and could be set to a
 * value where neither did - on one client that left a window where the
 * required margin dipped under the amount it was meant to guarantee.
 */
function roomForUs(sellingPrice, merchantFields, kind) {
  const method = text(merchantFields?.["Margin Method"] ?? merchantFields?.["Client Margin Method"]);
  const baseCosts = num(merchantFields?.["Base Costs"] ?? merchantFields?.["Client Base Costs"]);

  if (method === "Percentage") {
    const pct = num(
      kind === "target"
        ? merchantFields?.["Target Margin (%)"] ?? merchantFields?.["Client Target Margin (%)"]
        : merchantFields?.["Minimum Margin (%)"] ?? merchantFields?.["Client Minimum Margin (%)"]
    );

    const amount = num(merchantFields?.["Min Margin Amount"]);

    return Math.min(sellingPrice - amount, sellingPrice * (1 - pct)) - baseCosts;
  }

  if (method === "Firm Range") {
    const bands = readMarginBands(merchantFields, kind);

    return sellingPrice - bandMarginFor(sellingPrice, bands) - baseCosts;
  }

  return null;
}

function toStepUp(value) {
  return Math.ceil(value / STEP) * STEP;
}

function buyingPrice({ sellingPrice, vatType, merchantFields, kind }) {
  const selling = Number(sellingPrice);

  if (!Number.isFinite(selling) || selling <= 0) return null;

  const room = roomForUs(selling, merchantFields, kind);

  if (room === null) return null;

  const divisor = vatDivisor(vatType, merchantFields);

  if (divisor === null) return null;

  return toStepUp(room / divisor);
}

// The gate. Pay more than this and the store does not make its margin.
export function maximumBuyingPrice({ sellingPrice, vatType, merchantFields }) {
  return buyingPrice({ sellingPrice, vatType, merchantFields, kind: "min" });
}

// What we aim for, which is the same shape with the target percentages.
export function targetBuyingPrice({ sellingPrice, vatType, merchantFields }) {
  return buyingPrice({ sellingPrice, vatType, merchantFields, kind: "target" });
}

/* ------------------------------------------------------------------ *
 * Buying price -> selling price
 * ------------------------------------------------------------------ */

/*
 * The least a pair may be sold for, given what it costs us.
 *
 * Only needed when Price Sync is on and we set the price ourselves. With it
 * off the store already has a price and maximumBuyingPrice answers directly.
 *
 * The percentage side falls out in closed form BECAUSE the rule takes the
 * lower of the two: both conditions have to hold, so both are solved and the
 * higher answer wins. With the old threshold this needed a branch and a check
 * that the answer landed on the right side of it.
 *
 * Firm Range has no closed form, since the required margin steps with the
 * price. The bands are walked from the bottom: the first one whose answer
 * lands inside it is the answer.
 */
export function minimumSellingPrice({ buyingPrice: cost, vatType, merchantFields, round = true }) {
  const paid = Number(cost);

  if (!Number.isFinite(paid) || paid <= 0) return null;

  const method = text(merchantFields?.["Margin Method"] ?? merchantFields?.["Client Margin Method"]);
  const baseCosts = num(merchantFields?.["Base Costs"] ?? merchantFields?.["Client Base Costs"]);
  const divisor = vatDivisor(vatType, merchantFields);

  if (divisor === null) return null;

  const grossed = paid * divisor + baseCosts;

  let raw = null;

  if (method === "Percentage") {
    const pct = num(merchantFields?.["Minimum Margin (%)"] ?? merchantFields?.["Client Minimum Margin (%)"]);
    const amount = num(merchantFields?.["Min Margin Amount"]);

    const byAmount = grossed + amount;
    const byPercentage = pct > 0 && pct < 1 ? grossed / (1 - pct) : grossed;

    raw = Math.max(byAmount, byPercentage);
  } else if (method === "Firm Range") {
    const bands = readMarginBands(merchantFields, "min");

    for (const band of bands) {
      const candidate = grossed + band.margin;

      if (candidate < band.upTo) {
        raw = candidate;
        break;
      }
    }

    // Above every band, so the top band's margin is the one that applies.
    if (raw === null && bands.length) {
      raw = grossed + bands[bands.length - 1].margin;
    }
  }

  if (raw === null) return null;

  if (!round) return raw;

  /*
    Rounded to whole euros, and then settled against the forward rule rather
    than trusted.

    Rounding is where an inverse quietly stops being one. Two things pull in
    opposite directions here: the arithmetic above ignores that the forward
    rule rounds its allowance UP to a 2.5 step, which makes it slightly
    generous, while rounding the price itself up makes it slightly strict.
    Neither is worth reasoning about in the abstract.

    So it walks. Up while the gate refuses, then down for as long as it still
    agrees. What comes back is the lowest whole euro the forward rule accepts,
    which makes the two exact inverses of each other by construction.
  */
  let selling = Math.ceil(raw);

  const accepts = (price) => {
    const allowed = maximumBuyingPrice({ sellingPrice: price, vatType, merchantFields });

    return allowed !== null && allowed >= paid;
  };

  for (let step = 0; step < 25 && !accepts(selling); step += 1) selling += 1;

  while (selling > 1 && accepts(selling - 1)) selling -= 1;

  return selling;
}

/*
 * Is this pair worth listing at all.
 *
 * With Price Sync off the selling price is the store's own, read live from
 * Shopify rather than from our copy of its catalogue - a store that lowered a
 * price this morning would otherwise be judged on last night's number.
 */
export function passesMarginGate({ buyingPrice: cost, sellingPrice, vatType, merchantFields }) {
  const paid = Number(cost);
  const allowed = maximumBuyingPrice({ sellingPrice, vatType, merchantFields });

  if (!Number.isFinite(paid) || allowed === null) return false;

  return paid <= allowed;
}

/* ------------------------------------------------------------------ *
 * Sizes
 * ------------------------------------------------------------------ */

/*
 * A European size as a number, so a ladder can be put in order.
 *
 * Halves and thirds both occur and they do not sort as text: "41 1/3" comes
 * before "40 2/3" alphabetically, which is how a product page ends up with
 * its sizes shuffled. adidas quotes thirds, most others quote halves.
 */
export function parseEuSize(value) {
  const cleaned = String(value ?? "")
    .replace(/eu/i, "")
    .replace(",", ".")
    .trim();

  if (!cleaned) return null;

  const fraction = cleaned.match(/^(\d+)\s+(\d)\s*\/\s*(\d)$/);

  if (fraction) {
    return Number(fraction[1]) + Number(fraction[2]) / Number(fraction[3]);
  }

  const plain = Number(cleaned);

  return Number.isFinite(plain) ? plain : null;
}

// Ascending, with anything unreadable left at the end rather than dropped.
export function sortEuSizes(sizes) {
  return [...(sizes || [])].sort((a, b) => {
    const left = parseEuSize(a);
    const right = parseEuSize(b);

    if (left === null && right === null) return 0;
    if (left === null) return 1;
    if (right === null) return -1;

    return left - right;
  });
}
