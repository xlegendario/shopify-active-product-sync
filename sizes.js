/*
 * One size, however a store chose to write it down.
 *
 * This is the only place that decides what a size label means. It was in two
 * places and they disagreed, which is the shape of nearly every bug in this
 * codebase: store_listings was filled by a regex that took the first number
 * it saw, and matching used a different rule that only tidied whitespace. So
 * "38 2/3" was written to our own table as "38", landing on top of the real
 * 38, and "EU 42" matched nothing at all.
 *
 * The shapes stores actually use, all meaning the same shoe:
 *
 *   42            38 2/3
 *   EU 42         EU 38 2/3        EU38 2/3
 *   42 EU         38 2/3 EU        42,5
 *   Maat 42       38 2/3 EU - 6 US
 *
 * And the one that must NOT be turned into a number: a label on another
 * scale. A US 9 is not an EU 9, and quietly reducing it to "9" would put a
 * child's shoe where a man's belongs. When there is no European reading, the
 * label is returned as it stands, matches nothing, and stays visible.
 */

/*
 * The words a store puts in front of or behind a European size.
 *
 * Longest first: JavaScript takes the first alternative that fits, not the
 * best one, so "eu" ahead of "eur" turned "EUR 42" into "R 42".
 */
const EU_WORDS = "euro|eur|eu|ue|pointure|taglia|talla|größe|grosse|maat|size|gr";

const EU_TAG = new RegExp(`(?:^|\\s)(?:${EU_WORDS})(?=\\s|\\d|$)`, "i");

// Stripped from either end, with or without a space between word and number.
const EU_LEADING = new RegExp(`^(?:${EU_WORDS})[\\s.:-]*`, "i");
const EU_TRAILING = new RegExp(`[\\s.:-]*(?:${EU_WORDS})$`, "i");

/*
 * Scales we cannot convert from.
 *
 * Bounded by word edges so a plain "M" or "S" stays a clothing size, and
 * "cm" does not fire on a style code.
 */
const OTHER_SCALE = /(?:^|\s)(?:us|usa|uk|gb|cm|jp|jpn|mondo|mx)(?=\s|\d|$)/i;

function tidy(value) {
  return String(value == null ? "" : value)
    .replace(/,/g, ".")
    .replace(/\s+/g, " ")
    .trim();
}

function stripEuWords(part) {
  return part.replace(EU_LEADING, "").replace(EU_TRAILING, "").trim();
}

/*
 * A European size, or the label unchanged when there is no reading of it.
 *
 * Used both when writing a row to store_listings and when matching our
 * stock against it, so the two can never drift.
 */
export function normalizeSize(value) {
  const clean = tidy(value);

  if (!clean) return "";

  /*
    A label can carry two scales at once - "38 2/3 EU - 6 US" - and those
    are separated by a dash or a pipe, never by a slash, because a slash is
    how two thirds is written.
  */
  const parts = clean
    .split(/\s*[|]\s*|\s+[-–—]\s+/)
    .map((part) => part.trim())
    .filter(Boolean);

  // Said to be European: that part wins, whichever end the word sits on.
  const tagged = parts.find((part) => EU_TAG.test(part) && !OTHER_SCALE.test(part));

  if (tagged) {
    const stripped = stripEuWords(tagged);

    if (stripped) return stripped;
  }

  // Nothing said at all: a bare number is read as European, which is what
  // every store that writes only a number means.
  const bare = parts.find((part) => !OTHER_SCALE.test(part) && /\d/.test(part));

  if (bare) return stripEuWords(bare) || bare;

  /*
    Only another scale, or no number anywhere. Returned as it stands: a
    clothing size like XL passes through, and a US-only label stays a US
    label so it matches nothing instead of matching the wrong thing.
  */
  return clean;
}
