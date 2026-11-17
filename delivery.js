/*
 * Express and Standard: two ways to buy the same size.
 *
 * A store with "Express Option?" on in Merchants (UNION, today) has a second
 * product option next to the size, "Levering":
 *
 *   42 / Express    the consignor's pair, on our location, 48h. Exactly the
 *                   variant the consignment run has always written.
 *   42 / Standard   sourced after the sale at the autopricing price. Always
 *                   orderable, never holds stock of ours.
 *
 * Everything that stocks a store works on Express variants only. Standard is
 * written by standardWriter.js and nothing else, so for the consignment run
 * a Standard variant simply does not exist - which keeps it at one variant
 * per size, the shape all of its logic was written for.
 *
 * Stores without the option have no "Levering" and none of this changes
 * anything for them.
 */
export const DELIVERY_OPTION = "Levering";
export const EXPRESS = "Express";
export const STANDARD = "Standard";

const OPTION_NAME = /^(levering|delivery)$/i;

export function deliveryOf(variant) {
  const option = (variant?.selectedOptions || []).find((o) => OPTION_NAME.test(String(o?.name || "").trim()));

  return option ? String(option.value || "").trim() : null;
}

export function isStandardVariant(variant) {
  return /^standard$/i.test(deliveryOf(variant) || "");
}

export function hasDeliveryOption(product) {
  return (product?.options || []).some((o) => OPTION_NAME.test(String(o?.name || "").trim()));
}

export function expressOptionOn(merchantFields) {
  return Boolean(merchantFields?.["Express Option?"]);
}
