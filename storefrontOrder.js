/*
 * The storefront's order, kept up by itself (UNION, 18-11-2026).
 *
 * Two things a store's catalogue does not do on its own:
 *
 *  - a new page has no model (product metafield custom.model), so it is
 *    missing from the model filter and from the model pills above a
 *    collection. modelFor reads it from the title, with the same rules the
 *    longlist was built with.
 *  - a new page lands at the bottom of every hand-sorted collection, however
 *    well it sells. planOrder puts each collection back in order: live pages
 *    first, full sets first, children's sizes last, then by sales score
 *    (storefront_scores), with stock that ships in 48 hours first among
 *    equals.
 *
 * Nothing is ever unpublished or removed here: what is on the shelf is the
 * store's choice. Collections sorted by hand on purpose (Bestsellers) are
 * simply not in the list.
 *
 * modelFor and planOrder decide and write nothing (storefrontOrder.test.mjs).
 */

const pyTitle = (s) => String(s).toLowerCase().replace(/[a-z]+/g, (w) => w[0].toUpperCase() + w.slice(1));

// Checked first: names the longlist rules get wrong or do not know.
const EXTRA = [
  [/travis scott/, "Travis Scott"],
  [/foam r(u?n)?n?r/, "Yeezy Foam Runner"],
  [/gel[- ]kayano (\d+)/, "Gel-Kayano {0}"],
  [/gel[- ]cumulus (\d+)/, "Gel-Cumulus {0}"],
  [/\bsl ?72\b/, "SL 72"],
  [/timberland 6["\- ]/, "Timberland 6-Inch"],
  [/birkenstock boston/, "Birkenstock Boston"],
  [/ugg funkette/, "UGG Funkette"],
  [/ugg classic micro/, "UGG Classic Micro"],
  [/pegasus/, "Pegasus"],
  [/vaporfly/, "Vaporfly"],
  [/abzorb 2000/, "New Balance Abzorb 2000"],
  [/nike gato/, "Gato"],
  [/rx moc/, "Salomon RX Moc 3.0"],
  [/on running (cloud\w*)/, "On {0}"],
  [/phantom/, "Phantom"],
  [/air max (2013)/, "Air Max {0}"],
  [/air force 3/, "Air Force 3"],
  [/vaporwaffle/, "Vaporwaffle"],
  [/lebron/, "LeBron"],
  [/winflo/, "Winflo"],
  [/zoom field jaxx/, "Zoom Field Jaxx"],
  [/general purpose shoe/, "Tom Sachs General Purpose Shoe"],
  [/air liquid max/, "Air Liquid Max"],
  [/air bakin/, "Air Bakin"],
  [/air dunk jumbo/, "Dunk Jumbo"],
  [/spzl|spezial/, "Handball Spezial"],
  [/autry medalist/, "Autry Medalist"],
  [/brick clog/, "Crocs Classic Clog"],
  [/fly cap/, "Caps"]
];

// The longlist's rules (longlist/build.py, 14-11-2026): regex on the lowercase title, model.
const MODELS = [
  [/jordan 1 (retro )?high|air jordan 1 high|jordan 1 retro high/, "Jordan 1 High"],
  [/jordan 1 (retro )?low|air jordan 1 low/, "Jordan 1 Low"],
  [/jordan 1 mid/, "Jordan 1 Mid"],
  [/jordan (\d{1,2})\b/, "Jordan {0}"],
  [/air max (1|90|95|97|plus|dn|270|180|tl 2\.5|tl|ltd|2090|720|bw|90 futura)\b/, "Air Max {0}"],
  [/air force 1/, "Air Force 1"],
  [/sb dunk (low|high)/, "SB Dunk {0}"],
  [/dunk (low|high)/, "Dunk {0}"],
  [/vomero 5|zoom vomero 5/, "Vomero 5"],
  [/vomero/, "Vomero"],
  [/p-?6000/, "P-6000"],
  [/kobe (\d+)/, "Kobe {0}"],
  [/shox/, "Shox"],
  [/cortez/, "Cortez"],
  [/total 90/, "Total 90"],
  [/v2k/, "V2K"],
  [/blazer/, "Blazer"],
  [/samba/, "Samba"],
  [/gazelle/, "Gazelle"],
  [/campus/, "Campus 00s"],
  [/spezial/, "Handball Spezial"],
  [/sl 72/, "SL 72"],
  [/taekwondo/, "Taekwondo"],
  [/superstar/, "Superstar"],
  [/forum/, "Forum"],
  [/yeezy (boost )?(350|380|700|500|450)/, "Yeezy {1}"],
  [/yeezy slide|yzy slide|ys-01|ys slide/, "Yeezy Slide"],
  [/foam r(unne)?r|foam rnnr/, "Yeezy Foam Runner"],
  [/mind 00(\d)/, "Nike Mind 00{0}"],
  [/air max sunder/, "Air Max Sunder"],
  [/moon shoe/, "Moon Shoe"],
  [/killshot/, "Killshot 2"],
  [/mercurial/, "Mercurial Vapor"],
  [/jumpman jack/, "Jumpman Jack"],
  [/cj1|t-rexx/, "CJ1 T-Rexx"],
  [/nocta/, "NOCTA"],
  [/(knvb|jersey)/, "Jersey"],
  [/xlg/, "XLG Runner Deluxe"],
  [/chinese track top|ctt/, "CTT Chinese Track Top"],
  [/bw army/, "BW Army"],
  [/adistar/, "Adistar Jellyfish"],
  [/ultra ?boost/, "Ultraboost"],
  [/f50/, "F50"],
  [/classic clog/, "Crocs Classic Clog"],
  [/chuck taylor|chuck 70/, "Chuck Taylor"],
  [/old skool/, "Old Skool"],
  [/cloudtilt/, "Cloudtilt"],
  [/progrid omni 9|omni 9/, "ProGrid Omni 9"],
  [/speedcat/, "Speedcat"],
  [/bailey button/, "UGG Bailey Button"],
  [/ua sola/, "UA Sola"],
  [/bambino/, "Le Grand Bambino"],
  [/royal pop/, "Royal Pop"],
  [/denim tears/, "Denim Tears"],
  [/new balance (\d{3,4}[a-z0-9]*)/, "New Balance {0}"],
  [/\b(9060|2002r|1906r|1906d|550|530|990v\d|991|992|993|1000|204l|740|860v2|327|574|1500)\b/, "New Balance {0}"],
  [/gel-?kayano (\d+)/, "Gel-Kayano {0}"],
  [/gel-?nyc/, "Gel-NYC"],
  [/gel-?(1130|1090|quantum|lyte iii|venture 6|nimbus \d+)/, "Gel-{0}"],
  [/gt-?2160/, "GT-2160"],
  [/ugg.*?(tasman|tazz|classic ultra mini|classic mini|lowmel|goldenstar|classic short|classic tall)/, "UGG {0}"],
  [/\b(xt-6|xt-4|acs pro|speedcross \d)/, "Salomon {0}"],
  [/6[- ]inch|6 in\b/, "Timberland 6-Inch"],
  [/essentials.*(summer set)/, "Essentials Summer Set"],
  [/essentials.*(full set|tracksuit)/, "Essentials Full Set"],
  [/essentials.*(hoodie|hooded)/, "Essentials Hoodie"],
  [/essentials.*(sweatpant|pant|broek)/, "Essentials Sweatpants"],
  [/essentials.*(short)/, "Essentials Shorts"],
  [/essentials.*(t-shirt|tee)/, "Essentials T-Shirt"],
  [/essentials.*(crewneck|sweater|sweatshirt)/, "Essentials Crewneck"],
  [/essentials/, "Essentials"],
  [/(peterson|hank|blakey|wayne)/, "Maison Mihara {0}"]
];

const FIX = {
  "Salomon Xt-6": "Salomon XT-6",
  "Salomon Xt-4": "Salomon XT-4",
  "New Balance 740V2": "New Balance 740v2"
};

const fill = (template, groups) => template.replace(/\{(\d)\}/g, (_, i) => groups[Number(i)] ?? "").replace(/ {2,}/g, " ").trim();

/* The model a page belongs to, read from its title; "" when no rule knows it. */
export function modelFor(title) {
  const t = String(title || "").toLowerCase();

  for (const [rx, model] of EXTRA) {
    const m = t.match(rx);
    if (m) return fill(model, m.slice(1).map((x) => (x && !/^\d/.test(x) ? pyTitle(x) : x || "")));
  }

  for (const [rx, model] of MODELS) {
    const m = t.match(rx);
    if (!m) continue;
    const groups = m.slice(1)
      .map((x) => (x && !/^\d/.test(x) ? pyTitle(x) : x || ""))
      .map((x) => (/^\d+[a-z]+$/i.test(x) ? x.toUpperCase() : x));
    const name = fill(model, groups.length ? groups : [""]);
    return FIX[name] || name;
  }

  return "";
}

const KIDS = /\((GS|PS|TD|Kids|Infants?|Toddler|Baby)\)|\b(GS|PS|TD)\b/i;
const FULL_SET = /full set/i;

/*
 * The order of one collection.
 *
 * products: [{ id, title, status, sku, express }] - express: a size that
 *           ships in 48 hours can be bought
 * scores:   Map(SKU -> sales score)
 */
export function planOrder(products, scores) {
  const rows = products.map((p) => ({
    id: p.id,
    live: p.status === "ACTIVE",
    set: FULL_SET.test(p.title || ""),
    kids: KIDS.test(p.title || ""),
    express: Boolean(p.express),
    score: Number(scores.get(String(p.sku || "").trim().toUpperCase()) || 0),
    title: p.title || ""
  }));

  rows.sort((a, b) =>
    (b.live - a.live) || (b.set - a.set) || (a.kids - b.kids) ||
    (b.score - a.score) || (b.express - a.express) || a.title.localeCompare(b.title));

  return rows.map((r) => r.id);
}

const COLLECTION = `
  query storefrontCollection($h: String!) {
    collectionByIdentifier(identifier: { handle: $h }) { id sortOrder }
  }
`;

const COLLECTION_PAGE = `
  query storefrontCollectionPage($id: ID!, $after: String) {
    collection(id: $id) {
      products(first: 100, after: $after, sortKey: COLLECTION_DEFAULT) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id title status
          variants(first: 100) {
            nodes { sku availableForSale inventoryQuantity selectedOptions { value } }
          }
        }
      }
    }
  }
`;

const ACTIVE_PAGE = `
  query storefrontActive($after: String) {
    products(first: 100, after: $after, query: "status:active") {
      pageInfo { hasNextPage endCursor }
      nodes { id title metafield(namespace: "custom", key: "model") { value } }
    }
  }
`;

const SET_MODELS = `
  mutation storefrontModels($m: [MetafieldsSetInput!]!) {
    metafieldsSet(metafields: $m) { userErrors { field message } }
  }
`;

const SORT_MANUAL = `
  mutation storefrontManual($input: CollectionInput!) {
    collectionUpdate(input: $input) { userErrors { field message } }
  }
`;

const REORDER = `
  mutation storefrontReorder($id: ID!, $moves: [MoveInput!]!) {
    collectionReorderProducts(id: $id, moves: $moves) { job { id } userErrors { field message } }
  }
`;

const errorsOf = (data, key) => (data?.[key]?.userErrors || []).map((e) => e.message).join("; ");

/* Active pages without a model get one from their title. */
export async function fillModels({ graphql, apply = false }) {
  const missing = [];
  let after = null;

  do {
    const data = await graphql(ACTIVE_PAGE, { after });
    const page = data.products;
    missing.push(...page.nodes.filter((p) => !p.metafield?.value));
    after = page.pageInfo.hasNextPage ? page.pageInfo.endCursor : null;
  } while (after);

  const plan = missing.map((p) => ({ id: p.id, title: p.title, model: modelFor(p.title) }));
  const known = plan.filter((p) => p.model);

  if (apply) {
    for (let i = 0; i < known.length; i += 25) {
      const data = await graphql(SET_MODELS, {
        m: known.slice(i, i + 25).map((p) => ({
          ownerId: p.id, namespace: "custom", key: "model", type: "single_line_text_field", value: p.model
        }))
      });
      const failure = errorsOf(data, "metafieldsSet");
      if (failure) throw new Error(`models: ${failure}`);
    }
  }

  return { missing: missing.length, set: known.length, unknown: plan.filter((p) => !p.model).map((p) => p.title) };
}

/* Every listed collection back in order. A collection already in order is left alone. */
export async function sortCollections({ graphql, handles, scores, apply = false }) {
  const report = [];

  for (const handle of handles) {
    const c = (await graphql(COLLECTION, { h: handle }))?.collectionByIdentifier;
    if (!c) { report.push({ handle, skipped: "not found" }); continue; }

    const products = [];
    let after = null;

    do {
      const data = await graphql(COLLECTION_PAGE, { id: c.id, after });
      const page = data.collection.products;
      for (const p of page.nodes) {
        const v = p.variants.nodes;
        products.push({
          id: p.id,
          title: p.title,
          status: p.status,
          sku: v[0]?.sku,
          express: v.some((x) => x.availableForSale && (x.inventoryQuantity || 0) > 0 && !x.selectedOptions.some((o) => o.value === "Standard"))
        });
      }
      after = page.pageInfo.hasNextPage ? page.pageInfo.endCursor : null;
    } while (after);

    const order = planOrder(products, scores);
    const current = products.map((p) => p.id);
    const moved = order.filter((id, i) => current[i] !== id).length;

    if (apply && (moved || c.sortOrder !== "MANUAL")) {
      if (c.sortOrder !== "MANUAL") {
        const data = await graphql(SORT_MANUAL, { input: { id: c.id, sortOrder: "MANUAL" } });
        const failure = errorsOf(data, "collectionUpdate");
        if (failure) throw new Error(`${handle} manual: ${failure}`);
      }

      const moves = order.map((id, i) => ({ id, newPosition: String(i) }));
      for (let i = 0; i < moves.length; i += 250) {
        const data = await graphql(REORDER, { id: c.id, moves: moves.slice(i, i + 250) });
        const failure = errorsOf(data, "collectionReorderProducts");
        if (failure) throw new Error(`${handle} reorder: ${failure}`);
      }
    }

    report.push({ handle, products: products.length, moved });
  }

  return report;
}
