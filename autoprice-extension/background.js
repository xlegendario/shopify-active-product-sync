/*
 * Autopricing: reads StockX Pro for one product at a time, all sizes at once.
 *
 * Separate from the Outsource Engine extension on purpose: its own Chrome
 * profile, its own queue, so it can never hold up the price checks on real
 * orders. It only reads the Buy Now table; it never bids or buys.
 *
 * Loop: ask the server which products are due (a batch since v1.3), read
 * their sizes and Total (incl. tax) for buying now, send them back, repeat.
 * GOAT is read by a second loop next to it, with its own queue.
 *
 * Since 23-11-2026 the read goes through Pro's own GraphQL, from inside an
 * open Pro page (readProApi): about a second per product instead of the
 * clicks through EU / All Sizes / Buy Now and the table text. The Pro page
 * stays open and is only reloaded now and then, so its login stays fresh.
 * The clicking read (readProPage) is kept as the fallback for a page whose
 * own data client is not there.
 *
 * When StockX shows a login page: try once to log in with the e-mail and
 * password saved in the popup, but only if a plain form appears. Anything
 * else (a verification code, a bot check) is never worked around: it
 * stops, alerts on Discord and waits for a person to log in.
 */

const ALARM = "autoprice-tick";
/*
 * A pause between products. It was 20-40 s when every product meant a page
 * load and four clicks; a read is now two kinds of request. 3-6 s ran a
 * full hour on 23-11-2026 without StockX slowing us down, so 1-2 s.
 *
 * The server can set it per product (pauseMs from /autoprice/pro-next,
 * AUTOPRICE_PRO_PAUSE_MS on Render), so the pace changes without a reload.
 */
const BETWEEN_PRODUCTS_MS = [1000, 2000];
let pauseRange = BETWEEN_PRODUCTS_MS;
const IDLE_WAIT_MS = 10 * 60 * 1000;
const LOGGED_OUT_WAIT_MS = 30 * 60 * 1000;

// When StockX answers "too many requests": stop for a while, then carry on.
const THROTTLED_WAIT_MS = 20 * 60 * 1000;

// The Pro page is reloaded this often, so the login behind it is renewed.
const RELOAD_PRO_EVERY_MS = 30 * 60 * 1000;

// Totals asked at the same time, per product.
const PRICING_PARALLEL = 5;

/*
 * v1.3: products are handed out in batches, and GOAT runs as its own loop in
 * its own tab instead of after every Pro read. GOAT's pause is its own too
 * (AUTOPRICE_GOAT_PAUSE_MS on Render); until that is set, 2-4 s, which is
 * about the pace GOAT had riding along with Pro.
 */
const PRO_BATCH = 20;
const GOAT_BATCH = 20;
const GOAT_BETWEEN_MS = [2000, 4000];
let goatPauseRange = GOAT_BETWEEN_MS;

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = ([a, b]) => a + Math.random() * (b - a);

async function settings() {
  return chrome.storage.local.get(["server", "secret", "email", "password", "enabled", "pausedUntil", "tabId", "goatTabId", "proLoadedAt"]);
}

async function status(text) {
  await chrome.storage.local.set({ lastStatus: new Date().toLocaleTimeString() + " " + text });
}

async function api(s, path, body) {
  const res = await fetch(s.server.replace(/\/+$/, "") + path, {
    method: body ? "POST" : "GET",
    headers: { "Content-Type": "application/json", "x-autoprice-secret": s.secret },
    body: body ? JSON.stringify(body) : undefined
  });

  if (!res.ok) throw new Error(`server ${res.status} on ${path}`);

  return res.json();
}

async function alert(s, message) {
  await api(s, "/autoprice/alert", { message }).catch(() => null);
}

async function waitForComplete(tabId, timeoutMs = 60000) {
  const started = Date.now();

  while (Date.now() - started < timeoutMs) {
    const tab = await chrome.tabs.get(tabId);
    if (tab.status === "complete") return tab;
    await wait(1000);
  }

  throw new Error("page load timeout");
}

// One tab, reused, so the browser does not fill up with Pro pages.
async function workTab(s, url) {
  if (s.tabId) {
    try {
      await chrome.tabs.update(s.tabId, { url });
      return s.tabId;
    } catch {
      // closed by hand; make a new one
    }
  }

  const tab = await chrome.tabs.create({ url, active: false });
  await chrome.storage.local.set({ tabId: tab.id });

  return tab.id;
}

/*
 * The Pro tab, if it is open on a product page, loaded and not older than
 * RELOAD_PRO_EVERY_MS; otherwise null and the caller loads it.
 */
async function openProTab(s) {
  if (!s.tabId || !s.proLoadedAt || Date.now() - s.proLoadedAt > RELOAD_PRO_EVERY_MS) return null;

  try {
    const tab = await chrome.tabs.get(s.tabId);

    return tab.status === "complete" && String(tab.url || "").startsWith("https://pro.stockx.com/purchasing/create/") ? tab.id : null;
  } catch {
    return null;
  }
}

/*
 * Runs inside an open Pro page, in the page's own world: any product's sizes
 * and totals through the GraphQL client the page itself uses
 * (window.__APOLLO_CLIENT__), so the login, the headers and the persisted
 * queries all come along exactly as when a person clicks.
 *
 *   ProductVariants  every size with its EU label and the lowest ask
 *                    (= the Buy Now column)
 *   PurchasePricing  what buying that ask costs in all: processing,
 *                    shipping, VAT for a VAT-registered NL buyer
 *                    (= the Total (incl. tax) column)
 *
 * The two query documents are taken from the page's own queries, which is
 * why the page has to be a product page that has loaded once. Checked on
 * 22-11-2026 against the table: 30 products, 472 sizes, the same totals.
 */
async function readProApi(productId, parallel) {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  if (location.hostname.startsWith("accounts.")) return { error: "logged_out" };

  let docs = window.__lojiqDocs;

  for (let i = 0; i < 30 && !(docs?.variants && docs?.pricing); i += 1) {
    const client = window.__APOLLO_CLIENT__;
    const queries = client?.queryManager?.queries ? [...client.queryManager.queries.values()] : [];
    const find = (name) => queries.find((q) => q.document?.definitions?.some((d) => d.name?.value === name))?.document;

    // The account's VAT number as the page itself sends it (a reference, "*01").
    const vatId = queries.map((q) => q.variables?.buyerVatId).find(Boolean);

    docs = { variants: find("ProductVariants"), pricing: find("PurchasePricing"), vatId };

    if (docs.variants && docs.pricing) window.__lojiqDocs = docs;
    else await sleep(1000);
  }

  const client = window.__APOLLO_CLIENT__;

  if (!client || !docs?.variants || !docs?.pricing) return { error: "not_ready" };

  const classify = (error) => {
    const status = error?.networkError?.statusCode || error?.networkError?.response?.status;
    const text = String(error?.message || error).toLowerCase();

    if (status === 401 || status === 403 || /unauthori[sz]ed|unauthenticated|forbidden|login required/.test(text)) return "logged_out";
    if (status === 429 || /too many|rate limit/.test(text)) return "throttled";

    return "failed: " + String(error?.message || error).slice(0, 150);
  };

  const ask = (query, variables) => client.query({ query, variables, fetchPolicy: "network-only" });

  let variants;

  try {
    const r = await ask(docs.variants, {
      id: productId, currency: "EUR", country: "NL", market: "NL.vat-registered",
      skipFlexEligible: true, skipGuidance: false
    });

    variants = r?.data?.product?.variants;
  } catch (error) {
    return { error: classify(error) };
  }

  if (!Array.isArray(variants)) return { error: "no product" };

  const rows = [];

  for (const v of variants) {
    if (v?.hidden) continue;

    const standard = v?.market?.state?.askServiceLevels?.standard;
    const buyNow = Number(standard?.lowest?.amount);

    if (!(buyNow > 0)) continue;

    const eu = (v?.sizeChart?.displayOptions || []).find((o) => o?.type === "eu")?.size;
    const size = String(eu || v?.traits?.size || "").replace(/^EU\s*/i, "").trim();

    if (size) rows.push({ id: v.id, size, buyNow, asks: Number(standard?.count) });
  }

  const sizes = {};

  try {
    for (let i = 0; i < rows.length; i += parallel) {
      await Promise.all(rows.slice(i, i + parallel).map(async (row) => {
        const r = await ask(docs.pricing, {
          tradeContext: "buying", currencyCode: "EUR", buyerVatId: docs.vatId || "*01", skipStoredPricing: true,
          variants: [{ amount: { currencyCode: "EUR", value: row.buyNow }, uuid: row.id, quantity: 1 }]
        });

        const total = Number(r?.data?.shippedPricing?.estimate?.total);

        if (total > 0) {
          sizes[row.size] = { total, buyNow: row.buyNow, ...(Number.isFinite(row.asks) ? { asks: row.asks } : {}) };
        }
      }));
    }
  } catch (error) {
    return { error: classify(error) };
  }

  return { sizes };
}

/*
 * Runs inside the Pro page. Same steps and the same reading as the
 * calibration run of 16-11-2026, which matched the table on 818 sizes.
 */
async function readProPage() {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const text = () => document.body.innerText.replace(/\s+/g, " ");
  const button = (re) => [...document.querySelectorAll("button")].find((b) => re.test((b.innerText || "").trim()));

  for (let i = 0; i < 20 && !text().includes("Create Bids"); i += 1) await sleep(1000);

  if (!text().includes("Create Bids")) {
    const t = text().toLowerCase();
    const loggedOut = location.hostname.startsWith("accounts.") || t.includes("log in") || t.includes("sign in");

    return { error: loggedOut ? "logged_out" : "page did not load" };
  }

  // A first visit shows a "Welcome to StockX Pro" box over the page.
  const close = button(/^Close$/);
  if (close && text().includes("Welcome to StockX Pro")) { close.click(); await sleep(1000); }

  // The table loads after the frame; wait for its controls rather than a fixed time.
  for (let i = 0; i < 40 && !button(/^All Sizes$/) && !button(/^EU$/); i += 1) await sleep(1000);

  const eu = button(/^EU$/);
  if (eu) { eu.click(); await sleep(1500); }

  const all = button(/^All Sizes$/);
  if (all) { all.click(); await sleep(3500); }

  const buyNow = button(/^Buy Now$/);
  if (buyNow) { buyNow.click(); await sleep(2500); }

  const t = text();
  const start = t.indexOf("Total (incl. tax)");
  const end = t.indexOf("Market Data", start);

  if (start < 0) return { error: "no price table" };

  const block = t.slice(start + "Total (incl. tax)".length, end > 0 ? end : start + 8000);
  const euro = (v) => {
    if (!v || v === "--") return null;
    const n = Number(String(v).replace(/[€,]/g, ""));
    return Number.isFinite(n) ? n : null;
  };

  const sizes = {};
  const re = /(\S+(?: \d\/\d)?) (?:EU )?€ (€[\d,.]+|--) (€[\d,.]+|--) (€[\d,.]+|--) (€[\d,.]+|--) (€[\d,.]+|--)/g;
  let m;

  while ((m = re.exec(block))) {
    const total = euro(m[6]);
    if (total) sizes[m[1].replace(/^EU\s*/i, "")] = { total, buyNow: euro(m[5]) };
  }

  return { sizes };
}

/*
 * Runs inside a goat.com tab: every size's lowest new ask, in euro cents.
 *
 * Through GOAT's own JSON endpoints, called from the goat.com tab so its
 * cookies and Cloudflare clearance come along - the same way the Outsource
 * Engine's price check does it since GOAT rebuilt the site (21-11-2026).
 * GOAT serves two versions side by side: the rebuilt one (/api-web/,
 * /api/v2/, /en-nl/product/ pages) and the older one (/web-api/). The
 * rebuilt one is tried first; the older one only when its endpoints are not
 * there. Search is fuzzy, so a product only counts when its own sku matches.
 */
async function readGoatPage(sku, storedSlug) {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const compact = (v) => String(v || "").toUpperCase().replace(/[\s\-._]/g, "");
  const matches = (a, b) => {
    const parts = (v) => String(v || "").split("/").map(compact).filter(Boolean);
    const right = new Set(parts(b));
    return parts(a).some((x) => right.has(x));
  };

  const challenged = () => /just a moment|attention required|checking your browser/i.test(document.title);

  for (let i = 0; i < 30; i += 1) {
    // "interactive" is enough: the read is fetches from this origin, not the page.
    if (!challenged() && document.readyState !== "loading") break;
    await sleep(1000);
  }

  // Cloudflare still checking after half a minute: GOAT wants us to slow down.
  if (challenged()) return { error: "Cloudflare check on goat.com", throttled: true };

  async function request(path, init = {}) {
    const res = await fetch(new URL(path, location.origin).toString(), {
      credentials: "include",
      ...init,
      headers: { Accept: "application/json", ...(init.headers || {}) }
    });
    if (res.ok) return res.json();
    const error = new Error(`GOAT ${path.split("?")[0]} answered ${res.status}`);
    error.siteMissing = [400, 404, 405, 410, 415].includes(res.status);
    error.throttled = [403, 429].includes(res.status);
    throw error;
  }

  /*
    Three kinds of no: the product is not on GOAT (its prices are cleared,
    as before), GOAT is slowing us down (the GOAT loop pauses), or something
    else went wrong (the old prices stay and the product goes to the back).
  */
  const failure = (error) => ({
    error: error.message,
    notFound: /no exact GOAT product/.test(error.message),
    throttled: Boolean(error.throttled)
  });

  const ranked = (slugs) => {
    const wanted = compact(sku);
    const out = storedSlug ? [storedSlug] : [];
    slugs.filter(Boolean)
      .sort((a, b) => Number(compact(b).includes(wanted)) - Number(compact(a).includes(wanted)))
      .forEach((slug) => { if (!out.includes(slug)) out.push(slug); });
    return out.slice(0, 5);
  };

  async function rebuilt() {
    const search = await request("/api/v2/consumer-search/search-suggestions", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ salesChannel: 1, queryString: sku, pageLimit: 10, productLimit: 10, recordTypes: [1, 4], queries: [] })
    });

    let slug = null;
    let pages404 = 0;
    const candidates = ranked((search?.productItems || []).map((p) => p.slug));

    for (const candidate of candidates) {
      const page = await fetch(`/en-nl/product/${encodeURIComponent(candidate)}`, { credentials: "include" });
      if (page.status === 404) { pages404 += 1; continue; }
      if (!page.ok) continue;
      const area = document.createElement("textarea");
      area.innerHTML = ((await page.text()).match(/<title>([^<]*)<\/title>/) || [])[1] || "";
      const parts = area.value.split("|").map((x) => x.trim());
      if (parts.length >= 3 && matches(sku, parts[1])) { slug = candidate; break; }
    }

    if (!slug) {
      const error = new Error("no exact GOAT product");
      error.siteMissing = candidates.length > 0 && pages404 === candidates.length;
      throw error;
    }

    const bar = await request(`/api-web/buy-bar/v2?productTemplateId=${encodeURIComponent(slug)}&countryCode=NL&currency=EUR`);
    const sizes = [];

    for (const o of bar?.sizeOptions || []) {
      if (o.shoeCondition !== "new_no_defects" || o.boxCondition !== "good_condition") continue;
      // "Buy New" is the ship-to-verify price; instant is the dearer one.
      const prices = (o?.shippingOptions?.shipToVerify || []).map((x) => Number(x?.amount)).filter((x) => Number.isFinite(x) && x > 0);
      if (prices.length) sizes.push({ us: Number(o?.sizeOption?.value), cents: Math.min(...prices) });
    }

    return { slug, sizes };
  }

  async function older() {
    const search = await request(`/web-api/consumer-search/get-product-search-results?salesChannelId=1&queryString=${encodeURIComponent(sku)}&sortType=1&pageLimit=12&pageNumber=1`);

    for (const candidate of ranked((search?.data?.productsList || []).map((p) => p.slug))) {
      const template = await request(`/web-api/v1/product_templates/${encodeURIComponent(candidate)}`).catch(() => null);
      if (!template?.id || !matches(sku, template.sku)) continue;

      const asks = await request(`/web-api/v1/product_variants/buy_bar_data?productTemplateId=${encodeURIComponent(template.id)}&countryCode=NL`);
      const sizes = (Array.isArray(asks) ? asks : [])
        .filter((a) => a.shoeCondition === "new_no_defects" && a.boxCondition === "good_condition")
        .map((a) => ({ us: Number(a?.sizeOption?.value), cents: Number(a?.lowestPriceCents?.amount) }))
        .filter((a) => a.cents > 0);

      return { slug: template.slug || candidate, sizes };
    }

    throw new Error("no exact GOAT product");
  }

  try {
    return await rebuilt();
  } catch (error) {
    if (!error.siteMissing) return failure(error);
    try { return await older(); } catch (e) { return failure(e); }
  }
}

/*
 * The goat.com tab, made or put back on goat.com when needed. Checked once
 * per batch rather than per product: the read is fetches from that origin,
 * so the tab only has to be there.
 */
async function goatTab(s) {
  let tabId = s.goatTabId;

  try {
    if (tabId) await chrome.tabs.get(tabId);
  } catch {
    tabId = null;
  }

  if (!tabId) {
    const tab = await chrome.tabs.create({ url: "https://www.goat.com/en-nl", active: false });
    tabId = tab.id;
    await chrome.storage.local.set({ goatTabId: tabId });
  }

  /*
    The GOAT tab only needs to be on goat.com: the read is a few fetches from
    that origin, not anything on the page. A tab that never reports
    "complete" (some request on goat.com that keeps it loading) used to fail
    every product after a 60 s wait - no GOAT price from 22-11 06:24 until
    23-11. Now: a short wait, one reload if it is stuck or not on goat.com,
    and the read goes ahead either way.
  */
  const onGoat = async () => /^https:\/\/(www\.)?goat\.com\//.test((await chrome.tabs.get(tabId)).url || "");

  if (!(await onGoat())) await chrome.tabs.update(tabId, { url: "https://www.goat.com/en-nl" });

  try {
    await waitForComplete(tabId, 15000);
  } catch {
    await chrome.tabs.reload(tabId);
    await waitForComplete(tabId, 15000).catch(() => null);
  }

  return tabId;
}

/*
 * Runs on the StockX login page. Only a plain e-mail + password form is
 * filled in; anything else is reported back untouched.
 */
async function tryPlainLogin(email, password) {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  await sleep(3000);

  const body = document.body.innerText.toLowerCase();

  if (/captcha|verify you are|press & hold|press and hold|verification code|security code/.test(body)) {
    return { ok: false, reason: "a check is shown" };
  }

  const emailInput = document.querySelector('input[type="email"], input[name="email"], input[name="username"]');
  const passwordInput = document.querySelector('input[type="password"]');

  if (!emailInput || !passwordInput) return { ok: false, reason: "no plain login form" };

  const set = (input, value) => {
    const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value").set;
    input.focus();
    setter.call(input, value);
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
  };

  set(emailInput, email);
  set(passwordInput, password);

  const submit = document.querySelector('button[type="submit"], input[type="submit"]');
  if (!submit) return { ok: false, reason: "no submit button" };

  submit.click();

  return { ok: true };
}

async function handleLoggedOut(s, tabId, productUrl) {
  if (s.email && s.password) {
    await status("logged out - trying a plain login");

    const [{ result }] = await chrome.scripting.executeScript({
      target: { tabId }, func: tryPlainLogin, args: [s.email, s.password]
    });

    if (result?.ok) {
      await wait(12000);
      await chrome.tabs.update(tabId, { url: productUrl });
      await waitForComplete(tabId);

      const [{ result: again }] = await chrome.scripting.executeScript({ target: { tabId }, func: readProPage });

      if (!again?.error) return again;
    }
  }

  await chrome.storage.local.set({ pausedUntil: Date.now() + LOGGED_OUT_WAIT_MS });
  await status("logged out - waiting for a manual login");
  await alert(s, "StockX Pro is logged out on the VPS. Log in once in the autopricing Chrome profile; it carries on by itself.");

  return null;
}

/*
 * One Pro read for one product. Returns what to post, or stop: true when the
 * Pro loop has to pause (throttled, logged out, page did not open) - the
 * pause and the alert are set here.
 */
async function readPro(item) {
  const s = await settings();
  const url = `https://pro.stockx.com/purchasing/create/${item.productId}`;
  const apiRead = async (tabId) => {
    const [{ result }] = await chrome.scripting.executeScript({
      target: { tabId }, world: "MAIN", func: readProApi, args: [item.productId, PRICING_PARALLEL]
    });

    return result || { error: "not_ready" };
  };

  /*
    The open Pro page is used as it is while it is fresh: no page load per
    product. A stale, missing or broken page is loaded again - on this
    product, so the clicking read can take over when the API read cannot.
  */
  let tabId = await openProTab(s);
  let read = tabId ? await apiRead(tabId) : null;

  if (!read || (read.error && !["throttled", "no product"].includes(read.error))) {
    tabId = await workTab(s, url);
    await waitForComplete(tabId);
    await chrome.storage.local.set({ proLoadedAt: Date.now() });

    read = await apiRead(tabId);

    if (read.error === "not_ready" || String(read.error).startsWith("failed")) {
      const [{ result }] = await chrome.scripting.executeScript({ target: { tabId }, func: readProPage });
      read = result;
    }
  }

  if (read?.error === "throttled") {
    await chrome.storage.local.set({ pausedUntil: Date.now() + THROTTLED_WAIT_MS });
    await status("StockX asked us to slow down - waiting 20 minutes");
    await alert(s, "StockX Pro answered 'too many requests' to the autopricing reader. It waits 20 minutes and carries on; if this repeats, the pace (AUTOPRICE_PRO_PAUSE_MS on Render) is too high.");
    return { stop: true };
  }

  if (read?.error === "logged_out") {
    read = await handleLoggedOut(s, tabId, url);
    if (!read) return { stop: true };
  }

  /*
    The Pro page itself did not come up: almost always a logged-out browser
    that StockX shows as a landing page rather than a login form. Nothing is
    marked as read - otherwise a logged-out browser would walk the whole
    queue and leave every product "done" with no prices. Pause and alert.
  */
  if (read?.error === "page did not load") {
    await chrome.storage.local.set({ pausedUntil: Date.now() + LOGGED_OUT_WAIT_MS });
    await status("StockX Pro did not open - logged out? Log in, then press Start");
    await alert(s, "StockX Pro did not open on the VPS (logged out?). Log in once in the autopricing Chrome profile and press Start in the extension.");
    return { stop: true };
  }

  /*
    No such product, or no table: an empty read, which clears its Pro prices.
    Anything else that went wrong keeps the old prices (failed: true); the
    product only goes to the back of the queue.
  */
  if (read?.error) {
    const empty = read.error === "no product" || read.error === "no price table";

    return { error: read.error, body: empty ? { sku: item.sku, sizes: {} } : { sku: item.sku, failed: true } };
  }

  return { body: { sku: item.sku, sizes: read.sizes } };
}

/*
 * A batch of Pro reads. Posts go out without waiting, so the next read can
 * start; the batch ends by waiting for all of them, and only then is the next
 * batch asked for - so the server never hands out a product whose answer is
 * still on its way.
 */
async function proBatch() {
  const s = await settings();
  const next = await api(s, `/autoprice/pro-next?n=${PRO_BATCH}`);

  if (next.idle || !next.items?.length) {
    await chrome.storage.local.set({ pausedUntil: Date.now() + IDLE_WAIT_MS });
    await status("Pro: nothing due");
    return;
  }

  pauseRange = validPause(next.pauseMs) || BETWEEN_PRODUCTS_MS;

  const started = Date.now();
  const pending = [];
  const tally = { read: 0, sizes: 0, errors: 0, postFailed: 0 };

  try {
    for (const item of next.items) {
      const { stop, body, error } = await readPro(item);

      if (stop) break;

      tally.read += 1;
      if (error) tally.errors += 1;

      pending.push(
        api(s, "/autoprice/stockx-pro", body)
          .then((r) => { tally.sizes += Number(r?.stored) || 0; })
          .catch(() => { tally.postFailed += 1; })
      );

      await wait(jitter(pauseRange));
    }
  } finally {
    await Promise.all(pending);
  }

  const perProduct = tally.read ? ((Date.now() - started) / tally.read / 1000).toFixed(1) : "-";

  await status(`Pro: ${tally.read} products, ${tally.sizes} sizes, ${perProduct} s each` +
    (tally.errors ? `, ${tally.errors} read errors` : "") + (tally.postFailed ? `, ${tally.postFailed} posts failed` : ""));
}

/*
 * A batch of GOAT reads, in the GOAT tab, next to the Pro loop. A product
 * not on GOAT is posted empty (its GOAT prices go), a failed read keeps the
 * old prices, and a GOAT that slows us down pauses only this loop.
 */
async function goatBatch() {
  const s = await settings();
  const next = await api(s, `/autoprice/goat-next?n=${GOAT_BATCH}`);

  if (next.idle || !next.items?.length) {
    await chrome.storage.local.set({ goatPausedUntil: Date.now() + IDLE_WAIT_MS });
    await goatStatus("nothing due");
    return;
  }

  goatPauseRange = validPause(next.pauseMs) || GOAT_BETWEEN_MS;

  const tabId = await goatTab(s);
  const started = Date.now();
  const pending = [];
  const tally = { read: 0, sizes: 0, notFound: 0, errors: 0, postFailed: 0 };

  try {
    for (const item of next.items) {
      const [{ result }] = await chrome.scripting.executeScript({ target: { tabId }, func: readGoatPage, args: [item.sku, item.goatSlug || null] });

      if (result?.throttled) {
        await chrome.storage.local.set({ goatPausedUntil: Date.now() + THROTTLED_WAIT_MS });
        await goatStatus(`GOAT asked us to slow down (${result.error}) - waiting 20 minutes`);
        await alert(s, `goat.com is slowing the autopricing reader down (${result.error}). The GOAT reads wait 20 minutes; if this repeats, raise AUTOPRICE_GOAT_PAUSE_MS on Render.`);
        break;
      }

      let body;

      if (result && !result.error) body = { sku: item.sku, slug: result.slug, sizes: result.sizes };
      else if (result?.notFound) { body = { sku: item.sku, slug: null, sizes: [] }; tally.notFound += 1; }
      else { body = { sku: item.sku, failed: true }; tally.errors += 1; }

      tally.read += 1;

      pending.push(
        api(s, "/autoprice/goat", body)
          .then((r) => { tally.sizes += Number(r?.stored) || 0; })
          .catch(() => { tally.postFailed += 1; })
      );

      await wait(jitter(goatPauseRange));
    }
  } finally {
    await Promise.all(pending);
  }

  const perProduct = tally.read ? ((Date.now() - started) / tally.read / 1000).toFixed(1) : "-";

  await goatStatus(`${tally.read} products, ${tally.sizes} sizes, ${perProduct} s each` +
    (tally.notFound ? `, ${tally.notFound} not on GOAT` : "") + (tally.errors ? `, ${tally.errors} errors` : "") +
    (tally.postFailed ? `, ${tally.postFailed} posts failed` : ""));
}

function validPause(range) {
  return Array.isArray(range) && range.length === 2 && range.every((n) => Number(n) >= 0) ? range.map(Number) : null;
}

async function goatStatus(text) {
  await chrome.storage.local.set({ goatStatus: new Date().toLocaleTimeString() + " " + text });
}

/*
 * Two loops side by side, each with its own pause: a GOAT problem never
 * stops the Pro reads, and a logged-out Pro never stops GOAT.
 */
const loops = { pro: false, goat: false };

async function runLoop(name, pausedKey, batch, onError) {
  if (loops[name]) return;

  loops[name] = true;

  try {
    for (;;) {
      const s = await chrome.storage.local.get(["enabled", "server", "secret", pausedKey]);

      if (!s.enabled || !s.server || !s.secret) break;
      if (s[pausedKey] && Date.now() < s[pausedKey]) break;

      try {
        await batch();
      } catch (error) {
        await onError("error: " + error.message);
        await wait(60000);
      }
    }
  } finally {
    loops[name] = false;
  }
}

function loop() {
  runLoop("pro", "pausedUntil", proBatch, status);
  runLoop("goat", "goatPausedUntil", goatBatch, goatStatus);
}

// The alarm wakes the service worker again after Chrome put it to sleep.
chrome.alarms.create(ALARM, { periodInMinutes: 1 });
chrome.alarms.onAlarm.addListener((a) => { if (a.name === ALARM) loop(); });
chrome.runtime.onMessage.addListener((m) => {
  if (m?.type === "kick") chrome.storage.local.set({ pausedUntil: 0, goatPausedUntil: 0 }).then(loop);
});
chrome.runtime.onStartup.addListener(loop);
chrome.runtime.onInstalled.addListener(loop);
