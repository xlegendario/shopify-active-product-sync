/*
 * Autopricing: reads StockX Pro for one product at a time, all sizes at once.
 *
 * Separate from the Outsource Engine extension on purpose: its own Chrome
 * profile, its own queue, so it can never hold up the price checks on real
 * orders. It only reads the Buy Now table; it never bids or buys.
 *
 * Loop: ask the server which product is due, open its Pro page, switch to
 * EU sizes, show all sizes, select the Buy Now column, read every row's
 * Total (incl. tax), send it back, wait, repeat.
 *
 * When StockX shows a login page: try once to log in with the e-mail and
 * password saved in the popup, but only if a plain form appears. Anything
 * else (a verification code, a bot check) is never worked around: it
 * stops, alerts on Discord and waits for a person to log in.
 */

const ALARM = "autoprice-tick";
const BETWEEN_PRODUCTS_MS = [20000, 40000];
const IDLE_WAIT_MS = 10 * 60 * 1000;
const LOGGED_OUT_WAIT_MS = 30 * 60 * 1000;

let running = false;

const wait = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = ([a, b]) => a + Math.random() * (b - a);

async function settings() {
  return chrome.storage.local.get(["server", "secret", "email", "password", "enabled", "pausedUntil", "tabId"]);
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

async function runOnce() {
  const s = await settings();

  if (!s.enabled || !s.server || !s.secret) return;
  if (s.pausedUntil && Date.now() < s.pausedUntil) return;

  const next = await api(s, "/autoprice/pro-next");

  if (next.idle) {
    await chrome.storage.local.set({ pausedUntil: Date.now() + IDLE_WAIT_MS });
    await status("nothing due");
    return;
  }

  const url = `https://pro.stockx.com/purchasing/create/${next.productId}`;
  const tabId = await workTab(s, url);

  await waitForComplete(tabId);

  const [{ result }] = await chrome.scripting.executeScript({ target: { tabId }, func: readProPage });
  let read = result;

  if (read?.error === "logged_out") {
    read = await handleLoggedOut(s, tabId, url);
    if (!read) return;
  }

  if (read?.error) {
    await status(`${next.sku}: ${read.error}`);
    // Still marked as read, so one broken page does not block the queue.
    await api(s, "/autoprice/stockx-pro", { sku: next.sku, sizes: {} });
    return;
  }

  const r = await api(s, "/autoprice/stockx-pro", { sku: next.sku, sizes: read.sizes });

  await chrome.storage.local.set({ pausedUntil: 0 });
  await status(`${next.sku}: ${r.stored} sizes`);
}

async function loop() {
  if (running) return;

  running = true;

  try {
    for (;;) {
      const s = await settings();

      if (!s.enabled) break;
      if (s.pausedUntil && Date.now() < s.pausedUntil) break;

      try {
        await runOnce();
      } catch (error) {
        await status("error: " + error.message);
        await wait(60000);
      }

      await wait(jitter(BETWEEN_PRODUCTS_MS));
    }
  } finally {
    running = false;
  }
}

// The alarm wakes the service worker again after Chrome put it to sleep.
chrome.alarms.create(ALARM, { periodInMinutes: 1 });
chrome.alarms.onAlarm.addListener((a) => { if (a.name === ALARM) loop(); });
chrome.runtime.onMessage.addListener((m) => { if (m?.type === "kick") { chrome.storage.local.set({ pausedUntil: 0 }); loop(); } });
chrome.runtime.onStartup.addListener(loop);
chrome.runtime.onInstalled.addListener(loop);
