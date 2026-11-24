const $ = (id) => document.getElementById(id);
const fields = ["server", "secret", "email", "password"];

async function render() {
  const s = await chrome.storage.local.get([...fields, "enabled", "lastStatus", "goatStatus"]);
  for (const f of fields) if (document.activeElement !== $(f)) $(f).value = s[f] || "";
  $("toggle").textContent = s.enabled ? "Stop" : "Start";
  $("status").textContent = [s.lastStatus, s.goatStatus && "GOAT: " + s.goatStatus].filter(Boolean).join("\n");
}

$("save").onclick = async () => {
  const values = Object.fromEntries(fields.map((f) => [f, $(f).value.trim()]));
  if (!values.server) values.server = "https://shopify-active-product-sync.onrender.com";
  await chrome.storage.local.set(values);
  render();
};

$("toggle").onclick = async () => {
  const { enabled } = await chrome.storage.local.get("enabled");
  await chrome.storage.local.set({ enabled: !enabled });
  chrome.runtime.sendMessage({ type: "kick" });
  render();
};

render();
setInterval(render, 2000);
