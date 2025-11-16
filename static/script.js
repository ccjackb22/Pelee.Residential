document.cookie = ""; // prevent browser cookie clutter

let lastFilters = null;
let currentLayer = null;

/* ---------- Leaflet Map ---------- */
const map = L.map("map").setView([37.8, -96], 4);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png").addTo(map);

function startLoad() {
  document.getElementById("progress").style.display = "block";
  let width = 0;
  const timer = setInterval(() => {
    width = Math.min(width + 8, 95);
    document.getElementById("bar").style.width = width + "%";
  }, 200);
  return timer;
}

function stopLoad(timer) {
  clearInterval(timer);
  document.getElementById("bar").style.width = "100%";
  setTimeout(() => {
    document.getElementById("progress").style.display = "none";
  }, 500);
}

function plotPts(rows) {
  if (currentLayer) map.removeLayer(currentLayer);
  const points = [];

  rows.forEach(r => {
    if (r.lat && r.lon) points.push([r.lat, r.lon]);
  });

  if (points.length) {
    currentLayer = L.layerGroup(points.map(pt => L.circleMarker(pt, { radius: 3 }))).addTo(map);
    map.fitBounds(points);
  }
}

/* ------------------------------------------------------------
   SIMPLE SEARCH → parse_query → search_advanced
------------------------------------------------------------ */
async function runSimple() {
  const q = document.getElementById("query").value.trim();
  if (!q) return;

  const timer = startLoad();

  // Step 1 → parse query
  const res1 = await fetch("/parse_query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: q })
  });

  const parsed = await res1.json();
  if (!parsed.ok) {
    stopLoad(timer);
    alert("Query parsing failed: " + parsed.error);
    return;
  }

  lastFilters = parsed.filters;

  // Step 2 → run the search using parsed filters
  const res2 = await fetch("/search_advanced", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(lastFilters)
  });

  stopLoad(timer);
  const data = await res2.json();

  if (!data.ok) {
    alert("Search failed: " + data.error);
    return;
  }

  updateUI(data.results, data.count);
}

/* ------------------------------------------------------------
   ADVANCED SEARCH (currently same as simple)
------------------------------------------------------------ */
async function runAdv() {
  // For now identical to simple until advanced UI exists
  return runSimple();
}

/* ------------------------------------------------------------
   UPDATE PAGE RESULTS
------------------------------------------------------------ */
function updateUI(results, total) {
  document.getElementById("result").textContent =
    `✅ Showing ${results.length} of ${total}`;

  document.getElementById("preview").textContent =
    JSON.stringify(results.slice(0, 20), null, 2);

  // show advanced CSV export
  document.getElementById("exportBtn").style.display = "none";
  document.getElementById("exportAdvBtn").style.display = "inline-block";

  plotPts(results);
}

/* ------------------------------------------------------------
   EXPORT CSV
------------------------------------------------------------ */
document.getElementById("exportAdvBtn").onclick = async () => {
  if (!lastFilters) {
    alert("Run a search first.");
    return;
  }

  const res = await fetch("/download_csv", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(lastFilters)
  });

  const blob = await res.blob();
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "addresses.csv";
  a.click();
};

/* ------------------------------------------------------------
   BUTTON EVENTS
------------------------------------------------------------ */
document.getElementById("btnSimple").onclick = runSimple;
document.getElementById("btnAdvanced").onclick = runAdv;

document.getElementById("year").textContent = new Date().getFullYear();
