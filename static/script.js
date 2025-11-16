document.cookie = ""; // prevent browser cookie clutter

let currentKey = null;
let lastQuery = null;
let currentLayer = null;
let lastAdvancedRows = null;

/* ---------- Leaflet Map ---------- */
const map = L.map("map").setView([37.8,-96],4);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png").addTo(map);

function startLoad(){
  document.getElementById("progress").style.display="block";
  let width = 0;
  const timer = setInterval(() => {
    width = Math.min(width + 8, 95);
    document.getElementById("bar").style.width = width + "%";
  }, 200);
  return timer;
}

function stopLoad(timer){
  clearInterval(timer);
  document.getElementById("bar").style.width = "100%";
  setTimeout(() => {
    document.getElementById("progress").style.display="none";
  }, 500);
}

function plotPts(rows){
  if (currentLayer) map.removeLayer(currentLayer);

  const points = [];
  rows.forEach(r => {
    if (r.geometry && r.geometry.coordinates) {
      const [lon, lat] = r.geometry.coordinates;
      points.push([lat, lon]);
    }
  });

  if (points.length) {
    currentLayer = L.layerGroup(
      points.map(pt => L.circleMarker(pt, { radius:3 }))
    ).addTo(map);
    map.fitBounds(points);
  }
}

/* ---------- SIMPLE SEARCH ---------- */
async function runSimple(){
  const q = document.getElementById("query").value;
  const fd = new FormData();
  fd.append("query", q);

  const timer = startLoad();
  const res = await fetch("/search", { method:"POST", body: fd });
  stopLoad(timer);

  const data = await res.json();
  currentKey = data.key;

  document.getElementById("result").textContent = data.message;
  document.getElementById("summary").style.display = "none";
  
  document.getElementById("exportBtn").style.display = "inline-block";
  document.getElementById("exportAdvBtn").style.display = "none";

  document.getElementById("preview").textContent =
    JSON.stringify(data.preview, null, 2);

  plotPts(data.preview);
}

/* ---------- ADVANCED SEARCH ---------- */
async function runAdv(){
  const q = document.getElementById("query").value;
  lastQuery = q;

  const timer = startLoad();
  const res = await fetch("/search_advanced", {
    method:"POST",
    headers:{ "Content-Type":"application/json" },
    body: JSON.stringify({ query: q })
  });
  stopLoad(timer);

  const data = await res.json();

  document.getElementById("result").textContent =
    `✅ Showing up to ${data.results.length} of ${data.count}`;

  document.getElementById("exportBtn").style.display = "none";
  document.getElementById("exportAdvBtn").style.display = "inline-block";

  lastAdvancedRows = data.results;

  document.getElementById("preview").textContent =
    JSON.stringify(data.results, null, 2);

  plotPts(data.results);
}

/* ---------- EXPORT HANDLERS ---------- */
document.getElementById("exportBtn").onclick = async () => {
  const fd = new FormData();
  fd.append("key", currentKey);

  const res = await fetch("/export", { method:"POST", body: fd });
  const blob = await res.blob();

  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "export.zip";
  a.click();
};

document.getElementById("exportAdvBtn").onclick = async () => {
  const fd = new FormData();
  fd.append("query", lastQuery);

  const res = await fetch("/export_advanced_csv", { method:"POST", body: fd });
  const blob = await res.blob();

  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "advanced_export.csv";
  a.click();
};

/* ---------- BIND BUTTONS ---------- */
document.getElementById("btnSimple").onclick = runSimple;
document.getElementById("btnAdvanced").onclick = runAdv;

document.getElementById("year").textContent = new Date().getFullYear();
