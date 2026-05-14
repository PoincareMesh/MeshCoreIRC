import asyncio
import html as _html
import json
import logging
import math
import time
import urllib.parse

import neighbours_store
from bridge import TYPE_FILTERS, sanitize_nick

logger = logging.getLogger(__name__)

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  body {{ margin: 0; font-family: sans-serif; }}
  #map {{ height: 100vh; }}
  #legend {{
    position: absolute; bottom: 30px; right: 10px; z-index: 1000;
    background: white; padding: 8px 12px; border-radius: 6px;
    box-shadow: 0 1px 5px rgba(0,0,0,.3); font-size: 13px; line-height: 1.9;
  }}
  .dot {{
    display: inline-block; width: 12px; height: 12px; border-radius: 50%;
    border: 2px solid #fff; box-shadow: 0 1px 3px rgba(0,0,0,.4);
    margin-right: 4px; vertical-align: middle;
  }}
  #title {{
    position: absolute; top: 10px; left: 50%; transform: translateX(-50%);
    z-index: 1000; background: white; padding: 5px 14px; border-radius: 6px;
    box-shadow: 0 1px 5px rgba(0,0,0,.3); font-size: 14px; font-weight: bold;
    white-space: nowrap;
  }}
</style>
</head>
<body>
<div id="map"></div>
<div id="title">{title}</div>
<div id="legend">
  <span class="dot" style="background:#e74c3c"></span>repeater<br>
  <span class="dot" style="background:#3388ff"></span>companion<br>
  <span class="dot" style="background:#27ae60"></span>room<br>
  <span class="dot" style="background:#888888"></span>sensor
</div>
<script>
const TYPE_COLOR = {{0: '#888888', 1: '#3388ff', 2: '#e74c3c', 3: '#27ae60', 4: '#888888'}};
const NODES = {nodes_json};
const REP_LAT = {rep_lat};
const REP_LON = {rep_lon};
const REP_NAME = {rep_name_json};

const map = L.map('map');
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  maxZoom: 19,
  attribution: '&copy; <a href="https://openstreetmap.org/copyright">OpenStreetMap</a> contributors'
}}).addTo(map);

function fmtAge(ts) {{
  if (!ts) return null;
  const secs = Math.round(Date.now() / 1000 - ts);
  if (secs < 120) return secs + 's ago';
  if (secs < 7200) return Math.floor(secs / 60) + 'm ago';
  if (secs < 172800) return Math.floor(secs / 3600) + 'h ago';
  return Math.floor(secs / 86400) + 'd ago';
}}

function makeIcon(color, size) {{
  size = size || 14;
  const half = size / 2;
  return L.divIcon({{
    html: '<div style="background:' + color + ';width:' + size + 'px;height:' + size + 'px;' +
          'border-radius:50%;border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.5)"></div>',
    iconSize: [size, size], iconAnchor: [half, half], popupAnchor: [0, -half - 2], className: ''
  }});
}}

function distKm(lat1, lon1, lat2, lon2) {{
  var R = 6371, dLat = (lat2-lat1)*Math.PI/180, dLon = (lon2-lon1)*Math.PI/180;
  var a = Math.sin(dLat/2)*Math.sin(dLat/2) + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLon/2)*Math.sin(dLon/2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}}

// Labels behind lines, lines behind node markers
map.createPane('labelPane');
map.getPane('labelPane').style.zIndex = 350;
map.getPane('labelPane').style.pointerEvents = 'none';

if (REP_LAT || REP_LON) {{
  NODES.forEach(function(n) {{
    if (!n.lat && !n.lon) return;
    if (n.snr != null) {{
      const mid = [(REP_LAT + n.lat) / 2, (REP_LON + n.lon) / 2];
      const d = distKm(REP_LAT, REP_LON, n.lat, n.lon);
      L.marker(mid, {{
        pane: 'labelPane',
        icon: L.divIcon({{
          html: '<div style="display:inline-block;transform:translate(-50%,-50%);background:rgba(255,255,255,0.88);' +
                'padding:{snr_padding};border-radius:3px;font-size:{snr_font_size}px;border:1px solid #bbb;' +
                'white-space:nowrap">' + d.toFixed(1) + ' km  ' + n.snr.toFixed(1) + ' dB</div>',
          className: '', iconSize: [0, 0]
        }}),
        interactive: false
      }}).addTo(map);
    }}
  }});
  NODES.forEach(function(n) {{
    if (!n.lat && !n.lon) return;
    L.polyline([[REP_LAT, REP_LON], [n.lat, n.lon]], {{
      color: '#888', weight: {line_weight}, dashArray: '5,5', opacity: 0.7
    }}).addTo(map);
  }});
}}

const markers = [];
NODES.forEach(function(n) {{
  const color = TYPE_COLOR[n.type] || '#3388ff';
  const age = fmtAge(n.ts);
  const popup = '<b>' + n.name + '</b>' + (age ? '<br><span style="font-size:0.85em;color:#555">' + age + '</span>' : '');
  const m = L.marker([n.lat, n.lon], {{icon: makeIcon(color)}})
    .bindPopup(popup)
    .addTo(map);
  markers.push(m);
}});

// Repeater marker on top (larger, with ring)
if (REP_LAT || REP_LON) {{
  L.marker([REP_LAT, REP_LON], {{
    icon: L.divIcon({{
      html: '<div style="background:#e74c3c;width:18px;height:18px;border-radius:50%;' +
            'border:3px solid #fff;box-shadow:0 0 0 2px #e74c3c,0 1px 6px rgba(0,0,0,.5)"></div>',
      iconSize: [18, 18], iconAnchor: [9, 9], popupAnchor: [0, -12], className: ''
    }})
  }}).bindPopup('<b>' + REP_NAME + '</b>').addTo(map);
}}

const allLatLngs = markers.map(function(m) {{ return m.getLatLng(); }});
if (REP_LAT || REP_LON) {{ allLatLngs.push(L.latLng(REP_LAT, REP_LON)); }}
if (allLatLngs.length > 0) {{
  map.fitBounds(L.latLngBounds(allLatLngs).pad(0.15));
}} else {{
  map.setView([51, 10], 6);
}}
</script>
</body>
</html>
"""

_404 = b"HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 9\r\nConnection: close\r\n\r\nNot found"

_PATH_TEMPLATE = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  body {{ margin: 0; font-family: sans-serif; }}
  #map {{ height: 100vh; }}
  #title {{
    position: absolute; top: 10px; left: 50%; transform: translateX(-50%);
    z-index: 1000; background: white; padding: 5px 14px; border-radius: 6px;
    box-shadow: 0 1px 5px rgba(0,0,0,.3); font-size: 14px; font-weight: bold;
    white-space: nowrap;
  }}
  #info {{
    position: absolute; bottom: 30px; right: 10px; z-index: 1000;
    background: white; padding: 8px 12px; border-radius: 6px;
    box-shadow: 0 1px 5px rgba(0,0,0,.3); font-size: 13px; line-height: 1.9;
  }}
  .dot {{
    display: inline-block; width: 12px; height: 12px; border-radius: 50%;
    border: 2px solid #fff; box-shadow: 0 1px 3px rgba(0,0,0,.4);
    margin-right: 4px; vertical-align: middle;
  }}
  .leg-line {{
    display: inline-block; width: 24px; height: 3px;
    margin-right: 5px; vertical-align: middle;
  }}
</style>
</head>
<body>
<div id="map"></div>
<div id="title">{title}</div>
<div id="info"></div>
<script>
const ADVERT_CHAIN = {advert_chain_json};
const MSG_CHAIN    = {msg_chain_json};
const ADVERT_HOPS  = {advert_hops};
const MSG_HOPS     = {msg_hops};
const ADVERT_HASH_CHARS = {advert_hash_chars};
const MSG_HASH_CHARS    = {msg_hash_chars};
const DIRECT_WEIGHT = {direct_weight};
const COLOR_SELF    = '{color_self}';
const COLOR_HOP     = '{color_hop}';
const COLOR_TARGET  = '{color_target}';
const COLOR_REMOTE  = '{color_remote}';
const COLOR_UNKNOWN_BG = '{color_unknown_bg}';

const map = L.map('map');
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  maxZoom: 19,
  attribution: '&copy; <a href="https://openstreetmap.org/copyright">OpenStreetMap</a> contributors'
}}).addTo(map);

const ROLE_COLOR = {{'self': COLOR_SELF, 'hop': COLOR_HOP, 'target': COLOR_TARGET}};
const ROLE_SIZE  = {{'self': 16, 'hop': 12, 'target': 18}};
const ROLE_LABEL = {{'self': 'Gateway', 'hop': 'Via', 'target': 'Target'}};

function makeIcon(color, size, remote, unknown) {{
  const half = size / 2;
  var html;
  if (unknown) {{
    html = '<div style="background:#fff;width:' + size + 'px;height:' + size + 'px;' +
           'border-radius:50%;border:2px dashed ' + color + ';box-shadow:0 1px 3px rgba(0,0,0,.25)"></div>';
  }} else if (remote) {{
    html = '<div style="background:' + COLOR_REMOTE + ';width:' + size + 'px;height:' + size + 'px;' +
           'border-radius:50%;border:2px solid #fff;box-shadow:0 1px 3px rgba(0,0,0,.25)"></div>';
  }} else {{
    html = '<div style="background:' + color + ';width:' + size + 'px;height:' + size + 'px;' +
           'border-radius:50%;border:2px solid #fff;' +
           'box-shadow:0 0 0 2px ' + color + ',0 1px 6px rgba(0,0,0,.5)"></div>';
  }}
  return L.divIcon({{
    html: html,
    iconSize: [size, size], iconAnchor: [half, half], popupAnchor: [0, -half - 2], className: ''
  }});
}}

function distKm(lat1, lon1, lat2, lon2) {{
  var R = 6371.0, dLat = (lat2-lat1)*Math.PI/180, dLon = (lon2-lon1)*Math.PI/180;
  var a = Math.sin(dLat/2)*Math.sin(dLat/2) +
          Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*
          Math.sin(dLon/2)*Math.sin(dLon/2);
  return R*2*Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}}

function drawPath(chain, lineColor, dashArray, weight, labelBg, labelBorder, labelTextColor) {{
  var pts = chain.filter(function(n) {{ return n.lat || n.lon; }});
  if (pts.length < 2) return 0;
  L.polyline(pts.map(function(n) {{ return [n.lat, n.lon]; }}), {{
    color: lineColor, weight: weight, dashArray: dashArray, opacity: 0.85
  }}).addTo(map);
  var total = 0;
  for (var i = 0; i < pts.length - 1; i++) {{
    var a = pts[i], b = pts[i+1];
    if (a.src === 'interpolated' || b.src === 'interpolated') continue;
    var d = distKm(a.lat, a.lon, b.lat, b.lon);
    total += d;
    var mid = [(a.lat+b.lat)/2, (a.lon+b.lon)/2];
    L.marker(mid, {{
      icon: L.divIcon({{
        html: '<div style="display:inline-block;transform:translate(-50%,-50%);background:' + labelBg + ';' +
              'padding:2px 6px;border-radius:3px;font-size:12px;border:1px solid ' + labelBorder + ';' +
              'white-space:nowrap;color:' + labelTextColor + '">' + d.toFixed(1) + ' km</div>',
        className: '', iconSize: [0, 0]
      }}), interactive: false
    }}).addTo(map);
  }}
  var realPts = pts.filter(function(n) {{ return n.src !== 'interpolated'; }});
  if (realPts.length >= 2) {{
    var p0 = realPts[0], p1 = realPts[realPts.length-1];
    var dd = distKm(p0.lat, p0.lon, p1.lat, p1.lon);
    L.polyline([[p0.lat,p0.lon],[p1.lat,p1.lon]], {{color:'#3388ff',weight:DIRECT_WEIGHT,opacity:0.55}}).addTo(map);
    var dmid = [(p0.lat+p1.lat)/2, (p0.lon+p1.lon)/2];
    L.marker(dmid, {{
      icon: L.divIcon({{
        html: '<div style="display:inline-block;transform:translate(-50%,-50%);background:rgba(219,234,254,0.92);' +
              'padding:2px 6px;border-radius:3px;font-size:12px;border:1px solid #93c5fd;white-space:nowrap;color:#1d4ed8">' +
              dd.toFixed(1) + ' km</div>',
      className: '', iconSize: [0, 0]
    }}), interactive: false
  }}).addTo(map);
  }}
  return total;
}}

// Fill interpolated positions for unknown nodes so drawPath lines pass through them.
// No markers here — all markers come after drawPath so they appear on top.
function fillUnknown(chain) {{
  var i = 0;
  while (i < chain.length) {{
    if (chain[i].lat || chain[i].lon) {{ i++; continue; }}
    var runStart = i;
    while (i < chain.length && !chain[i].lat && !chain[i].lon) i++;
    var runLen = i - runStart;
    var prevNode = null, nextNode = null;
    for (var j = runStart - 1; j >= 0; j--) {{
      if (chain[j].lat || chain[j].lon) {{ prevNode = chain[j]; break; }}
    }}
    for (var j = i; j < chain.length; j++) {{
      if (chain[j].lat || chain[j].lon) {{ nextNode = chain[j]; break; }}
    }}
    var chainEnd = null;
    if (!prevNode && nextNode) {{
      for (var j = chain.length - 1; j > runStart; j--) {{
        if (chain[j].lat || chain[j].lon) {{ chainEnd = chain[j]; break; }}
      }}
    }}
    for (var k = 0; k < runLen; k++) {{
      var nd = chain[runStart + k];
      var lat, lon;
      if (prevNode && nextNode) {{
        var frac = (k + 1) / (runLen + 1);
        lat = prevNode.lat + frac * (nextNode.lat - prevNode.lat);
        lon = prevNode.lon + frac * (nextNode.lon - prevNode.lon);
      }} else if (prevNode) {{
        lat = prevNode.lat - (k + 1) * 0.008; lon = prevNode.lon;
      }} else if (nextNode) {{
        // chainEnd must be a DIFFERENT node from nextNode to give a usable direction.
        // If they're the same (only one known reference), skip — the other chain
        // (msg or advert) will place the marker correctly.
        if (chainEnd && chainEnd !== nextNode) {{
          var ddlat = nextNode.lat - chainEnd.lat, ddlon = nextNode.lon - chainEnd.lon;
          var ddlen = Math.sqrt(ddlat * ddlat + ddlon * ddlon);
          if (ddlen > 0) {{
            // Place farthest unknown (k=0, the target) at runLen steps, closest at 1 step,
            // so all segments are equal length in the same direction toward nextNode.
            // Perpendicular offset keeps the unknown tail visually distinct from the main line.
            var pLat = -ddlon / ddlen * 0.012, pLon = ddlat / ddlen * 0.012;
            lat = nextNode.lat + (runLen - k) * ddlat / ddlen * 0.04 + pLat;
            lon = nextNode.lon + (runLen - k) * ddlon / ddlen * 0.04 + pLon;
          }} else continue;
        }} else continue;
      }} else continue;
      nd.lat = lat; nd.lon = lon; nd.src = 'interpolated';
    }}
  }}
}}

// Fill interpolated positions before drawPath so lines connect through unknown nodes.
fillUnknown(ADVERT_CHAIN);
fillUnknown(MSG_CHAIN);

// Draw paths first so lines render below markers.
var advertDist = drawPath(ADVERT_CHAIN, '#666', '7,5', {advert_weight},
  'rgba(255,255,255,0.88)', '#bbb', '#333');
var msgDist = drawPath(MSG_CHAIN, '#e67e22', '5,4', {msg_weight},
  'rgba(255,237,213,0.92)', '#fdba74', '#9a3412');

// Unified marker loop — runs after drawPath so all markers appear on top of lines.
// Advert chain takes priority for role colour; deduplicate by name.
var seenNames = {{}};
var latlngs = [];
var hasRemote = false;
var unknownNames = {{}};
ADVERT_CHAIN.concat(MSG_CHAIN).forEach(function(n) {{
  if (!n.lat && !n.lon) return;
  if (seenNames[n.name]) return;
  seenNames[n.name] = true;
  var isInterp = n.src === 'interpolated';
  var isRemote = n.src === 'remote';
  if (isRemote) hasRemote = true;
  if (isInterp) unknownNames[n.name] = true;
  var color = ROLE_COLOR[n.role] || '#888';
  var size  = ROLE_SIZE[n.role]  || 12;
  var label = ROLE_LABEL[n.role] || n.role;
  var idLine = n.pub_prefix
    ? '<br><span style="font-size:0.8em;color:#888;font-family:monospace">' + n.pub_prefix + '</span>'
    : '';
  var popup;
  if (isInterp) {{
    popup = '<b>' + n.name + '</b>' + idLine +
            '<br><span style="font-size:0.85em;color:#555">' + label + '</span>' +
            '<br><span style="font-size:0.85em;color:#e67e22">position unknown</span>';
  }} else {{
    popup = '<b>' + n.name + '</b>' + idLine + '<br><span style="font-size:0.85em;color:#555">' + label +
      (isRemote ? ' <em style="color:#aaa">(meshcore.io)</em>' : '') + '</span>';
  }}
  L.marker([n.lat, n.lon], {{icon: makeIcon(color, size, isRemote, isInterp)}})
   .bindPopup(popup)
   .addTo(map);
  latlngs.push([n.lat, n.lon]);
}});

if (latlngs.length > 0) {{
  map.fitBounds(L.latLngBounds(latlngs).pad(0.25));
}} else {{
  map.setView([51, 10], 6);
}}

// Info box
var info = '';
if (ADVERT_HOPS >= 0) {{
  info += '<span class="leg-line" style="background:repeating-linear-gradient(90deg,#666 0,#666 7px,transparent 7px,transparent 12px)"></span>';
  info += '<b>Advert path:</b> ' + ADVERT_HOPS + ' hop' + (ADVERT_HOPS!==1?'s':'') + '<br>';
}}
if (MSG_CHAIN.length > 0) {{
  info += '<span class="leg-line" style="background:repeating-linear-gradient(90deg,#e67e22 0,#e67e22 5px,transparent 5px,transparent 9px)"></span>';
  var mh = MSG_HOPS >= 0 ? MSG_HOPS : MSG_CHAIN.length - 2;
  info += '<b>Msg path:</b> ' + mh + ' hop' + (mh!==1?'s':'') + '<br>';
}}
var refPts = (ADVERT_CHAIN.filter(function(n){{return (n.lat||n.lon)&&n.src!=='interpolated';}}).length >= 2 ? ADVERT_CHAIN : MSG_CHAIN)
              .filter(function(n){{return (n.lat||n.lon)&&n.src!=='interpolated';}});
if (refPts.length >= 2) {{
  var dd = distKm(refPts[0].lat, refPts[0].lon, refPts[refPts.length-1].lat, refPts[refPts.length-1].lon);
  info += '<span class="leg-line" style="background:#3388ff;opacity:0.7"></span>';
  info += '<b>Direct:</b> ' + dd.toFixed(1) + ' km<br>';
  var pathD = ADVERT_CHAIN.filter(function(n){{return n.lat||n.lon;}}).length >= 2 ? advertDist : msgDist;
  if (pathD > dd + 0.05) info += '<b>Path dist:</b> ' + pathD.toFixed(1) + ' km<br>';
}}
if (ADVERT_HASH_CHARS > 0 || MSG_HASH_CHARS > 0) {{
  var hashInfo;
  if (ADVERT_HASH_CHARS > 0 && MSG_HASH_CHARS > 0 && ADVERT_HASH_CHARS !== MSG_HASH_CHARS) {{
    hashInfo = 'Adv: ' + (ADVERT_HASH_CHARS / 2) + 'B, Msg: ' + (MSG_HASH_CHARS / 2) + 'B';
  }} else {{
    hashInfo = ((ADVERT_HASH_CHARS || MSG_HASH_CHARS) / 2) + 'B';
  }}
  info += '<span style="font-size:0.88em;color:#666">Hash: ' + hashInfo + '</span><br>';
}}
info += '<span class="dot" style="background:' + COLOR_SELF + '"></span>gateway ';
info += '<span class="dot" style="background:' + COLOR_HOP + '"></span>via ';
info += '<span class="dot" style="background:' + COLOR_TARGET + '"></span>target';
if (hasRemote) {{
  info += '<br><span style="font-size:0.88em;color:#999">' +
          '<span class="dot" style="background:' + COLOR_REMOTE + '"></span>' +
          'meshcore.io location</span>';
}}
if (Object.keys(unknownNames).length) {{
  info += '<br><span style="display:inline-block;width:11px;height:11px;border-radius:50%;' +
          'border:2px dashed #888;background:#fff;vertical-align:middle;margin-right:4px;' +
          'box-shadow:0 1px 3px rgba(0,0,0,.2)"></span>' +
          '<span style="font-size:0.88em">position unknown</span>';
}}
document.getElementById('info').innerHTML = info;
</script>
</body>
</html>
"""

_HEATH_TEMPLATE = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  body {{ margin: 0; font-family: sans-serif; }}
  #map {{ height: 100vh; }}
  #legend {{
    position: absolute; bottom: 30px; right: 10px; z-index: 1000;
    background: white; padding: 8px 12px; border-radius: 6px;
    box-shadow: 0 1px 5px rgba(0,0,0,.3); font-size: 13px; line-height: 1.9;
  }}
  #title {{
    position: absolute; top: 10px; left: 50%; transform: translateX(-50%);
    z-index: 1000; background: white; padding: 5px 14px; border-radius: 6px;
    box-shadow: 0 1px 5px rgba(0,0,0,.3); font-size: 14px; font-weight: bold;
    white-space: nowrap;
  }}
  .dot {{
    display: inline-block; width: 12px; height: 12px; border-radius: 50%;
    border: 2px solid #fff; box-shadow: 0 1px 3px rgba(0,0,0,.4);
    margin-right: 4px; vertical-align: middle;
  }}
</style>
</head>
<body>
<div id="map"></div>
<div id="title">{title}</div>
<div id="legend">
  <b style="font-size:0.9em">circle size = forwarding count</b><br>
  <span class="dot" style="background:#e74c3c"></span>repeater<br>
  <span class="dot" style="background:#3388ff"></span>companion<br>
  <span class="dot" style="background:#27ae60"></span>room<br>
  <span class="dot" style="background:#888888"></span>sensor/unknown<br>
  <span class="dot" style="background:#aaaaaa;border:2px dashed #888;box-shadow:none"></span><span style="font-size:0.88em;color:#666">meshcore.io loc</span><br>
  <span class="dot" style="background:#fff;border:2px dashed #888;box-shadow:none"></span><span style="font-size:0.88em;color:#666">position estimated</span>
</div>
<script>
const TYPE_COLOR = {{0: '#888888', 1: '#3388ff', 2: '#e74c3c', 3: '#27ae60', 4: '#888888'}};
const NODES = {nodes_json};
const EDGES = {edges_json};
const PATHS = {paths_json};

const map = L.map('map');
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  maxZoom: 19,
  attribution: '&copy; <a href="https://openstreetmap.org/copyright">OpenStreetMap</a> contributors'
}}).addTo(map);

function fmtAge(ts) {{
  if (!ts) return null;
  const secs = Math.round(Date.now() / 1000 - ts);
  if (secs < 120) return secs + 's ago';
  if (secs < 7200) return Math.floor(secs / 60) + 'm ago';
  if (secs < 172800) return Math.floor(secs / 3600) + 'h ago';
  return Math.floor(secs / 86400) + 'd ago';
}}

const HEATH_W_MIN = {heath_w_min};
const HEATH_W_MAX = {heath_w_max};
const maxCount = NODES.reduce(function(m, n) {{ return Math.max(m, n.count); }}, 1);
const maxWeight = EDGES.reduce(function(m, e) {{ return Math.max(m, e.weight); }}, 1);

// Circles first (background), lines on top
const markers = [];
NODES.forEach(function(n) {{
  const interpolated = n.src === 'interpolated';
  const remote = n.src === 'remote';
  const typeColor = TYPE_COLOR[n.type] !== undefined ? TYPE_COLOR[n.type] : '#888888';
  const borderColor = interpolated ? typeColor : (remote ? '#888' : '#fff');
  const fillColor  = interpolated ? '#fff' : (remote ? '#aaaaaa' : typeColor);
  const fillOpacity = interpolated ? 1.0 : (remote ? 0.45 : 0.75);
  const dashArray  = (interpolated || remote) ? '4,3' : null;
  const r = 6 + (n.count / maxCount) * 34;
  const age = fmtAge(n.ts);
  const srcLabel = interpolated
    ? '<br><span style="font-size:0.82em;color:#e67e22">position estimated</span>'
    : remote ? '<br><span style="font-size:0.82em;color:#999">meshcore.io location</span>' : '';
  const popup = '<b>' + n.name + '</b><br>forwarded: ' + n.count +
    (age ? '<br><span style="font-size:0.85em;color:#555">' + age + '</span>' : '') + srcLabel;
  const c = L.circleMarker([n.lat, n.lon], {{
    radius: r, color: borderColor, weight: 2,
    dashArray: dashArray, fillColor: fillColor,
    fillOpacity: fillOpacity, opacity: 0.9
  }}).bindPopup(popup).addTo(map);
  markers.push(c);
}});

var hlLayers = [];
function clearHighlight() {{
  hlLayers.forEach(function(l) {{ map.removeLayer(l); }});
  hlLayers = [];
}}
map.on('click', clearHighlight);

EDGES.forEach(function(e) {{
  const w = HEATH_W_MIN + (e.weight / maxWeight) * (HEATH_W_MAX - HEATH_W_MIN);
  const label = e.weight + ' packet' + (e.weight === 1 ? '' : 's') +
                '<br><span style="font-size:0.85em;color:#555">' + e.a + ' — ' + e.b + '</span>';
  const line = L.polyline([[e.alat, e.alon], [e.blat, e.blon]], {{
    color: '#f39c12', weight: w, opacity: 0.7
  }}).bindPopup(label).addTo(map);
  line.on('click', function(ev) {{
    L.DomEvent.stopPropagation(ev);
    clearHighlight();
    (e.pids || []).forEach(function(pid) {{
      const pts = PATHS[pid];
      if (!pts || pts.length < 2) return;
      hlLayers.push(L.polyline(pts, {{
        color: '#2980b9', weight: HEATH_W_MIN + 1, opacity: 0.9, dashArray: null
      }}).addTo(map));
    }});
  }});
}});

if (markers.length > 0) {{
  map.fitBounds(L.latLngBounds(markers.map(function(m) {{ return m.getLatLng(); }})).pad(0.15));
}} else {{
  map.setView([51, 10], 6);
}}
</script>
</body>
</html>
"""


def _heath_data(bridge, hash_mode_filter: int = None) -> tuple:
    """Return (nodes, edges) for the heath map.

    Equivalent to all /map/nodes/<name> pages overlaid: every stored path
    (advert and msg combined) is resolved through the same _resolve_path_locs
    pipeline used by the per-node view, then edges between consecutive
    positioned nodes are counted.

    hash_mode_filter: 0=1B, 1=2B, 2=3B, None=all.
    """
    if not bridge or not bridge.node_cache:
        return [], []

    si = bridge.self_info or {}
    self_name = sanitize_nick(si.get('name', 'gateway')) if si else 'gateway'
    self_lat = si.get('adv_lat', 0.0)
    self_lon = si.get('adv_lon', 0.0)
    mc_map = getattr(bridge, 'meshcore_map', None)
    max_hop_km = float(bridge.config.get('webserver', {})
                       .get('meshcore_map_max_hop_km', 100.0))

    hop_counts: dict = {}      # resolved_name → int
    edge_counts: dict = {}     # (name_a, name_b) → int
    node_info: dict = {}       # resolved_name → {lat, lon, type, ts, src}
    paths: list = []           # each entry: list of [lat, lon] for one resolved chain
    path_edge_map: dict = {}   # (name_a, name_b) → set of path indices

    def _update_node(name, lat, lon, src, ntype=-1, ts=0):
        ex = node_info.get(name)
        if not ex or (ex['src'] == 'remote' and src == 'local'):
            node_info[name] = {'lat': lat, 'lon': lon, 'src': src,
                               'type': ntype, 'ts': ts}

    def _process_chain(target_name, target_lat, target_lon, via_names):
        items = [(target_name, target_lat, target_lon)]
        for hop_name in via_names:
            hop = bridge.contact_for_nick(hop_name)
            h_lat = hop.get('adv_lat', 0.0) if hop else 0.0
            h_lon = hop.get('adv_lon', 0.0) if hop else 0.0
            if not (h_lat or h_lon):
                h_lat, h_lon = _remote_loc(hop_name, mc_map)
            items.append((hop_name, h_lat, h_lon))
        items.append((self_name, self_lat, self_lon))

        resolved = list(_resolve_path_locs(items, mc_map, max_hop_km))

        # Interpolate positions for nodes still at (0,0) between two known neighbours
        for i, (name, lat, lon, src) in enumerate(resolved):
            if lat or lon:
                continue
            pi = next((j for j in range(i - 1, -1, -1) if resolved[j][1] or resolved[j][2]), -1)
            ni = next((j for j in range(i + 1, len(resolved)) if resolved[j][1] or resolved[j][2]), -1)
            if pi >= 0 and ni >= 0:
                t = (i - pi) / (ni - pi)
                ilat = resolved[pi][1] + t * (resolved[ni][1] - resolved[pi][1])
                ilon = resolved[pi][2] + t * (resolved[ni][2] - resolved[pi][2])
                resolved[i] = (name, ilat, ilon, 'interpolated')

        path_pts: list = []
        path_edges: list = []
        prev = None
        for name, lat, lon, src in resolved:
            if not (lat or lon):
                prev = None
                continue
            _update_node(name, lat, lon, src)
            hop_counts[name] = hop_counts.get(name, 0) + 1
            path_pts.append([lat, lon])
            if prev is not None:
                key = (min(prev, name), max(prev, name))
                edge_counts[key] = edge_counts.get(key, 0) + 1
                path_edges.append(key)
            prev = name
        if len(path_pts) >= 2:
            pid = len(paths)
            paths.append(path_pts)
            for key in path_edges:
                path_edge_map.setdefault(key, set()).add(pid)

    def _infer_hm(entry, nodes_key, hm_key):
        hm = entry.get(hm_key, entry.get('out_path_hash_mode', -1))
        if hm >= 0:
            return hm
        for node in entry.get(nodes_key, []):
            if node.startswith('?'):
                hex_len = len(node) - 1
                if hex_len > 0 and hex_len % 2 == 0:
                    return hex_len // 2 - 1
        path_len = entry.get('out_path_len', -1)
        out_path = entry.get('out_path', '')
        if path_len > 0 and out_path and out_path != '0' * 128:
            for mode in range(3):
                hc = (mode + 1) * 2
                used = hc * path_len
                if (used <= len(out_path)
                        and out_path[:used] != '0' * used
                        and out_path[used:used + 2] in ('', '00')):
                    return mode
        return -1

    for _key, entry in bridge.node_cache.all_entries():
        target = entry.get('adv_name', '')
        if not target:
            continue

        contact = bridge.contact_for_nick(target)
        if contact:
            t_lat = contact.get('adv_lat', 0.0)
            t_lon = contact.get('adv_lon', 0.0)
        else:
            t_lat = entry.get('lat', 0.0)
            t_lon = entry.get('lon', 0.0)

        advert_hm = _infer_hm(entry, 'advert_path_nodes', 'advert_path_hash_mode')
        if hash_mode_filter is None or advert_hm == hash_mode_filter:
            via = entry.get('advert_path_nodes')
            if via is not None:
                _process_chain(target, t_lat, t_lon, via)

        msg_hm = _infer_hm(entry, 'msg_path_nodes', 'msg_path_hash_mode')
        if hash_mode_filter is None or msg_hm == hash_mode_filter:
            via = entry.get('msg_path_nodes')
            if via:
                _process_chain(target, t_lat, t_lon, via)

    if not hop_counts:
        return [], [], []

    # Enrich local entries with type/ts from cache
    for _pubkey, entry in bridge.node_cache.all_items():
        name = entry.get('adv_name', '')
        info = node_info.get(name)
        if info and info['src'] == 'local':
            info['type'] = entry.get('node_type', info['type'])
            info['ts'] = max(info['ts'], entry.get('last_seen', 0))

    nodes = [
        {'name': n, 'lat': info['lat'], 'lon': info['lon'],
         'type': info['type'], 'count': count,
         'ts': info['ts'], 'src': info['src']}
        for n, count in hop_counts.items()
        if (info := node_info.get(n))
    ]

    edges = [
        {'a': a, 'b': b,
         'alat': node_info[a]['lat'], 'alon': node_info[a]['lon'],
         'blat': node_info[b]['lat'], 'blon': node_info[b]['lon'],
         'weight': w,
         'pids': sorted(path_edge_map.get((a, b), set()))}
        for (a, b), w in edge_counts.items()
        if a in node_info and b in node_info
    ]

    return nodes, edges, paths


def _render_heath(nodes: list, edges: list, paths: list,
                  title: str = 'Heath — forwarding nodes',
                  heath_w_min: float = 3.0, heath_w_max: float = 10.0) -> bytes:
    html = _HEATH_TEMPLATE.format(
        title=_html.escape(title),
        nodes_json=json.dumps(nodes, ensure_ascii=False),
        edges_json=json.dumps(edges, ensure_ascii=False),
        paths_json=json.dumps(paths, ensure_ascii=False),
        heath_w_min=heath_w_min,
        heath_w_max=heath_w_max,
    )
    return html.encode('utf-8')


def _render_map_index() -> bytes:
    html = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Map index</title>
<style>
  body { margin: 0; font-family: sans-serif; background: #f4f4f4; }
  h1 { font-size: 1.3em; margin: 0 0 18px; color: #333; }
  h2 { font-size: 1em; margin: 24px 0 8px; color: #555; text-transform: uppercase;
       letter-spacing: .05em; border-bottom: 1px solid #ddd; padding-bottom: 4px; }
  .wrap { max-width: 520px; margin: 40px auto; padding: 0 20px; }
  ul { margin: 0; padding: 0; list-style: none; }
  li { margin: 5px 0; }
  a { color: #2980b9; text-decoration: none; font-size: 0.95em; }
  a:hover { text-decoration: underline; }
  .desc { font-size: 0.82em; color: #888; margin-left: 6px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>MeshCore map pages</h1>

  <h2>Contacts</h2>
  <ul>
    <li><a href="/map/contacts/all">All contacts</a></li>
    <li><a href="/map/contacts/repeater">Repeaters</a></li>
    <li><a href="/map/contacts/companion">Companions</a></li>
    <li><a href="/map/contacts/room">Rooms</a></li>
    <li><a href="/map/contacts/sensor">Sensors</a></li>
  </ul>

  <h2>Discovered nodes</h2>
  <ul>
    <li><a href="/map/discovered/all">All discovered</a></li>
    <li><a href="/map/discovered/repeater">Repeaters</a></li>
    <li><a href="/map/discovered/companion">Companions</a></li>
    <li><a href="/map/discovered/room">Rooms</a></li>
    <li><a href="/map/discovered/sensor">Sensors</a></li>
  </ul>

  <h2>Node paths</h2>
  <ul>
    <li><a href="/map/nodes">Node index</a>
        <span class="desc">sortable table with hop counts and distances</span></li>
  </ul>

  <h2>Heath — path heat maps</h2>
  <ul>
    <li><a href="/map/heath/1b">1-byte hash paths</a></li>
    <li><a href="/map/heath/2b">2-byte hash paths</a></li>
    <li><a href="/map/heath/3b">3-byte hash paths</a></li>
  </ul>

  <h2>Neighbours</h2>
  <ul>
    <li><a href="/map/neighbours">Neighbour lists</a></li>
  </ul>
</div>
</body>
</html>"""
    return html.encode('utf-8')


def _fmt_age(secs: int) -> str:
    if secs < 120:
        return f"{secs}s ago"
    if secs < 7200:
        return f"{secs // 60}m ago"
    if secs < 172800:
        return f"{secs // 3600}h ago"
    return f"{secs // 86400}d ago"


def _render(title: str, nodes: list,
            rep_lat: float = 0.0, rep_lon: float = 0.0, rep_name: str = '',
            snr_font_size: int = 13, snr_padding: str = '2px 6px',
            line_weight: float = 1.5) -> bytes:
    html = _HTML_TEMPLATE.format(
        title=_html.escape(title),
        nodes_json=json.dumps(nodes, ensure_ascii=False),
        rep_lat=rep_lat,
        rep_lon=rep_lon,
        rep_name_json=json.dumps(rep_name),
        snr_font_size=snr_font_size,
        snr_padding=snr_padding,
        line_weight=line_weight,
    )
    return html.encode('utf-8')


def _render_index(entries: list) -> bytes:
    if entries:
        items = ''
        for e in entries:
            name = _html.escape(e['repeater_name'])
            key = urllib.parse.quote(e['key'], safe='')
            age_secs = int(time.time() - e['timestamp'])
            items += f'<li><a href="/map/neighbours/{key}">{name}</a> &mdash; {_fmt_age(age_secs)}</li>\n'
        body_html = f'<ul>\n{items}</ul>'
    else:
        body_html = '<p>No neighbour data stored yet.</p>'

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Neighbour Lists</title>
<style>body{{font-family:sans-serif;padding:20px}}li{{margin:6px 0}}</style>
</head><body>
<h2>Neighbour Lists</h2>
{body_html}
</body></html>"""
    return html.encode('utf-8')


def _age_str(ts: float) -> str:
    if not ts:
        return '—'
    secs = int(time.time() - ts)
    if secs < 0:
        return '—'
    if secs < 120:
        return f'{secs}s ago'
    if secs < 7200:
        return f'{secs // 60}m ago'
    if secs < 172800:
        return f'{secs // 3600}h ago'
    return f'{secs // 86400}d ago'


def _remote_loc(name: str, mc_map) -> tuple:
    """Return (lat, lon) from meshcore.io, unique-only for ?hash names."""
    if mc_map:
        if name.startswith('?'):
            r = mc_map.lookup_by_prefix_unique(name[1:])
        else:
            r = mc_map.lookup_by_name(name)
        if r and (r.get('adv_lat') or r.get('adv_lon')):
            return r.get('adv_lat', 0.0), r.get('adv_lon', 0.0)
    return 0.0, 0.0


def _dist_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _resolve_path_locs(items: list, mc_map, max_hop_km: float = 100.0) -> list:
    """Two-pass closest-neighbor resolution of ?hash nodes via meshcore.io.

    items: list of (name, lat, lon)
    Returns: list of (name, lat, lon, src)  where src is 'local' or 'remote'
    Unresolved ?hash nodes that still have no reference neighbour stay at 0,0.
    """
    # Track which items originated as ?hash so Pass A covers pre-resolved ones too
    orig_is_hash = [it[0].startswith('?') for it in items]
    result = [(name, lat, lon, 'local') for name, lat, lon in items]
    if not mc_map:
        return result

    def _try_closest(name, ref_lat, ref_lon):
        if not name.startswith('?') or not (ref_lat or ref_lon):
            return None
        return mc_map.lookup_by_prefix_closest(name[1:], ref_lat, ref_lon)

    # Forward pass: use last resolved position as reference
    last_lat = last_lon = 0.0
    for i, (name, lat, lon, src) in enumerate(result):
        if lat or lon:
            last_lat, last_lon = lat, lon
        elif last_lat or last_lon:
            r = _try_closest(name, last_lat, last_lon)
            if r and (r.get('adv_lat') or r.get('adv_lon')):
                rname = sanitize_nick(r.get('adv_name', name))
                result[i] = (rname, r['adv_lat'], r['adv_lon'], 'remote')
                last_lat, last_lon = r['adv_lat'], r['adv_lon']

    # Backward pass: use next resolved position for still-unresolved nodes
    next_lat = next_lon = 0.0
    for i in range(len(result) - 1, -1, -1):
        name, lat, lon, src = result[i]
        if lat or lon:
            next_lat, next_lon = lat, lon
        elif next_lat or next_lon:
            r = _try_closest(name, next_lat, next_lon)
            if r and (r.get('adv_lat') or r.get('adv_lon')):
                rname = sanitize_nick(r.get('adv_name', name))
                result[i] = (rname, r['adv_lat'], r['adv_lon'], 'remote')
                next_lat, next_lon = r['adv_lat'], r['adv_lon']

    # Pass A (iterative): revert hash-resolved nodes that create a segment > max_hop_km.
    # Includes nodes pre-resolved via _remote_loc (src='local' but orig_is_hash=True).
    # Repeat until stable since reverting a node can invalidate its neighbours.
    changed = True
    while changed:
        changed = False
        for i in range(len(result)):
            name, lat, lon, src = result[i]
            if not (lat or lon):
                continue
            if src != 'remote' and not orig_is_hash[i]:
                continue
            prev_d = float('inf')
            for j in range(i - 1, -1, -1):
                _, pl, plo, _ = result[j]
                if pl or plo:
                    prev_d = _dist_km(pl, plo, lat, lon)
                    break
            if prev_d > max_hop_km:
                result[i] = (items[i][0], 0.0, 0.0, 'local')
                changed = True
                continue
            next_d = float('inf')
            for j in range(i + 1, len(result)):
                _, nl, nlo, _ = result[j]
                if nl or nlo:
                    next_d = _dist_km(lat, lon, nl, nlo)
                    break
            if next_d > max_hop_km:
                result[i] = (items[i][0], 0.0, 0.0, 'local')
                changed = True

    # Pass B: drop remote duplicates (keep local; if both remote keep first)
    seen: dict = {}
    for i, (name, lat, lon, src) in enumerate(result):
        if not (lat or lon):
            continue
        key = name.lower()
        if key in seen:
            j, j_src = seen[key]
            if src == 'remote':
                result[i] = (items[i][0], 0.0, 0.0, 'local')
            elif j_src == 'remote':
                result[j] = (items[j][0], 0.0, 0.0, 'local')
                seen[key] = (i, src)
        else:
            seen[key] = (i, src)

    return result


def _msg_distances(name, lat, lon, self_lat, self_lon, bridge, msg_hops: int = -1):
    """Return (msg_dist, msg_path_dist, dist_remote) from channel_msg_path_nodes.

    msg_hops must be the known hop count (>= 0) when a message was received, or -1
    if unknown.  Without this guard, via=[] + both positions known would produce a
    spurious straight-line distance identical to the advert distance — not a real
    message measurement.  Always pass msg_hops from the caller.
    """
    mc_map = bridge.meshcore_map
    max_hop_km = float(bridge.config.get('webserver', {}).get('meshcore_map_max_hop_km', 100.0))
    via = bridge.channel_msg_path_nodes.get(name, [])
    # Guard: no message data → return nothing so the index table shows '—' not a fake distance.
    if msg_hops < 0 and not via:
        return None, None, False
    if not (lat or lon):
        lat, lon = _remote_loc(name, mc_map)
    items = [(name, lat, lon)]
    for hop_name in via:
        hop = bridge.contact_for_nick(hop_name)
        h_lat = hop.get('adv_lat', 0.0) if hop else 0.0
        h_lon = hop.get('adv_lon', 0.0) if hop else 0.0
        if not (h_lat or h_lon):
            h_lat, h_lon = _remote_loc(hop_name, mc_map)
        items.append((hop_name, h_lat, h_lon))
    items.append(('self', self_lat, self_lon))
    resolved = _resolve_path_locs(items, mc_map, max_hop_km)
    dist_remote = any(src == 'remote' for _, _, _, src in resolved)
    pts = [(rlat, rlon) for _, rlat, rlon, _ in resolved if rlat or rlon]
    if len(pts) >= 2:
        d = bridge.distance_km(pts[0][0], pts[0][1], pts[-1][0], pts[-1][1])
        pd = sum(bridge.distance_km(pts[i][0], pts[i][1], pts[i+1][0], pts[i+1][1])
                 for i in range(len(pts) - 1))
        return d, pd, dist_remote
    return None, None, False


def _nodes_index_entries(bridge) -> list:
    """Build a sorted list of nodes that have advert or msg hop data."""
    si = bridge.self_info or {}
    self_lat = si.get('adv_lat', 0.0)
    self_lon = si.get('adv_lon', 0.0)

    seen_pubkeys: set = set()
    entries = []

    mc_map = bridge.meshcore_map
    max_hop_km = float(bridge.config.get('webserver', {}).get('meshcore_map_max_hop_km', 100.0))

    def _add(pubkey: str, advert_hops: int, msg_hops: int):
        if pubkey in seen_pubkeys:
            return
        contact = bridge.contacts.get(pubkey)
        node_type = contact.get('type', 0) if contact else 0
        cache_last_seen = 0.0
        if not contact and bridge.node_cache:
            entry = bridge.node_cache.get_by_pubkey(pubkey)
            if entry:
                node_type = entry.get('node_type', 0)
                cache_last_seen = entry.get('last_seen', 0.0)
                contact = {'public_key': pubkey,
                           'adv_name': entry.get('adv_name', ''),
                           'adv_lat': entry.get('lat', 0.0),
                           'adv_lon': entry.get('lon', 0.0)}
        elif contact and bridge.node_cache:
            ce = bridge.node_cache.get_by_pubkey(pubkey)
            cache_last_seen = ce.get('last_seen', 0.0) if ce else 0.0
        if not contact:
            return
        name = sanitize_nick(contact.get('adv_name', ''))
        if not name:
            return
        seen_pubkeys.add(pubkey)
        lat = contact.get('adv_lat', 0.0)
        lon = contact.get('adv_lon', 0.0)
        if not (lat or lon):
            lat, lon = _remote_loc(name, mc_map)
        # Build items [target, via..., self] then resolve ?hash hops
        advert_via = bridge.advert_path_nodes_by_pubkey.get(pubkey, [])
        msg_via = bridge.channel_msg_path_nodes.get(name, [])
        advert_hash_chars = next((len(vn) - 1 for vn in advert_via if vn.startswith('?')), 0)
        msg_hash_chars = next((len(vn) - 1 for vn in msg_via if vn.startswith('?')), 0)
        items = [(name, lat, lon)]
        for hop_name in advert_via:
            hop = bridge.contact_for_nick(hop_name)
            h_lat = hop.get('adv_lat', 0.0) if hop else 0.0
            h_lon = hop.get('adv_lon', 0.0) if hop else 0.0
            if not (h_lat or h_lon):
                h_lat, h_lon = _remote_loc(hop_name, mc_map)
            items.append((hop_name, h_lat, h_lon))
        items.append(('self', self_lat, self_lon))
        resolved = _resolve_path_locs(items, mc_map, max_hop_km)
        dist_remote = any(src == 'remote' for _, _, _, src in resolved)
        chain_pts = [(rl, rlo) for _, rl, rlo, _ in resolved if rl or rlo]
        if len(chain_pts) >= 2:
            dist = bridge.distance_km(
                chain_pts[0][0], chain_pts[0][1], chain_pts[-1][0], chain_pts[-1][1])
            path_dist = sum(
                bridge.distance_km(chain_pts[i][0], chain_pts[i][1],
                                   chain_pts[i + 1][0], chain_pts[i + 1][1])
                for i in range(len(chain_pts) - 1))
        else:
            dist = path_dist = None
        member = bridge.channel_member_info(name)
        member_ts = member.get('ts', 0.0) if member else 0.0
        last_seen = max(cache_last_seen, member_ts)
        msg_dist, msg_path_dist, msg_dist_remote = _msg_distances(
            name, lat, lon, self_lat, self_lon, bridge, msg_hops=msg_hops)
        entries.append({'name': name, 'advert_hops': advert_hops,
                        'msg_hops': msg_hops, 'dist': dist, 'path_dist': path_dist,
                        'dist_remote': dist_remote,
                        'msg_dist': msg_dist, 'msg_path_dist': msg_path_dist,
                        'msg_dist_remote': msg_dist_remote,
                        'node_type': node_type, 'last_seen': last_seen,
                        'advert_hash_chars': advert_hash_chars,
                        'msg_hash_chars': msg_hash_chars})

    for pubkey, advert_hops in bridge.advert_path_by_pubkey.items():
        msg_hops = bridge.min_msg_hops_by_pubkey.get(pubkey[:12], -1)
        _add(pubkey, advert_hops, msg_hops)

    for prefix, msg_hops in bridge.min_msg_hops_by_pubkey.items():
        contact = bridge.contact_for_pubkey_prefix(prefix)
        if contact:
            pubkey = contact.get('public_key', '')
            _add(pubkey, bridge.advert_path_by_pubkey.get(pubkey, -1), msg_hops)
        else:
            # Channel-only node: resolve nick from node_cache prefix entry or live channel_members
            pkey = f'p:{prefix}'
            if pkey in seen_pubkeys:
                continue
            nick = ''
            last_seen = 0.0
            if bridge.node_cache:
                nc = bridge.node_cache.get_prefix_node(prefix)
                if nc:
                    nick = sanitize_nick(nc.get('adv_name', ''))
                    last_seen = nc.get('last_seen', 0.0)
            if not nick:
                for ch_members in bridge.channel_members.values():
                    for n, m in ch_members.items():
                        if m.get('host', '') == prefix:
                            nick = n
                            last_seen = max(last_seen, m.get('ts', 0.0))
                            break
                    if nick:
                        break
            if not nick:
                continue
            seen_pubkeys.add(pkey)
            msg_dist, msg_path_dist, msg_dist_remote = _msg_distances(
                nick, 0.0, 0.0, self_lat, self_lon, bridge, msg_hops=msg_hops)
            entries.append({'name': nick, 'advert_hops': -1, 'msg_hops': msg_hops,
                            'dist': None, 'path_dist': None, 'dist_remote': False,
                            'msg_dist': msg_dist, 'msg_path_dist': msg_path_dist,
                            'msg_dist_remote': msg_dist_remote,
                            'node_type': 0, 'last_seen': last_seen})

    # Third loop: channel members with known path_len but no pubkey/prefix
    # (host='mesh' nodes — old firmware without pubkey_prefix in channel messages)
    added_names = {e['name'].lower() for e in entries}
    for ch_members in bridge.channel_members.values():
        for nick, member in ch_members.items():
            if nick.lower() in added_names:
                continue
            min_hops = member.get('min_path_len', -1)
            if min_hops < 0:
                continue
            last_seen = member.get('ts', 0.0)
            msg_dist, msg_path_dist, msg_dist_remote = _msg_distances(
                nick, 0.0, 0.0, self_lat, self_lon, bridge, msg_hops=min_hops)
            entries.append({'name': nick, 'advert_hops': -1, 'msg_hops': min_hops,
                            'dist': None, 'path_dist': None, 'dist_remote': False,
                            'msg_dist': msg_dist, 'msg_path_dist': msg_path_dist,
                            'msg_dist_remote': msg_dist_remote,
                            'node_type': 0, 'last_seen': last_seen})
            added_names.add(nick.lower())

    # Fourth loop: n:<nick> entries in node_cache — host='mesh' nodes persisted across restarts
    if bridge.node_cache:
        added_names = {e['name'].lower() for e in entries}
        for key, entry in bridge.node_cache.all_entries():
            if not key.startswith('n:'):
                continue
            nick = sanitize_nick(entry.get('adv_name', ''))
            if not nick or nick.lower() in added_names:
                continue
            min_hops = entry.get('min_msg_hops', -1)
            msg_via = entry.get('msg_path_nodes', [])
            if min_hops < 0 and not msg_via:
                continue
            last_seen = entry.get('last_seen', 0.0)
            msg_dist, msg_path_dist, msg_dist_remote = _msg_distances(
                nick, 0.0, 0.0, self_lat, self_lon, bridge, msg_hops=min_hops)
            msg_hash_chars = next((len(v) - 1 for v in msg_via if v.startswith('?')), 0)
            entries.append({'name': nick, 'advert_hops': -1, 'msg_hops': min_hops,
                            'dist': None, 'path_dist': None, 'dist_remote': False,
                            'msg_dist': msg_dist, 'msg_path_dist': msg_path_dist,
                            'msg_dist_remote': msg_dist_remote,
                            'node_type': 0, 'last_seen': last_seen,
                            'advert_hash_chars': 0, 'msg_hash_chars': msg_hash_chars})
            added_names.add(nick.lower())

    entries.sort(key=lambda e: (
        e['advert_hops'] if e['advert_hops'] >= 0 else 999,
        e['name'].lower(),
    ))
    return entries


_NODE_TYPE_LABEL = {0: 'sensor', 1: 'companion', 2: 'repeater', 3: 'room', 4: 'sensor'}


def _fmt_interval(secs: int) -> str:
    """Return the most readable label for a seconds value: Xs, Xm, or Xh."""
    if secs >= 3600 and secs % 3600 == 0:
        return f'{secs // 3600}h'
    if secs >= 60 and secs % 60 == 0:
        return f'{secs // 60}m'
    return f'{secs}s'


def _render_nodes_index(entries: list, base_url: str,
                        refresh_intervals: list | None = None) -> bytes:
    if entries:
        rows = ''
        type_counts: dict[str, int] = {}
        def _dtxt(val, remote):
            if val is None:
                return '—'
            s = f'{val:.1f} km'
            return (f'<span class="rdist" title="Location from meshcore.io">≈{s}</span>'
                    if remote else s)

        for e in entries:
            name     = _html.escape(e['name'])
            href     = base_url + '/map/nodes/' + urllib.parse.quote(e['name'])
            adv_val    = e['advert_hops']
            msg_val    = e['msg_hops']
            dist_val   = e['dist']       if e['dist']       is not None else -1
            pdist_val  = e['path_dist']  if e['path_dist']  is not None else -1
            mdist_val  = e.get('msg_dist')
            mpdist_val = e.get('msg_path_dist')
            ts_val     = e.get('last_seen', 0.0)
            dr         = e.get('dist_remote', False)
            mdr        = e.get('msg_dist_remote', False)
            adv_txt    = str(adv_val) if adv_val >= 0 else '—'
            msg_txt    = str(msg_val) if msg_val >= 0 else '—'
            ddist_val  = (e['path_dist'] - e['dist']
                          if e['dist'] is not None and e['path_dist'] is not None else -1)
            mddist_val = (mpdist_val - mdist_val
                          if mdist_val is not None and mpdist_val is not None else -1)
            dist_txt   = _dtxt(e['dist'], dr)
            pdist_txt  = _dtxt(e['path_dist'], dr)
            ddist_txt  = _dtxt(ddist_val if ddist_val >= 0 else None, dr)
            mdist_txt  = _dtxt(mdist_val, mdr)
            mpdist_txt = _dtxt(mpdist_val, mdr)
            mddist_txt = _dtxt(mddist_val if mddist_val >= 0 else None, mdr)
            mdist_sv   = f"{mdist_val:.4f}"           if mdist_val  is not None      else '-1'
            mpdist_sv  = f"{mpdist_val:.4f}"          if mpdist_val is not None      else '-1'
            mddist_sv  = f"{mddist_val:.4f}"          if mddist_val >= 0             else '-1'
            age_txt    = _age_str(ts_val)
            ntype     = e.get('node_type', 0)
            tlabel    = _NODE_TYPE_LABEL.get(ntype, 'sensor')
            type_counts[tlabel] = type_counts.get(tlabel, 0) + 1
            ahc       = e.get('advert_hash_chars', 0)
            mhc       = e.get('msg_hash_chars', 0)
            adv_hash_txt = f'{ahc // 2}B' if ahc else '—'
            msg_hash_txt = f'{mhc // 2}B' if mhc else '—'
            # data-hasmsg: "1" when msg hop or distance data exists — used by the Msg filter button
            has_msg = '1' if (msg_val >= 0 or mdist_val is not None) else '0'
            rows += (f'<tr data-name="{name.lower()}" data-adv="{adv_val}"'
                     f' data-msg="{msg_val}" data-dist="{dist_val:.4f}" data-pdist="{pdist_val:.4f}"'
                     f' data-ddist="{ddist_val:.4f}"'
                     f' data-mdist="{mdist_sv}" data-mpdist="{mpdist_sv}" data-mddist="{mddist_sv}"'
                     f' data-ts="{ts_val:.0f}" data-type="{tlabel}"'
                     f' data-advhash="{ahc}" data-msghash="{mhc}" data-hasmsg="{has_msg}">\n'
                     f'  <td><a href="{href}">{name}</a></td>'
                     f'<td>{adv_txt}</td>'
                     f'<td>{dist_txt}</td><td>{pdist_txt}</td><td>{ddist_txt}</td><td>{adv_hash_txt}</td>'
                     f'<td>{msg_txt}</td>'
                     f'<td>{mdist_txt}</td><td>{mpdist_txt}</td><td>{mddist_txt}</td><td>{msg_hash_txt}</td>'
                     f'<td>{age_txt}</td></tr>\n')
        # Build filter buttons for types, data presence, and hash sizes.
        # IMPORTANT: there are three independent filter groups (type / data / hash) — keep all three.
        #   type   → data-type attr   → activeType  in JS
        #   data   → data-hasmsg attr → activData   in JS  (shows only rows with msg data when "Msg" is active)
        #   hash   → data-advhash / data-msghash     → activeHash in JS (OR logic: matches adv OR msg hash size)
        type_order = ['repeater', 'companion', 'room', 'sensor']
        btn_html = '<div class="filters"><strong>Type:</strong> <button class="active" data-filter-group="type" data-type="">All</button>'
        for tlabel in type_order:
            if tlabel in type_counts:
                btn_html += f' <button data-filter-group="type" data-type="{tlabel}">{tlabel.capitalize()}s</button>'
        btn_html += ' &nbsp; <strong>Data:</strong> <button class="active" data-filter-group="data" data-data="">All</button>'
        btn_html += ' <button data-filter-group="data" data-data="msg">Has msg</button>'
        hash_sizes = sorted({v for e in entries
                             for v in (e.get('advert_hash_chars', 0), e.get('msg_hash_chars', 0))
                             if v > 0})
        if hash_sizes:
            btn_html += ' &nbsp; <strong>Hash:</strong> <button class="active" data-filter-group="hash" data-hash="">Any</button>'
            for hc in hash_sizes:
                btn_html += f' <button data-filter-group="hash" data-hash="{hc}">{hc // 2}B</button>'
        intervals = refresh_intervals if refresh_intervals else [120, 300, 600]
        opts = '<option value="0">Off</option>'
        for iv in intervals:
            opts += f'<option value="{iv}">{_fmt_interval(iv)}</option>'
        btn_html += (' &nbsp;&nbsp; <strong>Refresh:</strong>'
                     ' <select id="refresh-sel" style="padding:3px 6px;border:1px solid #ccc;'
                     f'border-radius:4px;font-size:0.9em;cursor:pointer">{opts}</select>'
                     ' <span id="refresh-cd" style="font-size:0.85em;color:#888;margin-left:4px"></span>'
                     '</div>')
        body_html = (
            f'{btn_html}\n'
            '<p id="row-count" style="margin:0 0 8px;font-size:0.9em;color:#555"></p>\n'
            '<table id="t">\n'
            '<thead><tr>'
            '<th data-col="name">Node</th>'
            '<th data-col="adv">Advert hops</th>'
            '<th data-col="dist">Adv dist</th>'
            '<th data-col="pdist">Adv path</th>'
            '<th data-col="ddist">Adv diff</th>'
            '<th data-col="advhash">Adv hash</th>'
            '<th data-col="msg">Msg hops</th>'
            '<th data-col="mdist">Msg dist</th>'
            '<th data-col="mpdist">Msg path</th>'
            '<th data-col="mddist">Msg diff</th>'
            '<th data-col="msghash">Msg hash</th>'
            '<th data-col="ts">Last seen</th>'
            '</tr></thead>\n'
            f'<tbody>\n{rows}</tbody>\n</table>'
        )
    else:
        body_html = '<p>No hop data stored yet.</p>'

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Node Paths</title>
<style>
  body{{font-family:sans-serif;padding:20px}}
  .filters{{margin-bottom:12px}}
  .filters button{{margin-right:6px;padding:4px 12px;border:1px solid #ccc;border-radius:4px;
                   background:#f5f5f5;cursor:pointer;font-size:0.9em}}
  .filters button:hover{{background:#e8e8e8}}
  .filters button.active{{background:#4a90d9;color:#fff;border-color:#3a7bc8}}
  table{{border-collapse:collapse;width:100%}}
  th,td{{text-align:left;padding:6px 14px;border-bottom:1px solid #eee}}
  th{{background:#f5f5f5;font-weight:bold;cursor:pointer;user-select:none;white-space:nowrap}}
  th:hover{{background:#e8e8e8}}
  th.asc::after{{content:" ▲"}}
  th.desc::after{{content:" ▼"}}
  tr:hover td{{background:#f9f9f9}}
  td:nth-child(n+2){{text-align:center}}
  th:nth-child(n+2){{text-align:center}}
  .rdist{{color:#999;font-style:italic}}
</style>
</head><body>
<h2>Node Paths</h2>
{body_html}
<script>
(function(){{
  var t = document.getElementById('t');
  if (!t) return;

  // localStorage helpers — 'mcnodes_' prefix avoids collisions with other pages
  var LS = 'mcnodes_';
  function lsGet(k, d) {{ try {{ var v = localStorage.getItem(LS+k); return v !== null ? v : d; }} catch(e) {{ return d; }} }}
  function lsSet(k, v) {{ try {{ localStorage.setItem(LS+k, v); }} catch(e) {{}} }}

  // Three independent filter groups — all must pass (AND logic between groups).
  // type: filter by node type;  data: "msg" shows only rows with data-hasmsg="1";  hash: OR-match adv or msg hash size.
  // IMPORTANT: keep all three groups and their localStorage keys ('type', 'data', 'hash').
  var asc = {{}}, cur = null;
  var activeType = lsGet('type', '');
  var activData  = lsGet('data', '');
  var activeHash = lsGet('hash', '');

  var rowCountEl = document.getElementById('row-count');
  function applyFilter() {{
    var visible = 0, total = 0;
    Array.from(t.tBodies[0].rows).forEach(function(r) {{
      total++;
      var typeOk = !activeType || r.dataset.type === activeType;
      var dataOk = !activData  || (activData === 'msg' && r.dataset.hasmsg === '1');
      var hashOk = !activeHash || r.dataset.advhash === activeHash || r.dataset.msghash === activeHash;
      var show = typeOk && dataOk && hashOk;
      r.style.display = show ? '' : 'none';
      if (show) visible++;
    }});
    if (rowCountEl) rowCountEl.textContent = visible === total
      ? total + ' nodes'
      : visible + ' of ' + total + ' nodes';
  }}

  // Restore button active states from localStorage on page load
  document.querySelectorAll('.filters button').forEach(function(btn) {{
    var grp = btn.dataset.filterGroup;
    var val    = grp === 'hash' ? btn.dataset.hash : grp === 'data' ? btn.dataset.data : btn.dataset.type;
    var stored = grp === 'type' ? activeType : grp === 'data' ? activData : activeHash;
    btn.classList.toggle('active', val === stored);
  }});
  applyFilter();

  document.querySelectorAll('.filters button').forEach(function(btn) {{
    btn.addEventListener('click', function() {{
      var grp = btn.dataset.filterGroup;
      document.querySelectorAll('.filters button[data-filter-group="' + grp + '"]').forEach(function(b) {{ b.classList.remove('active'); }});
      btn.classList.add('active');
      if      (grp === 'hash') {{ activeHash = btn.dataset.hash; lsSet('hash', activeHash); }}
      else if (grp === 'data') {{ activData  = btn.dataset.data; lsSet('data', activData);  }}
      else                     {{ activeType = btn.dataset.type; lsSet('type', activeType); }}
      applyFilter();
    }});
  }});

  // --- Auto-refresh: interval stored in localStorage, disabled by default ---
  var refreshSel = document.getElementById('refresh-sel');
  var refreshCd  = document.getElementById('refresh-cd');
  var refreshTid = null;

  function startRefresh(secs) {{
    clearTimeout(refreshTid);
    refreshCd.textContent = '';
    if (!secs) return;
    var rem = secs;
    (function tick() {{
      refreshCd.textContent = rem + 's';
      if (rem <= 0) {{ location.reload(); return; }}
      rem--;
      refreshTid = setTimeout(tick, 1000);
    }})();
  }}

  refreshSel.addEventListener('change', function() {{
    lsSet('refresh', refreshSel.value);
    startRefresh(parseInt(refreshSel.value) || 0);
  }});

  var savedRefresh = lsGet('refresh', '0');
  refreshSel.value = savedRefresh;
  startRefresh(parseInt(savedRefresh) || 0);

  // --- Column sort ---
  function applySort(col, ascending) {{
    cur = col; asc[col] = ascending;
    t.querySelectorAll('th').forEach(function(h) {{ h.className = ''; }});
    var th = t.querySelector('th[data-col="' + col + '"]');
    if (th) th.className = ascending ? 'asc' : 'desc';
    var rows = Array.from(t.tBodies[0].rows);
    rows.sort(function(a, b) {{
      var av = a.dataset[col], bv = b.dataset[col];
      var miss = ascending ? 1e18 : -1e18;
      if (col === 'name') {{ return ascending ? av.localeCompare(bv) : bv.localeCompare(av); }}
      var an = parseFloat(av), bn = parseFloat(bv);
      if (isNaN(an) || an < 0) an = miss;
      if (isNaN(bn) || bn < 0) bn = miss;
      return ascending ? an - bn : bn - an;
    }});
    rows.forEach(function(r) {{ t.tBodies[0].appendChild(r); }});
  }}

  t.querySelectorAll('th[data-col]').forEach(function(th) {{
    th.addEventListener('click', function() {{
      var col = th.dataset.col;
      var ascending = (cur === col) ? !asc[col] : (col !== 'ts');
      applySort(col, ascending);
      lsSet('sort', col);
      lsSet('sortasc', ascending ? '1' : '0');
    }});
  }});

  var savedCol = lsGet('sort', 'ts');
  var savedAsc = lsGet('sortasc', '0') === '1';
  applySort(savedCol, savedAsc);
}})();
</script>
</body></html>"""
    return html.encode('utf-8')


def _render_node_path(title: str, advert_chain: list, msg_chain: list,
                      advert_hops: int, msg_hops: int,
                      advert_hash_chars: int = 4,
                      msg_hash_chars: int = 4,
                      advert_weight: float = 2.5,
                      msg_weight: float = 2.0,
                      direct_weight: float = 1.5,
                      color_self: str = '#3388ff',
                      color_hop: str = '#f39c12',
                      color_target: str = '#e74c3c',
                      color_remote: str = '#888888',
                      color_unknown_bg: str = 'rgba(255,255,255,0.85)') -> bytes:
    html = _PATH_TEMPLATE.format(
        title=_html.escape(title),
        advert_chain_json=json.dumps(advert_chain, ensure_ascii=False),
        msg_chain_json=json.dumps(msg_chain, ensure_ascii=False),
        advert_hops=advert_hops,
        msg_hops=msg_hops,
        advert_hash_chars=advert_hash_chars,
        msg_hash_chars=msg_hash_chars,
        advert_weight=advert_weight,
        msg_weight=msg_weight,
        direct_weight=direct_weight,
        color_self=color_self,
        color_hop=color_hop,
        color_target=color_target,
        color_remote=color_remote,
        color_unknown_bg=color_unknown_bg,
    )
    return html.encode('utf-8')


def _build_chain(target_name, target_lat, target_lon, via_names, self_name, si, bridge):
    mc_map = bridge.meshcore_map
    max_hop_km = float(bridge.config.get('webserver', {}).get('meshcore_map_max_hop_km', 100.0))

    # Detect hash length from any ?xxxx name in via_names (default 4 = 2 bytes)
    hash_chars = next((len(vn) - 1 for vn in via_names if vn.startswith('?')), 4)

    if not (target_lat or target_lon):
        target_lat, target_lon = _remote_loc(target_name, mc_map)

    target_contact = bridge.contact_for_nick(target_name)
    target_pk = target_contact.get('public_key', '') if target_contact else ''

    items = [(target_name, target_lat, target_lon)]
    pub_prefixes = [target_pk[:hash_chars]]

    for hop_name in via_names:
        hop = bridge.contact_for_nick(hop_name)
        h_lat = hop.get('adv_lat', 0.0) if hop else 0.0
        h_lon = hop.get('adv_lon', 0.0) if hop else 0.0
        if not (h_lat or h_lon):
            h_lat, h_lon = _remote_loc(hop_name, mc_map)
        items.append((hop_name, h_lat, h_lon))
        if hop_name.startswith('?'):
            pub_prefixes.append(hop_name[1:])
        else:
            pub_prefixes.append(hop.get('public_key', '')[:hash_chars] if hop else '')

    self_pk = si.get('public_key', '')
    items.append((self_name, si.get('adv_lat', 0.0), si.get('adv_lon', 0.0)))
    pub_prefixes.append(self_pk[:hash_chars])

    roles = ['target'] + ['hop'] * len(via_names) + ['self']
    orig_names = [it[0] for it in items]
    resolved = _resolve_path_locs(items, mc_map, max_hop_km)
    return [{'name': n, 'lat': lat, 'lon': lon, 'role': role, 'src': src,
             'path_id': orig, 'pub_prefix': pp}
            for (n, lat, lon, src), role, orig, pp in zip(resolved, roles, orig_names, pub_prefixes)]


def _node_path_data(name: str, bridge) -> dict | None:
    contact = bridge.contact_for_nick(name)
    si = bridge.self_info or {}
    self_name = sanitize_nick(si.get('name', 'gateway')) if si else 'gateway'

    web_cfg = bridge.config.get('webserver', {})
    advert_weight = float(web_cfg.get('path_advert_line_weight', 2.5))
    msg_weight = float(web_cfg.get('path_msg_line_weight', 2.0))
    direct_weight = float(web_cfg.get('path_direct_line_weight', 1.5))
    color_self = str(web_cfg.get('path_color_self', '#3388ff'))
    color_hop = str(web_cfg.get('path_color_hop', '#f39c12'))
    color_target = str(web_cfg.get('path_color_target', '#e74c3c'))
    color_remote = str(web_cfg.get('path_color_remote', '#888888'))
    color_unknown_bg = str(web_cfg.get('path_color_unknown_bg', 'rgba(255,255,255,0.85)'))

    color_kwargs = dict(color_self=color_self, color_hop=color_hop, color_target=color_target,
                        color_remote=color_remote, color_unknown_bg=color_unknown_bg)

    if not contact:
        # Channel-only node — no advert, look up live member data for hop count
        member = bridge.channel_member_info(name)
        if not member:
            return None
        msg_hops = member.get('min_path_len', -1)
        target_name = sanitize_nick(name)
        msg_via_names = bridge.channel_msg_path_nodes.get(name, [])
        msg_chain = _build_chain(target_name, 0.0, 0.0, msg_via_names, self_name, si, bridge)
        msg_hash_chars = next((len(vn) - 1 for vn in msg_via_names if vn.startswith('?')), 0)
        return {'title': f'Path to {target_name}', 'advert_chain': [], 'msg_chain': msg_chain,
                'advert_hops': -1, 'msg_hops': msg_hops,
                'advert_hash_chars': 0, 'msg_hash_chars': msg_hash_chars,
                'advert_weight': advert_weight, 'msg_weight': msg_weight,
                'direct_weight': direct_weight, **color_kwargs}

    pubkey = contact.get('public_key', '')
    target_name = sanitize_nick(contact.get('adv_name', name))
    target_lat = contact.get('adv_lat', 0.0)
    target_lon = contact.get('adv_lon', 0.0)

    advert_hops = bridge.advert_path_by_pubkey.get(pubkey, -1)
    via_names = bridge.advert_path_nodes_by_pubkey.get(pubkey, [])
    prefix_key = pubkey[:12] if pubkey else contact.get('_pubkey_prefix', '')
    msg_hops = bridge.min_msg_hops_by_pubkey.get(prefix_key, -1)

    advert_chain = _build_chain(target_name, target_lat, target_lon,
                                via_names, self_name, si, bridge)

    msg_via = bridge.channel_msg_path_nodes.get(target_name,
              bridge.channel_msg_path_nodes.get(name, []))
    if not msg_via and bridge.node_cache and pubkey:
        ce = bridge.node_cache.get_by_pubkey(pubkey)
        if ce:
            msg_via = ce.get('msg_path_nodes', [])
    msg_chain = (_build_chain(target_name, target_lat, target_lon,
                              msg_via, self_name, si, bridge)
                 if msg_via or msg_hops >= 0 else [])

    advert_hash_chars = next((len(vn) - 1 for vn in via_names if vn.startswith('?')), 0)
    msg_hash_chars = next((len(vn) - 1 for vn in (msg_via or []) if vn.startswith('?')), 0) if msg_chain else 0

    return {'title': f'Path to {target_name}', 'advert_chain': advert_chain,
            'msg_chain': msg_chain, 'advert_hops': advert_hops, 'msg_hops': msg_hops,
            'advert_hash_chars': advert_hash_chars, 'msg_hash_chars': msg_hash_chars,
            'advert_weight': advert_weight, 'msg_weight': msg_weight,
            'direct_weight': direct_weight, **color_kwargs}


async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, bridge):
    try:
        first = await asyncio.wait_for(reader.readline(), timeout=5)
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            if not line or line in (b'\r\n', b'\n'):
                break

        parts = first.decode('utf-8', 'ignore').split()
        path = urllib.parse.unquote(parts[1].split('?')[0]) if len(parts) >= 2 else '/'

        body = None

        if path in ('/map', '/map/'):
            body = _render_map_index()

        elif path.startswith('/map/'):
            rest = path[5:]
            slash = rest.find('/')
            if slash == -1:
                map_type, sub = rest, ''
            else:
                map_type, sub = rest[:slash], rest[slash + 1:]

            if map_type == 'contacts' and sub and bridge:
                filter_key = sub.lower()
                if filter_key in TYPE_FILTERS:
                    nodes = bridge.contacts_map_nodes(TYPE_FILTERS[filter_key])
                    body = _render(f"Contacts ({filter_key})", nodes)

            elif map_type == 'discovered' and sub and bridge:
                filter_key = sub.lower()
                if filter_key in TYPE_FILTERS:
                    nodes = bridge.discovered_map_nodes(TYPE_FILTERS[filter_key])
                    body = _render(f"Discovered ({filter_key})", nodes)

            elif map_type == 'nodes' and bridge:
                if sub:
                    data = _node_path_data(sub, bridge)
                    if data:
                        body = _render_node_path(**data)
                else:
                    web_cfg = bridge.config.get('webserver', {})
                    base = web_cfg.get('url', 'http://{}:{}'.format(
                        bridge.config.get('irc', {}).get('host', 'localhost'),
                        web_cfg.get('port', 8080)))
                    refresh_ivs = web_cfg.get('nodes_refresh_intervals', [120, 300, 600])
                    if isinstance(refresh_ivs, int):
                        refresh_ivs = [refresh_ivs]
                    body = _render_nodes_index(_nodes_index_entries(bridge), base,
                                               refresh_intervals=[int(v) for v in refresh_ivs])

            elif map_type == 'heath' and bridge:
                _HEATH_FILTERS = {'1b': 0, '2b': 1, '3b': 2}
                if sub in _HEATH_FILTERS:
                    hm = _HEATH_FILTERS[sub]
                    nodes, edges, paths = _heath_data(bridge, hm)
                    _wcfg = bridge.config.get('webserver', {})
                    w_min = float(_wcfg.get('heath_line_weight_min', 3.0))
                    w_max = float(_wcfg.get('heath_line_weight_max', 10.0))
                    body = _render_heath(nodes, edges, paths,
                                        f'Heath — {sub} hash paths', w_min, w_max)

            elif map_type == 'neighbours':
                if sub:
                    data = neighbours_store.load(sub)
                    if data is not None:
                        nodes = data.get('nodes', [])
                        rep_name = data.get('repeater_name', sub)
                        rep_lat = data.get('repeater_lat', 0.0)
                        rep_lon = data.get('repeater_lon', 0.0)
                        age_secs = int(time.time() - data.get('timestamp', 0))
                        title = f"Neighbours of {rep_name} ({_fmt_age(age_secs)})"
                        web_cfg = bridge.config.get('webserver', {}) if bridge else {}
                        body = _render(
                            title, nodes, rep_lat, rep_lon, rep_name,
                            snr_font_size=int(web_cfg.get('snr_font_size', 13)),
                            snr_padding=str(web_cfg.get('snr_padding', '2px 6px')),
                            line_weight=float(web_cfg.get('neighbours_line_weight', 1.5)),
                        )
                else:
                    body = _render_index(neighbours_store.list_all())

        if body is not None:
            response = (
                b'HTTP/1.1 200 OK\r\n'
                b'Content-Type: text/html; charset=utf-8\r\n'
                + f'Content-Length: {len(body)}\r\n'.encode()
                + b'Connection: close\r\n\r\n'
                + body
            )
        else:
            response = _404

        writer.write(response)
        await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def run(bind: str, port: int, bridge=None):
    async def handle(reader, writer):
        await _handle(reader, writer, bridge)

    server = await asyncio.start_server(handle, bind, port)
    logger.info("Web server listening on %s:%d", bind, port)
    async with server:
        await server.serve_forever()
