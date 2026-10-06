'use strict';
/* =====================================================================
   GROOVETIME · servidor de sincronización maestro/esclavo (red local o nube)
   - Sirve la app (index.html, manifest, iconos, sw.js) si los archivos están junto a él.
   - /sync ...... WebSocket: salas de 4 dígitos con un maestro y N esclavos.
   - /info ...... IPs locales y puerto (para armar la URL del QR en la red local).
   - /qr.svg .... genera el código QR (funciona sin internet en la red local).
   - /health .... comprobación de estado para el hosting.
   Uso local:  npm install  →  npm start   (o PORT=9000 node server.js)
   En la nube: ALLOWED_ORIGINS=https://tu-app.github.io limita qué sitios pueden conectarse.
   ===================================================================== */
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { performance } = require('perf_hooks');
const { WebSocketServer } = require('ws');
const QRCode = require('qrcode');

const PORT = Number(process.env.PORT) || 8080;
const ROOT = __dirname;
const HEARTBEAT_MS = 15000;
const BPM_MIN = 30, BPM_MAX = 300;
// orígenes autorizados para el WebSocket (vacío: cualquiera)
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim().replace(/\/+$/, '')).filter(Boolean);

const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json', '.webmanifest': 'application/manifest+json',
  '.css': 'text/css; charset=utf-8', '.png': 'image/png', '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon', '.jpg': 'image/jpeg', '.webp': 'image/webp'
};
const PRIVATE_FILES = new Set(['server.js', 'package.json', 'package-lock.json', 'render.yaml']);

function lanIPs() {
  const out = [];
  for (const list of Object.values(os.networkInterfaces())) {
    for (const a of list || []) if (a.family === 'IPv4' && !a.internal) out.push(a.address);
  }
  // prioriza rangos domésticos típicos (192.168.x / 10.x) frente a adaptadores virtuales
  return out.sort((a, b) => Number(!a.startsWith('192.168.')) - Number(!b.startsWith('192.168.')));
}

// ---------- HTTP ----------
function send(res, status, type, body) {
  // la app puede estar alojada en otro dominio (p. ej. GitHub Pages) que el servidor
  res.writeHead(status, { 'Content-Type': type, 'Cache-Control': 'no-cache', 'Access-Control-Allow-Origin': '*' });
  res.end(body);
}
function serveFile(res, file) {
  fs.readFile(file, (err, data) => {
    if (err) return send(res, 404, 'text/plain', 'No encontrado');
    send(res, 200, MIME[path.extname(file).toLowerCase()] || 'application/octet-stream', data);
  });
}
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const pathname = decodeURIComponent(url.pathname);

  if (pathname === '/health') return send(res, 200, 'text/plain', 'ok');
  if (pathname === '/info') {
    return send(res, 200, MIME['.json'], JSON.stringify({ ips: lanIPs(), port: PORT }));
  }
  if (pathname === '/qr.svg') {
    const text = (url.searchParams.get('text') || '').slice(0, 512);
    if (!text) return send(res, 400, 'text/plain', 'Falta text');
    try {
      const svg = await QRCode.toString(text, { type: 'svg', margin: 1, errorCorrectionLevel: 'M',
        color: { dark: '#10261a', light: '#f2f2f4' } });
      return send(res, 200, MIME['.svg'], svg);
    } catch (_) { return send(res, 500, 'text/plain', 'Error al generar QR'); }
  }

  const rel = pathname === '/' ? 'index.html' : pathname.slice(1);
  const file = path.resolve(ROOT, rel);
  if (!file.startsWith(ROOT + path.sep) || PRIVATE_FILES.has(rel) || rel.startsWith('node_modules')) {
    return send(res, 403, 'text/plain', 'Prohibido');
  }
  serveFile(res, file);
});

// ---------- salas ----------
// room = { master: ws|null, slaves: Set<ws>, state: último estado del maestro }
const rooms = new Map();
const getRoom = id => {
  if (!rooms.has(id)) rooms.set(id, { master: null, slaves: new Set(), state: null });
  return rooms.get(id);
};
const json = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
function notifyPeers(room) {
  // maxRtt: peor latencia de los esclavos, para que el maestro calcule cuánto anticipar los cambios
  let maxRtt = 0;
  room.slaves.forEach(s => { maxRtt = Math.max(maxRtt, s.rtt || 0); });
  room.maxRtt = maxRtt;
  const msg = { type: 'peers', count: room.slaves.size, hasMaster: !!room.master, maxRtt };
  if (room.master) json(room.master, msg);
  room.slaves.forEach(s => json(s, msg));
}
function sanitizeState(m) {
  const bpm = Math.max(BPM_MIN, Math.min(BPM_MAX, Math.round(Number(m.bpm) || 120)));
  if (!m.playing) {
    const at = Number(m.at);
    return Number.isFinite(at) ? { type: 'state', playing: false, bpm, at } : { type: 'state', playing: false, bpm };
  }
  const at = Number(m.at), q = Number(m.q);
  if (!Number.isFinite(at) || !Number.isFinite(q)) return null;
  return { type: 'state', playing: true, bpm, at, q };
}

const wss = new WebSocketServer({
  server, path: '/sync', maxPayload: 4096, perMessageDeflate: false,
  verifyClient: ({ origin }) => !ALLOWED_ORIGINS.length || ALLOWED_ORIGINS.includes(origin)
});
wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch (_) { return; }
    switch (m.type) {
      case 'ping':   // reloj común: el cliente estima su desfase con este instante
        return json(ws, { type: 'pong', t0: m.t0, ts: performance.now() });

      case 'rtt': {   // el esclavo informa su latencia (p90 de ida y vuelta, ms)
        const room = rooms.get(ws.room);
        const ms = Math.max(0, Math.min(2000, Number(m.ms) || 0));
        if (!room || ws.role !== 'slave') return;
        const before = room.maxRtt || 0;
        ws.rtt = ms;
        let max = 0;
        room.slaves.forEach(s => { max = Math.max(max, s.rtt || 0); });
        if (Math.abs(max - before) >= 5) notifyPeers(room);
        return;
      }

      case 'hello': {
        if (ws.room || !/^\d{4}$/.test(String(m.room)) || !['master', 'slave'].includes(m.role)) return;
        const room = getRoom(String(m.room));
        if (m.role === 'master') {
          if (room.master) return json(ws, { type: 'error', msg: 'La sala ya tiene maestro' });
          room.master = ws; room.state = null;
        } else {
          room.slaves.add(ws);
        }
        ws.room = String(m.room); ws.role = m.role;
        json(ws, { type: 'welcome', peers: room.slaves.size, hasMaster: !!room.master,
          state: m.role === 'slave' ? room.state : null });
        return notifyPeers(room);
      }

      case 'state': {
        const room = rooms.get(ws.room);
        if (!room || room.master !== ws) return;
        const st = sanitizeState(m);
        if (!st) return;
        room.state = st;
        const out = JSON.stringify(st);
        room.slaves.forEach(s => { if (s.readyState === 1) s.send(out); });
      }
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.room);
    if (!room) return;
    if (room.master === ws) {
      room.master = null; room.state = null;
      room.slaves.forEach(s => json(s, { type: 'masterGone' }));
    } else {
      room.slaves.delete(ws);
    }
    if (!room.master && room.slaves.size === 0) rooms.delete(ws.room);
    else notifyPeers(room);
  });
});

// descarta conexiones muertas (teléfono bloqueado, Wi-Fi caído)
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.alive) return ws.terminate();
    ws.alive = false; ws.ping();
  });
}, HEARTBEAT_MS);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`GROOVETIME · servidor de sincronización en el puerto ${PORT}`);
  console.log(`  este equipo:  http://localhost:${PORT}`);
  lanIPs().forEach(ip => console.log(`  otros equipos: http://${ip}:${PORT}`));
});
