'use strict';
/* =====================================================================
   GROOVETIME, PFG Player y Stick Master · servidor de sincronización maestro/esclavo (red local o nube)
   - Sirve la app (index.html, manifest, iconos, sw.js) si los archivos están junto a él.
   - /sync ...... WebSocket: salas de 4 dígitos con un maestro y N esclavos.
   - /info ...... IPs locales y puerto (para armar la URL del QR en la red local).
   - /qr.svg .... genera el código QR (funciona sin internet en la red local).
   - /health .... comprobación de estado para el hosting.
   - Relevo del maestro (RELAY): configuración musical (config), guía de la canción (song) y
     secciones open (songctl). El servidor las reenvía a los esclavos y guarda la última de
     cada tipo para los que se unen después.
   - PFG Player (versión 4): set list (pset), tema en escena (pstage), reproducción (pplay) y
     salida de secciones OPEN (prel) por el mismo relevo. Las guías (y los PDF) viajan como
     archivos por partes (blob): el maestro los sube una vez por sala, el servidor los guarda en
     memoria mientras la sala exista y cada esclavo pide sólo los que no tiene en su caché.
   - Versión 5: cada esclavo puede contarle al maestro qué es (pdev): pantalla de video y/o equipo
     de audio (con los temas cuyas secuencias tiene). El maestro sabe así qué equipos hay en la sala.
   - Versión 6 (Stick Master): cada app puede tener su propio espacio de salas (hello.app), así un
     código de GROOVETIME y uno de Stick Master nunca se cruzan. Relevo smcfg (configuración de la
     clase) y marcador en vivo (score): cada alumno envía su precisión y racha, sólo al maestro.
   - Versión 7 (Stick Master): el maestro puede mirar a un alumno (smwatch); mientras lo mira, ese
     alumno le envía el resultado de cada nota (smlive) y el maestro dibuja su pantalla en vivo.
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
const PROTOCOL_VERSION = 7;          // 2: guías de canción (song / songctl) · 3: configuración musical (config)
                                     // 4: PFG Player (pset, pstage, pplay, prel) y archivos por partes (blob)
                                     // 5: equipos de la sala (pdev: pantalla de video, equipo de audio)
                                     // 6: salas por app (hello.app), Stick Master (smcfg) y marcador (score)
                                     // 7: ver a un alumno en vivo (smwatch maestro→alumno, smlive alumno→maestro)
const MAX_MESSAGE = 512 * 1024;      // una guía de varios instrumentos ocupa decenas de KB
const BLOB_CHUNK_MAX = 256 * 1024;   // caracteres por parte de un archivo
const BLOB_MAX_PARTS = 512;          // hasta ~128 MB por archivo
const ROOM_BLOB_MAX = 96 * 1024 * 1024; // memoria por sala: al pasarse se descartan los más antiguos
const BLOB_ID = /^[a-f0-9]{16,64}$/;
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

  // PFG Player vive en /player/ con los mismos archivos de la app (una sola base de código)
  const app = pathname.replace(/^\/player(?=\/|$)/, '') || '/';
  const rel = app === '/' ? 'index.html' : app.slice(1);
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
  if (!rooms.has(id)) rooms.set(id, { master: null, slaves: new Set(), state: null, relay: {}, blobs: new Map(), blobBytes: 0, waiting: new Map() });
  return rooms.get(id);
};
// mensajes del maestro que se reenvían tal cual a los esclavos: tipo → campo con el contenido
const RELAY = { config: 'cfg', song: 'song', songctl: 'exits', pset: 'set', pstage: 'stage', pplay: 'play', prel: 'rel', smcfg: 'cfg' };
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
// ---------- archivos por partes (guías y PDF de PFG Player) ----------
// blob = { n: partes, parts: [texto], got: recibidas, size: caracteres, at: último uso }
function sendBlob(ws, id, b) {
  b.at = Date.now();
  for (let i = 0; i < b.n; i++) json(ws, { type: 'blob', id, i, n: b.n, data: b.parts[i] });
}
function putBlobPart(room, master, m) {
  const id = String(m.id || ''), n = Number(m.n), i = Number(m.i), data = m.data;
  if (!BLOB_ID.test(id) || !Number.isInteger(n) || n < 1 || n > BLOB_MAX_PARTS || !Number.isInteger(i) || i < 0 || i >= n
      || typeof data !== 'string' || data.length > BLOB_CHUNK_MAX) return;
  let b = room.blobs.get(id);
  if (b && b.got === b.n) return json(master, { type: 'blobok', id });   // ya estaba completo
  if (!b || b.n !== n) {
    if (b) room.blobBytes -= b.size;
    b = { n, parts: new Array(n), got: 0, size: 0, at: Date.now() };
    room.blobs.set(id, b);
  }
  if (b.parts[i] === undefined) { b.parts[i] = data; b.got++; b.size += data.length; room.blobBytes += data.length; }
  b.at = Date.now();
  if (b.got < b.n) return;
  json(master, { type: 'blobok', id });
  const waiting = room.waiting.get(id);
  if (waiting) { waiting.forEach(s => { if (s.readyState === 1) sendBlob(s, id, b); }); room.waiting.delete(id); }
  // memoria de la sala: se descartan los archivos usados hace más tiempo
  const sorted = [...room.blobs.entries()].filter(([k]) => k !== id).sort((a, b2) => a[1].at - b2[1].at);
  while (room.blobBytes > ROOM_BLOB_MAX && sorted.length) {
    const [k, old] = sorted.shift();
    room.blobs.delete(k); room.blobBytes -= old.size;
  }
}
const doneBlobs = room => [...room.blobs.entries()].filter(([, b]) => b.got === b.n).map(([k]) => k);

function sanitizeState(m) {
  const bpm = Math.max(BPM_MIN, Math.min(BPM_MAX, Math.round(Number(m.bpm) || 120)));
  if (!m.playing) {
    const at = Number(m.at);
    return Number.isFinite(at) ? { type: 'state', playing: false, bpm, at } : { type: 'state', playing: false, bpm };
  }
  const at = Number(m.at), q = Number(m.q);
  if (!Number.isFinite(at) || !Number.isFinite(q)) return null;
  const st = { type: 'state', playing: true, bpm, at, q };
  const c = Number(m.c), cb = Number(m.cb);                 // clave con tempo propio: fase y tempo
  if (m.c !== undefined && Number.isFinite(c) && cb >= BPM_MIN && cb <= BPM_MAX) { st.c = c; st.cb = cb; }
  return st;
}

const wss = new WebSocketServer({
  server, path: '/sync', maxPayload: MAX_MESSAGE, perMessageDeflate: false,
  verifyClient: ({ origin }) => !ALLOWED_ORIGINS.length || ALLOWED_ORIGINS.includes(origin)
});
// lo que un esclavo cuenta de sí mismo (sólo lo que el maestro necesita saber)
function sanitizeDev(d) {
  if (!d || typeof d !== 'object') return null;
  const ids = v => Array.isArray(v) ? v.filter(x => typeof x === 'string' && x.length <= 64).slice(0, 500) : [];
  // audio: temas cuya secuencia toca · sounds: temas cuyos sonidos por sección toca
  return { name: String(d.name || '').slice(0, 40), screen: !!d.screen, sound: !!d.sound, audio: ids(d.audio), sounds: ids(d.sounds) };
}
// marcador de Stick Master: lo que un alumno cuenta de su práctica (sólo lo recibe el maestro)
function sanitizeScore(d) {
  if (!d || typeof d !== 'object') return null;
  const int = (v, a, b) => Math.max(a, Math.min(b, Math.round(Number(v) || 0)));
  return {
    n: String(d.n || '').slice(0, 24), m: ['book', 'free', 'capture'].includes(d.m) ? d.m : 'book', run: !!d.run,
    p: int(d.p ?? -1, -1, 100), st: int(d.st, 0, 1e6), b: int(d.b, 0, 1e6), h: int(d.h, 0, 1e7),
    ok: int(d.ok, 0, 1e7), er: int(d.er, 0, 1e7), off: int(d.off, -1000, 1000),
    t: String(d.t || '').slice(0, 12), s: ['mic', 'midi'].includes(d.s) ? d.s : 'touch'
  };
}
// Stick Master en vivo: resultado de una nota o golpe del alumno que el maestro está mirando
const LIVE_NUM = ['r', 'i', 'ok', 'o', 't', 'a', 'st', 'b', 'okc', 'er', 'p'];
function sanitizeLive(e) {
  if (!e || typeof e !== 'object' || !['n', 'm', 'x', 'h'].includes(e.k)) return null;
  const out = { k: e.k, hd: e.hd === 'L' || e.hd === 'R' ? e.hd : null };
  for (const k of LIVE_NUM) { const v = Number(e[k]); if (Number.isFinite(v)) out[k] = v; }
  return out;
}
// espacio de salas por app: sin app (GROOVETIME, PFG Player) la clave es el código tal cual
const roomKey = (room, app) => /^[a-z]{2,8}$/.test(String(app || '')) ? `${app}:${room}` : String(room);
let nextId = 1;
wss.on('connection', ws => {
  ws.alive = true; ws.id = nextId++;
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
        const key = roomKey(m.room, m.app);
        const room = getRoom(key);
        if (m.role === 'master') {
          if (room.master) return json(ws, { type: 'error', msg: 'La sala ya tiene maestro' });
          room.master = ws; room.state = null;
        } else {
          room.slaves.add(ws);
        }
        ws.room = key; ws.role = m.role;
        const slave = m.role === 'slave';
        json(ws, { type: 'welcome', v: PROTOCOL_VERSION, peers: room.slaves.size, hasMaster: !!room.master,
          state: slave ? room.state : null, relay: slave ? room.relay : null, blobs: slave ? undefined : doneBlobs(room) });
        // el maestro (nuevo o reconectado) recibe lo que ya contaron los esclavos
        if (!slave) room.slaves.forEach(s => {
          if (s.dev) json(ws, { type: 'pdev', id: s.id, dev: s.dev });
          if (s.score) json(ws, { type: 'score', id: s.id, s: s.score });
        });
        return notifyPeers(room);
      }

      case 'pdev': {     // un esclavo cuenta qué es (pantalla, audio): sólo lo recibe el maestro
        const room = rooms.get(ws.room);
        if (!room || ws.role !== 'slave') return;
        ws.dev = sanitizeDev(m.dev);
        if (room.master) json(room.master, { type: 'pdev', id: ws.id, dev: ws.dev });
        return;
      }

      case 'score': {    // Stick Master: un alumno informa su precisión y racha; sólo lo recibe el maestro
        const room = rooms.get(ws.room);
        if (!room || ws.role !== 'slave') return;
        ws.score = sanitizeScore(m.s);
        if (room.master) json(room.master, { type: 'score', id: ws.id, s: ws.score });
        return;
      }

      case 'smwatch': {  // Stick Master: el maestro empieza o deja de mirar a un alumno
        const room = rooms.get(ws.room);
        if (!room || room.master !== ws) return;
        room.slaves.forEach(s => { if (s.id === Number(m.id)) json(s, { type: 'smwatch', on: !!m.on }); });
        return;
      }

      case 'smlive': {   // Stick Master: resultado en vivo del alumno mirado (sólo al maestro)
        const room = rooms.get(ws.room);
        const e = sanitizeLive(m.e);
        if (!room || ws.role !== 'slave' || !room.master || !e) return;
        json(room.master, { type: 'smlive', id: ws.id, e });
        return;
      }

      case 'state': {
        const room = rooms.get(ws.room);
        if (!room || room.master !== ws) return;
        const st = sanitizeState(m);
        if (!st) return;
        room.state = st;
        const out = JSON.stringify(st);
        room.slaves.forEach(s => { if (s.readyState === 1) s.send(out); });
        return;
      }

      case 'blob': {     // el maestro sube un archivo por partes
        const room = rooms.get(ws.room);
        if (!room || room.master !== ws) return;
        return putBlobPart(room, ws, m);
      }

      case 'getblob': {  // un esclavo pide un archivo que no tiene en su caché
        const room = rooms.get(ws.room), id = String(m.id || '');
        if (!room || !BLOB_ID.test(id)) return;
        const b = room.blobs.get(id);
        if (b && b.got === b.n) return sendBlob(ws, id, b);
        if (!room.waiting.has(id)) room.waiting.set(id, new Set());
        room.waiting.get(id).add(ws);
        if (room.master) json(room.master, { type: 'needblob', id });   // p. ej. tras reiniciarse el servidor
        return;
      }

      default: {         // relevo: config, song, songctl (GROOVETIME) · pset, pstage, pplay, prel (PFG Player) · smcfg (Stick Master)
        const field = RELAY[m.type];
        const room = rooms.get(ws.room);
        if (!field || !room || room.master !== ws) return;
        const value = m[field] && typeof m[field] === 'object' ? m[field] : null;
        const payload = { type: m.type, [field]: value };
        room.relay[m.type] = payload;
        const out = JSON.stringify(payload);
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
      if (ws.dev && room.master) json(room.master, { type: 'pdev', id: ws.id, dev: null });
      if (ws.score && room.master) json(room.master, { type: 'score', id: ws.id, s: null });
    }
    room.waiting.forEach(set => set.delete(ws));
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
  console.log(`GROOVETIME / PFG Player / Stick Master · servidor de sincronización (v${PROTOCOL_VERSION}) en el puerto ${PORT}`);
  console.log(`  este equipo:  http://localhost:${PORT}`);
  lanIPs().forEach(ip => console.log(`  otros equipos: http://${ip}:${PORT}`));
});
