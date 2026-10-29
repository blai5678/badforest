const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { URL } = require('node:url');
const { randomUUID } = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 3000);
const PUBLIC = __dirname;
const rooms = new Map();
const mime = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml' };
const bounded = (n, a, b) => Math.max(a, Math.min(b, n));
function cleanRoom(v) { return String(v || 'bosque-maldito').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 32) || 'bosque-maldito'; }
function getRoom(name) { if (!rooms.has(name)) rooms.set(name, { clients: new Map(), trees: new Set(), fires: [], builds: [], enemies: [] }); return rooms.get(name); }
function send(ws, x) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(x)); }
function broadcast(room, x) { for (const ws of room.clients.keys()) send(ws, x); }
function peers(room) { broadcast(room, { type: 'peers', peers: [...room.clients.values()].map(p => ({ peer: p.id, presence: p.presence })) }); }

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  let file;
  try { file = decodeURIComponent(url.pathname === '/' ? '/index.html' : url.pathname); } catch { res.writeHead(400).end(); return; }
  const target = path.resolve(PUBLIC, '.' + file);
  if (target !== path.join(PUBLIC, 'index.html') && !target.startsWith(PUBLIC + path.sep)) { res.writeHead(403).end('Forbidden'); return; }
  fs.readFile(target, (err, data) => {
    if (err) { res.writeHead(404).end('No encontrado'); return; }
    res.writeHead(200, { 'Content-Type': mime[path.extname(target)] || 'application/octet-stream', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' }); res.end(data);
  });
});
const wss = new WebSocketServer({ noServer: true, maxPayload: 48 * 1024 });
server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  if (url.pathname !== '/ws') { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, cleanRoom(url.searchParams.get('room'))));
});
wss.on('connection', (ws, roomName) => {
  const room = getRoom(roomName), id = randomUUID().slice(0, 12);
  const client = { id, presence: null, stamps: [] }; room.clients.set(ws, client);
  send(ws, { type: 'welcome', peer: id, room: roomName });
  for (const id of room.trees) send(ws, { type: 'event', name: 'tree', peer: 'server', data: { id } });
  for (const data of room.fires) send(ws, { type: 'event', name: 'fire', peer: 'server', data });
  for (const data of room.builds) send(ws, { type: 'event', name: 'build', peer: 'server', data });
  if (room.enemies.length) send(ws, { type: 'event', name: 'en', peer: 'server', data: room.enemies });
  peers(room);
  ws.on('message', raw => {
    const now = Date.now(); client.stamps = client.stamps.filter(t => now - t < 1000); if (client.stamps.length > 40) return; client.stamps.push(now);
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m !== 'object') return;
    if (m.type === 'presence' && m.data && Number.isFinite(m.data.x) && Number.isFinite(m.data.z)) {
      client.presence = { x: bounded(m.data.x, -200, 200), z: bounded(m.data.z, -200, 200), yaw: Number(m.data.yaw) || 0, c: Number(m.data.c) || 0, ok: !!m.data.ok }; peers(room); return;
    }
    if (m.type !== 'event' || typeof m.name !== 'string' || m.name.length > 24) return;
    const d = m.data;
    if (m.name === 'chat' && (!d || typeof d.t !== 'string')) return;
    if (m.name === 'tree' && d && Number.isFinite(d.id)) room.trees.add(d.id);
    if (m.name === 'fire' && d && Number.isFinite(d.x) && Number.isFinite(d.z)) { room.fires.push({ x: bounded(d.x, -200, 200), z: bounded(d.z, -200, 200) }); if (room.fires.length > 8) room.fires.shift(); }
    if (m.name === 'build' && d && Number.isInteger(d.k) && Number.isFinite(d.x) && Number.isFinite(d.z)) { room.builds.push({ k: d.k, x: bounded(d.x, -200, 200), z: bounded(d.z, -200, 200), r: d.r ? 1 : 0 }); if (room.builds.length > 300) room.builds.shift(); }
    if (m.name === 'en' && Array.isArray(d)) room.enemies = d.slice(0, 20);
    broadcast(room, { type: 'event', name: m.name, peer: id, data: d });
  });
  ws.on('close', () => { room.clients.delete(ws); peers(room); });
  ws.on('error', () => { room.clients.delete(ws); peers(room); });
});
server.listen(PORT, '0.0.0.0', () => console.log(`Bosque Maldito listo en puerto ${PORT}`));
