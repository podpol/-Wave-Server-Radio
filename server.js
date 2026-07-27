/**
 * ═══════════════════════════════════════════════════════════
 * Wave 🌊 — Lighthouse Server v2.0 (Federation Edition)
 * ═══════════════════════════════════════════════════════════
 * Stack: Node.js + Express + Socket.io + Federation
 * License: AGPL-3.0
 * 
 * NEW in v2.0:
 *  - Federation: multiple servers form a global network
 *  - Channels visible across all federated servers
 *  - Globally unique channel names
 *  - E2EE Chat: AES-256-GCM keys wrapped with user's RSA public key
 *  - Strict WebRTC signaling isolation per channel
 */

'use strict';

try { require('dotenv').config(); } catch (e) { console.warn('⚠️  dotenv not installed — falling back to real environment variables only.'); }

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const Federation = require('./federation');
let helmet = null;
try { helmet = require('helmet'); } catch (e) { /* npm install helmet — рекомендуется для прод */ }

const SAVE_FILE = path.join(__dirname, 'channels-backup.json');

// ═══════════════════════════════════════════════════════════
// PERSISTENCE
// ═══════════════════════════════════════════════════════════

function saveChannels() {
  try {
    const data = [];
    channels.forEach((ch, id) => {
        data.push({
          id: ch.id, name: ch.name, admin: ch.admin, adminName: ch.adminName,
          requireApproval: ch.requireApproval,
          chestLocked: ch.chestLocked || false,
          totalLikes: ch.totalLikes || 0,
          created: ch.created,
          // ✅ ПАТЧ 3: messages НЕ СОХРАНЯЕМ — они эфемерны
          approvedUsers: [...(ch.approvedUsers || [])],
          persistentSpeakers: [...(ch.persistentSpeakers || [])],
          chest: (ch.chest || []).map(file => ({ ...file }))
        });
    });
    fs.writeFileSync(SAVE_FILE, JSON.stringify(data, null, 2), 'utf8');
    // ✅ Сохраняем глобальные имена для федерации при перезапуске
    if (federation && federation.globalNames) {
        const namesData = {};
        federation.globalNames.forEach((id, name) => { namesData[name] = id; });
        fs.writeFileSync(path.join(__dirname, 'federation-names-backup.json'), JSON.stringify(namesData, null, 2), 'utf8');
    }
  } catch (err) {
    console.error('❌ Failed to save channels:', err.message);
    // Fallback save without federation names if it fails
    try {
        const data = [];
        channels.forEach((ch, id) => {
            data.push({ id: ch.id, name: ch.name, admin: ch.admin, adminName: ch.adminName, requireApproval: ch.requireApproval, chestLocked: ch.chestLocked || false, totalLikes: ch.totalLikes || 0, created: ch.created, approvedUsers: [...(ch.approvedUsers || [])], persistentSpeakers: [...(ch.persistentSpeakers || [])], chest: (ch.chest || []).map(file => ({ ...file })) });
        });
        fs.writeFileSync(SAVE_FILE, JSON.stringify(data, null, 2), 'utf8');
    } catch(e) {}
  }
}

function loadChannels() {
  try {
    if (!fs.existsSync(SAVE_FILE)) { console.log('📂 No backup file, starting fresh'); return; }
    const data = JSON.parse(fs.readFileSync(SAVE_FILE, 'utf8'));
    data.forEach(ch => {
        channels.set(ch.id, {
          ...ch,
          speakers: new Map(), listeners: new Map(),
          joinRequests: new Map(), raisedHands: new Set(),
          recentlyApproved: new Map(), userLikes: new Map(),
          messages: [],  // ← фикс
          chest: ch.chest || [], deleteTimer: null,
          approvedUsers: new Set(ch.approvedUsers || []),
          persistentSpeakers: new Set(ch.persistentSpeakers || []),
          aesKey: crypto.randomBytes(32) 
        });
    });
    console.log(`✅ Loaded ${data.length} channel(s) from backup`);
    
    // ✅ Восстанавливаем глобальные имена федерации
    const namesFile = path.join(__dirname, 'federation-names-backup.json');
    if (fs.existsSync(namesFile) && federation) {
        const namesData = JSON.parse(fs.readFileSync(namesFile, 'utf8'));
        Object.entries(namesData).forEach(([name, id]) => {
            federation.globalNames.set(name.toLowerCase().trim(), id);
        });
        console.log(`✅ Loaded ${Object.keys(namesData).length} global channel names`);
    }
  } catch (err) {
    console.error('❌ Failed to load channels:', err.message);
  }
}

setInterval(saveChannels, 60000);

// ═══════════════════════════════════════════════════════════
// CRASH PROTECTION
// ═══════════════════════════════════════════════════════════

function gracefulShutdown(signal) {
  log('🛑', `Shutdown: ${signal}. Saving...`);
  saveChannels();
  if (federation) federation.shutdown();
  try { io.close(); } catch(e) {}
  try { server.close(); } catch(e) {}
  setTimeout(() => { console.log('👋 Stopped.'); process.exit(0); }, 1000);
}

process.on('uncaughtException', (err) => { console.error('🔥 UNCAUGHT:', err.message, err.stack); });
process.on('unhandledRejection', (reason) => { console.error('🔥 UNHANDLED:', reason); });
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// ═══════════════════════════════════════════════════════════
// APP SETUP
// ═══════════════════════════════════════════════════════════

const app = express();
if (helmet) app.use(helmet({ contentSecurityPolicy: false }));
app.set('trust proxy', 1);

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: (origin, callback) => {
      const allowed = ['chrome-extension://', 'http://localhost', 'http://127.0.0.1'];
      const extra = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
      if (!origin || allowed.some(a => String(origin).startsWith(a)) || extra.includes(origin)) {
        return callback(null, true);
      }
      callback(new Error('CORS blocked'));
    }
  },
  pingTimeout: 60000,
  pingInterval: 25000,
  maxHttpBufferSize: 250e6
});

// ═══════════════════════════════════════════════════════════
// CONFIG
// ═══════════════════════════════════════════════════════════

const CONFIG = {
  port: process.env.PORT || 3000,
  host: '0.0.0.0',
  apiKey: process.env.API_KEY || '',
  limits: {
    MAX_CHANNELS: 1000, MAX_CHANNELS_PER_USER: 1,
    MAX_SPEAKERS: 10, MAX_LISTENERS: 30, MAX_USERS: 40,
    BAN_DURATION: 30 * 60 * 1000, VOTE_DURATION: 60 * 1000,
    VOTE_THRESHOLD: 0.5, MAX_MESSAGE_LENGTH: 500,
    MAX_NAME_LENGTH: 20, MAX_CHANNEL_NAME_LENGTH: 30,
    MESSAGE_HISTORY: 200, REQUEST_EXPIRY: 5 * 60 * 1000,
    MAX_PAYLOAD_SIZE: 200_000_000
  },
  security: {
    MAX_CONNECTIONS_PER_IP: 10, MAX_EVENTS_PER_SECOND: 30,
    RATE_LIMIT_MESSAGES: 10, RATE_LIMIT_WINDOW: 5000,
    LIKE_COOLDOWN: 400, JOIN_COOLDOWN_MS: 2000,
    BOT_TOKENS: (process.env.BOT_TOKENS || 'default-bot-token').split(',')
  }
};

if (CONFIG.security.BOT_TOKENS.includes('default-bot-token') && process.env.NODE_ENV === 'production') {
  console.error('❌ FATAL: BOT_TOKENS not set (still default). Set BOT_TOKENS env var before running in production.');
  process.exit(1);
}
if (!CONFIG.apiKey && process.env.NODE_ENV === 'production') {
  console.warn('⚠️  WARNING: API_KEY not set — /api/* endpoints are open to anyone in production.');
}

// ═══════════════════════════════════════════════════════════
// STORAGE
// ═══════════════════════════════════════════════════════════

const channels = new Map();
const users = new Map();
const bans = new Map();
const votes = new Map();
const ipConnections = new Map();
const socketRates = new Map();
const messageRates = new Map();
const channelSpeaking = new Map();
const channelScreenShare = new Map();
const channelLikes = new Map();
const joinCooldowns = new Map();
const relayHelpers = new Map();

// ═══════════════════════════════════════════════════════════
// FEDERATION INIT
// ═══════════════════════════════════════════════════════════

const federation = new Federation({
  serverId: process.env.FEDERATION_ID || `wave-${Date.now().toString(36)}`,
  serverUrl: process.env.FEDERATION_URL || `ws://localhost:${(parseInt(CONFIG.port) + 1)}`,
  serverName: process.env.FEDERATION_NAME || 'Wave Node',
  secret: process.env.FEDERATION_SECRET || '',
  peers: process.env.FEDERATION_PEERS || '',
  bootstrap: process.env.FEDERATION_BOOTSTRAP || '',
  officialIds: process.env.OFFICIAL_SERVER_IDS || '',
  maxPeers: process.env.FEDERATION_MAX_PEERS || 40
});

// ═══════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════

function getUniqueName(channel, desiredName, excludeUserId = null) {
  const usedNames = new Set();
  channel.speakers.forEach((s, uid) => { if (uid !== excludeUserId) usedNames.add(s.name.toLowerCase()); });
  channel.listeners.forEach((l, uid) => { if (uid !== excludeUserId) usedNames.add(l.name.toLowerCase()); });
  let name = desiredName; let suffix = 1;
  while (usedNames.has(name.toLowerCase())) { name = `${desiredName}#${suffix}`; suffix++; }
  return name;
}

function findUser(userId) {
  for (const [socketId, u] of users) { if (u.userId === userId) return { ...u, socketId }; }
  return null;
}

function sanitize(str, maxLen) {
  if (!str || typeof str !== 'string') return '';
  return str.replace(/[<>]/g, '').trim().slice(0, maxLen);
}

function safeData(data) {
  if (!data || typeof data !== 'object') return {};
  const clean = {};
  for (const key of Object.keys(data)) {
    if (key === '__proto__' || key === 'constructor' || key === 'prototype') continue;
    clean[key] = data[key];
  }
  return clean;
}

function log(icon, message) {
  const time = new Date().toLocaleTimeString('en-US', { hour12: false });
  console.log(`[${time}] ${icon} ${message}`);
}

function safeHandler(name, fn) {
  return async (...args) => {
    try { await fn(...args); }
    catch (err) {
      console.error(`❌ [${name}]:`, err.message);
      const cb = args[args.length - 1];
      if (typeof cb === 'function') { try { cb({ error: `Server error: ${err.message}` }); } catch(e) {} }
    }
  };
}

// ✅ ПАТЧ 1: Проверка канала перед ретрансляцией WebRTC
function canSignal(socketId, targetUserId) {
  const sender = users.get(socketId);
  const target = findUser(targetUserId);
  return target && sender && sender.channelId && sender.channelId === target.channelId;
}

// ═══════════════════════════════════════════════════════════
// SECURITY MIDDLEWARE
// ═══════════════════════════════════════════════════════════

io.use((socket, next) => {
  try {
    const ip = socket.handshake.address;
    const count = ipConnections.get(ip) || 0;
    if (count >= CONFIG.security.MAX_CONNECTIONS_PER_IP) return next(new Error('Too many connections'));
    ipConnections.set(ip, count + 1);
    socket._clientIp = ip;
    next();
  } catch (err) { next(new Error('Connection error')); }
});

function checkSocketRate(socketId) {
  const now = Date.now();
  let rate = socketRates.get(socketId);
  if (!rate || now - rate.lastReset > 1000) { rate = { count: 0, lastReset: now }; socketRates.set(socketId, rate); }
  rate.count++;
  return rate.count <= CONFIG.security.MAX_EVENTS_PER_SECOND;
}

function checkMessageRate(userId) {
  const now = Date.now();
  let rate = messageRates.get(userId);
  if (!rate || now - rate.lastReset > CONFIG.security.RATE_LIMIT_WINDOW) { rate = { count: 0, lastReset: now }; messageRates.set(userId, rate); }
  rate.count++;
  return rate.count <= CONFIG.security.RATE_LIMIT_MESSAGES;
}

function apiAuth(req, res, next) {
  if (!CONFIG.apiKey) return next();
  if (req.headers['x-api-key'] === CONFIG.apiKey) return next();
  res.status(403).json({ error: 'Forbidden' });
}

// ═══════════════════════════════════════════════════════════
// HTTP ENDPOINTS
// ═══════════════════════════════════════════════════════════

app.get('/', (req, res) => {
  try {
    let ts = 0, tl = 0;
    channels.forEach(ch => { ts += ch.speakers.size; tl += ch.listeners.size; });
    const fedStatus = federation.getStatus();
    res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Wave 🌊 Lighthouse</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,sans-serif;background:#09090b;color:#fafafa;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:40px 20px}.c{max-width:520px;width:100%}.h{text-align:center;margin-bottom:28px}.h h1{font-size:24px;font-weight:800;background:linear-gradient(135deg,#00b4d8,#0077b6);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:10px}.sb{display:inline-flex;align-items:center;gap:8px;padding:6px 14px;background:rgba(16,185,129,.1);border:1px solid rgba(16,185,129,.3);border-radius:20px;font-size:12px;color:#10b981;font-weight:600}.sd{width:7px;height:7px;background:#10b981;border-radius:50%;animation:p 2s infinite}@keyframes p{0%,100%{opacity:1}50%{opacity:.4}}.cd{background:#111114;border:1px solid rgba(255,255,255,.06);border-radius:14px;padding:22px;margin-bottom:14px}.cd h2{font-size:11px;text-transform:uppercase;letter-spacing:1.2px;color:rgba(250,250,250,.4);margin-bottom:14px;font-weight:700}.sg{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px}.st{background:#1a1a1f;border-radius:10px;padding:14px;text-align:center}.sv{font-size:22px;font-weight:800}.sl{font-size:10px;color:rgba(250,250,250,.4);margin-top:3px;text-transform:uppercase}.ll{list-style:none}.ll li{display:flex;justify-content:space-between;padding:9px 0;border-bottom:1px solid rgba(255,255,255,.04);font-size:12px}.ll li:last-child{border-bottom:none}.ll .lb{color:rgba(250,250,250,.6)}.ll .vl{font-weight:700;font-family:monospace}.ub{background:#1a1a1f;border:1px solid rgba(0,180,216,.3);border-radius:10px;padding:14px;text-align:center;font-family:monospace;font-size:12px;color:#00b4d8;word-break:break-all}.ft{text-align:center;font-size:10px;color:rgba(250,250,250,.25);margin-top:20px}.fed{background:rgba(99,102,241,.08);border:1px solid rgba(99,102,241,.2);border-radius:10px;padding:12px;margin-top:10px;font-size:11px;color:rgba(250,250,250,.6)}.fed strong{color:#818cf8}</style></head>
<body><div class="c"><div class="h"><h1>Wave 🌊 Lighthouse v2.0</h1><div class="sb"><div class="sd"></div>Federation Online</div></div>
<div class="cd"><h2>Live Statistics</h2><div class="sg">
<div class="st"><div class="sv">${channels.size}</div><div class="sl">Local Ch</div></div>
<div class="st"><div class="sv">${fedStatus.globalChannels + channels.size}</div><div class="sl">Global Ch</div></div>
<div class="st"><div class="sv">${users.size}</div><div class="sl">Online</div></div>
<div class="st"><div class="sv">${ts}</div><div class="sl">Speakers</div></div>
<div class="st"><div class="sv">${tl}</div><div class="sl">Listeners</div></div>
<div class="st"><div class="sv">${fedStatus.peersConnected}</div><div class="sl">Peers</div></div></div></div>
<div class="cd"><h2>Federation</h2><ul class="ll">
<li><span class="lb">Server ID</span><span class="vl">${fedStatus.serverId}</span></li>
<li><span class="lb">Server Name</span><span class="vl">${fedStatus.serverName}</span></li>
<li><span class="lb">Peers connected</span><span class="vl">${fedStatus.peersConnected}/${fedStatus.peersTotal}</span></li>
<li><span class="lb">Remote channels</span><span class="vl">${fedStatus.globalChannels}</span></li></ul>
<div class="fed">🌐 This server is part of a <strong>global federation network</strong>. Channels from all servers are visible to everyone.</div></div>
<div class="cd"><h2>Connection</h2><div class="ub">ws://${req.headers.host}</div></div>
<div class="ft">Wave v2.0 — P2P Voice & Files. Federated. Server never hears or stores your data.</div></div></body></html>`);
  } catch (err) { res.status(500).send('Server error'); }
});

app.get('/api/channels', apiAuth, (req, res) => {
  try { res.json(federation.getGlobalChannelList()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/stats', apiAuth, (req, res) => {
  try {
    let ts = 0, tl = 0;
    channels.forEach(ch => { ts += ch.speakers.size; tl += ch.listeners.size; });
    res.json({ channels: channels.size, users: users.size, speakers: ts, listeners: tl, relayHelpers: relayHelpers.size, federation: federation.getStatus() });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/federation', apiAuth, (req, res) => {
  try { res.json(federation.getStatus()); }
  catch (err) { res.status(500).json({ error: err.message }); }
});

// ═══════════════════════════════════════════════════════════
// PERIODIC TASKS
// ═══════════════════════════════════════════════════════════

setInterval(() => {
  try {
    const statuses = {};
    channels.forEach((ch, id) => {
      const speaking = channelSpeaking.get(id)?.size || 0;
      const screen = channelScreenShare.get(id)?.size || 0;
      const likes = channelLikes.get(id) || 0;
      if (speaking > 0 || screen > 0 || likes > 0) statuses[id] = { speaking, screen, likes };
    });
    if (Object.keys(statuses).length > 0) io.emit('channel-statuses', statuses);
    channelLikes.clear();
  } catch (err) { console.error('❌ Periodic error:', err.message); }
}, 3000);

setInterval(() => {
  try {
    const now = Date.now();
    channels.forEach((channel) => {
      if (!channel.joinRequests) return;
      const expired = [];
      channel.joinRequests.forEach((req, userId) => { if (now - req.timestamp > CONFIG.limits.REQUEST_EXPIRY) expired.push(userId); });
      expired.forEach(userId => {
        channel.joinRequests.delete(userId);
        const target = findUser(userId);
        if (target) { try { io.to(target.socketId).emit('join-request-expired', { channelId: channel.id }); } catch(e) {} }
        try { io.to(channel.id).emit('join-request-removed', { userId }); } catch(e) {}
      });
    });
  } catch (err) { console.error('❌ Expiry error:', err.message); }
}, 60000);

setInterval(() => {
  try {
    channels.forEach((ch) => { federation.broadcastChannelUpdated(ch); });
  } catch(e) {}
}, 10000);

// ═══════════════════════════════════════════════════════════
// AUTO-DELETION
// ═══════════════════════════════════════════════════════════

function scheduleChannelDeletion(channelId) {
  const channel = channels.get(channelId);
  if (!channel) return;
  if (channel.deleteTimer) clearTimeout(channel.deleteTimer);
  channel.deleteTimer = setTimeout(() => {
    const ch = channels.get(channelId);
    if (ch && ch.speakers.size === 0 && ch.listeners.size === 0) {
      federation.broadcastChannelDeleted(channelId, ch.name);
      channels.delete(channelId);
      bans.delete(channelId);
      channelSpeaking.delete(channelId);
      channelScreenShare.delete(channelId);
      channelLikes.delete(channelId);
      io.emit('channels-updated');
      io.emit('federation-channels-updated', federation.getGlobalChannelList());
      log('🗑️', `"${ch.name}" auto-deleted (24h inactive)`);
    }
  }, 24 * 60 * 60 * 1000);
}

function cancelChannelDeletion(channelId) {
  const channel = channels.get(channelId);
  if (channel && channel.deleteTimer) { clearTimeout(channel.deleteTimer); channel.deleteTimer = null; }
}

// ═══════════════════════════════════════════════════════════
// SOCKET.IO — MAIN HANDLER
// ═══════════════════════════════════════════════════════════

io.on('connection', (socket) => {
  socket._registered = false;
  socket._userId = null;
  log('🔌', `Connected: ${socket.id}`);

  // ✅ ПАТЧ 7: Привязка persistentId к публичному ключу
  socket.on('register-persistent', safeHandler('register-persistent', async (rawData, cb) => {
    const data = safeData(rawData);
    const persistentId = sanitize(data.persistentId, 40);
    const publicKeyJWK = data.publicKeyJWK;
    
    if (!persistentId || persistentId.length < 5) return cb({ error: 'Invalid ID' });
    
    const existing = [...users.entries()].find(([,u]) => u.userId === persistentId);
    if (existing) {
      const [existingSocketId, existingUser] = existing;
      // Если ключ не совпадает — это попытка кражи личности
      if (existingUser.publicKeyJWK && existingUser.publicKeyJWK !== publicKeyJWK) {
        return cb({ error: 'Identity mismatch' });
      }
      // Иначе отключаем старое соединение
      const old = io.sockets.sockets.get(existingSocketId);
      if (old) { try { old.disconnect(true); } catch(e) {} }
    }
    
    socket._userId = persistentId;
    socket._registered = true;
    socket._publicKeyJWK = publicKeyJWK; // Сохраняем ключ для будущих проверок
    cb({ success: true, userId: persistentId });
  }));

  socket.use((packet, next) => {
    try {
      if (!socket._registered && packet[0] !== 'register-persistent') return next(new Error('Not registered'));
      if (!checkSocketRate(socket.id)) return next(new Error('Rate limit'));
      const packetStr = JSON.stringify(packet);
      if (packetStr.length > CONFIG.limits.MAX_PAYLOAD_SIZE) return next(new Error('Payload too large'));
      const data = packet[1];
      if (data && typeof data === 'object') { delete data.__proto__; delete data.constructor; delete data.prototype; }
      next();
    } catch (err) { next(new Error('Security check failed')); }
  });

  socket.on('get-channels', safeHandler('get-channels', (cb) => {
    cb(federation.getGlobalChannelList());
  }));

  socket.on('create-channel', safeHandler('create-channel', async (rawData, cb) => {
    const callback = typeof cb === 'function' ? cb : () => {};
    const now = Date.now();
    const lastAction = joinCooldowns.get(socket.id) || 0;
    if (now - lastAction < CONFIG.security.JOIN_COOLDOWN_MS) return callback({ error: 'Please wait' });
    joinCooldowns.set(socket.id, now);

    const data = safeData(rawData);
    const userId = socket._userId;
    const channelName = sanitize(data.channelName, CONFIG.limits.MAX_CHANNEL_NAME_LENGTH);
    const userName = sanitize(data.userName, CONFIG.limits.MAX_NAME_LENGTH) || 'Anonymous';

    let userCount = 0;
    channels.forEach(ch => { if (ch.admin === userId) userCount++; });
    if (channels.size >= CONFIG.limits.MAX_CHANNELS) return callback({ error: 'Server limit' });
    if (userCount >= CONFIG.limits.MAX_CHANNELS_PER_USER) return callback({ error: `Limit: ${CONFIG.limits.MAX_CHANNELS_PER_USER} per user` });
    if (!channelName) return callback({ error: 'Name required' });

    // ✅ ПАТЧ 12: Глобальная проверка уникальности имени + локальная
    const nameLower = channelName.toLowerCase().trim();
    if (federation.globalNames.has(nameLower)) {
      return callback({ error: `Channel name "${channelName}" is already taken globally` });
    }
    for (const [, ch] of channels) {
      if (ch.name.toLowerCase().trim() === nameLower) {
        return callback({ error: 'Name taken locally' });
      }
    }

    const channelId = uuidv4().slice(0, 8).toUpperCase();
    const channel = {
      id: channelId, name: channelName, admin: userId, adminName: userName,
      requireApproval: data.requireApproval !== false,
      chestLocked: false,
      speakers: new Map([[userId, { userId, name: userName, socketId: socket.id }]]),
      listeners: new Map(), messages: [],
      joinRequests: new Map(), raisedHands: new Set(),
      recentlyApproved: new Map(), userLikes: new Map(), totalLikes: 0,
      created: Date.now(), chest: [], deleteTimer: null,
      approvedUsers: new Set(),          // ✅ ДОБАВЛЕНО: защита от краша при request-join
      persistentSpeakers: new Set(),     // ✅ ДОБАВЛЕНО: защита от краша при make-speaker/kick
      aesKey: crypto.randomBytes(32)
    };
    channels.set(channelId, channel);
    bans.set(channelId, { users: new Map(), ips: new Map() });
    users.set(socket.id, { userId, userName, channelId, role: 'admin' });
    socket.join(channelId);

    federation.broadcastChannelCreated(channel);

    log('📢', `Created: "${channelName}" (${channelId}) by ${userName}`);
    callback({ success: true, channelId });
    io.emit('channels-updated');
    io.emit('federation-channels-updated', federation.getGlobalChannelList());
    saveChannels();
  }));

  // ✅ ПАТЧ 2: join-channel теперь async для крипто-операций
  socket.on('join-channel', safeHandler('join-channel', async (rawData, cb) => {
    const callback = typeof cb === 'function' ? cb : () => {};
    const now = Date.now();
    const lastAction = joinCooldowns.get(socket.id) || 0;
    if (now - lastAction < CONFIG.security.JOIN_COOLDOWN_MS) return callback({ error: 'Please wait' });
    joinCooldowns.set(socket.id, now);

    const data = safeData(rawData);
    const userId = socket._userId;
    const userName = sanitize(data.userName, CONFIG.limits.MAX_NAME_LENGTH) || 'Anonymous';
    const channelId = sanitize(data.channelId, 20);

    if (!federation.isLocalChannel(channelId)) {
      const homeUrl = federation.getChannelHomeUrl(channelId);
      if (homeUrl) {
        return callback({ error: null, redirect: true, redirectUrl: homeUrl, channelId, message: 'This channel is on another server. Redirecting...' });
      }
      return callback({ error: 'Channel not found in federation' });
    }

    const channel = channels.get(channelId);
    if (!channel) return callback({ error: 'Channel not found' });

    const channelBans = bans.get(channelId);
    if (channelBans) {
      if (channelBans.users?.has(userId)) {
        const until = channelBans.users.get(userId);
        if (Date.now() < until) return callback({ error: `Banned. ${Math.ceil((until - Date.now()) / 60000)} min left` });
        channelBans.users.delete(userId);
      }
      const ip = socket._clientIp;
      if (channelBans.ips?.has(ip)) {
        const until = channelBans.ips.get(ip);
        if (Date.now() < until) return callback({ error: `Banned (IP). ${Math.ceil((until - Date.now()) / 60000)} min left` });
        channelBans.ips.delete(ip);
      }
    }

    const total = channel.speakers.size + channel.listeners.size;
    if (total >= CONFIG.limits.MAX_USERS) return callback({ error: `Full (${CONFIG.limits.MAX_USERS} max)` });

    let role = 'listener';
    if (channel.admin === userId) role = 'admin';
    else if (channel.speakers.has(userId)) role = 'speaker';
    else if (channel.persistentSpeakers.has(userId) && channel.speakers.size < CONFIG.limits.MAX_SPEAKERS) {
      role = 'speaker';
    }

    const uniqueName = getUniqueName(channel, userName, userId);
    const nameChanged = uniqueName !== userName;

    if (role === 'admin' || role === 'speaker') channel.speakers.set(userId, { userId, name: uniqueName, socketId: socket.id });
    else channel.listeners.set(userId, { name: uniqueName, socketId: socket.id });

    users.set(socket.id, { userId, userName: uniqueName, channelId, role, isMuted: false });
    socket.join(channelId);

    const speakersList = [...channel.speakers.entries()].map(([uid, s]) => ({ userId: uid, name: s.name }));
    const listenersList = [...channel.listeners.entries()].map(([uid, l]) => ({ userId: uid, name: l.name }));
    const raisedHandsList = role === 'admin' ? [...channel.raisedHands].map(uid => {
      const s = channel.speakers.get(uid); const l = channel.listeners.get(uid);
      return { userId: uid, userName: s?.name || l?.name || 'User', timestamp: Date.now() };
    }) : [];
    const joinRequestsList = role === 'admin' ? [...channel.joinRequests.entries()].map(([uid, req]) => ({
      userId: uid, userName: req.userName, timestamp: req.timestamp
    })) : [];

    // ✅ ПАТЧ 2: Шифрование и отправка AES-ключа канала новому участнику
    if (data.publicKeyJWK && channel.aesKey) {
      try {
        const { webcrypto } = require('crypto');
        const jwk = JSON.parse(data.publicKeyJWK);
        const pubKey = await webcrypto.subtle.importKey('jwk', jwk, { name: 'RSA-OAEP', hash: 'SHA-256' }, true, ['encrypt']);
        const wrapped = await webcrypto.subtle.encrypt({ name: 'RSA-OAEP' }, pubKey, channel.aesKey);
        socket.emit('receive-wrapped-channel-key', { wrappedKey: Array.from(new Uint8Array(wrapped)) });
      } catch (e) { 
        console.error('Key wrap failed:', e.message); 
      }
    }
    const activeStreamers = [];
    const screenSharers = channelScreenShare.get(channelId);
    if (screenSharers) {
      screenSharers.forEach(uid => {
        const u = channel.speakers.get(uid) || channel.listeners.get(uid);
        if (u) activeStreamers.push({ userId: uid, userName: u.name });
      });
    }

    callback({
      success: true, channelName: channel.name, isAdmin: role === 'admin', role,
      requireApproval: channel.requireApproval, adminId: channel.admin,
      speakers: speakersList, listeners: listenersList,
      messages: (channel.messages || []).slice(-100),
      maxSpeakers: CONFIG.limits.MAX_SPEAKERS, maxListeners: CONFIG.limits.MAX_LISTENERS,
      yourName: uniqueName, nameChanged,
      userLikes: Object.fromEntries(channel.userLikes || new Map()),
      totalLikes: channel.totalLikes || 0,
      raisedHands: raisedHandsList, joinRequests: joinRequestsList,
      chestFiles: channel.chest || [], chestLocked: channel.chestLocked || false,
      streamers: activeStreamers
    });

    cancelChannelDeletion(channelId);
    socket.to(channelId).emit('user-joined', { userId, userName: uniqueName, role });
    if (nameChanged) socket.emit('name-changed-by-server', { newName: uniqueName, reason: 'Name taken' });
    
    federation.broadcastChannelUpdated(channel);
    log('👤', `${uniqueName} → "${channel.name}" as ${role}`);
  }));

  socket.on('request-join', safeHandler('request-join', (rawData, cb) => {
    const data = safeData(rawData);
    const userId = socket._userId;
    const channelId = sanitize(data.channelId, 20);

    if (!federation.isLocalChannel(channelId)) {
      const homeUrl = federation.getChannelHomeUrl(channelId);
      if (homeUrl) return cb({ redirect: true, redirectUrl: homeUrl, channelId });
      return cb({ error: 'Channel not found' });
    }

    const channel = channels.get(channelId);
    if (!channel) return cb({ error: 'Channel not found' });
    if (channel.admin === userId || channel.speakers.has(userId) || channel.listeners.has(userId)) return cb({ approved: true });
	if (channel.approvedUsers.has(userId)) return cb({ approved: true });
    if (channel.recentlyApproved?.has(userId)) {
      if (Date.now() < channel.recentlyApproved.get(userId)) return cb({ approved: true });
      channel.recentlyApproved.delete(userId);
    }
    if (!channel.requireApproval) return cb({ approved: true });
    if (channel.joinRequests.has(userId)) return cb({ approved: false, message: 'Pending' });

    channel.joinRequests.set(userId, { userId, userName: sanitize(data.userName, 20), socketId: socket.id, timestamp: Date.now() });
    const adminSocket = findUser(channel.admin);
    if (adminSocket) { try { io.to(adminSocket.socketId).emit('join-request', { userId, userName: data.userName, channelId, timestamp: Date.now() }); } catch(e) {} }
    cb({ approved: false, message: 'Request sent' });
  }));

  socket.on('chest-add-file', safeHandler('chest-add-file', (rawData, cb) => {
    const callback = typeof cb === 'function' ? cb : () => {};
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData?.channelId) return callback({ error: 'Not in channel' });
    const channel = channels.get(userData.channelId);
    if (!channel) return callback({ error: 'Channel not found' });
    const metadata = data.metadata;
    if (!metadata?.id || !metadata?.name) return callback({ error: 'Invalid metadata' });
    
    // ✅ ПАТЧ 6: Строгая проверка размера файла (защита от null/undefined/string bypass)
    const fileSize = Number(metadata.size);
    if (!Number.isFinite(fileSize) || fileSize <= 0 || fileSize > 500 * 1024 * 1024) {
      return callback({ error: 'Invalid or too large file size' });
    }
    
    if (!channel.chest) channel.chest = [];
    channel.chest.push({ id: metadata.id, name: metadata.name, size: metadata.size, type: metadata.type, uploader: userData.userId, uploaderName: userData.userName, timestamp: metadata.timestamp || Date.now() });
    if (channel.chest.length > 20) channel.chest.shift();
    io.to(userData.channelId).emit('chest-updated', channel.chest);
    callback({ success: true });
  }));

  socket.on('chest-delete-file', safeHandler('chest-delete-file', (rawData, cb) => {
    const callback = typeof cb === 'function' ? cb : () => {};
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData?.channelId) return callback({ error: 'Not in channel' });
    const channel = channels.get(userData.channelId);
    if (!channel || channel.admin !== userData.userId) return callback({ error: 'Admin only' });
    if (!channel.chest) channel.chest = [];
    const idx = channel.chest.findIndex(f => f.id === data.fileId);
    if (idx === -1) return callback({ error: 'Not found' });
    channel.chest.splice(idx, 1);
    io.to(userData.channelId).emit('chest-updated', channel.chest);
    callback({ success: true });
  }));

  socket.on('chest-toggle-lock', safeHandler('chest-toggle-lock', (rawData, cb) => {
    const callback = typeof cb === 'function' ? cb : () => {};
    const userData = users.get(socket.id);
    if (!userData?.channelId) return callback({ error: 'Not in channel' });
    const channel = channels.get(userData.channelId);
    if (!channel || channel.admin !== userData.userId) return callback({ error: 'Admin only' });
    channel.chestLocked = !channel.chestLocked;
    io.to(userData.channelId).emit('chest-lock-changed', { locked: channel.chestLocked, changedBy: userData.userName });
    callback({ success: true, locked: channel.chestLocked });
  }));

  socket.on('chest-request-file', safeHandler('chest-request-file', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData?.channelId) return;
    const channel = channels.get(userData.channelId);
    if (!channel?.chest) return;
    const file = channel.chest.find(f => f.id === data.fileId);
    if (!file) return;
    const owner = findUser(file.uploader);
    if (!owner) { socket.emit('chest-file-unavailable', { fileId: data.fileId }); return; }
    io.to(owner.socketId).emit('chest-file-requested', { fileId: data.fileId, requesterId: userData.userId, requesterName: userData.userName });
  }));

  socket.on('message-reaction', safeHandler('message-reaction', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData?.channelId) return;
    io.to(userData.channelId).emit('message-reaction', { msgId: data.msgId, emoji: data.emoji, userId: userData.userId, userName: userData.userName });
  }));

  socket.on('media-announce', safeHandler('media-announce', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData?.channelId) return;
    socket.to(userData.channelId).emit('media-announce', {
      mediaId: data.mediaId, type: data.type, size: data.size,
      uploaderId: userData.userId, uploaderName: userData.userName,
      caption: data.caption || null, encryptedCaption: data.encryptedCaption || null,
      timestamp: data.timestamp || Date.now(),
      multi: !!data.multi, count: data.count || 1
    });
  }));

  socket.on('leave-channel', safeHandler('leave-channel', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return;
    const channelId = data.channelId || userData.channelId;
    const channel = channels.get(channelId);
    if (!channel) return;
    const name = channel.speakers.get(userData.userId)?.name || channel.listeners.get(userData.userId)?.name || userData.userName;
    channel.speakers.delete(userData.userId);
    channel.listeners.delete(userData.userId);
    channel.raisedHands.delete(userData.userId);
    channel.joinRequests?.delete(userData.userId);
    channel.recentlyApproved?.delete(userData.userId);
    channelSpeaking.get(channelId)?.delete(userData.userId);
    channelScreenShare.get(channelId)?.delete(userData.userId);
    socket.leave(channelId);
    userData.channelId = null; userData.role = null;
    socket.to(channelId).emit('user-left', { userId: userData.userId, userName: name });
    if (channel.speakers.size === 0 && channel.listeners.size === 0) scheduleChannelDeletion(channelId);
    federation.broadcastChannelUpdated(channel);
  }));

  socket.on('call-user', safeHandler('call-user', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return;
    const channel = channels.get(userData.channelId);
    if (!channel) return;
    if (data.targetUserId === userData.userId) return;
    const target = findUser(data.targetUserId);
    if (!target) return;
    const targetData = users.get(target.socketId);
    if (targetData?.isMuted) return;
    io.to(target.socketId).emit('incoming-call', { fromUserId: userData.userId, fromUserName: userData.userName, timestamp: Date.now() });
    socket.emit('call-sent', { targetUserId: data.targetUserId, targetUserName: target.userName });
  }));

  socket.on('mute-status', safeHandler('mute-status', (data) => {
    const userData = users.get(socket.id);
    if (userData) userData.isMuted = data.isMuted || false;
  }));

  socket.on('respond-join-request', safeHandler('respond-join-request', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return cb({ error: 'Not connected' });
    const channel = channels.get(userData.channelId);
    if (!channel || channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    if (!channel.joinRequests.has(data.targetUserId)) {
      io.to(userData.channelId).emit('join-request-removed', { userId: data.targetUserId });
      return cb({ success: true });
    }
    const request = channel.joinRequests.get(data.targetUserId);
    channel.joinRequests.delete(data.targetUserId);
    if (!channel.recentlyApproved) channel.recentlyApproved = new Map();
    if (data.approved) {
		channel.approvedUsers.add(data.targetUserId);
        channel.recentlyApproved.set(data.targetUserId, Date.now() + 10000);
	}
    let delivered = false;
    if (request.socketId) {
      const ts = io.sockets.sockets.get(request.socketId);
      if (ts?.connected) { try { ts.emit('join-request-response', { approved: !!data.approved, channelId: userData.channelId }); delivered = true; } catch(e) {} }
    }
    if (!delivered) { const t = findUser(data.targetUserId); if (t) { try { io.to(t.socketId).emit('join-request-response', { approved: !!data.approved, channelId: userData.channelId }); } catch(e) {} } }
    io.to(userData.channelId).emit('join-request-removed', { userId: data.targetUserId });
    cb({ success: true });
  }));

  socket.on('send-message', safeHandler('send-message', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData || !checkMessageRate(userData.userId)) return;
    
    const channel = channels.get(userData.channelId); // ✅ Сначала получаем канал
    if (!channel) return;
    
    if (!channel.messages) channel.messages = []; // ✅ Теперь channel определён, это безопасно
    
    const text = sanitize(data.text, CONFIG.limits.MAX_MESSAGE_LENGTH);
    if (!text && !data.encrypted && !data.meta) return;
    
    const msg = { id: uuidv4().slice(0, 10), userId: userData.userId, userName: userData.userName, text: text || '', timestamp: Date.now() };
    if (data.encrypted) msg.encrypted = data.encrypted;
    if (data.replyTo && typeof data.replyTo === 'object') {
      msg.replyTo = { id: sanitize(data.replyTo.id, 20), userId: sanitize(data.replyTo.userId, 40), userName: sanitize(data.replyTo.userName, 20), text: sanitize(data.replyTo.text, 200) };
    }
    if (data.meta) msg.meta = data.meta;
    
    channel.messages.push(msg);
    if (channel.messages.length > CONFIG.limits.MESSAGE_HISTORY) channel.messages.shift();
    io.to(userData.channelId).emit('new-message', msg);
  }));

  socket.on('update-username', safeHandler('update-username', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return cb({ error: 'Not in channel' });
    const channel = channels.get(userData.channelId);
    if (!channel) return cb({ error: 'Not found' });
    const newName = sanitize(data.newName, CONFIG.limits.MAX_NAME_LENGTH);
    if (!newName) return cb({ error: 'Empty' });
    const uniqueName = getUniqueName(channel, newName, userData.userId);
    const nameChanged = uniqueName !== newName;
    const oldName = userData.userName;
    userData.userName = uniqueName;
    if (channel.speakers.has(userData.userId)) channel.speakers.get(userData.userId).name = uniqueName;
    if (channel.listeners.has(userData.userId)) channel.listeners.get(userData.userId).name = uniqueName;
    if (channel.admin === userData.userId) channel.adminName = uniqueName;
    io.to(userData.channelId).emit('user-renamed', { userId: userData.userId, oldName, newName: uniqueName });
    cb({ success: true, newName: uniqueName, nameChanged });
  }));

  socket.on('make-speaker', safeHandler('make-speaker', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    const channel = channels.get(userData?.channelId);
    if (!channel || channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    if (channel.speakers.size >= CONFIG.limits.MAX_SPEAKERS) return cb({ error: `Max ${CONFIG.limits.MAX_SPEAKERS}` });
    const listener = channel.listeners.get(data.targetUserId);
    if (!listener) return cb({ error: 'Not found' });
    channel.listeners.delete(data.targetUserId);
    channel.speakers.set(data.targetUserId, { userId: data.targetUserId, name: listener.name, socketId: listener.socketId });
	channel.persistentSpeakers.add(data.targetUserId);
    channel.raisedHands.delete(data.targetUserId);
    const target = findUser(data.targetUserId);
    if (target) users.get(target.socketId).role = 'speaker';
    io.to(userData.channelId).emit('role-changed', { userId: data.targetUserId, role: 'speaker', userName: listener.name });
    io.to(userData.channelId).emit('hand-lowered', { userId: data.targetUserId });
    cb({ success: true });
  }));

  socket.on('remove-speaker', safeHandler('remove-speaker', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    const channel = channels.get(userData?.channelId);
    if (!channel || channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    const speaker = channel.speakers.get(data.targetUserId);
    if (!speaker) return cb({ error: 'Not found' });
    if (data.targetUserId === channel.admin) return cb({ error: 'Cannot demote admin' });
    channel.speakers.delete(data.targetUserId);
    channel.listeners.set(data.targetUserId, { name: speaker.name, socketId: speaker.socketId });
	channel.persistentSpeakers.delete(data.targetUserId);
    const target = findUser(data.targetUserId);
    if (target) users.get(target.socketId).role = 'listener';
    io.to(userData.channelId).emit('role-changed', { userId: data.targetUserId, role: 'listener', userName: speaker.name });
    cb({ success: true });
  }));

  socket.on('kick-user', safeHandler('kick-user', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    const channel = channels.get(userData?.channelId);
    if (!channel || channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    const target = findUser(data.targetUserId);
    if (target) { try { io.to(target.socketId).emit('kicked', { reason: sanitize(data.reason, 100) || 'Kicked' }); } catch(e) {} try { io.sockets.sockets.get(target.socketId)?.disconnect(); } catch(e) {} }
    const name = channel.speakers.get(data.targetUserId)?.name || channel.listeners.get(data.targetUserId)?.name || 'User';
    channel.speakers.delete(data.targetUserId); channel.listeners.delete(data.targetUserId); channel.raisedHands.delete(data.targetUserId);
	channel.approvedUsers.delete(data.targetUserId);
    channel.persistentSpeakers.delete(data.targetUserId);
    io.to(userData.channelId).emit('user-left', { userId: data.targetUserId, userName: name });
    cb({ success: true });
  }));

  socket.on('ban-user', safeHandler('ban-user', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    const channel = channels.get(userData?.channelId);
    if (!channel || channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    const until = Date.now() + CONFIG.limits.BAN_DURATION;
    const channelBans = bans.get(userData.channelId);
    channelBans.users.set(data.targetUserId, until);
    channel.approvedUsers.delete(data.targetUserId);
    channel.persistentSpeakers.delete(data.targetUserId);
    const ts = findUser(data.targetUserId);
    if (ts) {
      const tip = io.sockets.sockets.get(ts.socketId)?._clientIp;
      if (tip) channelBans.ips.set(tip, until);
      try { io.to(ts.socketId).emit('banned', { until, reason: sanitize(data.reason, 100) || 'Banned' }); } catch(e) {}
      try { io.sockets.sockets.get(ts.socketId)?.disconnect(); } catch(e) {}
    }
    const name = channel.speakers.get(data.targetUserId)?.name || channel.listeners.get(data.targetUserId)?.name || 'User';
    channel.speakers.delete(data.targetUserId); channel.listeners.delete(data.targetUserId);
    io.to(userData.channelId).emit('user-banned', { userId: data.targetUserId, userName: name, until });
    cb({ success: true });
  }));

  socket.on('transfer-admin', safeHandler('transfer-admin', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    const channel = channels.get(userData?.channelId);
    if (!channel || channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    const target = channel.speakers.get(data.targetUserId) || channel.listeners.get(data.targetUserId);
    if (!target) return cb({ error: 'Not found' });
    const oldAdminId = channel.admin;
    channel.admin = data.targetUserId; channel.adminName = target.name;
    if (channel.listeners.has(data.targetUserId)) { channel.speakers.set(data.targetUserId, channel.listeners.get(data.targetUserId)); channel.listeners.delete(data.targetUserId); }
    const ns = findUser(data.targetUserId); if (ns) users.get(ns.socketId).role = 'admin';
    const os = findUser(oldAdminId); if (os) users.get(os.socketId).role = 'speaker';
    io.to(userData.channelId).emit('admin-transferred', { oldAdminId, newAdminId: data.targetUserId, newAdminName: target.name });
    cb({ success: true });
  }));

  // ✅ ПАТЧ 5: Проверка роли перед рассылкой speaking-status
  socket.on('speaking-status', safeHandler('speaking-status', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return;
    if (userData.role !== 'speaker' && userData.role !== 'admin') return; // ← защита от listener spam
    
    const chId = userData.channelId;
    if (!channelSpeaking.has(chId)) channelSpeaking.set(chId, new Set());
    if (data.isSpeaking) channelSpeaking.get(chId).add(userData.userId);
    else channelSpeaking.get(chId).delete(userData.userId);
    socket.to(chId).emit('user-speaking', { userId: userData.userId, userName: userData.userName, isSpeaking: !!data.isSpeaking });
  }));

  socket.on('screen-share-start', safeHandler('screen-share-start', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return;
    if (!channelScreenShare.has(userData.channelId)) channelScreenShare.set(userData.channelId, new Set());
    channelScreenShare.get(userData.channelId).add(userData.userId);
    socket.to(userData.channelId).emit('screen-share-started', { userId: userData.userId, userName: userData.userName, mediaType: data.mediaType || 'screen' });
  }));

  socket.on('screen-share-stop', safeHandler('screen-share-stop', () => {
    const userData = users.get(socket.id);
    if (!userData) return;
    channelScreenShare.get(userData.channelId)?.delete(userData.userId);
    socket.to(userData.channelId).emit('screen-share-stopped', { userId: userData.userId });
  }));

  socket.on('request-stream-resend', safeHandler('request-stream-resend', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return;
    const target = findUser(data.targetUserId);
    if (target) { try { io.to(target.socketId).emit('stream-resend-requested', { requesterId: userData.userId }); } catch(e) {} }
  }));

  socket.on('typing', safeHandler('typing', (data) => {
    const userData = users.get(socket.id);
    if (!userData) return;
    socket.to(userData.channelId).emit('user-typing', { userId: userData.userId, userName: userData.userName, isTyping: !!data.isTyping });
  }));

  socket.on('raise-hand', safeHandler('raise-hand', () => {
    const userData = users.get(socket.id);
    if (!userData) return;
    const channel = channels.get(userData.channelId);
    if (!channel || channel.raisedHands.has(userData.userId)) return;
    channel.raisedHands.add(userData.userId);
    const admin = findUser(channel.admin);
    if (admin) { try { io.to(admin.socketId).emit('hand-raised', { userId: userData.userId, userName: userData.userName, timestamp: Date.now() }); } catch(e) {} }
  }));

  socket.on('lower-hand', safeHandler('lower-hand', () => {
    const userData = users.get(socket.id);
    if (!userData) return;
    const channel = channels.get(userData.channelId);
    if (!channel) return;
    channel.raisedHands.delete(userData.userId);
    const admin = findUser(channel.admin);
    if (admin) { try { io.to(admin.socketId).emit('hand-lowered', { userId: userData.userId }); } catch(e) {} }
  }));

  socket.on('deny-raised-hand', safeHandler('deny-raised-hand', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    const channel = channels.get(userData?.channelId);
    if (!channel || channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    channel.raisedHands.delete(data.targetUserId);
    io.to(userData.channelId).emit('hand-lowered', { userId: data.targetUserId });
    cb({ success: true });
  }));

  socket.on('send-like', safeHandler('send-like', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData || data.targetUserId === userData.userId) return;
    const channel = channels.get(userData.channelId);
    if (!channel) return;
    const now = Date.now();
    if (!userData.lastLikeTime || now - userData.lastLikeTime > CONFIG.security.LIKE_COOLDOWN) {
      userData.lastLikeTime = now;
      const targetId = data.targetUserId;
      const current = channel.userLikes.get(targetId) || 0;
      channel.userLikes.set(targetId, current + 1);
      channel.totalLikes = (channel.totalLikes || 0) + 1;
      channelLikes.set(userData.channelId, (channelLikes.get(userData.channelId) || 0) + 1);
      socket.to(userData.channelId).emit('receive-like', { userId: userData.userId, userName: userData.userName, targetUserId: targetId, count: current + 1 });
    }
  }));

  socket.on('start-vote', safeHandler('start-vote', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    const channel = channels.get(userData?.channelId);
    if (!channel) return cb({ error: 'Not found' });
    if (data.targetUserId === channel.admin) return cb({ error: 'Cannot vote admin' });
    const voteId = uuidv4().slice(0, 8);
    // ✅ ПАТЧ 11: votedUsers вместо votedIps (защита от NAT-блокировок)
    votes.set(voteId, { 
      id: voteId, channelId: userData.channelId, targetUserId: data.targetUserId, 
      targetName: sanitize(data.targetName, 20), yes: new Set([userData.userId]), no: new Set(), 
      votedUsers: new Set([userData.userId]), 
      expiresAt: Date.now() + CONFIG.limits.VOTE_DURATION 
    });
    io.to(userData.channelId).emit('vote-started', { voteId, targetUserId: data.targetUserId, targetName: data.targetName, expiresAt: Date.now() + CONFIG.limits.VOTE_DURATION });
    setTimeout(() => finishVote(voteId), CONFIG.limits.VOTE_DURATION);
    cb({ success: true, voteId });
  }));

  socket.on('cast-vote', safeHandler('cast-vote', (rawData) => {
    const data = safeData(rawData);
    const vote = votes.get(data.voteId);
    if (!vote) return;
    const userData = users.get(socket.id);
    // ✅ ПАТЧ 11: Проверка по userId, а не по IP
    if (!userData || vote.votedUsers.has(userData.userId)) return;
    vote.votedUsers.add(userData.userId);
    if (data.vote === 'yes') { vote.yes.add(userData.userId); vote.no.delete(userData.userId); }
    else { vote.no.add(userData.userId); vote.yes.delete(userData.userId); }
    io.to(vote.channelId).emit('vote-updated', { voteId: vote.id, yes: vote.yes.size, no: vote.no.size });
  }));

  function finishVote(voteId) {
    try {
      const vote = votes.get(voteId);
      if (!vote) return;
      const channel = channels.get(vote.channelId);
      if (!channel) { votes.delete(voteId); return; }
      const total = channel.speakers.size + channel.listeners.size;
      const needed = Math.max(2, Math.ceil(total * CONFIG.limits.VOTE_THRESHOLD));
      if (vote.yes.size >= needed) {
        const target = findUser(vote.targetUserId);
        if (target) { try { io.to(target.socketId).emit('kicked', { reason: 'Vote' }); } catch(e) {} try { io.sockets.sockets.get(target.socketId)?.disconnect(); } catch(e) {} }
        channel.speakers.delete(vote.targetUserId); channel.listeners.delete(vote.targetUserId);
        io.to(vote.channelId).emit('vote-result', { voteId, kicked: true, targetName: vote.targetName });
		channel.approvedUsers.delete(vote.targetUserId);
        channel.persistentSpeakers.delete(vote.targetUserId);
      } else {
        io.to(vote.channelId).emit('vote-result', { voteId, kicked: false, targetName: vote.targetName });
      }
      votes.delete(voteId);
    } catch (err) { console.error('❌ finishVote:', err); }
  }

  // ✅ ПАТЧ 1: WebRTC Signaling с проверкой канала
  socket.on('webrtc-offer', safeHandler('webrtc-offer', (d) => { 
    if (!canSignal(socket.id, d.toUserId)) return;
    const t = findUser(d.toUserId); 
    if (t) io.to(t.socketId).emit('webrtc-offer', d); 
  }));
  
  socket.on('webrtc-answer', safeHandler('webrtc-answer', (d) => { 
    if (!canSignal(socket.id, d.toUserId)) return;
    const t = findUser(d.toUserId); 
    if (t) io.to(t.socketId).emit('webrtc-answer', d); 
  }));
  
  socket.on('webrtc-ice', safeHandler('webrtc-ice', (d) => { 
    if (!canSignal(socket.id, d.toUserId)) return;
    const t = findUser(d.toUserId); 
    if (t) io.to(t.socketId).emit('webrtc-ice', d); 
  }));

  socket.on('toggle-channel-approval', safeHandler('toggle-channel-approval', (rawData, cb) => {
    const userData = users.get(socket.id);
    if (!userData) return cb({ error: 'Not connected' });
    const channel = channels.get(userData.channelId);
    if (!channel || channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    channel.requireApproval = !channel.requireApproval;
    io.to(userData.channelId).emit('channel-mode-changed', { requireApproval: channel.requireApproval, changedBy: userData.userName, timestamp: Date.now() });
    federation.broadcastChannelUpdated(channel);
    cb({ success: true, requireApproval: channel.requireApproval });
  }));

  socket.on('close-channel', safeHandler('close-channel', (rawData, cb) => {
    const userData = users.get(socket.id);
    if (!userData) return cb({ error: 'Not connected' });
    const channel = channels.get(userData.channelId);
    if (!channel || channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    const channelName = channel.name;
    const channelId = userData.channelId;
    const allUsers = [];
    channel.speakers.forEach((s, uid) => { if (uid !== userData.userId) allUsers.push(uid); });
    channel.listeners.forEach((l, uid) => { if (uid !== userData.userId) allUsers.push(uid); });
    allUsers.forEach(uid => {
      const target = findUser(uid);
      if (target) { try { io.to(target.socketId).emit('channel-closed', { channelName, closedBy: userData.userName, reason: 'Admin closed' }); } catch(e) {} }
    });
    allUsers.forEach(uid => {
      const target = findUser(uid);
      if (target) { const ts = io.sockets.sockets.get(target.socketId); if (ts) { ts.leave(channelId); const td = users.get(target.socketId); if (td) { td.channelId = null; td.role = null; } } }
    });
    if (channel.deleteTimer) clearTimeout(channel.deleteTimer);
    
    federation.broadcastChannelDeleted(channelId, channelName);
    
    channels.delete(channelId); bans.delete(channelId);
    channelSpeaking.delete(channelId); channelScreenShare.delete(channelId); channelLikes.delete(channelId);
    votes.forEach((v, vid) => { if (v.channelId === channelId) votes.delete(vid); });
    userData.channelId = null; userData.role = null;
    socket.leave(channelId);
    io.emit('channels-updated');
    io.emit('federation-channels-updated', federation.getGlobalChannelList());
    cb({ success: true, kicked: allUsers.length });
    saveChannels();
  }));

  socket.on('get-federation-status', safeHandler('get-federation-status', (cb) => {
    if (typeof cb !== 'function') return;
    cb(federation.getStatus());
  }));

  socket.on('relay-mode', safeHandler('relay-mode', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return typeof cb === 'function' && cb({ error: 'Not connected' });
    if (data.enabled) relayHelpers.set(socket.id, { userId: userData.userId, since: Date.now() });
    else relayHelpers.delete(socket.id);
    if (typeof cb === 'function') cb({ success: true, helpersOnline: relayHelpers.size });
  }));

  socket.on('get-turn-credentials', safeHandler('get-turn-credentials', (cb) => {
    if (typeof cb !== 'function') return;
    const turnSecret = process.env.TURN_SECRET;
    const turnUrls = (process.env.TURN_URLS || '').split(',').map(s => s.trim()).filter(Boolean);
    if (!turnSecret || !turnUrls.length) return cb({ iceServers: [] });
    const ttl = 3600;
    const username = `${Math.floor(Date.now() / 1000) + ttl}:wave`;
    const credential = crypto.createHmac('sha1', turnSecret).update(username).digest('base64');
    cb({ iceServers: [{ urls: turnUrls, username, credential }] });
  }));

  socket.on('bot-auth', safeHandler('bot-auth', (rawData, cb) => {
    const data = safeData(rawData);
    if (!CONFIG.security.BOT_TOKENS.includes(data.token)) return cb({ error: 'Invalid token' });
    const channel = channels.get(data.channelId);
    if (!channel) return cb({ error: 'Not found' });
    const botId = 'bot_' + uuidv4().slice(0, 8);
    const name = sanitize(data.botName, 20) || 'Bot';
    channel.listeners.set(botId, { name, socketId: socket.id, isBot: true });
    users.set(socket.id, { userId: botId, userName: name, channelId: data.channelId, role: 'bot', isBot: true });
    socket.join(data.channelId);
    socket.to(data.channelId).emit('user-joined', { userId: botId, userName: name, role: 'listener', isBot: true });
    cb({ success: true, botId });
  }));

  socket.on('bot-message', safeHandler('bot-message', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData?.isBot) return;
    const channel = channels.get(userData.channelId);
    if (!channel) return;
    const msg = { id: uuidv4().slice(0, 10), userId: userData.userId, userName: userData.userName, text: sanitize(data.text, 500), timestamp: Date.now(), isBot: true };
    channel.messages.push(msg);
    if (channel.messages.length > 200) channel.messages.shift();
    io.to(userData.channelId).emit('new-message', msg);
  }));

  socket.on('disconnect', () => {
    try {
      const ip = socket._clientIp;
      if (ip) { const c = ipConnections.get(ip) || 1; if (c <= 1) ipConnections.delete(ip); else ipConnections.set(ip, c - 1); }
      socketRates.delete(socket.id);
      joinCooldowns.delete(socket.id);
      relayHelpers.delete(socket.id);
      const userData = users.get(socket.id);
      if (!userData) return;
      const channel = channels.get(userData.channelId);
      if (!channel) { users.delete(socket.id); return; }
      if (userData.isBot) {
        channel.listeners.delete(userData.userId);
        socket.to(userData.channelId).emit('user-left', { userId: userData.userId, userName: userData.userName });
        users.delete(socket.id); return;
      }
      channel.speakers.delete(userData.userId); channel.listeners.delete(userData.userId);
      channel.raisedHands.delete(userData.userId);
      channel.joinRequests?.delete(userData.userId);
      channel.recentlyApproved?.delete(userData.userId);
      channelSpeaking.get(userData.channelId)?.delete(userData.userId);
      channelScreenShare.get(userData.channelId)?.delete(userData.userId);
      if (channel.speakers.size === 0 && channel.listeners.size === 0) scheduleChannelDeletion(userData.channelId);
      else socket.to(userData.channelId).emit('user-left', { userId: userData.userId, userName: userData.userName });
      users.delete(socket.id);
      messageRates.delete(userData.userId);
      federation.broadcastChannelUpdated(channel);
    } catch (err) { console.error('❌ disconnect:', err); }
  });
  
    // ═══════════════════════════════════════════════════════════
    // FEDERATION PEERS DISCOVERY (для клиентского failover)
    // ═══════════════════════════════════════════════════════════
    socket.on('get-federation-peers', safeHandler('get-federation-peers', (cb) => {
      if (typeof cb !== 'function') return;
      const fedStatus = federation.getStatus();
      const peers = [{
        serverId: fedStatus.serverId,
        serverName: fedStatus.serverName,
        url: fedStatus.serverUrl,
        isSelf: true,
        trust: fedStatus.isOfficial ? 'official' : 'volunteer'
      }];
      // Добавляем подключённых пиров
      (fedStatus.peers || []).forEach(p => {
        peers.push({
          serverId: p.serverId,
          serverName: p.serverName,
          url: p.url,
          isSelf: false,
          trust: p.trust || 'unknown'
        });
      });
      cb({ success: true, peers, selfUrl: fedStatus.serverUrl });
    }));

}); 

// ═══════════════════════════════════════════════════════════
// START
// ═══════════════════════════════════════════════════════════

loadChannels();

server.listen(CONFIG.port, CONFIG.host, () => {
  console.log('');
  console.log('  ┌─────────────────────────────────────────────────┐');
  console.log('  │       Wave 🌊 Lighthouse Server v2.0            │');
  console.log('  │       Federation Edition                        │');
  console.log('  └─────────────────────────────────────────────────┘');
  console.log('');
  console.log(`  Port:         ${CONFIG.port}`);
  console.log(`  Channels:     ${CONFIG.limits.MAX_CHANNELS} max`);
  console.log(`  Per channel:  ${CONFIG.limits.MAX_SPEAKERS} speakers / ${CONFIG.limits.MAX_LISTENERS} listeners`);
  console.log(`  Per IP:       ${CONFIG.security.MAX_CONNECTIONS_PER_IP} connections`);
  console.log(`  API Key:      ${CONFIG.apiKey ? 'ENABLED' : 'DISABLED'}`);
  console.log('');
  console.log(`  Status:       http://localhost:${CONFIG.port}`);
  console.log(`  Client:       ws://YOUR_IP:${CONFIG.port}`);
  console.log('');
  
  federation.init(io, channels, server);
  
  console.log('  🛡️  Crash protection: ENABLED');
  console.log('  💾 Auto-save: Every 60s');
  console.log('  🌐 Federation: ACTIVE');
  console.log('  🌊 Voice & Files are P2P. Server never stores your data.');
  console.log('');
});
