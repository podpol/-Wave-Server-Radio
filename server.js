/**
 * ═══════════════════════════════════════════════════════════
 * Wave 🌊 — Lighthouse Server v1.0 (Crash-Protected)
 * ═══════════════════════════════════════════════════════════
 * Stack: Node.js + Express + Socket.io
 * License: AGPL-3.0
 * 
 * The server NEVER hears your voice and NEVER stores files.
 * Audio and Files flow P2P via WebRTC. Server only handles signaling.
 */

'use strict';

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

// Путь к файлу сохранения состояния каналов
const SAVE_FILE = path.join(__dirname, 'channels-backup.json');

// ═══════════════════════════════════════════════════════════
// PERSISTENCE (Сохранение каналов на диск)
// ═══════════════════════════════════════════════════════════

function saveChannels() {
  try {
    const data = [];
    channels.forEach((ch, id) => {
      data.push({
        id: ch.id,
        name: ch.name,
        admin: ch.admin,
        adminName: ch.adminName,
        requireApproval: ch.requireApproval,
        totalLikes: ch.totalLikes || 0,
        created: ch.created,
        messages: ch.messages || [],
        // ✅ ВАЖНО: сохраняем ТОЛЬКО метаданные файлов (без содержимого)
        chest: (ch.chest || []).map(file => ({
          id: file.id,
          name: file.name,
          size: file.size,
          type: file.type,
          uploader: file.uploader,
          uploaderName: file.uploaderName,
          timestamp: file.timestamp
        }))
      });
    });
    
    fs.writeFileSync(SAVE_FILE, JSON.stringify(data, null, 2), 'utf8');
    //console.log(`💾 Saved ${data.length} channel(s) to disk`);
  } catch (err) {
    console.error('❌ Failed to save channels:', err.message);
  }
}

function loadChannels() {
  try {
    if (!fs.existsSync(SAVE_FILE)) {
      console.log('📂 No backup file found, starting fresh');
      return;
    }
    const data = JSON.parse(fs.readFileSync(SAVE_FILE, 'utf8'));
    data.forEach(ch => {
      // Восстанавливаем канал с пустыми Map/Set
      channels.set(ch.id, {
        ...ch,
        speakers: new Map(),
        listeners: new Map(),
        joinRequests: new Map(),
        raisedHands: new Set(),
        recentlyApproved: new Map(),
        userLikes: new Map(),
        deleteTimer: null
      });
    });
    console.log(`✅ Loaded ${data.length} channel(s) from backup`);
  } catch (err) {
    console.error('❌ Failed to load channels:', err.message);
  }
}

// Авто-сохранение каждые 60 секунд
setInterval(saveChannels, 60000);

// ═══════════════════════════════════════════════════════════
// ГЛОБАЛЬНАЯ ЗАЩИТА ОТ ПАДЕНИЙ
// ═══════════════════════════════════════════════════════════

function gracefulShutdown(signal) {
  log('🛑', `Shutdown signal: ${signal}. Saving state...`);
  saveChannels();
  try { io.close(); } catch(e) {}
  try { server.close(); } catch(e) {}
  setTimeout(() => {
    console.log('👋 Server stopped gracefully.');
    process.exit(0);
  }, 1000);
}

process.on('uncaughtException', (err) => {
  console.error('🔥 UNCAUGHT EXCEPTION (server continues):', err.message);
  console.error(err.stack);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('🔥 UNHANDLED REJECTION (server continues):', reason);
});

// ✅ Объединенные обработчики сигналов (исправлен конфликт дубликатов)
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// ═══════════════════════════════════════════════════════════
// APP SETUP
// ═══════════════════════════════════════════════════════════

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: (origin, callback) => {
      const allowed = ['chrome-extension://', 'http://localhost', 'http://127.0.0.1', 'null'];
      if (!origin || allowed.some(a => String(origin).startsWith(a))) callback(null, true);
      else { securityLog('CORS_BLOCKED', { origin }); callback(new Error('CORS blocked')); }
    }
  },
  pingTimeout: 60000,
  pingInterval: 25000,
  maxHttpBufferSize: 250e6 // ✅ 250 МБ (с запасом для Base64 от 150 МБ файла)
});

// ═══════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════

const CONFIG = {
  port: process.env.PORT || 3000,
  host: '0.0.0.0',
  apiKey: process.env.API_KEY || '',
  limits: {
    MAX_CHANNELS: 1000,
    MAX_CHANNELS_PER_USER: 1,
    MAX_SPEAKERS: 10,
    MAX_LISTENERS: 30,
    MAX_USERS: 40,
    BAN_DURATION: 30 * 60 * 1000,
    VOTE_DURATION: 60 * 1000,
    VOTE_THRESHOLD: 0.5,
    MAX_MESSAGE_LENGTH: 500,
    MAX_NAME_LENGTH: 20,
    MAX_CHANNEL_NAME_LENGTH: 30,
    MESSAGE_HISTORY: 200,
    REQUEST_EXPIRY: 5 * 60 * 1000,
    // ✅ 200 МБ достаточно для метаданных и чата. Это предотвращает DDoS.
    MAX_PAYLOAD_SIZE: 200_000_000 
  },
  security: {
    MAX_CONNECTIONS_PER_IP: 10,
    MAX_EVENTS_PER_SECOND: 30,
    RATE_LIMIT_MESSAGES: 10,
    RATE_LIMIT_WINDOW: 5000,
    LIKE_COOLDOWN: 400,
    BOT_TOKENS: (process.env.BOT_TOKENS || 'default-bot-token').split(',')
  }
};

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

function securityLog(event, details) {
  const time = new Date().toISOString();
  try {
    console.warn(`[SECURITY] [${time}] ${event}: ${JSON.stringify(details)}`);
  } catch(e) {
    console.warn(`[SECURITY] [${time}] ${event}: [unserializable]`);
  }
}

function safeHandler(name, fn) {
  return async (...args) => {
    try {
      await fn(...args);
    } catch (err) {
      console.error(`❌ Error in [${name}]:`, err.message);
      console.error(err.stack);
      const cb = args[args.length - 1];
      if (typeof cb === 'function') {
        try { cb({ error: `Server error: ${err.message}` }); } catch(e) {}
      }
    }
  };
}

// ═══════════════════════════════════════════════════════════
// SECURITY MIDDLEWARE
// ═══════════════════════════════════════════════════════════

io.use((socket, next) => {
  try {
    const ip = socket.handshake.address;
    const count = ipConnections.get(ip) || 0;
    if (count >= CONFIG.security.MAX_CONNECTIONS_PER_IP) {
      securityLog('IP_LIMIT', { ip, count });
      return next(new Error('Too many connections'));
    }
    ipConnections.set(ip, count + 1);
    socket._clientIp = ip;
    next();
  } catch (err) {
    console.error('❌ Middleware error:', err);
    next(new Error('Connection error'));
  }
});

function checkSocketRate(socketId) {
  const now = Date.now();
  let rate = socketRates.get(socketId);
  if (!rate || now - rate.lastReset > 1000) { rate = { count: 0, lastReset: now }; socketRates.set(socketId, rate); }
  rate.count++;
  if (rate.count > CONFIG.security.MAX_EVENTS_PER_SECOND) {
    securityLog('RATE_LIMIT', { socketId, count: rate.count });
    return false;
  }
  return true;
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
    res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Wave 🌊 Lighthouse</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,sans-serif;background:#09090b;color:#fafafa;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:40px 20px}.c{max-width:480px;width:100%}.h{text-align:center;margin-bottom:28px}.h h1{font-size:24px;font-weight:800;background:linear-gradient(135deg,#00b4d8,#0077b6);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:10px}.sb{display:inline-flex;align-items:center;gap:8px;padding:6px 14px;background:rgba(16,185,129,.1);border:1px solid rgba(16,185,129,.3);border-radius:20px;font-size:12px;color:#10b981;font-weight:600}.sd{width:7px;height:7px;background:#10b981;border-radius:50%;animation:p 2s infinite}@keyframes p{0%,100%{opacity:1}50%{opacity:.4}}.cd{background:#111114;border:1px solid rgba(255,255,255,.06);border-radius:14px;padding:22px;margin-bottom:14px}.cd h2{font-size:11px;text-transform:uppercase;letter-spacing:1.2px;color:rgba(250,250,250,.4);margin-bottom:14px;font-weight:700}.sg{display:grid;grid-template-columns:1fr 1fr;gap:10px}.st{background:#1a1a1f;border-radius:10px;padding:14px;text-align:center}.sv{font-size:22px;font-weight:800}.sl{font-size:10px;color:rgba(250,250,250,.4);margin-top:3px;text-transform:uppercase}.ll{list-style:none}.ll li{display:flex;justify-content:space-between;padding:9px 0;border-bottom:1px solid rgba(255,255,255,.04);font-size:12px}.ll li:last-child{border-bottom:none}.ll .lb{color:rgba(250,250,250,.6)}.ll .vl{font-weight:700;font-family:monospace}.ub{background:#1a1a1f;border:1px solid rgba(0,180,216,.3);border-radius:10px;padding:14px;text-align:center;font-family:monospace;font-size:12px;color:#00b4d8;word-break:break-all}.ft{text-align:center;font-size:10px;color:rgba(250,250,250,.25);margin-top:20px}</style></head>
<body><div class="c"><div class="h"><h1>Wave 🌊 Lighthouse</h1><div class="sb"><div class="sd"></div>Server Online v1.0</div></div>
<div class="cd"><h2>Live Statistics</h2><div class="sg">
<div class="st"><div class="sv">${channels.size}</div><div class="sl">Channels</div></div>
<div class="st"><div class="sv">${users.size}</div><div class="sl">Online</div></div>
<div class="st"><div class="sv">${ts}</div><div class="sl">Navigators</div></div>
<div class="st"><div class="sv">${tl}</div><div class="sl">Travelers</div></div></div></div>
<div class="cd"><h2>Configuration</h2><ul class="ll">
<li><span class="lb">Max channels</span><span class="vl">${CONFIG.limits.MAX_CHANNELS}</span></li>
<li><span class="lb">Navigators / channel</span><span class="vl">${CONFIG.limits.MAX_SPEAKERS}</span></li>
<li><span class="lb">Travelers / channel</span><span class="vl">${CONFIG.limits.MAX_LISTENERS}</span></li>
<li><span class="lb">Total / channel</span><span class="vl">${CONFIG.limits.MAX_USERS}</span></li>
<li><span class="lb">Ban duration</span><span class="vl">30 min</span></li>
<li><span class="lb">Connections / IP</span><span class="vl">${CONFIG.security.MAX_CONNECTIONS_PER_IP}</span></li></ul></div>
<div class="cd"><h2>Connection URL</h2><div class="ub">ws://${req.headers.host}</div></div>
<div class="ft">Wave v1.0 — P2P Voice & Files. Server never hears or stores your data.</div></div></body></html>`);
  } catch (err) {
    console.error('❌ HTTP / error:', err);
    res.status(500).send('Server error');
  }
});

app.get('/api/channels', apiAuth, (req, res) => {
  try {
    const list = [];
    channels.forEach((ch, id) => { list.push({ id, name: ch.name, admin: ch.adminName, speakers: ch.speakers.size, listeners: ch.listeners.size, totalLikes: ch.totalLikes || 0 }); });
    res.json(list);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/stats', apiAuth, (req, res) => {
  try {
    let ts = 0, tl = 0;
    channels.forEach(ch => { ts += ch.speakers.size; tl += ch.listeners.size; });
    res.json({ channels: channels.size, users: users.size, speakers: ts, listeners: tl });
  } catch (err) { res.status(500).json({ error: err.message }); }
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
  } catch (err) {
    console.error('❌ Periodic statuses error:', err.message);
  }
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
        if (target) {
          try { io.to(target.socketId).emit('join-request-expired', { channelId: channel.id }); } catch(e) {}
        }
        try { io.to(channel.id).emit('join-request-removed', { userId }); } catch(e) {}
      });
      if (expired.length > 0) log('🧹', `Expired ${expired.length} request(s) in "${channel.name}"`);
    });
  } catch (err) {
    console.error('❌ Periodic expiry error:', err.message);
  }
}, 60000);

// ═══════════════════════════════════════════════════════════
// AUTO-DELETION SYSTEM (24 часа неактивности)
// ═══════════════════════════════════════════════════════════
function scheduleChannelDeletion(channelId) {
  const channel = channels.get(channelId);
  if (!channel) return;
  
  if (channel.deleteTimer) {
    clearTimeout(channel.deleteTimer);
  }
  
  channel.deleteTimer = setTimeout(() => {
    const ch = channels.get(channelId);
    if (ch && ch.speakers.size === 0 && ch.listeners.size === 0) {
      channels.delete(channelId);
      bans.delete(channelId);
      channelSpeaking.delete(channelId);
      channelScreenShare.delete(channelId);
      channelLikes.delete(channelId);
      io.emit('channels-updated');
      log('🗑️', `Channel "${ch.name}" auto-deleted after 24h of inactivity`);
    }
  }, 24 * 60 * 60 * 1000);
  
  log('⏳', `Channel "${channel.name}" scheduled for deletion in 24h`);
}

function cancelChannelDeletion(channelId) {
  const channel = channels.get(channelId);
  if (channel && channel.deleteTimer) {
    clearTimeout(channel.deleteTimer);
    channel.deleteTimer = null;
    log('🔄', `Channel "${channel.name}" deletion timer cancelled (activity detected)`);
  }
}

// ═══════════════════════════════════════════════════════════
// SOCKET.IO — MAIN HANDLER
// ═══════════════════════════════════════════════════════════

io.on('connection', (socket) => {
  socket._registered = false;
  socket._userId = null;

  log('🔌', `Socket connected: ${socket.id}`);

  socket.on('register-persistent', safeHandler('register-persistent', (rawData, cb) => {
    const data = safeData(rawData);
    const persistentId = sanitize(data.persistentId, 40);
    if (!persistentId || persistentId.length < 5) return cb({ error: 'Invalid persistent ID' });

    for (const [sid, u] of users) {
      if (u.userId === persistentId && sid !== socket.id) {
        const oldSocket = io.sockets.sockets.get(sid);
        if (oldSocket) { log('🔄', `Disconnecting duplicate: ${sid}`); try { oldSocket.disconnect(true); } catch(e) {} }
      }
    }

    socket._userId = persistentId;
    socket._registered = true;
    cb({ success: true, userId: persistentId });
    log('✅', `Registered: ${socket.id} → ${persistentId}`);
  }));
  
  // ── CHEST: Add File (ТОЛЬКО метаданные) ──
  socket.on('chest-add-file', safeHandler('chest-add-file', (rawData, cb) => {
    const callback = typeof cb === 'function' ? cb : () => {};
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData || !userData.channelId) return callback({ error: 'Not in channel' });
    
    const channel = channels.get(userData.channelId);
    if (!channel) return callback({ error: 'Channel not found' });
    
    const metadata = data.metadata;
    if (!metadata || !metadata.id || !metadata.name) return callback({ error: 'Invalid metadata' });
    
    // ✅ СЕРВЕРНАЯ ПРОВЕРКА: защита от подделки размера файла клиентом
    if (metadata.size > 150 * 1024 * 1024) {
      return callback({ error: 'File exceeds 150MB limit' });
    }
    
    if (!channel.chest) channel.chest = [];
    
    channel.chest.push({
      id: metadata.id,
      name: metadata.name,
      size: metadata.size,
      type: metadata.type,
      uploader: userData.userId,
      uploaderName: userData.userName,
      timestamp: metadata.timestamp || Date.now()
    });
    
    if (channel.chest.length > 20) {
      const removed = channel.chest.shift();
      console.log(`[Chest] Old file removed: ${removed.name}`);
    }
    
    io.to(userData.channelId).emit('chest-updated', channel.chest);
    log('🎁', `${userData.userName} added treasure to "${channel.name}"`);
    callback({ success: true });
  }));

  // ── CHEST: Request File (сигнализация для P2P) ──
  socket.on('chest-request-file', safeHandler('chest-request-file', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData || !userData.channelId) return;
    
    const channel = channels.get(userData.channelId);
    if (!channel || !channel.chest) return;
    
    const file = channel.chest.find(f => f.id === data.fileId);
    if (!file) return;
    
    const owner = findUser(file.uploader);
    if (!owner) {
      socket.emit('chest-file-unavailable', { fileId: data.fileId, reason: 'Owner offline' });
      return;
    }
    
    io.to(owner.socketId).emit('chest-file-requested', {
      fileId: data.fileId,
      requesterId: userData.userId,
      requesterName: userData.userName,
      requesterSocketId: socket.id
    });
  }));

  // ── CHEST: File Ready ──
  socket.on('chest-file-ready', safeHandler('chest-file-ready', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return;
    
    const requester = findUser(data.requesterId);
    if (requester) {
      io.to(requester.socketId).emit('chest-file-ready', {
        fileId: data.fileId,
        ownerSocketId: socket.id,
        ownerName: userData.userName
      });
    }
  }));
  
  // ── CHANNEL: Leave ──
  socket.on('leave-channel', safeHandler('leave-channel', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return;
    
    const channelId = data.channelId || userData.channelId;
    const channel = channels.get(channelId);
    if (!channel) return;
    
    const name = channel.speakers.get(userData.userId)?.name || 
                 channel.listeners.get(userData.userId)?.name || 
                 userData.userName;
    
    channel.speakers.delete(userData.userId);
    channel.listeners.delete(userData.userId);
    channel.raisedHands.delete(userData.userId);
    if (channel.joinRequests) channel.joinRequests.delete(userData.userId);
    if (channel.recentlyApproved) channel.recentlyApproved.delete(userData.userId);
    channelSpeaking.get(channelId)?.delete(userData.userId);
    channelScreenShare.get(channelId)?.delete(userData.userId);
    
    socket.leave(channelId);
    userData.channelId = null;
    userData.role = null;
    
    socket.to(channelId).emit('user-left', { userId: userData.userId, userName: name });
    
    if (channel.speakers.size === 0 && channel.listeners.size === 0) {
      scheduleChannelDeletion(channelId);
    }
    
    log('👋', `${name} left "${channel.name}" explicitly`);
  }));

  // ── CALL ──
  socket.on('call-user', safeHandler('call-user', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return;
    
    const channel = channels.get(userData.channelId);
    if (!channel) return;
    
    const targetUserId = data.targetUserId;
    if (targetUserId === userData.userId) return;
    
    const target = findUser(targetUserId);
    if (!target) return;
    
    const targetData = users.get(target.socketId);
    if (targetData && targetData.isMuted) return;
    
    io.to(target.socketId).emit('incoming-call', {
      fromUserId: userData.userId,
      fromUserName: userData.userName,
      timestamp: Date.now()
    });
    
    socket.emit('call-sent', { targetUserId, targetUserName: target.userName });
  }));

  // Rate limiter + security
  socket.use((packet, next) => {
    try {
      if (!socket._registered && packet[0] !== 'register-persistent') return next(new Error('Not registered'));
      if (!checkSocketRate(socket.id)) return next(new Error('Rate limit'));
      const packetStr = JSON.stringify(packet);
      if (packetStr.length > CONFIG.limits.MAX_PAYLOAD_SIZE) {
        securityLog('PAYLOAD_TOO_LARGE', { socketId: socket.id, size: packetStr.length });
        return next(new Error('Payload too large'));
      }
      const data = packet[1];
      if (data && typeof data === 'object') { delete data.__proto__; delete data.constructor; delete data.prototype; }
      next();
    } catch (err) {
      console.error('❌ Middleware error:', err);
      next(new Error('Security check failed'));
    }
  });

  // ── CHANNEL: Create ──
  socket.on('create-channel', safeHandler('create-channel', (rawData, cb) => {
    const callback = typeof cb === 'function' ? cb : () => { 
      console.log('[WARN] Client called create-channel without callback'); 
    };
    
    const data = safeData(rawData);
    const userId = socket._userId;
    const channelName = sanitize(data.channelName, CONFIG.limits.MAX_CHANNEL_NAME_LENGTH);
    const userName = sanitize(data.userName, CONFIG.limits.MAX_NAME_LENGTH) || 'Anonymous';

    let userCount = 0;
    channels.forEach(ch => { if (ch.admin === userId) userCount++; });
    if (channels.size >= CONFIG.limits.MAX_CHANNELS) return callback({ error: 'Server limit reached' });
    if (userCount >= CONFIG.limits.MAX_CHANNELS_PER_USER) return callback({ error: `Limit: ${CONFIG.limits.MAX_CHANNELS_PER_USER} per user` });
    if (!channelName) return callback({ error: 'Name required' });

    const channelId = uuidv4().slice(0, 8).toUpperCase();
    channels.set(channelId, {
      id: channelId, name: channelName, admin: userId, adminName: userName,
      requireApproval: data.requireApproval !== false,
      speakers: new Map([[userId, { userId, name: userName, socketId: socket.id }]]),
      listeners: new Map(), messages: [],
      joinRequests: new Map(), raisedHands: new Set(),
      recentlyApproved: new Map(),
      userLikes: new Map(), totalLikes: 0,
      created: Date.now(),
      chest: [],
      deleteTimer: null
    });
    bans.set(channelId, { users: new Map(), ips: new Map() });
    users.set(socket.id, { userId, userName, channelId, role: 'admin' });
    socket.join(channelId);

    log('📢', `Created: "${channelName}" (${channelId}) by ${userName} [ID: ${userId}]`);
    callback({ success: true, channelId });
    io.emit('channels-updated');
    saveChannels(); // ✅ Сохраняем при создании
  }));

  // ── CHANNEL: Join ──
  socket.on('join-channel', safeHandler('join-channel', (rawData, cb) => {
    const callback = typeof cb === 'function' ? cb : () => {};
    const data = safeData(rawData);
    const userId = socket._userId;
    const userName = sanitize(data.userName, CONFIG.limits.MAX_NAME_LENGTH) || 'Anonymous';
    const channelId = sanitize(data.channelId, 20);
    const channel = channels.get(channelId);
    if (!channel) return callback({ error: 'Channel not found' });

    const channelBans = bans.get(channelId);
    if (channelBans) {
      if (channelBans.users && channelBans.users.has(userId)) {
        const until = channelBans.users.get(userId);
        if (Date.now() < until) return callback({ error: `Banned. ${Math.ceil((until - Date.now()) / 60000)} min left` });
        channelBans.users.delete(userId);
      }
      const ip = socket._clientIp;
      if (channelBans.ips && channelBans.ips.has(ip)) {
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

    const uniqueName = getUniqueName(channel, userName, userId);
    const nameChanged = uniqueName !== userName;

    if (role === 'admin' || role === 'speaker') channel.speakers.set(userId, { userId, name: uniqueName, socketId: socket.id });
    else channel.listeners.set(userId, { name: uniqueName, socketId: socket.id });

    users.set(socket.id, { userId, userName: uniqueName, channelId, role, isMuted: false });
    socket.join(channelId);

    const speakersList = Array.from(channel.speakers.entries()).map(([uid, s]) => ({ userId: uid, name: s.name }));
    const listenersList = Array.from(channel.listeners.entries()).map(([uid, l]) => ({ userId: uid, name: l.name }));

    const raisedHandsList = role === 'admin' ? Array.from(channel.raisedHands).map(uid => {
      const s = channel.speakers.get(uid); const l = channel.listeners.get(uid);
      return { userId: uid, userName: s?.name || l?.name || 'User', timestamp: Date.now() };
    }) : [];

    const joinRequestsList = role === 'admin' ? Array.from(channel.joinRequests.entries()).map(([uid, req]) => ({
      userId: uid, userName: req.userName, timestamp: req.timestamp
    })) : [];

    callback({
      success: true, channelName: channel.name, isAdmin: role === 'admin', role, requireApproval: channel.requireApproval,
      adminId: channel.admin, speakers: speakersList, listeners: listenersList,
      messages: channel.messages.slice(-100),
      maxSpeakers: CONFIG.limits.MAX_SPEAKERS, maxListeners: CONFIG.limits.MAX_LISTENERS,
      yourName: uniqueName, nameChanged,
      userLikes: Object.fromEntries(channel.userLikes || new Map()),
      totalLikes: channel.totalLikes || 0,
      raisedHands: raisedHandsList,
      joinRequests: joinRequestsList,
      chestFiles: channel.chest || []
    });
	
    cancelChannelDeletion(channelId);
    socket.to(channelId).emit('user-joined', { userId, userName: uniqueName, role });
    if (nameChanged) socket.emit('name-changed-by-server', { newName: uniqueName, reason: 'Name taken' });
    log('👤', `${uniqueName} → "${channel.name}" as ${role} [${total + 1}/${CONFIG.limits.MAX_USERS}]`);
  }));
  
  socket.on('mute-status', safeHandler('mute-status', (data) => {
    const userData = users.get(socket.id);
    if (!userData) return;
    userData.isMuted = data.isMuted || false;
  }));

  socket.on('get-channels', safeHandler('get-channels', (cb) => {
    const list = [];
    channels.forEach((ch, id) => {
      list.push({ id, name: ch.name, admin: ch.adminName, adminId: ch.admin, speakers: ch.speakers.size, listeners: ch.listeners.size, totalLikes: ch.totalLikes || 0 });
    });
    cb(list);
  }));

  socket.on('request-join', safeHandler('request-join', (rawData, cb) => {
    const data = safeData(rawData);
    const userId = socket._userId;
    const channelId = sanitize(data.channelId, 20);
    const channel = channels.get(channelId);
    if (!channel) return cb({ error: 'Channel not found' });

    if (channel.admin === userId || channel.speakers.has(userId) || channel.listeners.has(userId)) return cb({ approved: true });

    if (channel.recentlyApproved && channel.recentlyApproved.has(userId)) {
      const expiresAt = channel.recentlyApproved.get(userId);
      if (Date.now() < expiresAt) return cb({ approved: true, message: 'Already approved' });
      channel.recentlyApproved.delete(userId);
    }

    if (!channel.requireApproval) return cb({ approved: true });
    if (channel.joinRequests.has(userId)) return cb({ approved: false, message: 'Request already pending' });

    channel.joinRequests.set(userId, {
      userId,
      userName: sanitize(data.userName, 20),
      socketId: socket.id,
      timestamp: Date.now()
    });

    const adminSocket = findUser(channel.admin);
    if (adminSocket) {
      try { io.to(adminSocket.socketId).emit('join-request', { userId, userName: data.userName, channelId, timestamp: Date.now() }); } catch(e) {}
    }
    cb({ approved: false, message: 'Request sent to admin' });
  }));

  socket.on('respond-join-request', safeHandler('respond-join-request', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return cb({ error: 'Not connected' });
    const channel = channels.get(userData.channelId);
    if (!channel) return cb({ error: 'Channel not found' });
    if (channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    if (!data.targetUserId) return cb({ error: 'Missing targetUserId' });

    if (!channel.joinRequests.has(data.targetUserId)) {
      try { io.to(userData.channelId).emit('join-request-removed', { userId: data.targetUserId }); } catch(e) {}
      return cb({ success: true });
    }

    const request = channel.joinRequests.get(data.targetUserId);
    channel.joinRequests.delete(data.targetUserId);

    if (!channel.recentlyApproved) channel.recentlyApproved = new Map();
    if (data.approved) channel.recentlyApproved.set(data.targetUserId, Date.now() + 10000);

    let delivered = false;
    if (request.socketId) {
      const targetSocket = io.sockets.sockets.get(request.socketId);
      if (targetSocket && targetSocket.connected) {
        try { targetSocket.emit('join-request-response', { approved: !!data.approved, channelId: userData.channelId }); delivered = true; } catch(e) {}
      }
    }
    if (!delivered) {
      const target = findUser(data.targetUserId);
      if (target) {
        try { io.to(target.socketId).emit('join-request-response', { approved: !!data.approved, channelId: userData.channelId }); } catch(e) {}
      }
    }

    try { io.to(userData.channelId).emit('join-request-removed', { userId: data.targetUserId }); } catch(e) {}
    log(data.approved ? '✅' : '❌', `Join ${data.approved ? 'approved' : 'denied'}: ${request.userName} in "${channel.name}"`);
    cb({ success: true });
  }));

  socket.on('get-join-requests', safeHandler('get-join-requests', (rawData, cb) => {
    const userData = users.get(socket.id);
    if (!userData) return cb([]);
    const channel = channels.get(userData.channelId);
    if (!channel || channel.admin !== userData.userId) return cb([]);
    if (!channel.joinRequests) return cb([]);
    const list = Array.from(channel.joinRequests.entries()).map(([userId, req]) => ({
      userId, userName: req.userName, timestamp: req.timestamp
    }));
    cb(list);
  }));

  socket.on('get-raised-hands', safeHandler('get-raised-hands', (rawData, cb) => {
    const userData = users.get(socket.id);
    if (!userData) return cb([]);
    const channel = channels.get(userData.channelId);
    if (!channel || channel.admin !== userData.userId) return cb([]);
    if (!channel.raisedHands) return cb([]);
    const list = Array.from(channel.raisedHands).map(userId => {
      const s = channel.speakers.get(userId);
      const l = channel.listeners.get(userId);
      return { userId, userName: s?.name || l?.name || 'User', timestamp: Date.now() };
    });
    cb(list);
  }));

  socket.on('switch-channel', safeHandler('switch-channel', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return cb({ error: 'Not connected' });
    const newId = sanitize(data.channelId, 20);
    const newChannel = channels.get(newId);
    if (!newChannel) return cb({ error: 'Channel not found' });

    const oldId = userData.channelId;
    if (oldId && oldId !== newId) {
      socket.leave(oldId);
      try { socket.to(oldId).emit('user-away', { userId: userData.userId, userName: userData.userName }); } catch(e) {}
    }
    socket.join(newId);
    userData.channelId = newId;

    cancelChannelDeletion(newId);

    if (oldId && oldId !== newId) {
       const oldChannel = channels.get(oldId);
       if (oldChannel && oldChannel.speakers.size === 0 && oldChannel.listeners.size === 0) {
          scheduleChannelDeletion(oldId);
       }
    }

    let role = 'listener';
    if (newChannel.admin === userData.userId) role = 'admin';
    else if (newChannel.speakers.has(userData.userId)) role = 'speaker';
    userData.role = role;

    try { socket.to(newId).emit('user-back', { userId: userData.userId, userName: userData.userName, role }); } catch(e) {}
    cb({ success: true, chestFiles: newChannel.chest || [], channelId: newId, role });
  }));

  socket.on('send-message', safeHandler('send-message', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData || !checkMessageRate(userData.userId)) return;
    const channel = channels.get(userData.channelId);
    if (!channel) return;
    
    const text = sanitize(data.text, CONFIG.limits.MAX_MESSAGE_LENGTH);
    if (!text) return;
    
    const msg = { 
      id: uuidv4().slice(0, 10), 
      userId: userData.userId, 
      userName: userData.userName, 
      text, 
      timestamp: Date.now() 
    };
    
    if (data.replyTo && typeof data.replyTo === 'object') {
      msg.replyTo = {
        id: sanitize(data.replyTo.id, 20),
        userId: sanitize(data.replyTo.userId, 40),
        userName: sanitize(data.replyTo.userName, CONFIG.limits.MAX_NAME_LENGTH),
        text: sanitize(data.replyTo.text, 200)
      };
    }
    
    channel.messages.push(msg);
    if (channel.messages.length > CONFIG.limits.MESSAGE_HISTORY) channel.messages.shift();
    io.to(userData.channelId).emit('new-message', msg);
  }));

  socket.on('update-username', safeHandler('update-username', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return cb({ error: 'Not in channel' });
    const channel = channels.get(userData.channelId);
    if (!channel) return cb({ error: 'Channel not found' });
    const newName = sanitize(data.newName, CONFIG.limits.MAX_NAME_LENGTH);
    if (!newName) return cb({ error: 'Name empty' });
    const uniqueName = getUniqueName(channel, newName, userData.userId);
    const nameChanged = uniqueName !== newName;
    const oldName = userData.userName;
    userData.userName = uniqueName;
    if (channel.speakers.has(userData.userId)) channel.speakers.get(userData.userId).name = uniqueName;
    if (channel.listeners.has(userData.userId)) channel.listeners.get(userData.userId).name = uniqueName;
    if (channel.admin === userData.userId) channel.adminName = uniqueName;
    io.to(userData.channelId).emit('user-renamed', { userId: userData.userId, oldName, newName: uniqueName });
    cb({ success: true, newName: uniqueName, nameChanged });
    if (nameChanged) socket.emit('name-changed-by-server', { newName: uniqueName, reason: 'Name taken' });
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
    channel.raisedHands.delete(data.targetUserId);

    const target = findUser(data.targetUserId);
    if (target) users.get(target.socketId).role = 'speaker';

    io.to(userData.channelId).emit('role-changed', { userId: data.targetUserId, role: 'speaker', userName: listener.name });
    io.to(userData.channelId).emit('hand-lowered', { userId: data.targetUserId });
    log('🎤', `${listener.name} → speaker in "${channel.name}"`);
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

    const target = findUser(data.targetUserId);
    if (target) users.get(target.socketId).role = 'listener';

    io.to(userData.channelId).emit('role-changed', { userId: data.targetUserId, role: 'listener', userName: speaker.name });
    log('🔇', `${speaker.name} → listener in "${channel.name}"`);
    cb({ success: true });
  }));

  socket.on('kick-user', safeHandler('kick-user', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    const channel = channels.get(userData?.channelId);
    if (!channel || channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    const target = findUser(data.targetUserId);
    if (target) {
      try { io.to(target.socketId).emit('kicked', { reason: sanitize(data.reason, 100) || 'Kicked' }); } catch(e) {}
      try { io.sockets.sockets.get(target.socketId)?.disconnect(); } catch(e) {}
    }
    const name = channel.speakers.get(data.targetUserId)?.name || channel.listeners.get(data.targetUserId)?.name || 'User';
    channel.speakers.delete(data.targetUserId);
    channel.listeners.delete(data.targetUserId);
    channel.raisedHands.delete(data.targetUserId);
    channelSpeaking.get(userData.channelId)?.delete(data.targetUserId);
    channelScreenShare.get(userData.channelId)?.delete(data.targetUserId);
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
    const targetSocket = findUser(data.targetUserId);
    if (targetSocket) {
      const targetIp = io.sockets.sockets.get(targetSocket.socketId)?._clientIp;
      if (targetIp) channelBans.ips.set(targetIp, until);
      try { io.to(targetSocket.socketId).emit('banned', { until, reason: sanitize(data.reason, 100) || 'Banned 30 min' }); } catch(e) {}
      try { io.sockets.sockets.get(targetSocket.socketId)?.disconnect(); } catch(e) {}
    }
    const name = channel.speakers.get(data.targetUserId)?.name || channel.listeners.get(data.targetUserId)?.name || 'User';
    channel.speakers.delete(data.targetUserId);
    channel.listeners.delete(data.targetUserId);
    channel.raisedHands.delete(data.targetUserId);
    channelSpeaking.get(userData.channelId)?.delete(data.targetUserId);
    channelScreenShare.get(userData.channelId)?.delete(data.targetUserId);
    io.to(userData.channelId).emit('user-banned', { userId: data.targetUserId, userName: name, until });
    log('🔒', `${name} banned in "${channel.name}"`);
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
    channel.admin = data.targetUserId;
    channel.adminName = target.name;
    if (channel.listeners.has(data.targetUserId)) {
      channel.speakers.set(data.targetUserId, channel.listeners.get(data.targetUserId));
      channel.listeners.delete(data.targetUserId);
    }
    const ns = findUser(data.targetUserId); if (ns) users.get(ns.socketId).role = 'admin';
    const os = findUser(oldAdminId); if (os) users.get(os.socketId).role = 'speaker';
    io.to(userData.channelId).emit('admin-transferred', { oldAdminId, newAdminId: data.targetUserId, newAdminName: target.name });
    cb({ success: true });
  }));

  socket.on('speaking-status', safeHandler('speaking-status', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return;
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
    const chId = userData.channelId;
    if (!channelScreenShare.has(chId)) channelScreenShare.set(chId, new Set());
    channelScreenShare.get(chId).add(userData.userId);
    socket.to(chId).emit('screen-share-started', { userId: userData.userId, userName: userData.userName, mediaType: data.mediaType || 'screen' });
    log('🖥️', `${userData.userName} started ${data.mediaType || 'screen'} in "${channels.get(chId)?.name}"`);
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
    const targetUser = findUser(data.targetUserId);
    if (!targetUser) return;
    try { io.to(targetUser.socketId).emit('stream-resend-requested', { requesterId: userData.userId, requesterSocketId: socket.id }); } catch(e) {}
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
    if (!channel) return;
    if (channel.raisedHands.has(userData.userId)) return;
    channel.raisedHands.add(userData.userId);
    const admin = findUser(channel.admin);
    if (admin) {
      try { io.to(admin.socketId).emit('hand-raised', { userId: userData.userId, userName: userData.userName, timestamp: Date.now() }); } catch(e) {}
    }
  }));

  socket.on('lower-hand', safeHandler('lower-hand', () => {
    const userData = users.get(socket.id);
    if (!userData) return;
    const channel = channels.get(userData.channelId);
    if (!channel) return;
    channel.raisedHands.delete(userData.userId);
    const admin = findUser(channel.admin);
    if (admin) {
      try { io.to(admin.socketId).emit('hand-lowered', { userId: userData.userId }); } catch(e) {}
    }
  }));

  socket.on('deny-raised-hand', safeHandler('deny-raised-hand', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    const channel = channels.get(userData?.channelId);
    if (!channel) return cb({ error: 'Not found' });
    if (channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    if (!data.targetUserId) return cb({ error: 'Missing targetUserId' });

    if (!channel.raisedHands.has(data.targetUserId)) {
      try { io.to(userData.channelId).emit('hand-lowered', { userId: data.targetUserId }); } catch(e) {}
      return cb({ success: true });
    }

    channel.raisedHands.delete(data.targetUserId);
    io.to(userData.channelId).emit('hand-lowered', { userId: data.targetUserId });
    cb({ success: true });
  }));

  socket.on('send-like', safeHandler('send-like', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData) return;
    if (data.targetUserId === userData.userId) return;
    const chId = userData.channelId;
    const channel = channels.get(chId);
    if (!channel) return;
    const now = Date.now();
    if (!userData.lastLikeTime || now - userData.lastLikeTime > CONFIG.security.LIKE_COOLDOWN) {
      userData.lastLikeTime = now;
      const targetId = data.targetUserId || userData.userId;
      const current = channel.userLikes.get(targetId) || 0;
      channel.userLikes.set(targetId, current + 1);
      channel.totalLikes = (channel.totalLikes || 0) + 1;
      channelLikes.set(chId, (channelLikes.get(chId) || 0) + 1);
      socket.to(chId).emit('receive-like', { userId: userData.userId, userName: userData.userName, targetUserId: targetId, count: current + 1 });
    }
  }));

  socket.on('start-vote', safeHandler('start-vote', (rawData, cb) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    const channel = channels.get(userData?.channelId);
    if (!channel) return cb({ error: 'Not found' });
    if (data.targetUserId === channel.admin) return cb({ error: 'Cannot vote admin' });
    const voteId = uuidv4().slice(0, 8);
    votes.set(voteId, { id: voteId, channelId: userData.channelId, targetUserId: data.targetUserId, targetName: sanitize(data.targetName, CONFIG.limits.MAX_NAME_LENGTH), startedBy: userData.userId, yes: new Set([userData.userId]), no: new Set(), votedIps: new Set([socket._clientIp]), expiresAt: Date.now() + CONFIG.limits.VOTE_DURATION });
    io.to(userData.channelId).emit('vote-started', { voteId, targetUserId: data.targetUserId, targetName: data.targetName, expiresAt: Date.now() + CONFIG.limits.VOTE_DURATION });
    setTimeout(() => finishVote(voteId), CONFIG.limits.VOTE_DURATION);
    cb({ success: true, voteId });
  }));

  socket.on('cast-vote', safeHandler('cast-vote', (rawData) => {
    const data = safeData(rawData);
    const vote = votes.get(data.voteId);
    if (!vote) return;
    const userData = users.get(socket.id);
    if (!userData) return;
    const ip = socket._clientIp;
    if (vote.votedIps.has(ip)) return;
    vote.votedIps.add(ip);
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
        if (target) {
          try { io.to(target.socketId).emit('kicked', { reason: 'Community vote' }); } catch(e) {}
          try { io.sockets.sockets.get(target.socketId)?.disconnect(); } catch(e) {}
        }
        channel.speakers.delete(vote.targetUserId);
        channel.listeners.delete(vote.targetUserId);
        channel.raisedHands.delete(vote.targetUserId);
        channelSpeaking.get(vote.channelId)?.delete(vote.targetUserId);
        channelScreenShare.get(vote.channelId)?.delete(vote.targetUserId);
        io.to(vote.channelId).emit('vote-result', { voteId, kicked: true, targetName: vote.targetName });
      } else {
        io.to(vote.channelId).emit('vote-result', { voteId, kicked: false, targetName: vote.targetName });
      }
      votes.delete(voteId);
    } catch (err) {
      console.error('❌ finishVote error:', err);
    }
  }

  socket.on('webrtc-offer', safeHandler('webrtc-offer', (d) => { const t = findUser(d.toUserId); if (t) io.to(t.socketId).emit('webrtc-offer', d); }));
  socket.on('webrtc-answer', safeHandler('webrtc-answer', (d) => { const t = findUser(d.toUserId); if (t) io.to(t.socketId).emit('webrtc-answer', d); }));
  socket.on('webrtc-ice', safeHandler('webrtc-ice', (d) => { const t = findUser(d.toUserId); if (t) io.to(t.socketId).emit('webrtc-ice', d); }));

  socket.on('bot-auth', safeHandler('bot-auth', (rawData, cb) => {
    const data = safeData(rawData);
    if (!CONFIG.security.BOT_TOKENS.includes(data.token)) { securityLog('BOT_AUTH_FAIL', { ip: socket._clientIp }); return cb({ error: 'Invalid token' }); }
    const channel = channels.get(data.channelId);
    if (!channel) return cb({ error: 'Channel not found' });
    const botId = 'bot_' + uuidv4().slice(0, 8);
    const name = sanitize(data.botName, 20) || 'Bot';
    channel.listeners.set(botId, { name, socketId: socket.id, isBot: true });
    users.set(socket.id, { userId: botId, userName: name, channelId: data.channelId, role: 'bot', isBot: true });
    socket.join(data.channelId);
    socket.to(data.channelId).emit('user-joined', { userId: botId, userName: name, role: 'listener', isBot: true });
    log('🤖', `Bot "${name}" → "${channel.name}"`);
    cb({ success: true, botId, channelId: data.channelId });
  }));

  socket.on('bot-message', safeHandler('bot-message', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData || !userData.isBot) return;
    const channel = channels.get(userData.channelId);
    if (!channel) return;
    const msg = { id: uuidv4().slice(0, 10), userId: userData.userId, userName: userData.userName, text: sanitize(data.text, CONFIG.limits.MAX_MESSAGE_LENGTH), timestamp: Date.now(), isBot: true, meta: data.meta || null };
    channel.messages.push(msg);
    if (channel.messages.length > CONFIG.limits.MESSAGE_HISTORY) channel.messages.shift();
    io.to(userData.channelId).emit('new-message', msg);
  }));

  socket.on('bot-media', safeHandler('bot-media', (rawData) => {
    const data = safeData(rawData);
    const userData = users.get(socket.id);
    if (!userData || !userData.isBot) return;
    const channel = channels.get(userData.channelId);
    if (!channel) return;
    const msg = { id: uuidv4().slice(0, 10), userId: userData.userId, userName: userData.userName, text: sanitize(data.caption, 200) || '', timestamp: Date.now(), isBot: true, meta: { type: data.type || 'image', url: data.url, title: data.title || '', description: data.description || '' } };
    channel.messages.push(msg);
    if (channel.messages.length > CONFIG.limits.MESSAGE_HISTORY) channel.messages.shift();
    io.to(userData.channelId).emit('new-message', msg);
  }));

  // ── DISCONNECT ──
  socket.on('disconnect', () => {
    try {
      const ip = socket._clientIp;
      if (ip) { const count = ipConnections.get(ip) || 1; if (count <= 1) ipConnections.delete(ip); else ipConnections.set(ip, count - 1); }
      socketRates.delete(socket.id);

      const userData = users.get(socket.id);
      if (!userData) return;

      const channel = channels.get(userData.channelId);
      if (!channel) { users.delete(socket.id); return; }

      if (userData.isBot) {
        channel.listeners.delete(userData.userId);
        channel.speakers.delete(userData.userId);
        socket.to(userData.channelId).emit('user-left', { userId: userData.userId, userName: userData.userName });
        users.delete(socket.id);
        return;
      }

      channel.speakers.delete(userData.userId);
      channel.listeners.delete(userData.userId);
      channel.raisedHands.delete(userData.userId);
      if (channel.joinRequests) channel.joinRequests.delete(userData.userId);
      if (channel.recentlyApproved) channel.recentlyApproved.delete(userData.userId);
      channelSpeaking.get(userData.channelId)?.delete(userData.userId);
      channelScreenShare.get(userData.channelId)?.delete(userData.userId);

      if (channel.speakers.size === 0 && channel.listeners.size === 0) {
        scheduleChannelDeletion(userData.channelId);
      } else {
        socket.to(userData.channelId).emit('user-left', { userId: userData.userId, userName: userData.userName });
      }

      users.delete(socket.id);
      messageRates.delete(userData.userId);
      log('❌', `${userData.userName} left [${users.size} online]`);
    } catch (err) {
      console.error('❌ disconnect error:', err);
    }
  });

  // ── CHANNEL: Close (Admin only) ──
  socket.on('close-channel', safeHandler('close-channel', (rawData, cb) => {
    const userData = users.get(socket.id);
    if (!userData) return cb({ error: 'Not connected' });
    
    const channel = channels.get(userData.channelId);
    if (!channel) return cb({ error: 'Channel not found' });
    if (channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    
    const channelName = channel.name;
    const channelId = userData.channelId;
    
    const allUsers = [];
    channel.speakers.forEach((s, uid) => { if (uid !== userData.userId) allUsers.push(uid); });
    channel.listeners.forEach((l, uid) => { if (uid !== userData.userId) allUsers.push(uid); });
    
    allUsers.forEach(uid => {
      const target = findUser(uid);
      if (target) {
        try {
          io.to(target.socketId).emit('channel-closed', {
            channelName,
            closedBy: userData.userName,
            reason: 'Admin closed the channel'
          });
        } catch(e) {}
      }
    });
    
    allUsers.forEach(uid => {
      const target = findUser(uid);
      if (target) {
        const targetSocket = io.sockets.sockets.get(target.socketId);
        if (targetSocket) {
          targetSocket.leave(channelId);
          const targetData = users.get(target.socketId);
          if (targetData) {
            targetData.channelId = null;
            targetData.role = null;
          }
        }
      }
    });
    
    if (channel.deleteTimer) {
      clearTimeout(channel.deleteTimer);
    }
    
    channels.delete(channelId);
    bans.delete(channelId);
    channelSpeaking.delete(channelId);
    channelScreenShare.delete(channelId);
    channelLikes.delete(channelId);
    
    votes.forEach((v, vid) => {
      if (v.channelId === channelId) votes.delete(vid);
    });
    
    userData.channelId = null;
    userData.role = null;
    socket.leave(channelId);
    
    io.emit('channels-updated');
    log('🚪', `Channel "${channelName}" closed by admin ${userData.userName}. Kicked ${allUsers.length} user(s)`);
    
    cb({ success: true, kicked: allUsers.length });
    saveChannels(); // ✅ Сохраняем при закрытии
  }));
  
  socket.on('toggle-channel-approval', safeHandler('toggle-channel-approval', (rawData, cb) => {
    const userData = users.get(socket.id);
    if (!userData) return cb({ error: 'Not connected' });
    
    const channel = channels.get(userData.channelId);
    if (!channel) return cb({ error: 'Channel not found' });
    if (channel.admin !== userData.userId) return cb({ error: 'Admin only' });
    
    channel.requireApproval = !channel.requireApproval;
    const newMode = channel.requireApproval;
    const modeText = newMode ? 'closed (approval required)' : 'open (free entry)';
    
    log('🔐', `${userData.userName} set "${channel.name}" to ${modeText}`);
    
    io.to(userData.channelId).emit('channel-mode-changed', {
      requireApproval: newMode,
      changedBy: userData.userName,
      timestamp: Date.now()
    });
    
    cb({ success: true, requireApproval: newMode });
  }));

});  // ← ЗАКРЫТИЕ io.on('connection')

// ═══════════════════════════════════════════════════════════
// START
// ═══════════════════════════════════════════════════════════

// Загружаем каналы из бэкапа перед запуском
loadChannels();

server.listen(CONFIG.port, CONFIG.host, () => {
  console.log('');
  console.log('  ┌─────────────────────────────────────────────────┐');
  console.log('  │           Wave 🌊 Lighthouse Server             │');
  console.log('  │           v1.0 (Crash-Protected)                │');
  console.log('  └─────────────────────────────────────────────────┘');
  console.log('');
  console.log(`  Port:         ${CONFIG.port}`);
  console.log(`  Channels:     ${CONFIG.limits.MAX_CHANNELS} max`);
  console.log(`  Per channel:  ${CONFIG.limits.MAX_SPEAKERS} navigators / ${CONFIG.limits.MAX_LISTENERS} travelers`);
  console.log(`  Per IP:       ${CONFIG.security.MAX_CONNECTIONS_PER_IP} connections`);
  console.log(`  Rate:         ${CONFIG.security.MAX_EVENTS_PER_SECOND} events/sec`);
  console.log(`  Payload:      ${CONFIG.limits.MAX_PAYLOAD_SIZE / 1024}KB max (Metadata only)`);
  console.log(`  API Key:      ${CONFIG.apiKey ? 'ENABLED' : 'DISABLED (open)'}`);
  console.log('');
  console.log(`  Status:       http://localhost:${CONFIG.port}`);
  console.log(`  Client:       ws://YOUR_IP:${CONFIG.port}`);
  console.log('');
  console.log('  🛡️  Crash protection: ENABLED');
  console.log('  💾 Auto-save: Every 60 seconds');
  console.log('  🌊 Voice & Files are P2P. Server never hears or stores your data.');
  console.log('');
});
