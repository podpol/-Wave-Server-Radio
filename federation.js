/**
 * ═══════════════════════════════════════════════════════════
 * Wave 🌊 Federation Module v3.0 — Self-growing mesh
 * ═══════════════════════════════════════════════════════════
 *
 * Протокол server-to-server для глобальной видимости каналов.
 *
 * Принципы:
 *  - Канал "живёт" на одном сервере (home server)
 *  - Все серверы видят метаданные всех каналов, независимо от того,
 *    к какому серверу подключён конкретный клиент
 *  - Имена каналов глобально уникальны
 *  - При входе в "чужой" канал — клиент получает redirect на home-сервер
 *  - Сеть растёт САМА: чтобы присоединиться, достаточно знать ОДИН
 *    любой уже работающий адрес (bootstrap). Дальше пиры обмениваются
 *    списками друг друга (gossip) и сеть доразворачивается сама —
 *    не нужно вручную прописывать всех участников у всех.
 *  - Два уровня доверия пиров:
 *      official   — сервер из вашего доверенного списка (OFFICIAL_SERVER_IDS)
 *      volunteer  — любой сторонний сервер с верным сетевым секретом.
 *                   Не может перехватывать чужие имена каналов и его
 *                   заявления реплицируются дальше только после того,
 *                   как мы САМИ дозвонились по заявленному им адресу
 *                   (reachability-проверка через обратное подключение —
 *                   она осмысленна именно тут, т.к. это настоящий Node.js
 *                   процесс с открытым портом, а не браузер).
 *
 * Config (env):
 *   FEDERATION_ID=server-01
 *   FEDERATION_URL=ws://YourIP:3001
 *   FEDERATION_BOOTSTRAP=ws://any-known-node.example.com:3001   (достаточно одного!)
 *   FEDERATION_PEERS=ws://berlin.example.com:3001,ws://tokyo.example.com:3001 (необязательно)
 *   FEDERATION_SECRET=my-shared-network-secret
 *   OFFICIAL_SERVER_IDS=server-moscow-01,server-berlin-01
 *   FEDERATION_MAX_PEERS=40
 */

'use strict';

const WebSocket = require('ws');
const crypto = require('crypto');

function timingSafeEqual(a, b) {
  const bufA = Buffer.from(String(a || ''));
  const bufB = Buffer.from(String(b || ''));
  if (bufA.length !== bufB.length) {
    // сравниваем с самим собой, чтобы не выдавать длину секрета через тайминг
    try { crypto.timingSafeEqual(bufA, bufA); } catch (e) {}
    return false;
  }
  try { return crypto.timingSafeEqual(bufA, bufB); } catch (e) { return false; }
}

class Federation {
  constructor(options = {}) {
    // Идентификация этого сервера
    this.serverId = options.serverId || process.env.FEDERATION_ID || `server-${Date.now().toString(36)}`;
    this.serverUrl = options.serverUrl || process.env.FEDERATION_URL || 'ws://localhost:3001';
    this.serverName = options.serverName || process.env.FEDERATION_NAME || 'Wave Node';
    this.secret = options.secret || process.env.FEDERATION_SECRET || '';

    // Официально доверенные ID серверов (ваши собственные) — для них снят ряд ограничений
    this.officialIds = new Set(
      (options.officialIds || process.env.OFFICIAL_SERVER_IDS || '')
        .split(',').map(s => s.trim()).filter(Boolean)
    );

    // Список пиров (других серверов)
    this.peers = new Map(); // url -> { ws, serverId, serverName, connected, lastSeen, channels: Map, trust }
    // FEDERATION_BOOTSTRAP — новый способ: достаточно ОДНОГО известного адреса,
    // остальное сеть узнает сама через gossip. FEDERATION_PEERS оставлен для
    // обратной совместимости и жёстко закреплённых связей (напр. между вашими VPS).
    const bootstrap = (options.bootstrap || process.env.FEDERATION_BOOTSTRAP || '')
      .split(',').map(u => u.trim()).filter(u => u && u !== this.serverUrl);
    const fixedPeers = (options.peers || process.env.FEDERATION_PEERS || '')
      .split(',').map(u => u.trim()).filter(u => u && u !== this.serverUrl);
    this.peerUrls = [...new Set([...fixedPeers, ...bootstrap])];
    this.maxPeers = parseInt(options.maxPeers || process.env.FEDERATION_MAX_PEERS || '40', 10);

    // Глобальный реестр каналов (channelId -> metadata)
    this.globalChannels = new Map();

    // Глобальный реестр имён (lowercase name -> channelId)
    this.globalNames = new Map();

    // Ссылки на основной сервер
    this.io = null;
    this.channels = null; // Map из основного сервера
    this.onChannelCreated = null;
    this.onChannelDeleted = null;

    // WSS сервер для входящих федеративных соединений
    this.wss = null;
    this.httpServer = null;

    // Защита WSS федерации от неаутентифицированного/избыточного трафика
    this._fedIpConnections = new Map();
    this.maxFedConnectionsPerIp = parseInt(process.env.FEDERATION_MAX_CONN_PER_IP || '3', 10);
    this.maxFedMessageSize = parseInt(process.env.FEDERATION_MAX_MSG_SIZE || String(64 * 1024), 10);

    // Reachability-проверки в процессе (nonce -> resolver)
    this._pendingProbes = new Map();

    // Статистика
    this.stats = {
      messagesSent: 0,
      messagesReceived: 0,
      reconnects: 0,
      lastSync: null
    };

    // Таймеры
    this._heartbeatTimer = null;
    this._reconnectTimers = new Map();
    this._syncTimer = null;
    this._gossipTimer = null;
  }

  /**
   * Инициализация федерации
   */
  init(io, channels, httpServer) {
    this.io = io;
    this.channels = channels;
    this.httpServer = httpServer;
    
    console.log('');
    console.log('  ┌─────────────────────────────────────────────────┐');
    console.log('  │         🌐 Wave Federation Module v1.0          │');
    console.log('  └─────────────────────────────────────────────────┘');
    console.log(`  Server ID:    ${this.serverId}`);
    console.log(`  Server URL:   ${this.serverUrl}`);
    console.log(`  Peers:        ${this.peerUrls.length || 'none (solo mode)'}`);
    console.log(`  Secret:       ${this.secret ? 'SET' : 'NONE (open)'}`);
    console.log('');
    
    // Запускаем WSS сервер для входящих соединений от пиров
    this._startFederationServer();
    
    // Подключаемся к пирам
    this.peerUrls.forEach(url => this._connectToPeer(url));
    
    // Периодическая синхронизация
    this._syncTimer = setInterval(() => this._broadcastFullSync(), 30000);
    
    // Heartbeat
    this._heartbeatTimer = setInterval(() => this._sendHeartbeats(), 15000);
    
    // Очистка мёртвых пиров
    setInterval(() => this._cleanupDeadPeers(), 60000);

    // ✅ Gossip: периодически рассылаем список известных нам пиров,
    // чтобы новый сервер, зашедший всего с ОДНИМ bootstrap-адресом,
    // сам доразведал остальную сеть
    this._gossipTimer = setInterval(() => this._gossipPeerList(), 20000);
    
    log('🌐', `Federation initialized. ID: ${this.serverId}`);
  }

  // ═══════════════════════════════════════════════════════════
  // FEDERATION SERVER (входящие соединения от других серверов)
  // ═══════════════════════════════════════════════════════════

  _startFederationServer() {
    // Создаём отдельный HTTP сервер на порту +1
    const fedPort = parseInt(this.serverUrl.split(':').pop()) || 3001;

    this.wss = new WebSocket.Server({
      port: fedPort,
      host: '0.0.0.0',
      maxPayload: this.maxFedMessageSize
    });

    this.wss.on('connection', (ws, req) => {
      const ip = req.socket.remoteAddress;

      // ✅ Лимит одновременных соединений с одного IP (до любой аутентификации)
      const count = this._fedIpConnections.get(ip) || 0;
      if (count >= this.maxFedConnectionsPerIp) {
        log('⚠️', `Federation: too many connections from ${ip}, rejecting`);
        ws.close(4029, 'Too many connections');
        return;
      }
      this._fedIpConnections.set(ip, count + 1);

      log('🌐', `Incoming federation connection from ${ip}`);

      ws._isServer = false;
      ws._serverId = null;
      ws._authenticated = false;
      ws._peerTrust = null;      // 'official' | 'volunteer-verified' | 'volunteer-unverified'
      ws._msgCount = 0;
      ws._msgWindowStart = Date.now();

      // Таймаут на аутентификацию
      const authTimeout = setTimeout(() => {
        if (!ws._authenticated) {
          log('⚠️', `Federation auth timeout from ${ip}`);
          ws.close(4001, 'Auth timeout');
        }
      }, 10000);

      const releaseIp = () => {
        const c = this._fedIpConnections.get(ip) || 1;
        if (c <= 1) this._fedIpConnections.delete(ip); else this._fedIpConnections.set(ip, c - 1);
      };

      ws.on('message', (raw) => {
        // ✅ Размер — самая первая проверка, до JSON.parse
        if (raw.length > this.maxFedMessageSize) {
          log('⚠️', `Federation: oversized message from ${ip}, closing`);
          ws.close(4013, 'Message too large');
          return;
        }

        let msg;
        try { msg = JSON.parse(raw.toString()); }
        catch (e) { log('⚠️', `Federation: bad JSON from ${ip}`); return; }

        // ✅ До аутентификации принимаем ТОЛЬКО fed:hello
        if (!ws._authenticated && msg.type !== 'fed:hello') return;

        // ✅ Rate limit на входящие сообщения от пира (в т.ч. после аутентификации)
        const now = Date.now();
        if (now - ws._msgWindowStart > 1000) { ws._msgWindowStart = now; ws._msgCount = 0; }
        ws._msgCount++;
        if (ws._msgCount > 50) {
          log('⚠️', `Federation: peer flood from ${ip}, closing`);
          ws.close(4029, 'Rate limit');
          return;
        }

        try {
          this._handleIncomingMessage(ws, msg, ip);
          if (msg.type === 'fed:hello') clearTimeout(authTimeout);
        } catch (e) {
          console.error('❌ Federation handling error:', e.message);
        }
      });

      ws.on('close', () => {
        clearTimeout(authTimeout);
        releaseIp();
        if (ws._serverId) {
          const peer = this.peers.get(ws._peerUrl);
          if (peer) {
            peer.connected = false;
            peer.ws = null;
            log('🔌', `Peer ${ws._serverId} disconnected`);
            // Переподключение
            this._scheduleReconnect(ws._peerUrl);
          }
        }
      });

      ws.on('error', (err) => {
        console.error(`❌ Federation WS error (${ip}):`, err.message);
      });
    });

    this.wss.on('error', (err) => {
      console.error('❌ Federation server error:', err.message);
    });

    log('🌐', `Federation WSS listening on port ${fedPort} (max ${this.maxFedConnectionsPerIp} conn/IP, ${Math.round(this.maxFedMessageSize/1024)}KB msg cap)`);
  }

  // ═══════════════════════════════════════════════════════════
  // OUTGOING CONNECTIONS (подключение к пирам)
  // ═══════════════════════════════════════════════════════════

  _connectToPeer(url, opts = {}) {
    if (!opts.probe) {
      if (this.peers.has(url) && this.peers.get(url).connected) return;
      // ✅ Верхняя граница размера меша — не подключаемся к новым пирам сверх лимита,
      // чтобы кто-то не заставил нас держать тысячи соединений
      if (!this.peers.has(url) && this.peers.size >= this.maxPeers) {
        log('⚠️', `Federation: peer limit (${this.maxPeers}) reached, skipping ${url}`);
        return;
      }
    }

    log('🌐', opts.probe ? `Probing reachability: ${url}` : `Connecting to peer: ${url}`);

    try {
      const ws = new WebSocket(url, {
        handshakeTimeout: opts.probe ? 5000 : 10000,
        maxPayload: this.maxFedMessageSize,
        headers: { 'X-Federation-Secret': this.secret }
      });

      // ✅ Reachability-проба: не регистрируем как полноценного пира,
      // используется только чтобы подтвердить "этот URL реально принимает соединения"
      if (opts.probe) {
        const timer = setTimeout(() => { try { ws.close(); } catch (e) {} }, 5000);
        ws.on('open', () => {
          this._send(ws, {
            type: 'fed:hello', serverId: this.serverId, serverName: this.serverName,
            serverUrl: this.serverUrl, secret: this.secret, version: '3.0', probe: true
          });
        });
        ws.on('message', (raw) => {
          try {
            const msg = JSON.parse(raw.toString());
            if (msg.type === 'fed:welcome') { clearTimeout(timer); opts.onSuccess?.(); try { ws.close(); } catch (e) {} }
          } catch (e) {}
        });
        ws.on('error', () => { clearTimeout(timer); opts.onFail?.(); });
        ws.on('close', () => { clearTimeout(timer); });
        return;
      }

      const peerData = {
        ws,
        url,
        serverId: null,
        serverName: null,
        connected: false,
        lastSeen: Date.now(),
        channels: new Map()
      };
      this.peers.set(url, peerData);

      ws.on('open', () => {
        log('🌐', `Connected to peer: ${url}`);
        this._send(ws, {
          type: 'fed:hello',
          serverId: this.serverId,
          serverName: this.serverName,
          serverUrl: this.serverUrl,
          secret: this.secret,
          peerRole: this.officialIds.has(this.serverId) ? 'official' : 'volunteer',
          version: '3.0'
        });
      });

      ws.on('message', (raw) => {
        if (raw.length > this.maxFedMessageSize) { log('⚠️', `Peer ${url} sent oversized message, disconnecting`); try { ws.close(); } catch(e){} return; }
        try {
          const msg = JSON.parse(raw.toString());
          this._handlePeerMessage(url, ws, msg);
        } catch (e) {
          console.error('❌ Peer message parse error:', e.message);
        }
      });

      ws.on('close', () => {
        peerData.connected = false;
        peerData.ws = null;
        log('🔌', `Peer connection closed: ${url}`);
        this._scheduleReconnect(url);
      });

      ws.on('error', (err) => {
        console.error(`❌ Peer connection error (${url}):`, err.message);
      });

    } catch (err) {
      console.error(`❌ Failed to connect to peer ${url}:`, err.message);
      if (!opts.probe) this._scheduleReconnect(url);
    }
  }

  /**
   * Reachability-проверка: подтверждаем, что заявленный кандидатом адрес
   * реально принимает входящие соединения, ПРЕЖДЕ чем начать доверять его
   * заявлениям о каналах на уровне полноценного волонтёрского узла.
   * Осмысленно только для настоящих Node.js серверов (не для браузера).
   */
  _verifyPeerReachable(url) {
    return new Promise((resolve) => {
      let settled = false;
      const done = (ok) => { if (settled) return; settled = true; resolve(ok); };
      this._connectToPeer(url, { probe: true, onSuccess: () => done(true), onFail: () => done(false) });
      setTimeout(() => done(false), 6000);
    });
  }

  _scheduleReconnect(url) {
    if (this._reconnectTimers.has(url)) return;
    
    const delay = 10000 + Math.random() * 5000; // 10-15 сек
    this.stats.reconnects++;
    
    const timer = setTimeout(() => {
      this._reconnectTimers.delete(url);
      this._connectToPeer(url);
    }, delay);
    
    this._reconnectTimers.set(url, timer);
  }

  // ═══════════════════════════════════════════════════════════
  // MESSAGE HANDLING
  // ═══════════════════════════════════════════════════════════

  _handleIncomingMessage(ws, msg, ip) {
    this.stats.messagesReceived++;
    
    switch (msg.type) {
      case 'fed:hello': {
        // ✅ Аутентификация timing-safe — сравнение по времени не должно выдавать длину/содержимое секрета
        if (this.secret && !timingSafeEqual(msg.secret, this.secret)) {
          log('⚠️', `Federation auth FAILED from ${ip} (server: ${msg.serverId})`);
          ws.close(4003, 'Invalid secret');
          return;
        }

        // ✅ Это просто reachability-проба от кого-то, кто проверяет нас — отвечаем и не регистрируем
        if (msg.probe) {
          this._send(ws, { type: 'fed:welcome', serverId: this.serverId, serverName: this.serverName, serverUrl: this.serverUrl });
          return;
        }

        ws._authenticated = true;
        ws._serverId = msg.serverId;
        ws._serverName = msg.serverName;
        ws._peerUrl = msg.serverUrl;
        // ✅ Уровень доверия: official — только если ID реально в нашем доверенном списке
        // (клиент не может сам себе присвоить эту роль, просто прислав peerRole:'official')
        ws._peerTrust = this.officialIds.has(msg.serverId) ? 'official' : 'volunteer-unverified';

        // Сохраняем/обновляем peer
        const existingPeer = this.peers.get(msg.serverUrl);
        const peerRecord = existingPeer || { url: msg.serverUrl, channels: new Map() };
        peerRecord.ws = ws;
        peerRecord.serverId = msg.serverId;
        peerRecord.serverName = msg.serverName;
        peerRecord.connected = true;
        peerRecord.lastSeen = Date.now();
        peerRecord.trust = ws._peerTrust;
        this.peers.set(msg.serverUrl, peerRecord);

        log('🌐', `Peer authenticated: ${msg.serverName} (${msg.serverId}) from ${ip} [${ws._peerTrust}]`);

        // Отправляем ответное приветствие + полный sync
        this._send(ws, {
          type: 'fed:welcome',
          serverId: this.serverId,
          serverName: this.serverName,
          serverUrl: this.serverUrl
        });

        this._sendFullSync(ws);
        // ✅ Делимся списком известных нам пиров, чтобы сеть доразведала себя сама
        this._send(ws, { type: 'fed:peer-list', urls: this._knownPeerUrls() });

        // ✅ Волонтёрский узел — проверяем, что заявленный им адрес реально
        // принимает входящие соединения, ПРЕЖДЕ чем полностью ему довериться
        if (ws._peerTrust === 'volunteer-unverified') {
          this._verifyPeerReachable(msg.serverUrl).then((ok) => {
            const p = this.peers.get(msg.serverUrl);
            if (!p) return;
            p.trust = ok ? 'volunteer-verified' : 'volunteer-unverified';
            if (p.ws) p.ws._peerTrust = p.trust;
            log(ok ? '✅' : '⚠️', `Reachability check for ${msg.serverId}: ${ok ? 'PASSED' : 'FAILED (kept as unverified, read-only trust)'}`);
          });
        }
        break;
      }

      case 'fed:peer-list': {
        if (!ws._authenticated) return;
        // ✅ Gossip: подключаемся к новым, ранее неизвестным пирам (в пределах maxPeers)
        (msg.urls || []).forEach(url => {
          if (typeof url !== 'string' || !url || url === this.serverUrl) return;
          if (this.peers.has(url)) return;
          if (this.peers.size >= this.maxPeers) return;
          this._connectToPeer(url);
        });
        break;
      }

      case 'fed:channel-created': {
        if (!ws._authenticated) return;
        // ✅ Непроверенный волонтёр не может застолбить уже занятое где-то имя
        const nameLower = (msg.channel?.name || '').toLowerCase().trim();
        if (ws._peerTrust !== 'official' && this.globalNames.has(nameLower)) {
          log('⚠️', `Peer ${ws._serverId} [${ws._peerTrust}] tried to claim existing name "${nameLower}", ignoring`);
          return;
        }
        this._registerRemoteChannel(msg.channel, ws._serverId, ws._serverName, msg.serverUrl);
        break;
      }
      
      case 'fed:channel-deleted': {
        if (!ws._authenticated) return;
        // ✅ Непроверенный волонтёр может удалять только каналы, которые сам же и завёл
        const existing = this.globalChannels.get(msg.channelId);
        if (existing && ws._peerTrust !== 'official' && existing.homeServer !== ws._serverId) return;
        this._unregisterRemoteChannel(msg.channelId);
        break;
      }
      
      case 'fed:channel-updated': {
        if (!ws._authenticated) return;
        this._updateRemoteChannel(msg.channel);
        break;
      }
      
      case 'fed:name-check': {
        if (!ws._authenticated) return;
        const nameLower = (msg.name || '').toLowerCase().trim();
        const exists = this.globalNames.has(nameLower) || this._localNameExists(nameLower);
        this._send(ws, {
          type: 'fed:name-check-result',
          requestId: msg.requestId,
          name: msg.name,
          exists,
          checkedBy: this.serverId
        });
        break;
      }
      
      case 'fed:name-check-result': {
        // Результат проверки имени (для callback)
        if (this._nameCheckCallbacks.has(msg.requestId)) {
          const cb = this._nameCheckCallbacks.get(msg.requestId);
          this._nameCheckCallbacks.delete(msg.requestId);
          cb(msg.exists);
        }
        break;
      }
      
      case 'fed:full-sync': {
        if (!ws._authenticated) return;
        this._handleFullSync(msg, ws._serverId, ws._serverName, msg.serverUrl);
        break;
      }
      
      case 'fed:heartbeat': {
        if (!ws._authenticated) return;
        const peer = this.peers.get(ws._peerUrl || msg.serverUrl);
        if (peer) peer.lastSeen = Date.now();
        break;
      }
      
      case 'fed:stats-request': {
        if (!ws._authenticated) return;
        this._send(ws, {
          type: 'fed:stats-response',
          serverId: this.serverId,
          channels: this.channels ? this.channels.size : 0,
          users: this.io ? this.io.engine.clientsCount : 0,
          peers: this.peers.size,
          uptime: process.uptime()
        });
        break;
      }
      
      default:
        break;
    }
  }

  _handlePeerMessage(peerUrl, ws, msg) {
    this.stats.messagesReceived++;
    const peer = this.peers.get(peerUrl);
    if (peer) peer.lastSeen = Date.now();
    
    switch (msg.type) {
      case 'fed:welcome': {
        if (peer) {
          peer.serverId = msg.serverId;
          peer.serverName = msg.serverName;
          peer.connected = true;
          // ✅ Тот же принцип доверия, что и на входящей стороне: официальным
          // считается только тот, чей serverId реально в нашем доверенном списке
          peer.trust = this.officialIds.has(msg.serverId) ? 'official' : 'volunteer-unverified';
          if (peer.trust === 'volunteer-unverified') {
            this._verifyPeerReachable(peerUrl).then((ok) => {
              const p = this.peers.get(peerUrl);
              if (!p) return;
              p.trust = ok ? 'volunteer-verified' : 'volunteer-unverified';
            });
          }
        }
        log('🌐', `Peer welcomed us: ${msg.serverName} (${msg.serverId})`);
        // Отправляем свой sync + делимся известными пирами
        this._sendFullSync(ws);
        this._send(ws, { type: 'fed:peer-list', urls: this._knownPeerUrls() });
        break;
      }

      case 'fed:peer-list': {
        (msg.urls || []).forEach(url => {
          if (typeof url !== 'string' || !url || url === this.serverUrl) return;
          if (this.peers.has(url)) return;
          if (this.peers.size >= this.maxPeers) return;
          this._connectToPeer(url);
        });
        break;
      }

      case 'fed:channel-created': {
        const nameLower = (msg.channel?.name || '').toLowerCase().trim();
        if (peer?.trust !== 'official' && this.globalNames.has(nameLower)) return;
        this._registerRemoteChannel(msg.channel, msg.serverId || peer?.serverId, msg.serverName || peer?.serverName, peerUrl);
        break;
      }
      
      case 'fed:channel-deleted': {
        const existing = this.globalChannels.get(msg.channelId);
        if (existing && peer?.trust !== 'official' && existing.homeServer !== (msg.serverId || peer?.serverId)) return;
        this._unregisterRemoteChannel(msg.channelId);
        break;
      }
      
      case 'fed:channel-updated': {
        this._updateRemoteChannel(msg.channel);
        break;
      }
      
      case 'fed:name-check': {
        const nameLower = (msg.name || '').toLowerCase().trim();
        const exists = this.globalNames.has(nameLower) || this._localNameExists(nameLower);
        this._send(ws, {
          type: 'fed:name-check-result',
          requestId: msg.requestId,
          name: msg.name,
          exists,
          checkedBy: this.serverId
        });
        break;
      }
      
      case 'fed:name-check-result': {
        if (this._nameCheckCallbacks.has(msg.requestId)) {
          const cb = this._nameCheckCallbacks.get(msg.requestId);
          this._nameCheckCallbacks.delete(msg.requestId);
          cb(msg.exists);
        }
        break;
      }
      
      case 'fed:full-sync': {
        this._handleFullSync(msg, msg.serverId || peer?.serverId, msg.serverName || peer?.serverName, peerUrl);
        break;
      }
      
      case 'fed:heartbeat': {
        if (peer) peer.lastSeen = Date.now();
        break;
      }
      
      default:
        break;
    }
  }

  // ═══════════════════════════════════════════════════════════
  // CHANNEL REGISTRY (глобальный реестр)
  // ═══════════════════════════════════════════════════════════

  _registerRemoteChannel(channelData, serverId, serverName, serverUrl) {
    if (!channelData || !channelData.id) return;
    
    const entry = {
      id: channelData.id,
      name: channelData.name,
      admin: channelData.admin,
      adminName: channelData.adminName,
      speakers: channelData.speakers || 0,
      listeners: channelData.listeners || 0,
      totalLikes: channelData.totalLikes || 0,
      requireApproval: channelData.requireApproval !== false,
      homeServer: serverId,
      homeServerName: serverName,
      homeServerUrl: serverUrl,
      isRemote: true,
      lastUpdate: Date.now()
    };
    
    this.globalChannels.set(channelData.id, entry);
    this.globalNames.set(channelData.name.toLowerCase().trim(), channelData.id);
    
    // Уведомляем клиентов
    if (this.io) {
      this.io.emit('federation-channels-updated', this.getGlobalChannelList());
    }
    
    log('🌐', `Remote channel registered: "${channelData.name}" from ${serverName}`);
  }

  _unregisterRemoteChannel(channelId) {
    const entry = this.globalChannels.get(channelId);
    if (entry) {
      this.globalNames.delete(entry.name.toLowerCase().trim());
      this.globalChannels.delete(channelId);
      
      if (this.io) {
        this.io.emit('federation-channels-updated', this.getGlobalChannelList());
      }
      
      log('🌐', `Remote channel removed: ${channelId}`);
    }
  }

  _updateRemoteChannel(channelData) {
    const existing = this.globalChannels.get(channelData.id);
    if (existing) {
      Object.assign(existing, {
        speakers: channelData.speakers ?? existing.speakers,
        listeners: channelData.listeners ?? existing.listeners,
        totalLikes: channelData.totalLikes ?? existing.totalLikes,
        requireApproval: channelData.requireApproval ?? existing.requireApproval,
        lastUpdate: Date.now()
      });
      
      if (this.io) {
        this.io.emit('federation-channels-updated', this.getGlobalChannelList());
      }
    }
  }

  _handleFullSync(msg, serverId, serverName, serverUrl) {
    if (!msg.channels || !Array.isArray(msg.channels)) return;
    
    // Удаляем старые каналы этого сервера
    for (const [id, ch] of this.globalChannels) {
      if (ch.homeServer === serverId) {
        this.globalNames.delete(ch.name.toLowerCase().trim());
        this.globalChannels.delete(id);
      }
    }
    
    // Регистрируем новые
    msg.channels.forEach(ch => {
      this._registerRemoteChannel(ch, serverId, serverName, serverUrl);
    });
    
    this.stats.lastSync = Date.now();
    log('🌐', `Full sync from ${serverName}: ${msg.channels.length} channel(s)`);
  }

  // ═══════════════════════════════════════════════════════════
  // PUBLIC API (вызывается из основного сервера)
  // ═══════════════════════════════════════════════════════════

  /**
   * Проверка глобальной уникальности имени канала
   * @returns {Promise<boolean>} true если имя занято
   */
  async isNameTakenGlobally(name) {
    const nameLower = name.toLowerCase().trim();
    
    // Проверяем локально
    if (this._localNameExists(nameLower)) return true;
    
    // Проверяем в глобальном реестре
    if (this.globalNames.has(nameLower)) return true;
    
    // Если нет подключённых пиров — только локальная проверка
    const connectedPeers = [...this.peers.values()].filter(p => p.connected && p.ws);
    if (connectedPeers.length === 0) return false;
    
    // Запрашиваем у всех пиров
    return new Promise((resolve) => {
      const requestId = `nc_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
      let responses = 0;
      let anyExists = false;
      const totalPeers = connectedPeers.length;
      
      const timeout = setTimeout(() => {
        this._nameCheckCallbacks.delete(requestId);
        resolve(anyExists); // Если таймаут — используем то что знаем
      }, 3000);
      
      this._nameCheckCallbacks.set(requestId, (exists) => {
        if (exists) anyExists = true;
        responses++;
        if (responses >= totalPeers) {
          clearTimeout(timeout);
          this._nameCheckCallbacks.delete(requestId);
          resolve(anyExists);
        }
      });
      
      connectedPeers.forEach(peer => {
        this._send(peer.ws, {
          type: 'fed:name-check',
          requestId,
          name: name
        });
      });
    });
  }

  /**
   * Уведомить пиров о создании канала
   */
  broadcastChannelCreated(channel) {
    const data = {
      id: channel.id,
      name: channel.name,
      admin: channel.admin,
      adminName: channel.adminName,
      speakers: channel.speakers ? channel.speakers.size : 0,
      listeners: channel.listeners ? channel.listeners.size : 0,
      totalLikes: channel.totalLikes || 0,
      requireApproval: channel.requireApproval
    };
    
    this._broadcast({
      type: 'fed:channel-created',
      channel: data,
      serverId: this.serverId,
      serverName: this.serverName,
      serverUrl: this.serverUrl
    });
    
    // Регистрируем локально
    this.globalNames.set(channel.name.toLowerCase().trim(), channel.id);
  }

  /**
   * Уведомить пиров об удалении канала
   */
  broadcastChannelDeleted(channelId, channelName) {
    this._broadcast({
      type: 'fed:channel-deleted',
      channelId,
      serverId: this.serverId
    });
    
    if (channelName) {
      this.globalNames.delete(channelName.toLowerCase().trim());
    }
  }

  /**
   * Уведомить пиров об обновлении канала
   */
  broadcastChannelUpdated(channel) {
    this._broadcast({
      type: 'fed:channel-updated',
      channel: {
        id: channel.id,
        speakers: channel.speakers ? channel.speakers.size : 0,
        listeners: channel.listeners ? channel.listeners.size : 0,
        totalLikes: channel.totalLikes || 0,
        requireApproval: channel.requireApproval
      },
      serverId: this.serverId
    });
  }

  /**
   * Получить глобальный список каналов (локальные + удалённые)
   */
  getGlobalChannelList() {
    const list = [];
    
    // Локальные каналы
    if (this.channels) {
      this.channels.forEach((ch, id) => {
        list.push({
          id,
          name: ch.name,
          admin: ch.adminName,
          adminId: ch.admin,
          speakers: ch.speakers.size,
          listeners: ch.listeners.size,
          totalLikes: ch.totalLikes || 0,
          requireApproval: ch.requireApproval,
          homeServer: this.serverId,
          homeServerName: this.serverName,
          homeServerUrl: this.serverUrl,
          isRemote: false
        });
      });
    }
    
    // Удалённые каналы
    this.globalChannels.forEach((ch) => {
      list.push({
        id: ch.id,
        name: ch.name,
        admin: ch.adminName,
        adminId: ch.admin,
        speakers: ch.speakers,
        listeners: ch.listeners,
        totalLikes: ch.totalLikes,
        requireApproval: ch.requireApproval,
        homeServer: ch.homeServer,
        homeServerName: ch.homeServerName,
        homeServerUrl: ch.homeServerUrl,
        isRemote: true
      });
    });
    
    return list;
  }

  /**
   * Проверить, является ли канал локальным
   */
  isLocalChannel(channelId) {
    if (this.channels && this.channels.has(channelId)) return true;
    const remote = this.globalChannels.get(channelId);
    return !remote || !remote.isRemote;
  }

  /**
   * Получить URL домашнего сервера канала
   */
  getChannelHomeUrl(channelId) {
    if (this.channels && this.channels.has(channelId)) return this.serverUrl;
    const remote = this.globalChannels.get(channelId);
    return remote ? remote.homeServerUrl : null;
  }

  /**
   * Статус федерации
   */
  getStatus() {
    const connectedPeers = [...this.peers.values()].filter(p => p.connected);
    return {
      serverId: this.serverId,
      serverName: this.serverName,
      serverUrl: this.serverUrl,
      isOfficial: this.officialIds.has(this.serverId),
      peersTotal: this.peers.size,
      peersConnected: connectedPeers.length,
      maxPeers: this.maxPeers,
      globalChannels: this.globalChannels.size,
      localChannels: this.channels ? this.channels.size : 0,
      stats: this.stats,
      peers: connectedPeers.map(p => ({
        serverId: p.serverId,
        serverName: p.serverName,
        url: p.url,
        trust: p.trust || 'unknown',
        lastSeen: p.lastSeen
      }))
    };
  }

  // ═══════════════════════════════════════════════════════════
  // INTERNAL HELPERS
  // ═══════════════════════════════════════════════════════════

  _nameCheckCallbacks = new Map();

  _localNameExists(nameLower) {
    if (!this.channels) return false;
    for (const [, ch] of this.channels) {
      if (ch.name.toLowerCase().trim() === nameLower) return true;
    }
    return false;
  }

  _send(ws, data) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    try {
      ws.send(JSON.stringify(data));
      this.stats.messagesSent++;
      return true;
    } catch (e) {
      console.error('❌ Federation send error:', e.message);
      return false;
    }
  }

  _broadcast(data) {
    this.peers.forEach((peer) => {
      if (peer.connected && peer.ws) {
        this._send(peer.ws, data);
      }
    });
  }

  _sendFullSync(ws) {
    const channelList = [];
    if (this.channels) {
      this.channels.forEach((ch, id) => {
        channelList.push({
          id,
          name: ch.name,
          admin: ch.admin,
          adminName: ch.adminName,
          speakers: ch.speakers.size,
          listeners: ch.listeners.size,
          totalLikes: ch.totalLikes || 0,
          requireApproval: ch.requireApproval
        });
      });
    }
    
    this._send(ws, {
      type: 'fed:full-sync',
      serverId: this.serverId,
      serverName: this.serverName,
      serverUrl: this.serverUrl,
      channels: channelList,
      timestamp: Date.now()
    });
  }

  _broadcastFullSync() {
    this.peers.forEach((peer) => {
      if (peer.connected && peer.ws) {
        this._sendFullSync(peer.ws);
      }
    });
  }

  /**
   * Список известных нам адресов пиров — рассылается остальным,
   * чтобы сеть достраивала себя сама (нужен только один bootstrap-адрес на старте)
   */
  _knownPeerUrls() {
    const urls = [this.serverUrl];
    this.peers.forEach((p) => { if (p.connected) urls.push(p.url); });
    return [...new Set(urls)];
  }

  _gossipPeerList() {
    if (this.peers.size >= this.maxPeers) return; // сеть уже достаточно большая для нас
    const payload = { type: 'fed:peer-list', urls: this._knownPeerUrls() };
    this.peers.forEach((peer) => { if (peer.connected && peer.ws) this._send(peer.ws, payload); });
  }

  _sendHeartbeats() {
    this._broadcast({
      type: 'fed:heartbeat',
      serverId: this.serverId,
      timestamp: Date.now()
    });
  }

  _cleanupDeadPeers() {
    const now = Date.now();
    const DEAD_TIMEOUT = 90000; // 90 секунд без heartbeat
    
    this.peers.forEach((peer, url) => {
      if (!peer.connected && now - peer.lastSeen > DEAD_TIMEOUT) {
        // Удаляем каналы мёртвого пира
        for (const [id, ch] of this.globalChannels) {
          if (ch.homeServer === peer.serverId) {
            this.globalNames.delete(ch.name.toLowerCase().trim());
            this.globalChannels.delete(id);
          }
        }
        
        if (peer.ws) {
          try { peer.ws.close(); } catch(e) {}
        }
        
        this.peers.delete(url);
        log('🗑️', `Dead peer removed: ${url}`);
        
        if (this.io) {
          this.io.emit('federation-channels-updated', this.getGlobalChannelList());
        }
      }
    });
  }

  /**
   * Остановка федерации
   */
  shutdown() {
    if (this._heartbeatTimer) clearInterval(this._heartbeatTimer);
    if (this._syncTimer) clearInterval(this._syncTimer);
    if (this._gossipTimer) clearInterval(this._gossipTimer);
    this._reconnectTimers.forEach(t => clearTimeout(t));
    this._reconnectTimers.clear();
    
    this.peers.forEach(peer => {
      if (peer.ws) {
        try { peer.ws.close(1001, 'Server shutting down'); } catch(e) {}
      }
    });
    
    if (this.wss) {
      try { this.wss.close(); } catch(e) {}
    }
    
    log('🌐', 'Federation module shut down');
  }
}

function log(icon, message) {
  const time = new Date().toLocaleTimeString('en-US', { hour12: false });
  console.log(`[${time}] ${icon} ${message}`);
}

module.exports = Federation;