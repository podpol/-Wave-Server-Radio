/**
 * ═══════════════════════════════════════════════════════════
 * Wave 🌊 — STUN Server v1.0 (RFC 5389/8489)
 * ═══════════════════════════════════════════════════════════
 * Самописный UDP STUN. Нужен только для того, чтобы клиенты
 * WebRTC узнали свой внешний адрес (NAT mapping).
 * Не хранит состояния, не требует БД.
 */

'use strict';

const dgram = require('dgram');

const PORT = process.env.STUN_PORT || 3478;
const HOST = process.env.STUN_HOST || '0.0.0.0';

const MAGIC_COOKIE = 0x2112A442;
const MAGIC_COOKIE_BUF = Buffer.alloc(4);
MAGIC_COOKIE_BUF.writeUInt32BE(MAGIC_COOKIE, 0);

// ─── STUN Constants ───
const BINDING_REQUEST  = 0x0001;
const BINDING_SUCCESS  = 0x0101;
const ATTR_XOR_MAPPED_ADDRESS = 0x0020;
const ATTR_SOFTWARE           = 0x8022;
const ATTR_FINGERPRINT        = 0x8028;

// ─── CRC32 table for FINGERPRINT ───
let crcTable = null;
function makeCrcTable() {
  if (crcTable) return crcTable;
  crcTable = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) {
      c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    }
    crcTable[i] = c >>> 0;
  }
  return crcTable;
}
function crc32(buf) {
  const table = makeCrcTable();
  let c = 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) {
    c = table[(c ^ buf[i]) & 0xFF] ^ (c >>> 8);
  }
  return (c ^ 0xFFFFFFFF) >>> 0;
}

// ─── Attribute builders ───
function writeXorMappedAddress(rinfo) {
  const parts = rinfo.address.split('.').map(Number);
  const port = rinfo.port;
  const attr = Buffer.alloc(12);
  attr.writeUInt16BE(ATTR_XOR_MAPPED_ADDRESS, 0);
  attr.writeUInt16BE(8, 2);
  attr.writeUInt8(0, 4);
  attr.writeUInt8(0x01, 5); // IPv4
  attr.writeUInt16BE(port ^ (MAGIC_COOKIE >> 16), 6);
  attr.writeUInt32BE(
    (((parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]) ^ MAGIC_COOKIE) >>> 0,
    8
  );
  return attr;
}
function writeSoftware() {
  const software = 'Wave STUN v1.0';
  const buf = Buffer.from(software, 'utf8');
  const pad = (4 - (buf.length % 4)) % 4;
  const attr = Buffer.alloc(4 + buf.length + pad);
  attr.writeUInt16BE(ATTR_SOFTWARE, 0);
  attr.writeUInt16BE(buf.length, 2);
  buf.copy(attr, 4);
  return attr;
}
function writeFingerprint(msg) {
  const xorCrc = (crc32(msg) ^ 0x5354554e) >>> 0;
  const attr = Buffer.alloc(8);
  attr.writeUInt16BE(ATTR_FINGERPRINT, 0);
  attr.writeUInt16BE(4, 2);
  attr.writeUInt32BE(xorCrc, 4);
  return attr;
}

// ─── Build Binding Success Response ───
function createBindingResponse(req, rinfo) {
  const tid = req.slice(8, 20);
  const xorAddr = writeXorMappedAddress(rinfo);
  const software = writeSoftware();
  const body = Buffer.concat([xorAddr, software]);
  const header = Buffer.alloc(20);
  header.writeUInt16BE(BINDING_SUCCESS, 0);
  header.writeUInt16BE(body.length, 2);
  MAGIC_COOKIE_BUF.copy(header, 4);
  tid.copy(header, 8);
  const withoutFp = Buffer.concat([header, body]);
  return Buffer.concat([withoutFp, writeFingerprint(withoutFp)]);
}

// ─── UDP Socket ───
const server = dgram.createSocket('udp4');

server.on('message', (msg, rinfo) => {
  if (msg.length < 20) return;
  const msgType = msg.readUInt16BE(0);
  const magic   = msg.readUInt32BE(4);
  if (magic !== MAGIC_COOKIE || msgType !== BINDING_REQUEST) return;

  const response = createBindingResponse(msg, rinfo);
  server.send(response, rinfo.port, rinfo.address, () => {});
});

server.on('error', (err) => {
  console.error(`[STUN] Error: ${err.message}`);
});

server.on('listening', () => {
  const a = server.address();
  console.log(`[STUN] Listening on ${a.address}:${a.port}`);
  console.log(`[STUN] Clients use: stun:${a.address}:${a.port}`);
});

server.bind(PORT, HOST);

// Graceful shutdown
process.on('SIGTERM', () => server.close());
process.on('SIGINT',  () => server.close());
