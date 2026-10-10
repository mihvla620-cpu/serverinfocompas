/* =========================================================
   Info Compas — backend server
   ---------------------------------------------------------
   Что делает:
     • Раздаёт статику из ./public
     • WebSocket /ws — real-time синхронизация по комнатам
     • Хранит состояние в ./data/rooms.json (автосохранение)
     • Ротация бэкапов в ./data/backups/
     • Presence — кто онлайн в каждой комнате

   Запуск:  node server.js
   Порт:    PORT=3000 (или .env)
   ========================================================= */
'use strict';

require('dotenv').config();

const express     = require('express');
const http        = require('http');
const path        = require('path');
const fs          = require('fs');
const fsp         = fs.promises;
const compression = require('compression');
const { WebSocketServer } = require('ws');

/* ---------- конфиг ---------- */
const PORT             = parseInt(process.env.PORT || '3000', 10);
const HOST             = process.env.HOST || '0.0.0.0';
const ROOM_PASSWORD    = process.env.ROOM_PASSWORD || '';        // '' = без пароля
const SAVE_DEBOUNCE_MS = 2000;
const BACKUP_KEEP      = 20;
const PRESENCE_TTL_MS  = 5 * 60 * 1000;
const MSG_RATE_LIMIT   = 60;                                    // сообщений / минуту / соединение

const DATA_DIR    = path.join(__dirname, 'data');
const DATA_FILE   = path.join(DATA_DIR, 'rooms.json');
const BACKUP_DIR  = path.join(DATA_DIR, 'backups');
const PUBLIC_DIR  = path.join(__dirname, 'public');

/* ---------- гарантируем папки ---------- */
fs.mkdirSync(DATA_DIR,   { recursive: true });
fs.mkdirSync(BACKUP_DIR, { recursive: true });

/* =========================================================
   Хранилище комнат
   =========================================================
   rooms = {
     [roomId]: {
       state: { articles:[], news:[], ... },
       clients: Set<WebSocket>,
       presence: { [userId]: { name, role, isDev, lastSeen, device } },
       dirty: false,
       saveTimer: null,
       passOk: Set<WebSocket>   // соединения, прошедшие пароль
     }
   }
   ========================================================= */
const rooms = new Map();
let   globalSaveTimer = null;

function getRoom(id) {
  if (!rooms.has(id)) {
    rooms.set(id, {
      state: {
        articles: [], news: [], categories: [], gallery: [],
        dictionaries: [], forum: [], users: [], trash: [],
        customTabs: [], discussions: {},
        siteDescription: '', siteLinks: [],
        adBanner: { enabled: false, image: '', link: '', text: '' },
        theme: 'default', customLogo: '',
        updatedAt: Date.now()
      },
      clients: new Set(),
      presence: {},
      dirty: false,
      saveTimer: null,
      passOk: new Set()
    });
  }
  return rooms.get(id);
}

/* ---------- загрузка/сохранение ---------- */
async function loadData() {
  try {
    const raw = await fsp.readFile(DATA_FILE, 'utf8');
    const obj = JSON.parse(raw);
    Object.keys(obj).forEach(roomId => {
      const room = getRoom(roomId);
      room.state = { ...room.state, ...obj[roomId] };
    });
    console.log(`[data] загружено комнат: ${Object.keys(obj).length}`);
  } catch (e) {
    if (e.code !== 'ENOENT') console.warn('[data] ошибка чтения:', e.message);
    else console.log('[data] файл не найден — начинаем с чистого листа');
  }
}

function scheduleSave() {
  if (globalSaveTimer) return;
  globalSaveTimer = setTimeout(flushAll, SAVE_DEBOUNCE_MS);
}

async function flushAll() {
  globalSaveTimer = null;
  const out = {};
  for (const [id, room] of rooms) {
    out[id] = room.state;
  }
  try {
    const tmp = DATA_FILE + '.tmp';
    await fsp.writeFile(tmp, JSON.stringify(out, null, 2), 'utf8');
    await fsp.rename(tmp, DATA_FILE);
  } catch (e) {
    console.error('[data] ошибка записи:', e.message);
  }
}

function scheduleBackup() {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const file  = path.join(BACKUP_DIR, `rooms-${stamp}.json`);
  const out   = {};
  for (const [id, room] of rooms) out[id] = room.state;
  fsp.writeFile(file, JSON.stringify(out), 'utf8')
    .then(async () => {
      const files = (await fsp.readdir(BACKUP_DIR))
        .filter(f => f.startsWith('rooms-') && f.endsWith('.json'))
        .sort();
      while (files.length > BACKUP_KEEP) {
        await fsp.unlink(path.join(BACKUP_DIR, files.shift())).catch(() => {});
      }
    })
    .catch(() => {});
}
setInterval(scheduleBackup, 30 * 60 * 1000);   // бэкап раз в 30 минут

/* =========================================================
   Merge (серверная сторона) — умное слияние
   =========================================================
   Для массивов: заменяем целиком (уважаем удаления)
   Для объекта discussions: мержим по ключам
   Для скаляров: заменяем
   ========================================================= */
function mergeState(current, patch) {
  if (!patch || typeof patch !== 'object') return current;
  const out = { ...current };
  for (const key of Object.keys(patch)) {
    if (key === 'discussions' && patch.discussions && typeof patch.discussions === 'object') {
      out.discussions = { ...(current.discussions || {}) };
      for (const dk of Object.keys(patch.discussions)) {
        out.discussions[dk] = patch.discussions[dk];
      }
    } else {
      out[key] = patch[key];
    }
  }
  out.updatedAt = Date.now();
  return out;
}

/* =========================================================
   HTTP-сервер (Express)
   ========================================================= */
const app = express();
app.use(compression());
app.use(express.json({ limit: '25mb' }));
app.use(express.static(PUBLIC_DIR, { maxAge: '1h' }));

// REST: получить полное состояние комнаты
app.get('/api/state', (req, res) => {
  const roomId = String(req.query.room || 'main').slice(0, 100);
  if (ROOM_PASSWORD && String(req.query.pass || '') !== ROOM_PASSWORD) {
    return res.status(403).json({ ok: false, error: 'password required' });
  }
  const room = getRoom(roomId);
  res.json({ ok: true, room: roomId, state: room.state });
});

// REST: healthcheck
app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    uptime: process.uptime(),
    rooms: [...rooms.keys()],
    clients: [...rooms.values()].reduce((s, r) => s + r.clients.size, 0)
  });
});

// SPA-fallback: всё, что не api и не статика — отдаём index.html
app.get('*', (req, res, next) => {
  if (req.path.startsWith('/api') || req.path.startsWith('/ws')) return next();
  res.sendFile(path.join(PUBLIC_DIR, 'index.html'), err => {
    if (err) res.status(404).send('index.html not found');
  });
});

const server = http.createServer(app);

/* =========================================================
   WebSocket-сервер
   ========================================================= */
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  ws.isAlive  = true;
  ws.roomId   = null;
  ws.userId   = null;
  ws.msgCount = 0;
  ws.msgReset = Date.now();

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    // rate-limit
    const now = Date.now();
    if (now - ws.msgReset > 60_000) { ws.msgCount = 0; ws.msgReset = now; }
    if (++ws.msgCount > MSG_RATE_LIMIT) {
      return ws.send(JSON.stringify({ type: 'error', message: 'rate limit' }));
    }

    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { return; }

    handleMessage(ws, msg);
  });

  ws.on('close', () => {
    if (ws.roomId) {
      const room = rooms.get(ws.roomId);
      if (room) {
        room.clients.delete(ws);
        room.passOk.delete(ws);
        if (ws.userId != null) {
          delete room.presence[ws.userId];
          broadcastPresence(room, ws.userId, null);
        }
      }
    }
  });

  ws.on('error', () => {});
});

/* --- обработка входящих сообщений --- */
function handleMessage(ws, msg) {
  if (!msg || typeof msg !== 'object') return;

  switch (msg.type) {
    case 'hello': {
      const roomId = String(msg.topic || 'main').slice(0, 100);
      const pass   = String(msg.pass || '');

      // Проверка пароля комнаты
      if (ROOM_PASSWORD && pass !== ROOM_PASSWORD) {
        return ws.send(JSON.stringify({ type: 'auth-required' }));
      }

      ws.roomId = roomId;
      ws.userId = msg.userId ?? null;
      const room = getRoom(roomId);

      // Отключаем от старой комнаты, если была
      if (ws.roomId && rooms.has(roomId)) {
        // (повторный hello — просто переподключение)
      }

      room.clients.add(ws);
      room.passOk.add(ws);

      // Приветственное сообщение с полным состоянием
      send(ws, {
        type: 'welcome',
        room: roomId,
        state: room.state,
        presence: room.presence
      });

      // Оповещаем остальных о presence
      if (ws.userId != null && msg.userInfo) {
        room.presence[ws.userId] = {
          id: ws.userId,
          name: msg.userInfo.name || 'Аноним',
          role: msg.userInfo.role || 'guest',
          isDev: !!msg.userInfo.isDev,
          device: (msg.userInfo.device || '').slice(0, 120),
          lastSeen: Date.now()
        };
        broadcastPresence(room, ws.userId, room.presence[ws.userId]);
      }

      console.log(`[ws] + ${roomId} (клиентов: ${room.clients.size}, юзер: ${ws.userId})`);
      break;
    }

    case 'patch': {
      if (!ws.roomId) return;
      const room = getRoom(ws.roomId);
      const next = mergeState(room.state, msg.payload);
      room.state = next;
      room.dirty = true;
      scheduleSave();

      // Рассылаем всем в комнате, кроме отправителя
      broadcast(room, { type: 'patch', payload: msg.payload, from: ws.userId }, ws);
      break;
    }

    case 'request-full': {
      if (!ws.roomId) return;
      const room = getRoom(ws.roomId);
      send(ws, { type: 'state-full', state: room.state, presence: room.presence });
      break;
    }

    case 'presence': {
      if (!ws.roomId || ws.userId == null) return;
      const room = getRoom(ws.roomId);
      room.presence[ws.userId] = {
        ...(room.presence[ws.userId] || {}),
        ...(msg.data || {}),
        id: ws.userId,
        lastSeen: Date.now()
      };
      broadcastPresence(room, ws.userId, room.presence[ws.userId]);
      break;
    }

    case 'ping': {
      send(ws, { type: 'pong', t: msg.t || Date.now() });
      break;
    }
  }
}

/* --- утилиты рассылки --- */
function send(ws, obj) {
  if (ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch {}
  }
}
function broadcast(room, obj, exceptWs) {
  const str = JSON.stringify(obj);
  for (const client of room.clients) {
    if (client === exceptWs) continue;
    if (client.readyState !== client.OPEN) continue;
    try { client.send(str); } catch {}
  }
}
function broadcastPresence(room, userId, data) {
  broadcast(room, { type: 'presence', userId, data });
}

/* --- heartbeat: убиваем мёртвые соединения каждые 30 сек --- */
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { try { ws.terminate(); } catch {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
  // чистим протухший presence
  const cutoff = Date.now() - PRESENCE_TTL_MS;
  for (const room of rooms.values()) {
    for (const [uid, p] of Object.entries(room.presence)) {
      if ((p.lastSeen || 0) < cutoff) {
        delete room.presence[uid];
        broadcastPresence(room, Number(uid), null);
      }
    }
  }
}, 30_000);

/* =========================================================
   Запуск
   ========================================================= */
async function start() {
  await loadData();

  server.listen(PORT, HOST, () => {
    console.log('');
    console.log('  ┌───────────────────────────────────────────┐');
    console.log(`  │  Info Compas backend                      │`);
    console.log(`  │  http://${HOST}:${PORT}                       │`);
    console.log(`  │  WebSocket: ws://${HOST}:${PORT}/ws           │`);
    console.log(`  │  Пароль комнаты: ${ROOM_PASSWORD ? 'да' : 'нет'}                        │`);
    console.log('  └───────────────────────────────────────────┘');
    console.log('');
  });
}

/* --- graceful shutdown --- */
async function shutdown(sig) {
  console.log(`\n[server] ${sig} — сохраняю данные…`);
  await flushAll();
  console.log('[server] готово, выходим');
  process.exit(0);
}
process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

process.on('uncaughtException', e => {
  console.error('[server] uncaughtException:', e);
  flushAll().finally(() => process.exit(1));
});

start();