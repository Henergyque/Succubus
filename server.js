'use strict';
// v1.1.0
const express = require('express');
const rateLimit = require('express-rate-limit');
const Database = require('better-sqlite3');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const fs = require('fs');

// ---------- Config (env) ----------
const PORT = parseInt(process.env.PORT || '3000', 10);
const GAME_TOKEN = process.env.GAME_TOKEN || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DB_DIR = process.env.DB_DIR || '/data';
const DB_PATH = path.join(DB_DIR, 'telemetry.db');
const ACTIVE_WINDOW_MS = 2 * 60 * 1000; // session counted "online" if event within 2 min
const DISCORD_WEBHOOK_URL = process.env.DISCORD_WEBHOOK_URL || '';

if (!GAME_TOKEN) console.warn('[boot] GAME_TOKEN env not set; /v1/event will reject everything.');
if (!ADMIN_TOKEN) console.warn('[boot] ADMIN_TOKEN env not set; admin endpoints will reject everything.');

// ---------- DB ----------
try { fs.mkdirSync(DB_DIR, { recursive: true }); } catch (e) {}
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
try { db.exec(`ALTER TABLE announcements ADD COLUMN view_count INTEGER NOT NULL DEFAULT 0`); } catch (e) {}

db.exec(`
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  player_id TEXT NOT NULL,
  start_ts INTEGER NOT NULL,
  end_ts INTEGER,
  last_seen INTEGER NOT NULL,
  last_map_id INTEGER,
  last_zone TEXT,
  version TEXT,
  platform TEXT,
  locale TEXT
);
CREATE INDEX IF NOT EXISTS idx_sessions_last_seen ON sessions(last_seen);
CREATE INDEX IF NOT EXISTS idx_sessions_end_ts ON sessions(end_ts);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  player_id TEXT NOT NULL,
  ts INTEGER NOT NULL,
  type TEXT NOT NULL,
  map_id INTEGER,
  zone TEXT,
  payload TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);

CREATE TABLE IF NOT EXISTS concurrent_snapshots (
  ts INTEGER PRIMARY KEY,
  count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v TEXT
);
CREATE TABLE IF NOT EXISTS announcements (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  url TEXT,
  type TEXT DEFAULT 'info',
  version TEXT,
  expiresAt INTEGER,
  active INTEGER NOT NULL DEFAULT 1,
  created_at INTEGER NOT NULL
);
`);

const insertEvent = db.prepare(`
  INSERT INTO events (session_id, player_id, ts, type, map_id, zone, payload)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const upsertSessionStart = db.prepare(`
  INSERT INTO sessions (id, player_id, start_ts, last_seen, last_map_id, last_zone, version, platform, locale)
  VALUES (@id, @player_id, @ts, @ts, NULL, NULL, @version, @platform, @locale)
  ON CONFLICT(id) DO UPDATE SET last_seen=@ts, version=COALESCE(@version, version), platform=COALESCE(@platform, platform), locale=COALESCE(@locale, locale)
`);

const updateSessionTick = db.prepare(`
  UPDATE sessions SET last_seen=@ts, last_map_id=COALESCE(@map_id, last_map_id), last_zone=COALESCE(@zone, last_zone)
  WHERE id=@id
`);

const ensureSessionRow = db.prepare(`
  INSERT OR IGNORE INTO sessions (id, player_id, start_ts, last_seen, last_map_id, last_zone)
  VALUES (@id, @player_id, @ts, @ts, @map_id, @zone)
`);

const endSession = db.prepare(`
  UPDATE sessions SET end_ts=@ts, last_seen=@ts, last_map_id=COALESCE(@map_id, last_map_id), last_zone=COALESCE(@zone, last_zone)
  WHERE id=@id
`);

const getMeta = db.prepare(`SELECT v FROM meta WHERE k=?`);
const setMeta = db.prepare(`INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`);

const getAnnouncement = db.prepare(`SELECT id, title, body, url, type, version, expiresAt, created_at, view_count FROM announcements WHERE active = 1 ORDER BY created_at DESC LIMIT 1`);
const incrementViewCount = db.prepare(`UPDATE announcements SET view_count = view_count + 1 WHERE id = ?`);
const insertAnnouncement = db.prepare(`INSERT INTO announcements (title, body, url, type, version, expiresAt, active, created_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?)`);
const deactivateAnnouncements = db.prepare(`UPDATE announcements SET active = 0 WHERE active = 1`);

function currentAnnouncement() {
  const row = getAnnouncement.get();
  if (!row) return null;
  if (row.expiresAt && row.expiresAt < Date.now()) return null;
  return {
    id: row.id,
    title: row.title,
    body: row.body,
    url: row.url,
    type: row.type,
    version: row.version,
    expiresAt: row.expiresAt,
    createdAt: row.created_at,
    viewCount: row.view_count || 0
  };
}

function publishAnnouncement({ title, body, url, type, version, expiresAt }) {
  deactivateAnnouncements.run();
  insertAnnouncement.run(
    String(title || '').slice(0, 128),
    String(body || '').slice(0, 2048),
    url ? String(url).slice(0, 1024) : null,
    String(type || 'info').slice(0, 32),
    version ? String(version).slice(0, 32) : null,
    expiresAt ? parseInt(expiresAt, 10) : null,
    Date.now()
  );
}

// ---------- App ----------
const app = express();
app.set('trust proxy', 1);
app.use(express.json({ limit: '256kb' }));

const eventLimiter = rateLimit({ windowMs: 60 * 1000, max: 120 });

app.get('/health', (req, res) => res.json({ ok: true }));

// ---------- Ingest ----------
app.post('/v1/event', eventLimiter, (req, res) => {
  if (!GAME_TOKEN || req.get('X-Game-Token') !== GAME_TOKEN) {
    return res.status(401).json({ error: 'invalid token' });
  }
  const body = req.body || {};
  const events = Array.isArray(body.events) ? body.events : [];
  if (events.length === 0) return res.json({ ok: true, accepted: 0 });
  if (events.length > 50) return res.status(413).json({ error: 'batch too large' });

  const now = Date.now();
  let dirtyTypes = new Set();

  const tx = db.transaction((list) => {
    for (const e of list) {
      if (!e || typeof e !== 'object') continue;
      const sid = String(e.sessionId || '').slice(0, 64);
      const pid = String(e.playerId || '').slice(0, 64);
      const type = String(e.type || '').slice(0, 32);
      if (!sid || !pid || !type) continue;
      const ts = Math.min(now, Math.max(now - 24*3600*1000, parseInt(e.ts || now, 10)));
      const mapId = Number.isFinite(e.mapId) ? parseInt(e.mapId, 10) : null;
      const zone = e.zone ? String(e.zone).slice(0, 32) : null;
      const payload = JSON.stringify(e);

      insertEvent.run(sid, pid, ts, type, mapId, zone, payload);

      if (type === 'session_start') {
        upsertSessionStart.run({
          id: sid, player_id: pid, ts,
          version: e.version ? String(e.version).slice(0, 32) : null,
          platform: e.platform ? String(e.platform).slice(0, 32) : null,
          locale: e.locale ? String(e.locale).slice(0, 16) : null
        });
      } else if (type === 'session_end') {
        ensureSessionRow.run({ id: sid, player_id: pid, ts, map_id: mapId, zone });
        endSession.run({ id: sid, ts, map_id: mapId, zone });
      } else {
        ensureSessionRow.run({ id: sid, player_id: pid, ts, map_id: mapId, zone });
        updateSessionTick.run({ id: sid, ts, map_id: mapId, zone });
      }
      dirtyTypes.add(type);
    }
  });
  tx(events);

  scheduleBroadcast();
  res.json({ ok: true, accepted: events.length });
});

// ---------- Admin auth ----------
function requireAdmin(req, res, next) {
  const h = req.get('Authorization') || '';
  if (!ADMIN_TOKEN || h !== `Bearer ${ADMIN_TOKEN}`) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

// ---------- Stats queries ----------
function liveStats() {
  const cutoff = Date.now() - ACTIVE_WINDOW_MS;
  const rows = db.prepare(`
    SELECT id, last_zone AS zone, last_map_id AS mapId
    FROM sessions
    WHERE last_seen >= ? AND end_ts IS NULL
  `).all(cutoff);

  const byZone = {};
  const byMap = {};
  for (const r of rows) {
    const z = r.zone || 'unknown';
    byZone[z] = (byZone[z] || 0) + 1;
    if (r.mapId != null) byMap[r.mapId] = (byMap[r.mapId] || 0) + 1;
  }
  const recordRow = getMeta.get('record_concurrent');
  const record = recordRow ? parseInt(recordRow.v, 10) : 0;
  const totalUniques = db.prepare(`SELECT COUNT(DISTINCT player_id) AS n FROM sessions`).get().n;

  if (rows.length > record) setMeta.run('record_concurrent', String(rows.length));

  return {
    totalOnline: rows.length,
    byZone,
    byMap,
    record: Math.max(record, rows.length),
    totalUniques
  };
}

function dropoffStats(rangeMs) {
  const since = Date.now() - rangeMs;
  const ended = db.prepare(`
    SELECT last_zone AS zone, last_map_id AS mapId, COUNT(*) AS n
    FROM sessions
    WHERE COALESCE(end_ts, last_seen) >= ?
      AND (end_ts IS NOT NULL OR last_seen < ?)
    GROUP BY last_zone, last_map_id
  `).all(since, Date.now() - ACTIVE_WINDOW_MS);

  const byZone = {}, byMap = {};
  for (const r of ended) {
    if (r.zone) byZone[r.zone] = (byZone[r.zone] || 0) + r.n;
    if (r.mapId != null) byMap[r.mapId] = (byMap[r.mapId] || 0) + r.n;
  }
  const toSorted = (obj) => Object.entries(obj).map(([k, v]) => ({ key: k, count: v })).sort((a, b) => b.count - a.count).slice(0, 10);
  return { byZone: toSorted(byZone), byMap: toSorted(byMap) };
}

function concurrentHistory(rangeMs, bucketMs) {
  const since = Date.now() - rangeMs;
  return db.prepare(`
    SELECT (ts / ?) * ? AS bucket, MAX(count) AS count
    FROM concurrent_snapshots
    WHERE ts >= ?
    GROUP BY bucket
    ORDER BY bucket ASC
  `).all(bucketMs, bucketMs, since);
}

const LATEST_DASHBOARD_VERSION = process.env.DASHBOARD_LATEST_VERSION || '1.0.0';
const DASHBOARD_RELEASE_URL = process.env.DASHBOARD_RELEASE_URL || '';
const DASHBOARD_RELEASE_NOTES = process.env.DASHBOARD_RELEASE_NOTES || '';

app.get('/v1/version', requireAdmin, (req, res) => {
  res.json({
    latest: LATEST_DASHBOARD_VERSION,
    url: DASHBOARD_RELEASE_URL,
    notes: DASHBOARD_RELEASE_NOTES
  });
});

app.get('/v1/announcement', (req, res) => {
  const authHeader = req.get('Authorization') || '';
  const gameToken = req.get('X-Game-Token') || '';
  const isAdmin = ADMIN_TOKEN && authHeader === `Bearer ${ADMIN_TOKEN}`;
  const isGame = GAME_TOKEN && gameToken === GAME_TOKEN;
  if (!isAdmin && !isGame) return res.status(401).json({ error: 'unauthorized' });
  res.json({ announcement: currentAnnouncement() });
});

app.post('/v1/announcement/:id/view', (req, res) => {
  const gameToken = req.get('X-Game-Token') || '';
  if (!GAME_TOKEN || gameToken !== GAME_TOKEN) return res.status(401).json({ error: 'unauthorized' });
  const id = parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'invalid id' });
  incrementViewCount.run(id);
  res.json({ ok: true });
});

app.post('/v1/announcement', requireAdmin, (req, res) => {
  const body = req.body || {};
  if (!body.title || !body.body) {
    return res.status(400).json({ error: 'title and body are required' });
  }
  publishAnnouncement(body);
  if (DISCORD_WEBHOOK_URL) {
    const embed = {
      embeds: [{
        title: body.title,
        description: body.body,
        color: 0x9B2CB8,
        footer: { text: 'Succubus Games — Kutushmurf' },
        timestamp: new Date().toISOString(),
        ...(body.url ? { url: body.url } : {}),
      }]
    };
    fetch(DISCORD_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(embed),
    }).catch(err => console.error('[webhook] Discord post failed:', err.message));
  }
  res.json({ ok: true, announcement: currentAnnouncement() });
});

app.delete('/v1/announcement', requireAdmin, (req, res) => {
  deactivateAnnouncements.run();
  res.json({ ok: true });
});

app.get('/v1/stats/live', requireAdmin, (req, res) => res.json(liveStats()));
app.get('/v1/stats/dropoff', requireAdmin, (req, res) => {
  const range = parseInt(req.query.rangeMs || (24 * 3600 * 1000), 10);
  res.json(dropoffStats(range));
});
app.get('/v1/stats/concurrent', requireAdmin, (req, res) => {
  const range = parseInt(req.query.rangeMs || (24 * 3600 * 1000), 10);
  const bucket = parseInt(req.query.bucketMs || (5 * 60 * 1000), 10);
  res.json(concurrentHistory(range, bucket));
});

// ---------- Snapshot cron ----------
function takeSnapshot() {
  const cutoff = Date.now() - ACTIVE_WINDOW_MS;
  const n = db.prepare(`SELECT COUNT(*) AS n FROM sessions WHERE last_seen >= ? AND end_ts IS NULL`).get(cutoff).n;
  db.prepare(`INSERT OR REPLACE INTO concurrent_snapshots(ts, count) VALUES (?, ?)`).run(Date.now(), n);
  // retention 60 days
  db.prepare(`DELETE FROM concurrent_snapshots WHERE ts < ?`).run(Date.now() - 60 * 24 * 3600 * 1000);
}
setInterval(takeSnapshot, 60 * 1000);

// retention events 30 days
setInterval(() => {
  db.prepare(`DELETE FROM events WHERE ts < ?`).run(Date.now() - 30 * 24 * 3600 * 1000);
}, 3600 * 1000);

// ---------- HTTP + WS ----------
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/v1/stream' });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://x');
  const token = url.searchParams.get('token') || (req.headers['sec-websocket-protocol'] || '');
  if (!ADMIN_TOKEN || token !== ADMIN_TOKEN) {
    ws.close(4401, 'unauthorized');
    return;
  }
  ws.send(JSON.stringify({ type: 'snapshot', live: liveStats() }));
});

let broadcastTimer = null;
function scheduleBroadcast() {
  if (broadcastTimer) return;
  broadcastTimer = setTimeout(() => {
    broadcastTimer = null;
    const payload = JSON.stringify({ type: 'snapshot', live: liveStats() });
    wss.clients.forEach((c) => { if (c.readyState === 1) c.send(payload); });
  }, 500);
}

setInterval(() => scheduleBroadcast(), 5000); // keep clients fresh even without events

server.listen(PORT, () => {
  console.log(`[boot] telemetry server listening on :${PORT}`);
});
