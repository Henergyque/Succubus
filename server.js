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
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT || '3000', 10);
const GAME_TOKEN = process.env.GAME_TOKEN || '';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const DB_DIR = process.env.DB_DIR || '/data';
const DB_PATH = path.join(DB_DIR, 'telemetry.db');
const UPDATES_DIR = path.join(DB_DIR, 'updates');
try { fs.mkdirSync(UPDATES_DIR, { recursive: true }); } catch(e) {}
const ACTIVE_WINDOW_MS = 2 * 60 * 1000; // session counted "online" if event within 2 min
const DISCORD_WEBHOOK_URL = process.env.DISCORD_WEBHOOK_URL || '';

if (!GAME_TOKEN) console.warn('[boot] GAME_TOKEN env not set; /v1/event will reject everything.');
if (!ADMIN_TOKEN) console.warn('[boot] ADMIN_TOKEN env not set; admin endpoints will reject everything.');

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const isValidUUID = (s) => typeof s === 'string' && UUID_RE.test(s);

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
CREATE INDEX IF NOT EXISTS idx_sessions_player_id ON sessions(player_id);

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
CREATE TABLE IF NOT EXISTS bug_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  player_id TEXT,
  error TEXT,
  stack TEXT,
  zone TEXT,
  version TEXT,
  platform TEXT,
  ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS discord_links (
  player_id TEXT PRIMARY KEY,
  discord_username TEXT NOT NULL,
  discord_id TEXT,
  linked_at INTEGER NOT NULL
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
  created_at INTEGER NOT NULL,
  view_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_bug_reports_ts ON bug_reports(ts);
CREATE INDEX IF NOT EXISTS idx_announcements_created ON announcements(created_at);
`);

try { db.exec(`ALTER TABLE discord_links ADD COLUMN discord_id TEXT`); } catch(e) {}
try { db.exec(`ALTER TABLE bug_reports ADD COLUMN screenshot TEXT`); } catch(e) {}

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
const getChangelog = db.prepare(`SELECT id, title, body, url, version, created_at FROM announcements ORDER BY created_at DESC LIMIT 20`);

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

// CORS — allow all origins so Android/Cordova (null origin from file://) can reach the server
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, X-Game-Token, Authorization');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

app.use(express.json({ limit: '5mb' }));

const eventLimiter = rateLimit({ windowMs: 60 * 1000, max: 120 });
const gameLimiter  = rateLimit({ windowMs: 60 * 1000, max: 30 });
const adminLimiter = rateLimit({ windowMs: 60 * 1000, max: 60 });

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
      if (!isValidUUID(pid)) continue;
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

// ---------- Live endpoint (for dashboard polling fallback) ----------
app.get('/v1/live', adminLimiter, requireAdmin, (req, res) => {
  res.json({ live: liveStats() });
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

app.get('/v1/version', adminLimiter, requireAdmin, (req, res) => {
  res.json({
    latest: LATEST_DASHBOARD_VERSION,
    url: DASHBOARD_RELEASE_URL,
    notes: DASHBOARD_RELEASE_NOTES
  });
});

app.get('/v1/announcement', gameLimiter, (req, res) => {
  const authHeader = req.get('Authorization') || '';
  const gameToken = req.get('X-Game-Token') || '';
  const isAdmin = ADMIN_TOKEN && authHeader === `Bearer ${ADMIN_TOKEN}`;
  const isGame = GAME_TOKEN && gameToken === GAME_TOKEN;
  if (!isAdmin && !isGame) return res.status(401).json({ error: 'unauthorized' });
  res.json({ announcement: currentAnnouncement() });
});

app.post('/v1/announcement/:id/view', gameLimiter, (req, res) => {
  const gameToken = req.get('X-Game-Token') || '';
  if (!GAME_TOKEN || gameToken !== GAME_TOKEN) return res.status(401).json({ error: 'unauthorized' });
  const id = parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'invalid id' });
  incrementViewCount.run(id);
  res.json({ ok: true });
});

app.post('/v1/announcement', adminLimiter, requireAdmin, (req, res) => {
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

app.delete('/v1/announcement', adminLimiter, requireAdmin, (req, res) => {
  deactivateAnnouncements.run();
  res.json({ ok: true });
});

app.delete('/v1/changelog', adminLimiter, requireAdmin, (req, res) => {
  db.prepare('DELETE FROM announcements').run();
  res.json({ ok: true });
});

// ---------- Game auto-update ----------
app.get('/v1/game/update', gameLimiter, (req, res) => {
  const gameToken = req.get('X-Game-Token') || '';
  if (!GAME_TOKEN || gameToken !== GAME_TOKEN) return res.status(401).json({ error: 'unauthorized' });
  const row = getMeta.get('game_update_manifest');
  if (!row) return res.json({ manifest: null });
  try {
    const manifest = JSON.parse(row.v);
    if (!manifest) return res.json({ manifest: null });
    if (manifest.hosted && Array.isArray(manifest.files)) {
      const baseUrl = `${req.protocol}://${req.get('host')}`;
      manifest.files = manifest.files.map(f => ({
        path: f.path,
        url: `${baseUrl}/v1/game/update/files/${f.filename.split('/').map(encodeURIComponent).join('/')}`,
        sha256: f.sha256
      }));
      delete manifest.hosted;
    }
    res.json({ manifest });
  } catch(e) { res.json({ manifest: null }); }
});

app.get('/v1/game/update/admin', adminLimiter, requireAdmin, (req, res) => {
  const row = getMeta.get('game_update_manifest');
  if (!row) return res.json({ manifest: null });
  try { res.json({ manifest: JSON.parse(row.v) }); }
  catch(e) { res.json({ manifest: null }); }
});

app.get('/v1/game/update/files/*', gameLimiter, (req, res) => {
  const gameToken = req.get('X-Game-Token') || '';
  if (!GAME_TOKEN || gameToken !== GAME_TOKEN) return res.status(401).json({ error: 'unauthorized' });
  const relative = (req.params[0] || '').replace(/\.\./g, '');
  const filePath = path.resolve(path.join(UPDATES_DIR, relative));
  if (!filePath.startsWith(path.resolve(UPDATES_DIR))) return res.status(403).json({ error: 'forbidden' });
  if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'not found' });
  res.sendFile(filePath);
});

app.post('/v1/game/update', adminLimiter, requireAdmin, (req, res) => {
  const body = req.body || {};

  // New format from dashboard: { version, files: [{ name, path, content (base64) }] }
  if (body.version && Array.isArray(body.files) && body.files.length > 0 && body.files[0].content) {
    const version = String(body.version).trim();
    if (!version) return res.status(400).json({ error: 'version is required' });
    const manifestFiles = [];
    for (const f of body.files) {
      const name = path.basename(String(f.name || ''));
      const filePath = String(f.path || 'www/js/plugins/' + name).replace(/\.\./g, '').replace(/\\/g, '/');
      if (!name || !filePath) continue;
      const buf = Buffer.from(f.content, 'base64');
      const sha256 = crypto.createHash('sha256').update(buf).digest('hex');
      const dest = path.join(UPDATES_DIR, filePath);
      fs.mkdirSync(path.dirname(dest), { recursive: true });
      fs.writeFileSync(dest, buf);
      manifestFiles.push({ path: filePath, filename: filePath, sha256 });
    }
    if (manifestFiles.length === 0) return res.status(400).json({ error: 'no valid files' });
    const manifest = { version, files: manifestFiles, hosted: true };
    setMeta.run('game_update_manifest', JSON.stringify(manifest));
    return res.json({ ok: true, manifest });
  }

  // Old format from set-update.js: { manifest: { version, files: [{ path, url, sha256 }] } }
  const { manifest } = body;
  if (!manifest || !manifest.version || !Array.isArray(manifest.files))
    return res.status(400).json({ error: 'invalid manifest' });
  setMeta.run('game_update_manifest', JSON.stringify(manifest));
  res.json({ ok: true, manifest });
});

app.delete('/v1/game/update', adminLimiter, requireAdmin, (req, res) => {
  setMeta.run('game_update_manifest', 'null');
  try { fs.rmSync(UPDATES_DIR, { recursive: true, force: true }); fs.mkdirSync(UPDATES_DIR, { recursive: true }); } catch(e) {}
  res.json({ ok: true });
});

app.get('/v1/changelog', gameLimiter, (req, res) => {
  const authHeader = req.get('Authorization') || '';
  const gameToken = req.get('X-Game-Token') || '';
  const isAdmin = ADMIN_TOKEN && authHeader === `Bearer ${ADMIN_TOKEN}`;
  const isGame = GAME_TOKEN && gameToken === GAME_TOKEN;
  if (!isAdmin && !isGame) return res.status(401).json({ error: 'unauthorized' });
  const rows = getChangelog.all();
  res.json({ changelog: rows.map(r => ({ id: r.id, title: r.title, body: r.body, url: r.url, version: r.version, createdAt: r.created_at })) });
});

app.get('/v1/stats/live', adminLimiter, requireAdmin, (req, res) => res.json(liveStats()));

app.get('/v1/stats/today', adminLimiter, requireAdmin, (req, res) => {
  const startOfDay = new Date();
  startOfDay.setHours(0, 0, 0, 0);
  const n = db.prepare(`SELECT COUNT(DISTINCT player_id) AS n FROM sessions WHERE start_ts >= ?`).get(startOfDay.getTime()).n;
  res.json({ today: n });
});

app.post('/v1/report', gameLimiter, (req, res) => {
  const gameToken = req.get('X-Game-Token') || '';
  if (!GAME_TOKEN || gameToken !== GAME_TOKEN) return res.status(401).json({ error: 'unauthorized' });
  const b = req.body || {};
  db.prepare(`INSERT INTO bug_reports (player_id, error, stack, zone, version, platform, screenshot, ts) VALUES (?,?,?,?,?,?,?,?)`).run(
    isValidUUID(b.playerId) ? String(b.playerId) : null,
    String(b.error      || '').slice(0, 512),
    String(b.stack      || '').slice(0, 4096),
    String(b.zone       || '').slice(0, 32),
    String(b.version    || '').slice(0, 32),
    String(b.platform   || '').slice(0, 32),
    String(b.screenshot || '').slice(0, 200000),
    Date.now()
  );
  broadcastBugReport();
  res.json({ ok: true });
});

app.post('/v1/players/link', adminLimiter, requireAdmin, (req, res) => {
  const { uuid, discordUsername, discordId } = req.body || {};
  if (!uuid || !discordUsername) return res.status(400).json({ error: 'uuid and discordUsername required' });
  if (!isValidUUID(uuid)) return res.status(400).json({ error: 'invalid uuid format' });
  db.prepare(`INSERT OR REPLACE INTO discord_links (player_id, discord_username, discord_id, linked_at) VALUES (?, ?, ?, ?)`)
    .run(String(uuid).slice(0, 64), String(discordUsername).slice(0, 64), discordId ? String(discordId).slice(0, 32) : null, Date.now());
  res.json({ ok: true });
});

app.get('/v1/players/links', adminLimiter, requireAdmin, (req, res) => {
  const rows = db.prepare(`SELECT player_id, discord_id FROM discord_links WHERE discord_id IS NOT NULL`).all();
  res.json({ links: rows.map(r => ({ uuid: r.player_id, discordId: r.discord_id })) });
});

app.get('/v1/players/discord', gameLimiter, (req, res) => {
  const gameToken = req.get('X-Game-Token') || '';
  if (!GAME_TOKEN || gameToken !== GAME_TOKEN) return res.status(401).json({ error: 'unauthorized' });
  const uuid = String(req.query.uuid || '');
  if (!uuid) return res.status(400).json({ error: 'uuid required' });
  if (!isValidUUID(uuid)) return res.status(400).json({ error: 'invalid uuid format' });
  const row = db.prepare(`SELECT discord_username FROM discord_links WHERE player_id = ?`).get(uuid);
  res.json({ username: row ? row.discord_username : null });
});

app.get('/v1/players/zones', adminLimiter, requireAdmin, (req, res) => {
  const rows = db.prepare(`
    SELECT player_id, last_zone FROM sessions
    GROUP BY player_id HAVING last_seen = MAX(last_seen)
  `).all();
  const result = {};
  for (const row of rows) result[row.player_id] = row.last_zone || 'unknown';
  res.json(result);
});
app.get('/v1/reports', adminLimiter, requireAdmin, (req, res) => {
  const limit = Math.min(parseInt(req.query.limit || '100', 10), 500);
  const rows = db.prepare(`SELECT id, player_id, error, stack, zone, version, platform, screenshot, ts FROM bug_reports ORDER BY ts DESC LIMIT ?`).all(limit);
  res.json({ reports: rows });
});

app.delete('/v1/reports', adminLimiter, requireAdmin, (req, res) => {
  db.prepare(`DELETE FROM bug_reports`).run();
  res.json({ ok: true });
});

app.get('/v1/stats/dropoff', adminLimiter, requireAdmin, (req, res) => {
  const range = Math.max(1, Math.min(parseInt(req.query.rangeMs || (24 * 3600 * 1000), 10), 90 * 24 * 3600 * 1000));
  res.json(dropoffStats(range));
});
app.get('/v1/stats/concurrent', adminLimiter, requireAdmin, (req, res) => {
  const range  = Math.max(1, Math.min(parseInt(req.query.rangeMs  || (24 * 3600 * 1000), 10), 90 * 24 * 3600 * 1000));
  const bucket = Math.max(60000, Math.min(parseInt(req.query.bucketMs || (5 * 60 * 1000), 10), 24 * 3600 * 1000));
  res.json(concurrentHistory(range, bucket));
});

app.get('/v1/stats/newplayers', adminLimiter, requireAdmin, (req, res) => {
  // A "new player" on day D = a player whose FIRST session ever started on day D.
  const days = Math.max(1, Math.min(parseInt(req.query.days || '30', 10), 365));
  const since = Date.now() - days * 24 * 3600 * 1000;
  const rows = db.prepare(`
    WITH firsts AS (
      SELECT player_id, MIN(start_ts) AS first_ts
      FROM sessions
      GROUP BY player_id
    )
    SELECT strftime('%Y-%m-%d', first_ts / 1000, 'unixepoch', 'localtime') AS day,
           COUNT(*) AS count
    FROM firsts
    WHERE first_ts >= ?
    GROUP BY day
    ORDER BY day ASC
  `).all(since);
  res.json({ points: rows });
});

app.get('/v1/stats/platforms', adminLimiter, requireAdmin, (req, res) => {
  const totalRows = db.prepare(`
    SELECT platform, COUNT(DISTINCT player_id) AS players
    FROM sessions WHERE platform IS NOT NULL GROUP BY platform
  `).all();
  const total = {};
  for (const r of totalRows) total[r.platform] = r.players;

  const cutoff = Date.now() - ACTIVE_WINDOW_MS;
  const onlineRows = db.prepare(`
    SELECT platform, COUNT(*) AS online
    FROM sessions WHERE end_ts IS NULL AND last_seen >= ? AND platform IS NOT NULL GROUP BY platform
  `).all(cutoff);
  const online = {};
  for (const r of onlineRows) online[r.platform] = r.online;

  res.json({ total, online });
});

app.get('/v1/stats/languages', adminLimiter, requireAdmin, (req, res) => {
  // Normalize locale to its primary subtag (fr-FR -> fr) then count distinct players.
  const rows = db.prepare(`
    SELECT CASE
             WHEN locale IS NULL OR locale = '' OR locale = 'unknown' THEN 'unknown'
             ELSE LOWER(SUBSTR(locale, 1, 2))
           END AS lang,
           COUNT(DISTINCT player_id) AS players
    FROM sessions
    GROUP BY lang
  `).all();
  const languages = {};
  for (const r of rows) languages[r.lang] = r.players;
  res.json({ languages });
});

app.get('/v1/stats/sessions', adminLimiter, requireAdmin, (req, res) => {
  // Average is based on FINISHED sessions only (a partial session isn't a "short session").
  const finished = db.prepare(`
    SELECT COUNT(*) AS total_sessions,
           AVG(end_ts - start_ts) AS avg_ms,
           SUM(end_ts - start_ts) AS total_ms,
           MAX(end_ts - start_ts) AS longest_ms
    FROM sessions WHERE end_ts IS NOT NULL AND end_ts > start_ts
  `).get();

  // Median duration of finished sessions (middle value; avg of two middles if even count).
  let medianMs = 0;
  const n = finished.total_sessions || 0;
  if (n > 0) {
    const off = Math.floor((n - 1) / 2);
    const lim = (n % 2 === 0) ? 2 : 1;
    const mids = db.prepare(`
      SELECT (end_ts - start_ts) AS d
      FROM sessions WHERE end_ts IS NOT NULL AND end_ts > start_ts
      ORDER BY d ASC LIMIT ? OFFSET ?
    `).all(lim, off);
    if (mids.length) medianMs = mids.reduce((a, r) => a + r.d, 0) / mids.length;
  }

  // Total cumulative time also counts time currently being played by ACTIVE sessions
  // (last_seen recent), so it climbs live. Crashed/ghost sessions are excluded.
  const now = Date.now();
  const cutoff = now - ACTIVE_WINDOW_MS;
  const active = db.prepare(`
    SELECT SUM(? - start_ts) AS ongoing_ms
    FROM sessions WHERE end_ts IS NULL AND last_seen >= ? AND start_ts <= ?
  `).get(now, cutoff, now);

  const totalMs = (finished.total_ms || 0) + (active.ongoing_ms || 0);
  res.json({
    total_sessions: finished.total_sessions || 0,
    avg_ms: Math.round(finished.avg_ms || 0),
    median_ms: Math.round(medianMs),
    longest_ms: Math.round(finished.longest_ms || 0),
    total_ms: Math.round(totalMs)
  });
});

const DEFAULT_ZONE_LABELS = {
  intro: 'Intro / Maison', jeu1: 'Trial 1', jeu2_hub: 'Trial 2 — Hub',
  jeu2_gauche: 'Trial 2 — Left', jeu2_droite: 'Trial 2 — Right', jeu2_arbre: 'Trial 2 — Tree',
  endgame: 'Endgame', speciales: 'Special rooms', unknown: 'Unknown'
};

app.get('/v1/admin/zones', adminLimiter, requireAdmin, (req, res) => {
  const row = getMeta.get('zone_labels');
  try {
    const labels = row ? Object.assign({}, DEFAULT_ZONE_LABELS, JSON.parse(row.v)) : DEFAULT_ZONE_LABELS;
    res.json({ labels });
  } catch(e) {
    res.json({ labels: DEFAULT_ZONE_LABELS });
  }
});

app.put('/v1/admin/zones', adminLimiter, requireAdmin, (req, res) => {
  const labels = req.body || {};
  const cleaned = {};
  for (const k of Object.keys(DEFAULT_ZONE_LABELS)) {
    cleaned[k] = labels[k] !== undefined ? String(labels[k]).slice(0, 64) : DEFAULT_ZONE_LABELS[k];
  }
  setMeta.run('zone_labels', JSON.stringify(cleaned));
  res.json({ ok: true, labels: cleaned });
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

function broadcastBugReport() {
  const payload = JSON.stringify({ type: 'bug_report' });
  wss.clients.forEach((c) => { if (c.readyState === 1) c.send(payload); });
}

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
