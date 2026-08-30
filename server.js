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
  game_lang TEXT
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
/* Les trois tables ci-dessous echappent volontairement a la purge des 30 jours
   qui frappe la table events (voir plus bas la retention). Ce sont des compteurs de
   progression cumulatifs : « combien de joueurs ont eu la fin Chad », « quel
   ennemi tue le plus » n'ont aucun sens sur une fenetre glissante d'un mois.
   Le volume reste minuscule : une ligne par mort, par fin, par choix. */

CREATE TABLE IF NOT EXISTS deaths (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  player_id TEXT NOT NULL,
  session_id TEXT,
  ts INTEGER NOT NULL,
  map_id INTEGER,
  zone TEXT,
  x INTEGER,
  y INTEGER,
  enemy TEXT,
  enemy_instance TEXT,
  game TEXT,
  version TEXT
);

CREATE TABLE IF NOT EXISTS endings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  player_id TEXT NOT NULL,
  session_id TEXT,
  ts INTEGER NOT NULL,
  ending TEXT NOT NULL,
  favourite TEXT,
  version TEXT
);

CREATE TABLE IF NOT EXISTS gameover_choices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  player_id TEXT NOT NULL,
  ts INTEGER NOT NULL,
  choice TEXT NOT NULL
);

/* Sondages. Meme logique de retention que les trois tables ci-dessus : un avis
   sur la difficulte ne perime pas au bout d'un mois.

   Particularite : ces deux tables ne contiennent AUCUN player_id, par choix.
   Le jeu envoie une reponse et rien d'autre. Consequence directe : le serveur
   est incapable de savoir qui a deja repondu quoi, donc le dedoublonnage
   « jamais deux fois la meme question » se fait cote jeu, en localStorage.

   La colonne submission est un identifiant tire a chaque envoi, cote serveur, et
   jamais conserve par le client. Elle sert uniquement a regrouper les reponses
   d'un meme formulaire (croiser question 1 x question 2) sans identifier
   personne. */

CREATE TABLE IF NOT EXISTS survey_questions (
  id TEXT PRIMARY KEY,
  spec TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT 1,
  sort_order INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS survey_answers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  question_id TEXT NOT NULL,
  answer TEXT NOT NULL,
  submission TEXT NOT NULL,
  ts INTEGER NOT NULL,
  version TEXT,
  lang TEXT,
  platform TEXT
);

CREATE INDEX IF NOT EXISTS idx_bug_reports_ts ON bug_reports(ts);
CREATE INDEX IF NOT EXISTS idx_announcements_created ON announcements(created_at);
CREATE INDEX IF NOT EXISTS idx_deaths_map ON deaths(map_id);
CREATE INDEX IF NOT EXISTS idx_deaths_enemy ON deaths(enemy);
CREATE INDEX IF NOT EXISTS idx_deaths_ts ON deaths(ts);
CREATE INDEX IF NOT EXISTS idx_endings_ending ON endings(ending);
CREATE INDEX IF NOT EXISTS idx_survey_answers_q ON survey_answers(question_id);
CREATE INDEX IF NOT EXISTS idx_survey_questions_active ON survey_questions(active);
`);

try { db.exec(`ALTER TABLE discord_links ADD COLUMN discord_id TEXT`); } catch(e) {}
try { db.exec(`ALTER TABLE bug_reports ADD COLUMN screenshot TEXT`); } catch(e) {}

/* RGPD — suppression de la colonne `locale`.
   Elle recevait navigator.language, c'est-a-dire la langue du systeme
   d'exploitation du joueur : une donnee prelevee sur sa machine, collectee
   sans finalite declaree. Elle est remplacee par `game_lang`, la langue que
   le joueur choisit lui-meme dans les options du jeu.
   Le DROP efface definitivement les valeurs deja collectees ; c'est
   volontaire, arreter la collecte sans effacer l'existant ne reglerait
   qu'a moitie le probleme. Aucun index ne porte sur cette colonne.
   Les deux instructions sont idempotentes : au redemarrage suivant elles
   echouent silencieusement, la base etant deja a jour. */
try { db.exec(`ALTER TABLE sessions ADD COLUMN game_lang TEXT`); } catch(e) {}
try { db.exec(`ALTER TABLE sessions DROP COLUMN locale`); } catch(e) {}

/* La table `deaths` enregistre desormais CHAQUE capture, pas seulement celle qui
   vide les PV : `fatal` distingue la derniere des precedentes. Les lignes
   anterieures a cette colonne etaient toutes des morts, d'ou le defaut a 1. */
try { db.exec(`ALTER TABLE deaths ADD COLUMN fatal INTEGER NOT NULL DEFAULT 1`); } catch(e) {}

/* Id de l'evenement RPG Maker qui a attrape le joueur. Unique par carte
   seulement : c'est le couple (map_id, enemy_event_id) qui designe un ennemi
   precis. NULL sur les lignes anterieures, et sur les captures dont le
   coupable n'a pas pu etre identifie. */
try { db.exec(`ALTER TABLE deaths ADD COLUMN enemy_event_id INTEGER`); } catch(e) {}

const insertEvent = db.prepare(`
  INSERT INTO events (session_id, player_id, ts, type, map_id, zone, payload)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

const upsertSessionStart = db.prepare(`
  INSERT INTO sessions (id, player_id, start_ts, last_seen, last_map_id, last_zone, version, platform, game_lang)
  VALUES (@id, @player_id, @ts, @ts, NULL, NULL, @version, @platform, @game_lang)
  ON CONFLICT(id) DO UPDATE SET last_seen=@ts, version=COALESCE(@version, version), platform=COALESCE(@platform, platform), game_lang=COALESCE(@game_lang, game_lang)
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

const insertDeath = db.prepare(`
  INSERT INTO deaths (player_id, session_id, ts, map_id, zone, x, y, enemy, enemy_instance, game, version, fatal, enemy_event_id)
  VALUES (@player_id, @session_id, @ts, @map_id, @zone, @x, @y, @enemy, @enemy_instance, @game, @version, @fatal, @enemy_event_id)
`);

const insertEnding = db.prepare(`
  INSERT INTO endings (player_id, session_id, ts, ending, favourite, version)
  VALUES (@player_id, @session_id, @ts, @ending, @favourite, @version)
`);

const insertGameoverChoice = db.prepare(`
  INSERT INTO gameover_choices (player_id, ts, choice) VALUES (@player_id, @ts, @choice)
`);

// ---------- Sondages ----------
const getActiveQuestions = db.prepare(`SELECT id, spec FROM survey_questions WHERE active = 1 ORDER BY sort_order, created_at`);
const getAllQuestions = db.prepare(`
  SELECT q.id, q.spec, q.active, q.sort_order, q.created_at,
         (SELECT COUNT(DISTINCT submission) FROM survey_answers a WHERE a.question_id = q.id) AS responses
  FROM survey_questions q ORDER BY q.sort_order, q.created_at
`);
const getQuestion = db.prepare(`SELECT id, spec, active FROM survey_questions WHERE id = ?`);
const upsertQuestion = db.prepare(`
  INSERT INTO survey_questions (id, spec, active, sort_order, created_at)
  VALUES (@id, @spec, @active, @sort_order, @created_at)
  ON CONFLICT(id) DO UPDATE SET spec=excluded.spec, active=excluded.active, sort_order=excluded.sort_order
`);
const deleteQuestion = db.prepare(`DELETE FROM survey_questions WHERE id = ?`);
const deleteAnswersOf = db.prepare(`DELETE FROM survey_answers WHERE question_id = ?`);
const insertAnswer = db.prepare(`
  INSERT INTO survey_answers (question_id, answer, submission, ts, version, lang, platform)
  VALUES (@question_id, @answer, @submission, @ts, @version, @lang, @platform)
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

app.get('/health', (req, res) => res.json({ ok: true, uptimeSec: Math.round(process.uptime()) }));

// ---------- Ingest ----------

// borne une chaine venue du jeu, ou null si vide
function txt(v, max) {
  if (v === undefined || v === null) return null;
  const s = String(v).slice(0, max);
  return s === '' ? null : s;
}
function num(v) { return Number.isFinite(v) ? parseInt(v, 10) : null; }

// la version n'est envoyee qu'au session_start ; on la retrouve sur la session
// pour pouvoir dire plus tard « cette fin a ete obtenue en 0.4.1 »
const getSessionVersion = db.prepare(`SELECT version FROM sessions WHERE id = ?`);

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
          // code a deux lettres choisi dans les options du jeu (en, fr, ru, ko, ja, zh)
          game_lang: e.lang ? String(e.lang).toLowerCase().slice(0, 8) : null
        });
      } else if (type === 'session_end') {
        ensureSessionRow.run({ id: sid, player_id: pid, ts, map_id: mapId, zone });
        endSession.run({ id: sid, ts, map_id: mapId, zone });
      } else {
        ensureSessionRow.run({ id: sid, player_id: pid, ts, map_id: mapId, zone });
        updateSessionTick.run({ id: sid, ts, map_id: mapId, zone });
      }

      /* Progression cumulative. Ces trois types passent aussi par le tick de
         session ci-dessus (branche `else`) : on ne remplace rien, on ajoute une
         ligne dans la table dediee qui, elle, survit a la purge des 30 jours. */
      if (type === 'death') {
        const vrow = getSessionVersion.get(sid);
        insertDeath.run({
          player_id: pid, session_id: sid, ts,
          map_id: mapId, zone,
          x: num(e.x), y: num(e.y),
          enemy: txt(e.enemy, 48),
          enemy_instance: txt(e.enemyInstance, 64),
          game: txt(e.game, 16),
          version: vrow ? vrow.version : null,
          /* Absent = client anterieur au suivi des captures, qui n'envoyait que
             les morts : on le compte donc comme fatal. */
          fatal: (e.fatal === undefined || e.fatal) ? 1 : 0,
          enemy_event_id: num(e.enemyId)
        });
        broadcastCapture();
      } else if (type === 'ending') {
        const vrow = getSessionVersion.get(sid);
        const ending = txt(e.ending, 32);
        if (ending) {
          insertEnding.run({
            player_id: pid, session_id: sid, ts,
            ending,
            favourite: txt(e.favourite, 32),
            version: vrow ? vrow.version : null
          });
        }
      } else if (type === 'gameover_choice') {
        const choice = txt(e.choice, 16);
        if (choice) insertGameoverChoice.run({ player_id: pid, ts, choice });
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

// ---------- Sondages ----------

const SURVEY_TYPES = ['choice', 'multi', 'scale'];
const SCALE_MAX = 5;

/* Une spec mal formee publiee depuis le dashboard casserait la fenetre du jeu
   chez tous les joueurs a la fois — donc on valide a la publication, pas a la
   lecture. Renvoie une chaine d'erreur, ou null si tout va bien. */
function validateQuestionSpec(spec) {
  if (!spec || typeof spec !== 'object') return 'spec must be an object';
  if (!/^[a-z0-9][a-z0-9-]{0,47}$/.test(String(spec.id || ''))) {
    return 'id must be a slug: lowercase, digits and dashes, 48 chars max';
  }
  if (SURVEY_TYPES.indexOf(spec.type) === -1) return 'type must be choice, multi or scale';

  // L'anglais est le repli de toutes les autres langues : sans lui, un joueur
  // coreen sur une question non traduite verrait une fenetre vide.
  const label = spec.label;
  if (!label || typeof label !== 'object' || !String(label.en || '').trim()) {
    return 'label.en is required (it is the fallback for every other language)';
  }

  if (spec.type === 'scale') return null;

  if (!Array.isArray(spec.options) || spec.options.length < 2) return 'at least 2 options are required';
  if (spec.options.length > 8) return '8 options maximum (the window would overflow)';
  const seen = new Set();
  for (const o of spec.options) {
    if (!o || !/^[a-z0-9][a-z0-9-]{0,31}$/.test(String(o.id || ''))) return 'each option needs a slug id';
    if (seen.has(o.id)) return 'duplicate option id: ' + o.id;
    seen.add(o.id);
    if (!o.label || !String(o.label.en || '').trim()) return 'option ' + o.id + ' needs an English label';
  }
  return null;
}

// Une reponse valide pour cette question ? On borne ici, jamais a l'affichage.
function isValidAnswer(spec, answer) {
  const s = String(answer);
  if (spec.type === 'scale') {
    const n = parseInt(s, 10);
    return String(n) === s && n >= 1 && n <= SCALE_MAX;
  }
  return (spec.options || []).some(o => o.id === s);
}

function parseSpec(row) {
  try { return JSON.parse(row.spec); } catch (e) { return null; }
}

app.get('/v1/survey', gameLimiter, (req, res) => {
  const gameToken = req.get('X-Game-Token') || '';
  const isAdmin = ADMIN_TOKEN && (req.get('Authorization') || '') === `Bearer ${ADMIN_TOKEN}`;
  if (!isAdmin && (!GAME_TOKEN || gameToken !== GAME_TOKEN)) return res.status(401).json({ error: 'unauthorized' });
  /* Le pool ENTIER, et non « la prochaine question pour toi ». C'est ce qui
     permet au jeu de filtrer ce qu'il a deja repondu sans jamais avoir a le
     dire au serveur : l'anonymat tient a ce detail. Le volume est negligeable. */
  const questions = getActiveQuestions.all().map(parseSpec).filter(Boolean);
  res.json({ questions });
});

app.post('/v1/survey/answer', gameLimiter, (req, res) => {
  const gameToken = req.get('X-Game-Token') || '';
  if (!GAME_TOKEN || gameToken !== GAME_TOKEN) return res.status(401).json({ error: 'unauthorized' });

  const b = req.body || {};
  const answers = Array.isArray(b.answers) ? b.answers : [];
  if (answers.length === 0) return res.status(400).json({ error: 'no answers' });
  if (answers.length > 20) return res.status(413).json({ error: 'too many answers' });

  const now = Date.now();
  const submission = crypto.randomUUID();
  const meta = {
    version: txt(b.version, 32),
    lang: txt(b.lang, 8),
    platform: txt(b.platform, 32)
  };

  let accepted = 0;
  const rejected = [];
  const tx = db.transaction((list) => {
    for (const a of list) {
      if (!a || typeof a !== 'object') continue;
      const qid = txt(a.questionId, 48);
      if (!qid) continue;
      const row = getQuestion.get(qid);
      // Question inconnue ou desactivee : on refuse plutot que d'accumuler des
      // lignes orphelines que personne ne saura jamais interpreter.
      if (!row || !row.active) { rejected.push(qid); continue; }
      const spec = parseSpec(row);
      if (!spec) { rejected.push(qid); continue; }
      if (!isValidAnswer(spec, a.answer)) { rejected.push(qid); continue; }
      insertAnswer.run({
        question_id: qid, answer: String(a.answer).slice(0, 32),
        submission, ts: now, ...meta
      });
      accepted++;
    }
  });
  tx(answers);

  if (accepted === 0) return res.status(400).json({ error: 'no valid answer', rejected });
  res.json({ ok: true, accepted, rejected });
});

app.get('/v1/survey/admin', adminLimiter, requireAdmin, (req, res) => {
  const questions = getAllQuestions.all().map(r => {
    const spec = parseSpec(r);
    return spec ? { ...spec, active: !!r.active, sortOrder: r.sort_order, createdAt: r.created_at, responses: r.responses } : null;
  }).filter(Boolean);
  res.json({ questions });
});

app.post('/v1/survey/question', adminLimiter, requireAdmin, (req, res) => {
  const spec = req.body || {};
  const err = validateQuestionSpec(spec);
  if (err) return res.status(400).json({ error: err });

  const existing = getQuestion.get(spec.id);
  const active = spec.active === false ? 0 : 1;
  const sortOrder = Number.isFinite(spec.sortOrder) ? parseInt(spec.sortOrder, 10) : 0;
  /* Le slug est la cle : reediter une question la met a jour sans la reposer aux
     joueurs qui y ont deja repondu. Changer le SENS d'une question impose donc
     de changer son slug, sinon les anciennes reponses se melangent aux neuves. */
  const stored = { id: spec.id, type: spec.type, label: spec.label };
  if (spec.type === 'scale') {
    if (spec.lowLabel) stored.lowLabel = spec.lowLabel;
    if (spec.highLabel) stored.highLabel = spec.highLabel;
  } else {
    stored.options = spec.options;
  }
  // created_at est absent du DO UPDATE : sur conflit la valeur d'origine reste,
  // donc on peut passer maintenant sans risque d'ecraser la date de creation.
  upsertQuestion.run({
    id: spec.id, spec: JSON.stringify(stored), active, sort_order: sortOrder, created_at: Date.now()
  });
  res.json({ ok: true, created: !existing });
});

app.delete('/v1/survey/question/:id', adminLimiter, requireAdmin, (req, res) => {
  const id = String(req.params.id || '');
  const tx = db.transaction(() => {
    deleteAnswersOf.run(id);
    return deleteQuestion.run(id);
  });
  const info = tx();
  if (!info.changes) return res.status(404).json({ error: 'not found' });
  res.json({ ok: true });
});

app.get('/v1/stats/survey', adminLimiter, requireAdmin, (req, res) => {
  const rows = getAllQuestions.all();
  const results = [];
  for (const r of rows) {
    const spec = parseSpec(r);
    if (!spec) continue;
    const counts = db.prepare(`
      SELECT answer, COUNT(*) AS n FROM survey_answers WHERE question_id = ? GROUP BY answer
    `).all(r.id);
    const byLang = db.prepare(`
      SELECT COALESCE(NULLIF(lang, ''), 'unknown') AS lang, COUNT(DISTINCT submission) AS n
      FROM survey_answers WHERE question_id = ? GROUP BY lang ORDER BY n DESC
    `).all(r.id);
    /* Les repondants se comptent en `submission` distincts, pas en lignes : une
       question multi-select produit plusieurs lignes pour une seule personne. */
    const respondents = db.prepare(`
      SELECT COUNT(DISTINCT submission) AS n FROM survey_answers WHERE question_id = ?
    `).get(r.id).n;

    const entry = {
      id: r.id, type: spec.type, label: spec.label, active: !!r.active,
      respondents, counts: {}, byLang: {}
    };
    for (const c of counts) entry.counts[c.answer] = c.n;
    for (const l of byLang) entry.byLang[l.lang] = l.n;

    if (spec.type === 'scale') {
      let sum = 0, n = 0;
      for (const c of counts) { sum += parseInt(c.answer, 10) * c.n; n += c.n; }
      entry.average = n ? Math.round(sum / n * 100) / 100 : null;
    } else {
      entry.options = spec.options;
    }
    results.push(entry);
  }
  res.json({ questions: results });
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

// suppression unitaire — la route globale ci-dessus efface tout, ce qui obligeait
// a repartir de zero pour se debarrasser d'un seul rapport deja traite
app.delete('/v1/reports/:id', adminLimiter, requireAdmin, (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isFinite(id)) return res.status(400).json({ error: 'invalid id' });
  const info = db.prepare(`DELETE FROM bug_reports WHERE id = ?`).run(id);
  if (info.changes === 0) return res.status(404).json({ error: 'not found' });
  res.json({ ok: true, deleted: id });
});

// ---------- Archive et remise a zero entre deux versions ----------

// Tables qui portent des mesures de jeu : elles repartent de zero a chaque
// version. Le reste (liens Discord, questions de sondage, annonces, manifeste
// de mise a jour, libelles de zones) est du contenu, pas de la mesure, et survit.
const STAT_TABLES = [
  'events',
  'sessions',
  'concurrent_snapshots',
  'deaths',
  'endings',
  'gameover_choices',
  'survey_answers'
];

// Archive complete des mesures, a prendre AVANT un reset : une fois les tables
// videes plus rien ne permet de les reconstituer.
app.get('/v1/admin/export', adminLimiter, requireAdmin, (req, res) => {
  const dump = { exportedAt: Date.now(), tables: {} };
  for (const t of STAT_TABLES) dump.tables[t] = db.prepare('SELECT * FROM ' + t).all();
  // les questions de sondage accompagnent leurs reponses, sinon l'archive est illisible
  dump.tables.survey_questions = db.prepare('SELECT * FROM survey_questions').all();
  const rec = getMeta.get('record_concurrent');
  dump.recordConcurrent = rec ? Number(rec.v) : 0;
  dump.counts = {};
  for (const t of Object.keys(dump.tables)) dump.counts[t] = dump.tables[t].length;

  const name = 'succubus-stats-' + new Date().toISOString().slice(0, 10) + '.json';
  res.setHeader('Content-Disposition', 'attachment; filename="' + name + '"');
  res.json(dump);
});

// Irreversible. Confirmation explicite exigee dans le corps pour qu'un appel
// accidentel ne vide pas la base.
app.post('/v1/admin/reset', adminLimiter, requireAdmin, (req, res) => {
  if (!req.body || req.body.confirm !== 'reset') {
    return res.status(400).json({ error: 'confirmation manquante', hint: 'POST {"confirm":"reset"}' });
  }

  const deleted = {};
  db.transaction(() => {
    for (const t of STAT_TABLES) deleted[t] = db.prepare('DELETE FROM ' + t).run().changes;
    // record_concurrent est une mesure, pas un reglage : il repart avec le reste.
    // Les autres cles de meta (manifeste de maj, libelles de zones) restent.
    deleted.record_concurrent = db.prepare("DELETE FROM meta WHERE k = 'record_concurrent'").run().changes;
  })();

  // Sans VACUUM le fichier garde la taille des donnees effacees sur le volume.
  // Un echec ici ne remet pas en cause la suppression, deja committee.
  let vacuumed = true;
  try { db.exec('VACUUM'); } catch (e) { vacuumed = false; }

  res.json({ ok: true, deleted, vacuumed });
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
  /* Langue choisie dans les options du jeu. Deja un code a deux lettres,
     donc aucune normalisation a faire. Les sessions anterieures au retrait
     de `locale` ressortent en 'unknown' : leur langue de jeu n'a jamais
     ete connue, seule celle du systeme l'etait. */
  const rows = db.prepare(`
    SELECT CASE
             WHEN game_lang IS NULL OR game_lang = '' THEN 'unknown'
             ELSE LOWER(game_lang)
           END AS lang,
           COUNT(DISTINCT player_id) AS players
    FROM sessions
    GROUP BY lang
  `).all();
  const languages = {};
  for (const r of rows) languages[r.lang] = r.players;
  res.json({ languages });
});

// compare deux versions « 0.4.1 » facon numerique : 0.10 > 0.9, ce qu'un tri
// alphabetique se trompe a faire
function cmpVersion(a, b) {
  const pa = String(a).split(/[^\d]+/).filter(s => s !== '').map(Number);
  const pb = String(b).split(/[^\d]+/).filter(s => s !== '').map(Number);
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const d = (pa[i] || 0) - (pb[i] || 0);
    if (d !== 0) return d;
  }
  return 0;
}

app.get('/v1/stats/versions', adminLimiter, requireAdmin, (req, res) => {
  const totalRows = db.prepare(`
    SELECT version, COUNT(DISTINCT player_id) AS players
    FROM sessions WHERE version IS NOT NULL AND version != '' GROUP BY version
  `).all();
  const total = {};
  for (const r of totalRows) total[r.version] = r.players;

  const cutoff = Date.now() - ACTIVE_WINDOW_MS;
  const onlineRows = db.prepare(`
    SELECT version, COUNT(*) AS online
    FROM sessions WHERE end_ts IS NULL AND last_seen >= ? AND version IS NOT NULL AND version != '' GROUP BY version
  `).all(cutoff);
  const online = {};
  for (const r of onlineRows) online[r.version] = r.online;

  /* Version de reference : celle publiee dans le manifest d'update, qui est ce
     que le jeu propose reellement de telecharger. A defaut (aucun update publie),
     la plus haute version vue passer. */
  let latest = null;
  try {
    const row = getMeta.get('game_update_manifest');
    const m = row && row.v ? JSON.parse(row.v) : null;
    if (m && m.version) latest = String(m.version);
  } catch (e) {}
  const seen = Object.keys(total);
  if (!latest && seen.length) latest = seen.slice().sort(cmpVersion).pop();

  /* Les retardataires : derniere version connue de chaque joueur, quand elle est
     inferieure a la reference. C'est la liste a pinger vers itch quand une MAJ sort.
     Le pseudo Discord n'est present que pour les joueurs qui ont lie leur compte. */
  let laggards = [];
  if (latest) {
    const rows = db.prepare(`
      SELECT s.player_id, s.version, MAX(s.last_seen) AS last_seen, d.discord_username
      FROM sessions s
      LEFT JOIN discord_links d ON d.player_id = s.player_id
      WHERE s.version IS NOT NULL AND s.version != ''
      GROUP BY s.player_id
      ORDER BY last_seen DESC
      LIMIT 500
    `).all();
    laggards = rows.filter(r => cmpVersion(r.version, latest) < 0);
  }

  res.json({ total, online, latest, laggards });
});

app.get('/v1/stats/deaths', adminLimiter, requireAdmin, (req, res) => {
  const mapId = Number.isFinite(parseInt(req.query.mapId, 10)) ? parseInt(req.query.mapId, 10) : null;

  const byMap = db.prepare(`
    SELECT map_id, COUNT(*) AS count, COUNT(DISTINCT player_id) AS players
    FROM deaths WHERE map_id IS NOT NULL GROUP BY map_id ORDER BY count DESC
  `).all();

  const byEnemy = db.prepare(`
    SELECT enemy, COUNT(*) AS count, COUNT(DISTINCT player_id) AS players
    FROM deaths WHERE enemy IS NOT NULL AND enemy != '' GROUP BY enemy ORDER BY count DESC
  `).all();

  /* Classement de l'ennemi PRECIS, et non de sa famille : deux Nymphes du meme
     type n'ont pas du tout le meme taux de capture selon ou elles patrouillent.
     La cle est (map_id, enemy_event_id), le nom n'etant qu'une etiquette. Les
     captures anterieures a la colonne, ou sans coupable, restent hors classement.
     MAX(enemy_instance) : le nom est constant dans un groupe, l'agregat ne sert
     qu'a satisfaire le GROUP BY. */
  const byInstance = db.prepare(`
    SELECT map_id, enemy_event_id, MAX(enemy_instance) AS name, enemy AS family,
           COUNT(*) AS count, COUNT(DISTINCT player_id) AS players
    FROM deaths
    WHERE enemy_event_id IS NOT NULL
    GROUP BY map_id, enemy_event_id
    ORDER BY count DESC
    LIMIT 40
  `).all();

  /* Les croix de la carte. Agrege cote SQL : le dashboard recoit une position
     unique par case avec son poids, pas les dizaines de milliers de morts brutes.
     L'ennemi retenu est celui qui tue le plus souvent a cet endroit precis. */
  const points = db.prepare(`
    SELECT map_id, x, y, COUNT(*) AS count,
           (SELECT d2.enemy FROM deaths d2
             WHERE d2.map_id = d.map_id AND d2.x = d.x AND d2.y = d.y AND d2.enemy IS NOT NULL
             GROUP BY d2.enemy ORDER BY COUNT(*) DESC LIMIT 1) AS enemy
    FROM deaths d
    WHERE x IS NOT NULL AND y IS NOT NULL AND (@map_id IS NULL OR map_id = @map_id)
    GROUP BY map_id, x, y
    ORDER BY count DESC
    LIMIT 5000
  `).all({ map_id: mapId });

  /* `total` compte les captures, `fatalTotal` les seules qui ont vide les PV.
     Le rapport entre les deux dit combien de fois un joueur se fait attraper
     avant d'y rester. */
  const totalRow = db.prepare(`
    SELECT COUNT(*) AS n,
           COUNT(DISTINCT player_id) AS players,
           SUM(CASE WHEN fatal = 1 THEN 1 ELSE 0 END) AS fatal
    FROM deaths
  `).get();

  res.json({
    byMap, byEnemy, byInstance, points,
    total: totalRow.n, players: totalRow.players, fatalTotal: totalRow.fatal || 0
  });
});

app.get('/v1/stats/endings', adminLimiter, requireAdmin, (req, res) => {
  /* On compte des JOUEURS, pas des lignes : « 50% fin Chad » parle de gens, et
     un meme joueur peut rejouer et revoir la meme fin dix fois. */
  const rows = db.prepare(`
    SELECT ending, COUNT(DISTINCT player_id) AS players FROM endings GROUP BY ending
  `).all();
  const endings = {};
  for (const r of rows) endings[r.ending] = r.players;

  const reached = db.prepare(`SELECT COUNT(DISTINCT player_id) AS n FROM endings`).get().n;

  const favRows = db.prepare(`
    SELECT favourite, COUNT(*) AS n FROM endings
    WHERE favourite IS NOT NULL AND favourite != '' GROUP BY favourite ORDER BY n DESC
  `).all();
  const favourites = {};
  for (const r of favRows) favourites[r.favourite] = r.n;

  const choiceRows = db.prepare(`SELECT choice, COUNT(*) AS n FROM gameover_choices GROUP BY choice`).all();
  const afterDeath = {};
  for (const r of choiceRows) afterDeath[r.choice] = r.n;

  res.json({ endings, reached, favourites, afterDeath });
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

// Demo completion: players who reached BOTH Trial 2 branches (the 2 crystals),
// plus per-map depth reached in each branch to locate where crystals are picked up.
app.get('/v1/stats/completion', adminLimiter, requireAdmin, (req, res) => {
  const bothBranches = db.prepare(`
    SELECT COUNT(*) AS n FROM (
      SELECT player_id FROM events WHERE zone='jeu2_gauche'
      INTERSECT
      SELECT player_id FROM events WHERE zone='jeu2_droite'
    )
  `).get().n;
  const gaucheOnly = db.prepare(`
    SELECT COUNT(*) AS n FROM (
      SELECT player_id FROM events WHERE zone='jeu2_gauche'
      EXCEPT
      SELECT player_id FROM events WHERE zone='jeu2_droite'
    )
  `).get().n;
  const droiteOnly = db.prepare(`
    SELECT COUNT(*) AS n FROM (
      SELECT player_id FROM events WHERE zone='jeu2_droite'
      EXCEPT
      SELECT player_id FROM events WHERE zone='jeu2_gauche'
    )
  `).get().n;
  const maps = db.prepare(`
    SELECT map_id, COUNT(DISTINCT player_id) AS players
    FROM events WHERE map_id IN (19,21,22,28,29,23,24,25,26,27,30,31)
    GROUP BY map_id ORDER BY map_id
  `).all();
  res.json({ bothBranches, gaucheOnly, droiteOnly, maps });
});

// Progression funnel: distinct players who EVER reached each zone (from events).
app.get('/v1/stats/progression', adminLimiter, requireAdmin, (req, res) => {
  const rows = db.prepare(`
    SELECT zone, COUNT(DISTINCT player_id) AS players
    FROM events WHERE zone IS NOT NULL AND zone != '' AND zone != 'unknown'
    GROUP BY zone
  `).all();
  const byZone = {};
  for (const r of rows) byZone[r.zone] = r.players;
  const eventsPlayers = db.prepare(`SELECT COUNT(DISTINCT player_id) AS n FROM events`).get().n;
  const sessionsPlayers = db.prepare(`SELECT COUNT(DISTINCT player_id) AS n FROM sessions`).get().n;
  const eventsTotal = db.prepare(`SELECT COUNT(*) AS n FROM events`).get().n;
  res.json({ byZone, eventsPlayers, sessionsPlayers, eventsTotal });
});

// Diagnostic: distribution of finished-session durations across buckets.
app.get('/v1/stats/sessions/distribution', adminLimiter, requireAdmin, (req, res) => {
  const rows = db.prepare(`
    SELECT CASE
             WHEN d < 10000    THEN '00 <10s'
             WHEN d < 60000    THEN '01 10-60s'
             WHEN d < 300000   THEN '02 1-5m'
             WHEN d < 900000   THEN '03 5-15m'
             WHEN d < 1800000  THEN '04 15-30m'
             WHEN d < 3600000  THEN '05 30-60m'
             WHEN d < 7200000  THEN '06 1-2h'
             WHEN d < 21600000 THEN '07 2-6h'
             WHEN d < 43200000 THEN '08 6-12h'
             WHEN d < 86400000 THEN '09 12-24h'
             ELSE                   '10 >24h'
           END AS bucket,
           COUNT(*) AS n
    FROM (SELECT (end_ts - start_ts) AS d FROM sessions WHERE end_ts IS NOT NULL AND end_ts > start_ts)
    GROUP BY bucket
    ORDER BY bucket ASC
  `).all();
  const total = rows.reduce((a, r) => a + r.n, 0);
  res.json({ total, buckets: rows.map(r => ({ range: r.bucket.slice(3), n: r.n, pct: total ? Math.round(r.n / total * 1000) / 10 : 0 })) });
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

/* Previent le dashboard qu'une capture vient d'etre enregistree, pour qu'il
   redessine la carte sans attendre son cycle d'une minute. Le message ne porte
   aucune donnee : le client rappelle /v1/stats/deaths, seule source de verite.
   Groupe sur 300 ms, car un lot d'evenements peut en contenir plusieurs et
   plusieurs joueurs peuvent se faire prendre en meme temps. */
let captureTimer = null;
function broadcastCapture() {
  if (captureTimer) return;
  captureTimer = setTimeout(() => {
    captureTimer = null;
    const payload = JSON.stringify({ type: 'capture' });
    wss.clients.forEach((c) => { if (c.readyState === 1) c.send(payload); });
  }, 300);
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
