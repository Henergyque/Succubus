'use strict';
// Publishes a game update manifest to the telemetry server.
//
// Usage:
//   set ADMIN_TOKEN=yourtoken && set SERVER_URL=https://succubus-production.up.railway.app
//   node set-update.js <game_version> <local_file> <download_url> [<local_file2> <url2> ...]
//
// Example:
//   node set-update.js 0.3.3 ..\Succubus Games 0.3.2\www\js\plugins\SG_LanguageSystem.js https://raw.githubusercontent.com/Henergyque/.../SG_LanguageSystem.js
//
// To clear the current manifest (no more update):
//   node set-update.js clear

const https  = require('https');
const http   = require('http');
const fs     = require('fs');
const path   = require('path');
const crypto = require('crypto');
const url    = require('url');

const SERVER = process.env.SERVER_URL  || 'https://succubus-production.up.railway.app';
const TOKEN  = process.env.ADMIN_TOKEN || '';

if (!TOKEN) {
  console.error('Set ADMIN_TOKEN env var first.');
  process.exit(1);
}

// --- Clear mode ---
if (process.argv[2] === 'clear') {
  sendRequest('DELETE', '/v1/game/update', null, (err, data) => {
    if (err) { console.error('Error:', err); process.exit(1); }
    console.log('Manifest cleared.');
  });
  return;
}

const version = process.argv[2];
if (!version) {
  console.error('Usage: node set-update.js <version> <file> <url> [<file2> <url2> ...]');
  console.error('       node set-update.js clear');
  process.exit(1);
}

// Parse file/url pairs
const args = process.argv.slice(3);
if (args.length === 0 || args.length % 2 !== 0) {
  console.error('Provide pairs of <local_file> <url>.');
  process.exit(1);
}

// Hash is computed from the URL content (what players actually download)
// to avoid CRLF mismatches from git line-ending conversions.
const filePairs = [];
for (let i = 0; i < args.length; i += 2) {
  const localPath   = args[i];
  const downloadUrl = args[i + 1];
  if (!fs.existsSync(localPath)) { console.error('File not found:', localPath); process.exit(1); }
  const relPath = localPath.replace(/\\/g, '/').replace(/^.*?www\//, 'www/');
  filePairs.push({ relPath, downloadUrl });
}

let pending = filePairs.length;
const files = [];

filePairs.forEach(({ relPath, downloadUrl }) => {
  fetchRaw(downloadUrl, (err, content) => {
    if (err) { console.error('Failed to fetch', downloadUrl, err); process.exit(1); }
    const sha256 = crypto.createHash('sha256').update(content).digest('hex');
    files.push({ path: relPath, url: downloadUrl, sha256 });
    console.log(`  ${relPath}`);
    console.log(`  sha256: ${sha256}`);
    if (--pending === 0) publish();
  });
});

function publish() {
  const manifest = { version, files };
  console.log('\nPublishing manifest:', JSON.stringify(manifest, null, 2));
  sendRequest('POST', '/v1/game/update', { manifest }, (err) => {
    if (err) { console.error('Error:', err); process.exit(1); }
    console.log('✓ Manifest published. Players will receive this update on next launch.');
  });
}

function fetchRaw(urlStr, cb) {
  const parsed = new url.URL(urlStr);
  const isHttps = parsed.protocol === 'https:';
  const transport = isHttps ? https : http;
  const req = transport.request({ hostname: parsed.hostname, port: parsed.port || (isHttps ? 443 : 80), path: parsed.pathname + (parsed.search || ''), method: 'GET' }, (res) => {
    if (res.statusCode === 301 || res.statusCode === 302) return fetchRaw(res.headers.location, cb);
    const chunks = [];
    res.on('data', c => chunks.push(c));
    res.on('end', () => cb(null, Buffer.concat(chunks)));
  });
  req.on('error', e => cb(e.message));
  req.end();
}

// --- Helpers ---
function sendRequest(method, pathname, body, cb) {
  const parsed  = new url.URL(SERVER + pathname);
  const isHttps = parsed.protocol === 'https:';
  const bodyStr = body ? JSON.stringify(body) : '';

  const options = {
    hostname: parsed.hostname,
    port:     parsed.port || (isHttps ? 443 : 80),
    path:     parsed.pathname,
    method,
    headers:  {
      'Authorization': 'Bearer ' + TOKEN,
      'Content-Type':  'application/json',
      'Content-Length': Buffer.byteLength(bodyStr)
    }
  };

  const transport = isHttps ? https : http;
  const req = transport.request(options, (res) => {
    let data = '';
    res.on('data', c => data += c);
    res.on('end', () => {
      if (res.statusCode >= 400) return cb(res.statusCode + ' ' + data);
      try { cb(null, JSON.parse(data)); } catch(e) { cb(null, data); }
    });
  });
  req.on('error', e => cb(e.message));
  if (bodyStr) req.write(bodyStr);
  req.end();
}
