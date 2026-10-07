# Succubus Telemetry Server

Backend for `SG_Telemetry.js` (in-game) and `SuccubusStats.exe` (owner dashboard).

## Endpoints

- `POST /v1/event` — game sends batches of events. Header `X-Game-Token: <GAME_TOKEN>`.
- `GET /v1/stats/live` — admin. Current online + per-zone + per-map.
- `GET /v1/stats/dropoff?rangeMs=86400000` — admin. Top quit zones/maps. `rangeMs=all` = since forever.
- `GET /v1/stats/deaths?mapId=24` — admin. Captures; with `mapId` the enemy rankings cover that map only.
- `GET /v1/stats/bonuses` — admin. Share of players who found each bonus (events `bonus` / `bonus_sync`).
- `GET /v1/stats/links` — admin. Clicks on in-game links (event `link_click`) and per-announcement click-through.
- `GET /v1/stats/concurrent?rangeMs=86400000&bucketMs=300000` — admin. Historical curve.
- `WS /v1/stream?token=<ADMIN_TOKEN>` — admin live push (snapshot on connect + on event).

All admin routes require header `Authorization: Bearer <ADMIN_TOKEN>`.

## Railway deploy

1. Create a new Railway project from this folder (push to GitHub then "Deploy from repo").
2. Add a **Volume** mounted at `/data`.
3. Set env vars:
   - `GAME_TOKEN` — random string, baked into the game plugin.
   - `ADMIN_TOKEN` — random string, baked into `SuccubusStats.exe`.
   - `DB_DIR` — `/data` (default).
4. Railway will run `npm start`. Note the public HTTPS URL → put it in the game's `Endpoint` plugin parameter.

## Local test

```
npm install
GAME_TOKEN=test ADMIN_TOKEN=admin DB_DIR=./data node server.js
```
