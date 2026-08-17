# Media Graph

Unified media graph for cross-media awareness in MuxCore.

Exposes `muxcore.mediagraph.v1.MediaGraphService` — nodes across media kinds (movie, book, album, …) with typed edges (`adaptation_of`, `soundtrack_of`, `same_franchise`, …), neighbor walk, shortest path, and **related titles**.

## Ports

| Service | Default |
|---------|---------|
| gRPC | `:9730` |
| Health + admin JSON | `:9731` |

## Env

| Variable | Default | Purpose |
|----------|---------|---------|
| `GRAPH_DB_PATH` | `./data/graph/graph.db` | SQLite store |
| `GRAPH_AUTO_LINK` | `true` | Same-title `same_franchise` edges |
| `GRAPH_INGEST_ENABLED` | `true` | Poll + event library ingest |
| `GRAPH_INGEST_INTERVAL` | `15m` | Poll period |
| `GRAPH_INGEST_PAGE_SIZE` | `100` | List page size |

Library nodes use `external_id` `tmdb:movie:{id}` / `tmdb:tv:{id}` and attrs `movie_id` / `series_id`.

## Offline fixtures

`internal/testdata/library/movies_tv.json` — local movies/TV rows + franchise edges. Tests call `IngestLibraryFixtures` (no network).

## Admin graph browser (JSON stub)

| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/graph` | node count |
| GET | `/api/graph/nodes?kind=` | list |
| GET | `/api/graph/node?id=` / `?external_id=` | detail |
| GET | `/api/graph/related?id=` / `?external_id=` | related titles |
| GET | `/api/graph/search?q=` | title search |

gRPC: `GetRelatedTitles`.

## Status

v0.1.3 — SQLite persistence, same-title auto-link, library auto-ingest, offline fixtures, related-title query, admin JSON browser stub.
