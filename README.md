# Media Graph

Unified media graph for cross-media awareness in MuxCore.

Exposes `muxcore.mediagraph.v1.MediaGraphService` — nodes across media kinds (movie, book, album, …) with typed edges (`adaptation_of`, `soundtrack_of`, `same_franchise`, …), neighbor walk, and shortest path.

## Ports

| Service | Default |
|---------|---------|
| gRPC | `:9730` |
| Health | `:9731` |

## Env

| Variable | Default | Purpose |
|----------|---------|---------|
| `GRAPH_DB_PATH` | `./data/graph/graph.db` | SQLite store |
| `GRAPH_AUTO_LINK` | `true` | Same-title `same_franchise` edges |
| `GRAPH_INGEST_ENABLED` | `true` | Poll + event library ingest |
| `GRAPH_INGEST_INTERVAL` | `15m` | Poll period |
| `GRAPH_INGEST_PAGE_SIZE` | `100` | List page size |

Library nodes use `external_id` `tmdb:movie:{id}` / `tmdb:tv:{id}` and attrs `movie_id` / `series_id`.

## Status

v0.1.2 — SQLite persistence, same-title auto-link, library auto-ingest. UI graph browser is a follow-up.
