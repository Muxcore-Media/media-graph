# Media Graph

Unified media graph for cross-media awareness in MuxCore.

Exposes `muxcore.mediagraph.v1.MediaGraphService` — nodes across media kinds (movie, book, album, comic, audiobook, …) with typed edges (`adaptation_of`, `soundtrack_of`, `same_franchise`, …), neighbor walk, shortest path, and **related titles**.

Enable in the MVP stack with `MVP_ENABLE_MEDIA_GRAPH=1` in `_mvp/run-host.sh` / vault env.

## Ports

| Service | Default |
|---------|---------|
| gRPC | `:9730` (`GRAPH_GRPC_ADDR`) |
| Health + admin JSON | `127.0.0.1:9731` (`MUXCORE_HTTP_ADDR`) |

## Env

| Variable | Default | Purpose |
|----------|---------|---------|
| `GRAPH_GRPC_ADDR` | `:9730` | gRPC listen address |
| `GRAPH_DB_PATH` | `./data/graph/graph.db` | SQLite store |
| `GRAPH_DEFAULT_REL` | `related_to` | Default edge rel when omitted |
| `GRAPH_AUTO_LINK` | `false`* | Same-title `same_franchise` edges |
| `GRAPH_INGEST_ENABLED` | `false`* | Poll + event library ingest |
| `GRAPH_INGEST_INTERVAL` | `15m` | Poll period |
| `GRAPH_INGEST_PAGE_SIZE` | `100` | List page size |
| `GRAPH_FIXTURE_PATH` | — | Offline fixture dir/file loaded at boot |
| `GRAPH_MODULE_TOKEN` | — | Admin token for `/api/graph*` (also `MUXCORE_MODULE_TOKEN`, `MUXCORE_TOKEN`) |
| `MUXCORE_HTTP_ADDR` | `127.0.0.1:9731` | Health + admin JSON bind |
| `MVP_ENABLE_MEDIA_GRAPH` | `0` | Enable module in `_mvp/run-host.sh` |

\*When unset in code, `Config` zero-values apply; `run-host.sh` sets `GRAPH_AUTO_LINK` and `GRAPH_INGEST_ENABLED` to `true` when the module is enabled.

Library nodes use `external_id` schemes:

- Movies/TV: `tmdb:movie:{id}` / `tmdb:tv:{id}` with attrs `movie_id` / `series_id`
- Books: `isbn:{isbn}` with `book_id`
- Albums: `mbid:{musicbrainz_id}` with `album_id`
- Comics: `comicvine:{id}` with `series_id`
- Audiobooks: `asin:{asin}` with `audiobook_id`

Ingest discovers peers: `media.library.movies`, `media.library.tv`, `media.books`, `media.library.music`, `media.comics`, `media.audiobooks`.

## Offline fixtures

`internal/testdata/library/movies_tv.json` — local movies/TV rows + franchise edges. Set `GRAPH_FIXTURE_PATH` to that directory (or a copy) to load at boot without live library peers. Tests call `IngestLibraryFixtures` directly.

## Admin graph browser (JSON)

Requires admin token (`GRAPH_MODULE_TOKEN` / `MUXCORE_TOKEN`) or loopback client.

| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/graph` | node + edge counts |
| GET | `/api/graph/nodes?kind=` | list |
| GET | `/api/graph/node?id=` / `?external_id=` | detail |
| POST | `/api/graph/node` | upsert node JSON |
| DELETE | `/api/graph/node?id=` | delete (`?cascade=true`) |
| GET | `/api/graph/related?id=` / `?external_id=` | related titles |
| GET | `/api/graph/search?q=` | title search |
| GET | `/api/graph/neighbors?id=` | neighbor walk |
| GET | `/api/graph/path?from_id=&to_id=` | shortest path |
| GET | `/api/graph/edges` | all edges |
| POST | `/api/graph/link` | create/update link |
| DELETE | `/api/graph/link?edge_id=` | remove link |

gRPC: `GetRelatedTitles`, `GetNode` (by id or `external_id`).

## Status

v0.1.3 — SQLite persistence, same-title auto-link, multi-library ingest, offline fixtures, related-title query, admin JSON browser.
