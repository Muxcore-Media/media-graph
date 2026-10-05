# Changelog

## [0.1.4] - 2026-10-05


### Changed
- Reported version comes from muxcore.json (ADR-0021); built on core v0.6.12 / sdk/go/module v0.6.3 (mesh enrollment, ADR-0017).

## [0.1.3] - 2026-10-05

### Changed
- Dependencies resolve from published GitHub tags (core v0.6.2, media-music v0.3.0); no filesystem `replace`.
- CI runs on GitHub-hosted runners from the umbrella template; legacy workflow directory removed.

## [v0.1.3] — 2026-08-10

### Added
- Offline movies/TV library fixtures (`internal/testdata/library`) + `IngestLibraryFixtures`
- `GetRelatedTitles` gRPC query API (by node id or `external_id`)
- Admin graph browser stub: HTTP/JSON `/api/graph`, `/nodes`, `/node`, `/related`, `/search`

## [v0.1.2] — 2026-08-10

### Added
- Library auto-ingest from `media-movies` / `media-tvshows` (poll + add/remove/update events)
- `external_id` scheme `tmdb:movie:{id}` / `tmdb:tv:{id}`
- Settings: `ingest_enabled`, `ingest_interval` (`GRAPH_INGEST_*`)

## [v0.1.1] — 2026-08-10

### Added
- SQLite persistence (`GRAPH_DB_PATH`)
- Same-title cross-kind auto-link (`same_franchise`)
- Upsert by `external_id` reuse

## [v0.1.0] — 2026-08-10

### Added
- `MediaGraphService` (Upsert/Search/Link/Neighbors/Path)
- In-memory graph store + SettingsProvider (`default_rel`)
- Health `:9731`
