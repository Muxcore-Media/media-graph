# Changelog

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
