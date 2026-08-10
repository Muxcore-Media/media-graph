# Media Graph

Unified media graph for cross-media awareness in MuxCore.

Exposes `muxcore.mediagraph.v1.MediaGraphService` — nodes across media kinds (movie, book, album, …) with typed edges (`adaptation_of`, `soundtrack_of`, `same_franchise`, …), neighbor walk, and shortest path.

## Ports

| Service | Default |
|---------|---------|
| gRPC | `:9730` |
| Health | `:9731` |

## Status

v0.1.0 in-memory scaffold — persistence, library auto-ingest, and UI graph browser are follow-ups.
