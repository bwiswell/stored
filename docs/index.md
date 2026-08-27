# stored — docs

Lean, source-tracking docs (seared/zeared style). One page per area as the
package grows.

## Design

The authoritative design lives in the workspace plan doc:
`~/stored/project-plans/01-architecture-and-scaffold.md` — what `stored` is, the
`seared` / `zeared` layering, the storage/ingest/query design, TTL + the
archival roadmap, and the milestone plan.

## Layout

- `stored/` — seared-only core: `store`, `registry`, `schema`, `row`, `writer`,
  `query`, `ttl`, `config`, `backends/`.
- `stored/zenoh/` — optional chronicler (`stored[zenoh]`): `chronicler`,
  `serve`, `session`, `daemon`.
- `systemd/stored.service` — production unit template.

## Status

M0 scaffold: the package imports and the public surface is stubbed. Storage,
ingest, and query land across M1–M4 (see the plan doc's build order).
