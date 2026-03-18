# Changelog


## [v0.7.0](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.7.0) - 2026-03-18

### Changes
- feat(browser): derive virtual folder timestamps for object stores (c8d8731)
- chore(changelog): update for v0.6.4 [skip ci] (a96dfa1)

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.6.4...v0.7.0)


## [v0.6.4](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.6.4) - 2026-03-18

### Changes
- fix(release): stop publishing duplicate docker version tags (f08b834)
- chore(changelog): update for v0.6.3 [skip ci] (c0a643f)

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.6.3...v0.6.4)


## [v0.6.3](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.6.3) - 2026-03-18

### Changes
- fix(ui): show folder modification date when available (4e8a00e)
- chore(changelog): update for v0.6.2 [skip ci] (fac5f5d)

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.6.2...v0.6.3)


## [v0.6.2](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.6.2) - 2026-03-13

### Changes
- docs(ui): add usage disclaimer to app and README (446931d)
- chore(changelog): update for v0.6.1 [skip ci] (529a3b9)

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.6.1...v0.6.2)


## [v0.6.1](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.6.1) - 2026-03-13

### Changes
- fix(minio): restore https runtime helpers (fe4ed27)
- chore(changelog): update for v0.6.0 [skip ci] (179f4bf)

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.6.0...v0.6.1)


## [v0.6.0](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.6.0) - 2026-03-13

### Changes
- feat(server): require https on port 8086 by default (42ef205)
- chore(changelog): update for v0.5.1 [skip ci] (20a4e1b)

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.5.1...v0.6.0)


## [v0.5.1](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.5.1) - 2026-03-12

### Changes
- fix(ui): streamline disconnected explorer state (eac1856)
- chore(changelog): update for v0.5.0 [skip ci] (cff1b97)

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.5.0...v0.5.1)


## [v0.5.0](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.5.0) - 2026-03-12

### Changes
- feat(ui): refactor provider connection flow into modal-based quick connect (3f741dd)
- chore(changelog): update for v0.4.2 [skip ci] (5f75865)

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.4.2...v0.5.0)


## [v0.4.2](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.4.2) - 2026-03-11

### Changes
- fix(adls): restore folder navigation and iceberg sample seeding (5d414e9)
- chore(changelog): update for v0.4.1 [skip ci] (66e0c3f)

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.4.1...v0.4.2)


## [v0.4.1](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.4.1) - 2026-03-11

### Changes
- docs(changelog): expand recent release details (c31c6fa)
- chore(changelog): update for v0.4.0 [skip ci] (81747e5)

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.4.0...v0.4.1)


## [v0.4.0](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.4.0) - 2026-03-11

### Changes
- feat(iceberg): add snapshot browsing and dev sample tooling (22e761d)
- chore(changelog): update for v0.3.0 [skip ci] (94fea93)

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.3.0...v0.4.0)


## [v0.3.0](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.3.0) - 2026-03-11

### Changes
- feat(preview): add avro support (3e27eac)
  - Added backend preview support for Avro Object Container Files in `server.js`, including schema/column extraction and row sampling through the existing `/api/preview` contract.
  - Extended format detection so Avro previews also work for compressed name variants such as `.avro.gz`, `.avro.snappy`, `.gz.avro`, and `.snappy.avro`.
  - Added `avsc` as a backend dependency and configured Avro `long` decoding to avoid runtime failures when records contain 64-bit integer values outside JavaScript's safe integer range.
  - Reused the existing preview panel in `app.js` instead of introducing a new UI mode, so Avro rows render with the same table preview path already used for CSV/JSON/Parquet/ORC.
  - Updated user-facing preview copy and supported-format messaging so the frontend explicitly advertises Avro as a previewable format.
  - Documented the new preview capability in `README.md` and persisted the operational decision in `AGENTS.md` because Avro became a first-class backend preview format.
  - Validation performed for the release included syntax checks and a local smoke test against real Avro sample content, including files that use the Avro `snappy` codec.
- chore(changelog): update for v0.2.2 [skip ci] (cc535e2)
  - Prepended the generated `v0.2.2` release notes to `CHANGELOG.md` as part of the release automation path.
  - Marked the changelog-only commit with `[skip ci]` so the release bookkeeping update would not trigger another publish cycle.

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.2.2...v0.3.0)


## [v0.2.2](https://github.com/ivanyort/multibucket_explorer/releases/tag/v0.2.2) - 2026-03-11

### Changes
- Add changelog automation to release workflow (f327c84)
  - Updated the GitHub Actions release pipeline so every automated Docker publish also prepends a new version section to `CHANGELOG.md`.
  - Ensured the changelog update is committed back to `main` by the workflow itself, keeping the repository history aligned with the published Docker and GitHub Release artifacts.
  - Made changelog generation part of the same semantic-versioning flow that already derives major/minor/patch bumps from commit messages.
  - Established `CHANGELOG.md` as the durable in-repo release history instead of leaving release details only in GitHub Releases or workflow logs.
  - This change was operational rather than UI-facing: it altered the release process and repository bookkeeping, not runtime storage browsing behavior.

[Full diff](https://github.com/ivanyort/multibucket_explorer/compare/v0.2.1...v0.2.2)

All notable changes to this project will be documented in this file.
