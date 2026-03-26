# Purpose
This project is a local web explorer for object and hierarchical cloud storage, with a Node.js backend serving the UI and acting as a storage proxy. The main flow is:

1. receive storage credentials and target details from the frontend
2. create a temporary local session in the backend
3. list prefixes, folders, and files
4. preview supported files
5. download files and remove all objects or paths under a prefix

# Stack And Structure
- `server.js`: Node HTTPS server by default, `/api/*` endpoints, in-memory session handling, and storage-provider access
- `app.js`: UI logic, local state, backend calls, and rendering
- `index.html`: page structure
- `styles.css`: interface styling
- `start.sh`: local bootstrap script with automatic `npm install` when `node_modules` is missing
- `CHANGELOG.md`: repository release history maintained by the release workflow
- `samples/`: sample files used for local development

The project uses plain ESM JavaScript and no frontend framework. Keep that simplicity unless explicitly asked to change it.
Folders that contain `metadata/*.metadata.json` should be treated as Iceberg table roots by the frontend and previewed through backend snapshot inspection instead of raw file listing.
When a prefix is in Iceberg mode, the preview toolbar should expose the table snapshots available in metadata so the user can switch the sampled snapshot explicitly.
The object browser supports a client-side name filter persisted in browser storage per exact folder context using provider plus stable target/location identity and prefix; the filter applies only in raw folder mode, not in Iceberg mode.
Directory creation from the object browser must create exactly one immediate child under the current prefix; object-store providers materialize folder marker objects while ADLS creates a native directory, and the action stays unavailable in Iceberg mode.
The object browser distinguishes between clearing the current folder contents and deleting a child folder entirely: the toolbar destructive action preserves the current folder, while the row-level folder delete removes the selected child folder plus all nested contents across supported providers.
Temporary Iceberg sample seeding writes only under `<current-prefix>iceberg/` in the connected storage target, stages the fixture files locally in the backend before upload, and the seed action should only be exposed when the current folder is empty.
The temporary Iceberg sample seed UI must remain disabled when the backend is running inside Docker or with `NODE_ENV=production`; it is a local development-only feature.
Avro preview is handled in the backend for Avro Object Container Files, including files that use the Avro `snappy` codec.
ORC preview now relies on Java being available on the backend host and caches the Apache ORC tools jar under `.cache/orc-tools/` on first use.
The Docker image must include a Java runtime so ORC preview works in containerized runs too.
When built by GitHub Actions for release publishing, the Docker image must receive `APP_VERSION` so the header version matches the published Docker tag.
Release automation keeps Git tags and GitHub Releases in `vX.Y.Z` format, while Docker image tags use `X.Y.Z` plus `latest`.
For AWS S3, MinIO, and GCS, folder timestamps in the browser are derived from the newest object found under each immediate child prefix, including a real marker object when present; this adds an extra recursive listing cost to object-browser loads for those providers.

# Supported Providers
- AWS S3
- Azure Data Lake Storage Gen2 via the DFS endpoint
- Google Cloud Storage
- MinIO via S3-compatible endpoint access

Until instructed otherwise, provider authentication should remain:
- ADLS: `account name + container name + access key`
- GCS: `bucket + service account JSON`
- MinIO: `endpoint + bucket + access keys`

For MinIO, ignoring HTTPS certificate validation must remain an explicit per-connection opt-in in the UI and must not affect other providers.

# Commands
- install dependencies: `npm install`
- start the server: `npm start`
- standard local flow: `./start.sh`
- build container image: `docker build -t multibucket-explorer .`
- run container: `docker run --rm -p 8086:8086 -v "$(pwd)/certs:/run/certs:ro" -e TLS_CERT_FILE=/run/certs/tls.crt -e TLS_KEY_FILE=/run/certs/tls.key multibucket-explorer`
- run container in read-only mode for destructive actions: `docker run --rm -p 8086:8086 -v "$(pwd)/certs:/run/certs:ro" -e TLS_CERT_FILE=/run/certs/tls.crt -e TLS_KEY_FILE=/run/certs/tls.key -e DISABLE_DESTRUCTIVE_OPERATIONS=true multibucket-explorer`
- publish Docker Hub image automatically from `main` after configuring `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` GitHub secrets; semantic versioning is derived from commit messages (`BREAKING CHANGE`/`!` = major, `feat:` = minor, otherwise patch), each automated tag must also create a GitHub Release, and the workflow must prepend the new entry to `CHANGELOG.md`

By default the application runs at `https://localhost:8086`.

# Development Rules
1. Before changing behavior, read `README.md`, `server.js`, and `app.js` to preserve the current flow.
2. After every delivery, explicitly evaluate whether `AGENTS.md` should be updated to preserve long-term project memory.
3. After any relevant change, update `README.md` and this `AGENTS.md` if there is a new convention, command, operational risk, or architectural decision.
4. The entire project, including documentation and user-facing text, must remain in English unless the user explicitly requests another language.
5. Avoid adding new dependencies without a clear need. The project is intentionally small and direct.
6. Preserve compatibility with Node running in ESM mode and with simple local execution through `npm start`.
7. If you add new operational scripts, document their usage in `README.md`.
8. When creating commits, always include new files that are part of the delivery unless the user explicitly asks to exclude them.
9. When closing out work with a commit, run `git push` by default unless the user explicitly asks not to push or there is a technical constraint that blocks it.
10. HTTPS on port `8086` is the default runtime contract. `TLS_CERT_FILE` and `TLS_KEY_FILE` are required unless `ALLOW_INSECURE_HTTP=true` is set explicitly for local or emergency use.
11. `README.md` should document a local self-signed OpenSSL flow for developers who need to test HTTPS without an existing host certificate.

# Backend Rules
1. All storage access must continue to go through the backend. Do not move provider SDK access directly into the browser.
2. Treat route inputs and query string values as untrusted. Validate `sessionId`, `prefix`, `key`, preview limits, and preview modes.
3. When changing session behavior, preserve in-memory expiration and review credential exposure risks.
4. Changes to the destructive `/api/delete-prefix` endpoint require extra care. Never allow deleting the storage root.
5. When `DISABLE_DESTRUCTIVE_OPERATIONS=true` is set, all destructive endpoints must remain blocked server-side regardless of frontend state.
6. Prefer useful error messages without exposing secrets, keys, or unnecessary stack traces.
7. Provider-specific behavior should be isolated behind storage helper functions or provider abstractions rather than spread through unrelated preview code.

# Frontend Rules
1. Keep the interface in English unless the user explicitly requests localization.
2. Preserve the current local-app experience: provider card quick-connect, credential modal editing/testing, object browser, and preview panel.
3. Avoid frameworks, bundlers, or build steps if the problem can be solved within the current HTML/CSS/JS setup.
4. When adding controls, wire state, visual feedback, and error handling consistently with the rest of `app.js`.
5. Provider selection must remain explicit in the UI when behavior or required credentials differ across backends.
6. Provider selection must stay explicit through the provider cards; clicking a card is the quick-connect path and the pencil action opens the credential modal for that provider.
7. The credential modal must show only the selected provider's fields; users should not have to infer which fields belong to which backend.
8. Destructive actions in the UI should prefer first-class confirmation modals over browser-native dialogs, and the default focused action must be the safe non-destructive option.
9. Long-running destructive actions should show explicit in-progress feedback in the UI even when the backend only provides completion status.
10. The frontend must remain multilingual with English as the default plus Brazilian Portuguese (`pt-BR`), Spanish (`es`), and Italian (`it`) for user-facing UI text and frontend-generated messages.
11. Backend error responses surfaced in the frontend must follow the same language set when they originate from application-controlled validation or guardrails.
12. When destructive operations are disabled by server configuration, the UI should hide or disable delete affordances instead of inviting an action that will always fail.

# Security And Sensitive Data
1. Storage credentials are entered through the UI and must remain persistable for all provider-specific connection fields, credentials, secrets, textarea content, and toggles unless the behavior is intentionally redesigned. They are now encrypted at rest in browser `localStorage` behind a user-provided master passphrase, with same-tab reload unlock convenience carried through `sessionStorage`. Any change in this area must consider security impact and be documented.
2. Never log secrets such as AWS secret keys or Azure access keys in logs, error messages, state dumps, or documentation.
3. Do not commit real credentials, private buckets, private file systems, or customer-sensitive data.
4. If testing against real cloud storage is necessary, prefer minimally invasive validation and clearly confirm any destructive action.
5. Self-signed certificates are acceptable for local/internal use, but certificate trust distribution is an operational concern and should be documented instead of handled in app code.

# Validation
1. There is no automated test suite in the repository today, so manually validate the changed flow whenever possible.
2. For UI changes, verify at least: connect, list objects, navigate folders, preview, and download.
3. For deletion changes, validate first with controlled prefixes and never assume the behavior is safe without executing the flow.
4. When changing provider support, validate both S3 and ADLS paths unless the task is explicitly provider-specific.

# Persistent Decisions
1. This file should contain only durable project guidance.
2. Temporary task notes or experiments should not live here.
3. If a decision changes how the app should be run, developed, or operated, record it in this file.
4. `DISABLE_DESTRUCTIVE_OPERATIONS=true` is the runtime switch for read-only delete behavior and must block prefix deletion, row-level folder deletion, and single-file deletion.
5. Prefix deletion must remove only the contents under the selected folder and preserve that folder across all supported providers.
6. Row-level folder deletion must fully remove the selected child folder, including nested contents and any marker object used by object-store providers.
