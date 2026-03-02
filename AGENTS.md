# Purpose
This project is a local web explorer for object and hierarchical cloud storage, with a Node.js backend serving the UI and acting as a storage proxy. The main flow is:

1. receive storage credentials and target details from the frontend
2. create a temporary local session in the backend
3. list prefixes, folders, and files
4. preview supported files
5. download files and remove all objects or paths under a prefix

# Stack And Structure
- `server.js`: Node HTTP server, `/api/*` endpoints, in-memory session handling, and storage-provider access
- `app.js`: UI logic, local state, backend calls, and rendering
- `index.html`: page structure
- `styles.css`: interface styling
- `start.sh`: local bootstrap script with automatic `npm install` when `node_modules` is missing
- `samples/`: sample files used for local development

The project uses plain ESM JavaScript and no frontend framework. Keep that simplicity unless explicitly asked to change it.

# Supported Providers
- AWS S3
- Azure Data Lake Storage Gen2 via the DFS endpoint

Until instructed otherwise, the ADLS path should remain based on shared key authentication with `account name + container name + access key`.

# Commands
- install dependencies: `npm install`
- start the server: `npm start`
- standard local flow: `./start.sh`

By default the application runs at `http://localhost:8086`.

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

# Backend Rules
1. All storage access must continue to go through the backend. Do not move provider SDK access directly into the browser.
2. Treat route inputs and query string values as untrusted. Validate `sessionId`, `prefix`, `key`, preview limits, and preview modes.
3. When changing session behavior, preserve in-memory expiration and review credential exposure risks.
4. Changes to the destructive `/api/delete-prefix` endpoint require extra care. Never allow deleting the storage root.
5. Prefer useful error messages without exposing secrets, keys, or unnecessary stack traces.
6. Provider-specific behavior should be isolated behind storage helper functions or provider abstractions rather than spread through unrelated preview code.

# Frontend Rules
1. Keep the interface in English unless the user explicitly requests localization.
2. Preserve the current local-app experience: connection form, object browser, and preview panel.
3. Avoid frameworks, bundlers, or build steps if the problem can be solved within the current HTML/CSS/JS setup.
4. When adding controls, wire state, visual feedback, and error handling consistently with the rest of `app.js`.
5. Provider selection must remain explicit in the UI when behavior or required credentials differ across backends.

# Security And Sensitive Data
1. Storage credentials are entered through the UI and are currently persisted in `localStorage`. Any change in this area must consider security impact and be documented.
2. Never log secrets such as AWS secret keys or Azure access keys in logs, error messages, state dumps, or documentation.
3. Do not commit real credentials, private buckets, private file systems, or customer-sensitive data.
4. If testing against real cloud storage is necessary, prefer minimally invasive validation and clearly confirm any destructive action.

# Validation
1. There is no automated test suite in the repository today, so manually validate the changed flow whenever possible.
2. For UI changes, verify at least: connect, list objects, navigate folders, preview, and download.
3. For deletion changes, validate first with controlled prefixes and never assume the behavior is safe without executing the flow.
4. When changing provider support, validate both S3 and ADLS paths unless the task is explicitly provider-specific.

# Persistent Decisions
1. This file should contain only durable project guidance.
2. Temporary task notes or experiments should not live here.
3. If a decision changes how the app should be run, developed, or operated, record it in this file.
