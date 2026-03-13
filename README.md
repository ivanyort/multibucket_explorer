# MultiBucket Explorer

Web application with a local backend for:

- browsing prefixes in AWS S3 buckets
- browsing prefixes in Azure Data Lake Storage Gen2 containers
- browsing prefixes in Google Cloud Storage buckets
- browsing prefixes in MinIO buckets
- listing folders and files
- detecting Iceberg table roots on folder click and previewing selectable snapshots through the backend
- previewing CSV, JSON, DFM, Markdown, plain text, Parquet, Avro, and ORC files, including `.gz` and `.snappy` compressed variants
- creating temporary Iceberg sample data under the current browser prefix plus `_sample_data/iceberg/` in the connected storage target when running in local development mode
- downloading files through a local proxy
- deleting a specific file from the current listing
- deleting all files under a prefix while preserving the selected folder
- switching the frontend language between English, Brazilian Portuguese, Spanish, and Italian

## Running

Install dependencies:

```bash
npm install
```

Start the server:

```bash
npm start
```

By default the application runs at `https://localhost:8086`.

HTTPS is mandatory unless you explicitly opt into insecure local HTTP with `ALLOW_INSECURE_HTTP=true`.
Provide PEM files through these environment variables before starting:

- `TLS_CERT_FILE`: certificate or full chain PEM file
- `TLS_KEY_FILE`: private key PEM file

For local testing, you can generate a self-signed certificate with OpenSSL:

```bash
mkdir -p certs

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout certs/localhost-key.pem \
  -out certs/localhost.pem \
  -days 365 \
  -subj "/CN=localhost"
```

Browsers will warn that the certificate is not trusted until you install it in the local trust store. For local testing, you can usually continue manually after the warning.

Example:

```bash
TLS_CERT_FILE=certs/localhost.pem TLS_KEY_FILE=certs/localhost-key.pem npm start
```

ORC preview requires `java` to be available in the backend host `PATH`.

To start in read-only mode for destructive operations, set `DISABLE_DESTRUCTIVE_OPERATIONS=true`:

```bash
TLS_CERT_FILE=certs/localhost.pem TLS_KEY_FILE=certs/localhost-key.pem DISABLE_DESTRUCTIVE_OPERATIONS=true npm start
```

To opt into insecure HTTP for local or emergency use only:

```bash
ALLOW_INSECURE_HTTP=true npm start
```

## Docker

Build the image:

```bash
docker build -t multibucket-explorer .
```

Run the container:

```bash
docker run --rm -p 8086:8086 \
  -v "$(pwd)/certs:/run/certs:ro" \
  -e TLS_CERT_FILE=/run/certs/tls.crt \
  -e TLS_KEY_FILE=/run/certs/tls.key \
  multibucket-explorer
```

If you already have a certificate on the host, mount that directory read-only and point the container to the mounted PEM files:

```bash
docker run -d --name multibucket-explorer -p 8086:8086 \
  -v /path/to/host/certs:/path/to/host/certs:ro \
  -e TLS_CERT_FILE=/path/to/host/certs/fullchain.pem \
  -e TLS_KEY_FILE=/path/to/host/certs/privkey.pem \
  ivanyort/multibucket-explorer:latest
```

The image now includes Java so ORC preview works inside Docker as well.

Run the container with destructive operations disabled:

```bash
docker run --rm -p 8086:8086 \
  -v "$(pwd)/certs:/run/certs:ro" \
  -e TLS_CERT_FILE=/run/certs/tls.crt \
  -e TLS_KEY_FILE=/run/certs/tls.key \
  -e DISABLE_DESTRUCTIVE_OPERATIONS=true \
  multibucket-explorer
```

If you want a different port inside the container, set `PORT` explicitly:

```bash
docker run --rm -p 8090:8090 \
  -v "$(pwd)/certs:/run/certs:ro" \
  -e TLS_CERT_FILE=/run/certs/tls.crt \
  -e TLS_KEY_FILE=/run/certs/tls.key \
  -e PORT=8090 \
  multibucket-explorer
```

## Releases And Changelog

The repository includes a GitHub Actions workflow at `.github/workflows/publish-docker.yml` that automatically calculates the next semantic version, updates `CHANGELOG.md`, creates the Git tag, creates a GitHub Release, and publishes the image to Docker Hub whenever code is pushed to `main`.
The published container also injects that same calculated version into the app header through the `APP_VERSION` environment variable at image build time.

Configure these GitHub repository secrets before using it:

- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`

By default the workflow publishes to:

```text
DOCKERHUB_USERNAME/multibucket-explorer
```

Version bump rules:

- commits with `BREAKING CHANGE` or `type!:` trigger a major bump
- commits starting with `feat:` trigger a minor bump
- all other commits trigger a patch bump

Examples:

- `feat(preview): add avro support` -> minor
- `fix(docker): install java runtime` -> patch
- `feat(api)!: change session contract` -> major

The workflow tags and publishes the resulting version as both `X.Y.Z` and `vX.Y.Z`, updates `CHANGELOG.md`, creates a matching GitHub Release, and also refreshes `latest`.

`CHANGELOG.md` is the durable release history in the repository. Each automated release prepends a new entry and pushes a dedicated changelog commit back to `main`.

## How It Works

- the browser talks only to the local server over HTTPS by default
- the local server accesses S3 or ADLS through the provider SDK
- the storage account does not need to respond directly to the browser

This avoids browser-side CORS issues in this flow.

## Disclaimer

This project is provided on an "as is" basis, without warranties or guarantees of any kind, whether express or implied, including availability, security, integrity, merchantability, or fitness for a particular purpose.

You are solely responsible for:

- validating credentials, permissions, and target selection before connecting
- ensuring appropriate backups, retention policies, and recovery procedures exist
- reviewing the impact of preview, download, file deletion, and prefix deletion operations before using them

The author is not liable for data loss, downtime, service disruption, cloud charges, security exposure, or misuse arising from the use of this project.

## Supported Providers

### AWS S3

Fill these fields in the interface:

- click the provider card to connect immediately with the saved credentials for that provider
- use the pencil icon on the provider card to open the credential modal
- in the credential modal, use `Test connection` to validate access without replacing the active session
- use `Save and connect` to persist the credentials and connect immediately

- `Region`
- `Bucket`
- `Access Key ID`
- `Secret Access Key`

Minimum permissions:

- `s3:ListBucket` on the bucket
- `s3:GetObject` on the objects
- `s3:DeleteObject` if you want to delete a specific file
- `s3:DeleteObject` if you want to use prefix deletion

### Azure Data Lake Storage Gen2

Fill these fields in the interface:

- click the provider card to connect immediately with the saved credentials for that provider
- use the pencil icon on the provider card to open the credential modal
- in the credential modal, use `Test connection` to validate access without replacing the active session
- use `Save and connect` to persist the credentials and connect immediately

- `Account Name`
- `Container Name`
- `Access Key`

The current implementation uses ADLS Gen2 shared key authentication through the DFS endpoint.

Minimum effective permissions:

- list paths in the container
- read file content
- delete a specific file if you want to use file deletion
- delete paths recursively if you want to use prefix deletion

### Google Cloud Storage

Fill these fields in the interface:

- click the provider card to connect immediately with the saved credentials for that provider
- use the pencil icon on the provider card to open the credential modal
- in the credential modal, use `Test connection` to validate access without replacing the active session
- use `Save and connect` to persist the credentials and connect immediately

- `Bucket`
- `Project ID (get it in the first lines of the JSON)` (optional if already present in the service account JSON)
- `Service Account JSON`

The `Bucket` field accepts either a plain bucket name such as `my-bucket` or a URL such as `gs://my-bucket`.

The current implementation uses a service account JSON key loaded directly in the local backend session.

### MinIO

Fill these fields in the interface:

- click the provider card to connect immediately with the saved credentials for that provider
- use the pencil icon on the provider card to open the credential modal
- in the credential modal, use `Test connection` to validate access without replacing the active session
- use `Save and connect` to persist the credentials and connect immediately

- `Endpoint`
- `Bucket`
- `Region` (optional, defaults to `us-east-1`)
- `Access Key ID`
- `Secret Access Key`

The current implementation uses the AWS S3 SDK in S3-compatible mode with a custom endpoint and path-style access.

If a MinIO deployment uses a self-signed or otherwise untrusted HTTPS certificate, the UI now exposes an explicit option to ignore TLS certificate errors for that MinIO connection only.

## Notes

- all connection fields are persisted in browser `localStorage`, including credentials, secret fields, textarea content, and provider-specific toggles
- the backend keeps an in-memory session for 12 hours after connecting
- the backend now requires `TLS_CERT_FILE` and `TLS_KEY_FILE` to serve HTTPS on port `8086`; `ALLOW_INSECURE_HTTP=true` is an explicit non-default escape hatch
- preview compression support now includes both `.gz` and `.snappy` variants for supported previewable formats
- folders containing `metadata/*.metadata.json` are now inspected as potential Iceberg table roots and switch to an Iceberg snapshot preview when detected
- Iceberg preview now exposes a snapshot dropdown so the current table can be sampled from older snapshots without leaving folder mode
- the UI exposes the temporary Iceberg seed button only when the backend is running outside Docker and not in `NODE_ENV=production`
- Avro preview reads Avro Object Container Files in the backend, including files using the Avro `snappy` codec
- ORC preview uses Apache ORC's Java tooling in the backend and downloads a cached jar on first ORC preview if it is not already available locally
- setting `DISABLE_DESTRUCTIVE_OPERATIONS=true` disables prefix deletion and single-file deletion server-side and hides delete controls in the UI
- prefix deletion removes child files but preserves the selected folder across all supported providers
- this solution is intended for local or internal use
- for production, the preferred approach is to avoid sending credentials through the frontend and use server-side authentication
- self-signed certificates are acceptable for local/internal use, but browser trust installation is an operator responsibility
