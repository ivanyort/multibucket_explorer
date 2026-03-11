# MultiBucket Explorer

Web application with a local backend for:

- browsing prefixes in AWS S3 buckets
- browsing prefixes in Azure Data Lake Storage Gen2 containers
- browsing prefixes in Google Cloud Storage buckets
- browsing prefixes in MinIO buckets
- listing folders and files
- previewing CSV, JSON, DFM, Parquet, and ORC files, including `.gz` and `.snappy` compressed variants
- previewing ORC files
- downloading files through a local proxy
- deleting a specific file from the current listing
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

By default the application runs at `http://localhost:8086`.

ORC preview requires `java` to be available in the backend host `PATH`.

To start in read-only mode for destructive operations, set `DISABLE_DESTRUCTIVE_OPERATIONS=true`:

```bash
DISABLE_DESTRUCTIVE_OPERATIONS=true npm start
```

## Docker

Build the image:

```bash
docker build -t multibucket-explorer .
```

Run the container:

```bash
docker run --rm -p 8086:8086 multibucket-explorer
```

The image now includes Java so ORC preview works inside Docker as well.

Run the container with destructive operations disabled:

```bash
docker run --rm -p 8086:8086 -e DISABLE_DESTRUCTIVE_OPERATIONS=true multibucket-explorer
```

If you want a different port inside the container, set `PORT` explicitly:

```bash
docker run --rm -p 8090:8090 -e PORT=8090 multibucket-explorer
```

## Docker Hub Publishing

The repository includes a GitHub Actions workflow at `.github/workflows/publish-docker.yml` that automatically calculates the next semantic version, creates the Git tag, creates a GitHub Release with generated release notes, and publishes the image to Docker Hub whenever code is pushed to `main`.
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

- `feat(preview): add orc support` -> minor
- `fix(docker): install java runtime` -> patch
- `feat(api)!: change session contract` -> major

The workflow tags and publishes the resulting version as both `X.Y.Z` and `vX.Y.Z`, creates a matching GitHub Release, and also refreshes `latest`.

## How It Works

- the browser talks only to the local server
- the local server accesses S3 or ADLS through the provider SDK
- the storage account does not need to respond directly to the browser

This avoids browser-side CORS issues in this flow.

## Supported Providers

### AWS S3

Fill these fields in the interface:

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

- `Bucket`
- `Project ID (get it in the first lines of the JSON)` (optional if already present in the service account JSON)
- `Service Account JSON`

The `Bucket` field accepts either a plain bucket name such as `my-bucket` or a URL such as `gs://my-bucket`.

The current implementation uses a service account JSON key loaded directly in the local backend session.

### MinIO

Fill these fields in the interface:

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
- preview compression support now includes both `.gz` and `.snappy` variants for supported previewable formats
- ORC preview uses Apache ORC's Java tooling in the backend and downloads a cached jar on first ORC preview if it is not already available locally
- setting `DISABLE_DESTRUCTIVE_OPERATIONS=true` disables prefix deletion and single-file deletion server-side and hides delete controls in the UI
- this solution is intended for local or internal use
- for production, the preferred approach is to avoid sending credentials through the frontend and use server-side authentication
