# MultiBucket Explorer

Web application with a local backend for:

- browsing prefixes in AWS S3 buckets
- browsing prefixes in Azure Data Lake Storage Gen2 containers
- browsing prefixes in Google Cloud Storage buckets
- browsing prefixes in MinIO buckets
- listing folders and files
- previewing CSV, JSON, DFM, and Parquet files
- downloading files through a local proxy
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

## Docker

Build the image:

```bash
docker build -t multibucket-explorer .
```

Run the container:

```bash
docker run --rm -p 8086:8086 multibucket-explorer
```

If you want a different port inside the container, set `PORT` explicitly:

```bash
docker run --rm -p 8090:8090 -e PORT=8090 multibucket-explorer
```

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
- this solution is intended for local or internal use
- for production, the preferred approach is to avoid sending credentials through the frontend and use server-side authentication
