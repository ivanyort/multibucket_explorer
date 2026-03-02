# MultiBucket Explorer

Web application with a local backend for:

- browsing prefixes in AWS S3 buckets
- browsing prefixes in Azure Data Lake Storage Gen2 containers
- listing folders and files
- previewing CSV, JSON, DFM, and Parquet files
- downloading files through a local proxy

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

## Notes

- connection fields are persisted in browser `localStorage`, including secret fields
- the backend keeps an in-memory session for 12 hours after connecting
- this solution is intended for local or internal use
- for production, the preferred approach is to avoid sending credentials through the frontend and use server-side authentication
