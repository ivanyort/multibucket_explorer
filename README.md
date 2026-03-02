# MultiBucket Explorer

Web application with a local backend for:

- browsing prefixes in an AWS S3 bucket
- listing folders and files
- previewing CSV, JSON, and Parquet files
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
- the local server accesses AWS through the SDK
- the S3 bucket does not need to respond directly to the browser

This avoids CORS issues between the browser and S3 in this flow.

## Connection Fields

Fill these fields in the interface:

- `Region`
- `Bucket`
- `Access Key ID`
- `Secret Access Key`

These fields are persisted in browser `localStorage`, including the `Secret Access Key`.

## AWS Permissions

The credentials must allow at least:

- `s3:ListBucket` on the bucket
- `s3:GetObject` on the objects

## Notes

- the backend keeps an in-memory session for 12 hours after connecting
- this solution is intended for local or internal use
- for production, the preferred approach is to avoid sending credentials through the frontend and use server-side authentication
