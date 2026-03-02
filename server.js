import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import https from "node:https";
import { Readable } from "node:stream";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGunzip, gunzipSync } from "node:zlib";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { DataLakeServiceClient, StorageSharedKeyCredential } from "@azure/storage-file-datalake";
import { Storage as GoogleCloudStorage } from "@google-cloud/storage";
import { parquetMetadataAsync, parquetReadObjects } from "hyparquet";
import { compressors as hyparquetCompressors } from "hyparquet-compressors";
import {
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from "@aws-sdk/client-s3";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = Number.parseInt(process.env.PORT ?? "8086", 10);
const SESSION_TTL_MS = 12 * 60 * 60 * 1000;
const sessions = new Map();

const MIME_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
};

const server = createServer(async (request, response) => {
  try {
    const url = new URL(request.url ?? "/", `http://${request.headers.host}`);

    if (request.method === "POST" && url.pathname === "/api/connect") {
      await handleConnect(request, response);
      return;
    }

    if (request.method === "GET" && url.pathname === "/api/objects") {
      await handleListObjects(url, response);
      return;
    }

    if (request.method === "GET" && url.pathname === "/api/preview") {
      await handlePreview(url, response);
      return;
    }

    if (request.method === "GET" && url.pathname === "/api/download") {
      await handleDownload(url, response);
      return;
    }

    if (request.method === "POST" && url.pathname === "/api/delete-prefix") {
      await handleDeletePrefix(request, response);
      return;
    }

    if (request.method === "GET") {
      await serveStatic(url.pathname, response);
      return;
    }

    sendJson(response, 404, { error: "Route not found." });
  } catch (error) {
    sendJson(response, 500, { error: getErrorMessage(error) });
  }
});

server.listen(PORT, () => {
  console.log(`MultiBucket Explorer available at http://localhost:${PORT}`);
});

async function handleConnect(request, response) {
  const body = await readJsonBody(request);
  const connection = normalizeConnection(body);
  validateConnection(connection);

  const storage = await createStorageSession(connection);

  const sessionId = randomUUID();
  sessions.set(sessionId, {
    ...connection,
    storage,
    createdAt: Date.now(),
  });
  pruneSessions();

  sendJson(response, 200, {
    sessionId,
    provider: connection.provider,
    targetName: getConnectionTargetName(connection),
    locationName: getConnectionLocationName(connection),
  });
}

async function handleListObjects(url, response) {
  const prefix = url.searchParams.get("prefix") ?? "";
  const session = getSession(url.searchParams.get("sessionId"));
  const { folders, files } = await listStorageObjects(session, prefix);

  sendJson(response, 200, {
    items: [...folders, ...files],
    summary: {
      folders: folders.length,
      files: files.length,
    },
  });
}

async function handlePreview(url, response) {
  const key = url.searchParams.get("key") ?? "";
  const limit = parsePreviewLimit(url.searchParams.get("limit"));
  const order = parsePreviewOrder(url.searchParams.get("order"));
  const mode = parsePreviewMode(url.searchParams.get("mode"));
  const session = getSession(url.searchParams.get("sessionId"));

  if (!key) {
    throw new Error("The key parameter is required.");
  }

  const previewData = await loadPreviewData(session, key, limit, order, mode);

  sendJson(response, 200, {
    rows: previewData.rows,
    metadataColumns: previewData.metadataColumns,
    dfmKey: previewData.dfmKey,
    order,
    previewFormat: previewData.previewFormat,
    previewMode: previewData.previewMode,
    rawText: previewData.rawText,
    lineCount: previewData.lineCount,
  });
}

async function handleDownload(url, response) {
  const key = url.searchParams.get("key") ?? "";
  const session = getSession(url.searchParams.get("sessionId"));

  if (!key) {
    throw new Error("The key parameter is required.");
  }

  const result = await getDownloadResponse(session, key);
  const contentType = result.contentType || "application/octet-stream";
  const fileName = path.basename(key);

  response.writeHead(200, {
    "Content-Type": contentType,
    "Content-Disposition": `attachment; filename="${encodeURIComponent(fileName)}"`,
    ...(result.contentLength ? { "Content-Length": String(result.contentLength) } : {}),
  });

  if (!result.body) {
    response.end();
    return;
  }

  for await (const chunk of result.body) {
    response.write(chunk);
  }

  response.end();
}

async function handleDeletePrefix(request, response) {
  const body = await readJsonBody(request);
  const session = getSession(typeof body.sessionId === "string" ? body.sessionId : "");
  const prefix = typeof body.prefix === "string" ? body.prefix.trim() : "";

  if (!prefix) {
    throw new Error("For safety, deleting the storage root is not allowed.");
  }

  const deletedCount = await deleteStoragePrefix(session, prefix);

  sendJson(response, 200, {
    deletedCount,
    prefix,
  });
}

async function serveStatic(requestPath, response) {
  const safePath = requestPath === "/" ? "/index.html" : requestPath;
  const resolvedPath = path.resolve(__dirname, `.${safePath}`);

  if (!resolvedPath.startsWith(__dirname)) {
    sendJson(response, 403, { error: "Access denied." });
    return;
  }

  let fileStat;
  try {
    fileStat = await stat(resolvedPath);
  } catch {
    sendJson(response, 404, { error: "File not found." });
    return;
  }

  if (!fileStat.isFile()) {
    sendJson(response, 404, { error: "File not found." });
    return;
  }

  const ext = path.extname(resolvedPath);
  response.writeHead(200, {
    "Content-Type": MIME_TYPES[ext] ?? "application/octet-stream",
  });

  createReadStream(resolvedPath).pipe(response);
}

function createS3Client(connection) {
  return new S3Client({
    region: connection.region,
    credentials: {
      accessKeyId: connection.accessKeyId,
      secretAccessKey: connection.secretAccessKey,
      ...(connection.sessionToken ? { sessionToken: connection.sessionToken } : {}),
    },
  });
}

function createMinioClient(connection) {
  const useInsecureTls =
    connection.ignoreTlsErrors && connection.endpoint.toLowerCase().startsWith("https://");

  return new S3Client({
    region: connection.region || "us-east-1",
    endpoint: connection.endpoint,
    forcePathStyle: true,
    ...(useInsecureTls
      ? {
          requestHandler: new NodeHttpHandler({
            httpsAgent: new https.Agent({
              rejectUnauthorized: false,
            }),
          }),
        }
      : {}),
    credentials: {
      accessKeyId: connection.accessKeyId,
      secretAccessKey: connection.secretAccessKey,
    },
  });
}

function createAdlsServiceClient(connection) {
  const credential = new StorageSharedKeyCredential(connection.accountName, connection.accountKey);
  return new DataLakeServiceClient(`https://${connection.accountName}.dfs.core.windows.net`, credential);
}

function createGcsClient(connection) {
  const credentials = JSON.parse(connection.serviceAccountJson);
  return new GoogleCloudStorage({
    projectId: connection.projectId || credentials.project_id,
    credentials,
  });
}

async function createStorageSession(connection) {
  if (connection.provider === "adls") {
    const serviceClient = createAdlsServiceClient(connection);
    const fileSystemClient = serviceClient.getFileSystemClient(connection.fileSystem);
    await fileSystemClient.getProperties();

    return {
      provider: "adls",
      serviceClient,
      fileSystemClient,
      accountName: connection.accountName,
      fileSystem: connection.fileSystem,
    };
  }

  if (connection.provider === "gcs") {
    const client = createGcsClient(connection);
    const bucket = client.bucket(connection.bucket);
    await bucket.getFiles({ maxResults: 1, autoPaginate: false });

    return {
      provider: "gcs",
      client,
      bucket,
      bucketName: connection.bucket,
      projectId: connection.projectId,
    };
  }

  const client = connection.provider === "minio" ? createMinioClient(connection) : createS3Client(connection);
  await client.send(
    new ListObjectsV2Command({
      Bucket: connection.bucket,
      MaxKeys: 1,
    }),
  );

  return {
    provider: connection.provider,
    client,
    bucket: connection.bucket,
    region: connection.region,
    endpoint: connection.endpoint,
  };
}

function getConnectionTargetName(connection) {
  if (connection.provider === "adls") {
    return connection.fileSystem;
  }

  return connection.bucket;
}

function getConnectionLocationName(connection) {
  if (connection.provider === "adls") {
    return connection.accountName;
  }

  if (connection.provider === "gcs") {
    return connection.projectId || "GCP";
  }

  if (connection.provider === "minio") {
    return connection.endpoint;
  }

  return connection.region;
}

async function listStorageObjects(session, prefix) {
  if (session.storage.provider === "adls") {
    return listAdlsObjects(session, prefix);
  }

  if (session.storage.provider === "gcs") {
    return listGcsObjects(session, prefix);
  }

  const result = await session.storage.client.send(
    new ListObjectsV2Command({
      Bucket: session.storage.bucket,
      Prefix: prefix,
      Delimiter: "/",
    }),
  );

  const folders =
    result.CommonPrefixes?.map((item) => ({
      type: "folder",
      key: item.Prefix,
      name: trimCurrentPrefix(item.Prefix ?? "", prefix).replace(/\/$/, ""),
    })) ?? [];

  const files =
    result.Contents?.filter((item) => item.Key !== prefix).map((item) => ({
      type: "file",
      key: item.Key,
      name: trimCurrentPrefix(item.Key ?? "", prefix),
      size: item.Size ?? 0,
      lastModified: item.LastModified?.toISOString() ?? null,
    })) ?? [];

  return { folders, files };
}

async function listGcsObjects(session, prefix) {
  const [files, , apiResponse] = await session.storage.bucket.getFiles({
    prefix,
    delimiter: "/",
    autoPaginate: false,
  });

  const folders = (apiResponse.prefixes ?? []).map((folderPrefix) => ({
    type: "folder",
    key: folderPrefix,
    name: trimCurrentPrefix(folderPrefix, prefix).replace(/\/$/, ""),
  }));

  const fileItems = files
    .filter((file) => file.name !== prefix)
    .map((file) => ({
      type: "file",
      key: file.name,
      name: trimCurrentPrefix(file.name, prefix),
      size: Number(file.metadata.size ?? 0),
      lastModified: file.metadata.updated ?? null,
    }));

  return { folders, files: fileItems };
}

function normalizeGcsBucketName(value) {
  if (typeof value !== "string") {
    return "";
  }

  const trimmed = value.trim();

  if (!trimmed) {
    return "";
  }

  const withoutScheme = trimmed.replace(/^gs:\/\//i, "");
  return withoutScheme.split("/")[0]?.trim() ?? "";
}

async function listAdlsObjects(session, prefix) {
  const items = [];
  const path = normalizeAdlsDirectory(prefix);
  const iterator = session.storage.fileSystemClient.listPaths({
    path: path || undefined,
    recursive: false,
  });

  for await (const item of iterator) {
    if (!item.name) {
      continue;
    }

    const normalizedName = item.isDirectory ? ensureTrailingSlash(item.name) : item.name;

    if (normalizedName === prefix || item.name === path) {
      continue;
    }

    items.push({
      type: item.isDirectory ? "folder" : "file",
      key: item.isDirectory ? ensureTrailingSlash(item.name) : item.name,
      name: trimCurrentPrefix(normalizedName, prefix).replace(/\/$/, ""),
      size: Number(item.contentLength ?? 0),
      lastModified: item.lastModified ? new Date(item.lastModified).toISOString() : null,
    });
  }

  return {
    folders: items.filter((item) => item.type === "folder"),
    files: items.filter((item) => item.type === "file"),
  };
}

async function listStoragePaths(session, prefix, recursive = true) {
  if (session.storage.provider === "adls") {
    const path = normalizeAdlsDirectory(prefix);
    const paths = [];

    for await (const item of session.storage.fileSystemClient.listPaths({
      path: path || undefined,
      recursive,
    })) {
      if (item.name) {
        paths.push(item);
      }
    }

    return paths;
  }

  if (session.storage.provider === "gcs") {
    const items = [];
    let query = {
      prefix,
      autoPaginate: false,
      ...(recursive ? {} : { delimiter: "/" }),
    };

    do {
      const [files, nextQuery] = await session.storage.bucket.getFiles(query);
      items.push(...files);
      query = nextQuery;
    } while (query);

    return items;
  }

  let continuationToken;
  const items = [];

  do {
    const listResult = await session.storage.client.send(
      new ListObjectsV2Command({
        Bucket: session.storage.bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      }),
    );

    items.push(...(listResult.Contents ?? []));
    continuationToken = listResult.IsTruncated ? listResult.NextContinuationToken : undefined;
  } while (continuationToken);

  return items;
}

async function deleteStoragePrefix(session, prefix) {
  if (session.storage.provider === "adls") {
    const normalizedPrefix = normalizeAdlsDirectory(prefix);

    if (!normalizedPrefix) {
      throw new Error("For safety, deleting the storage root is not allowed.");
    }

    const paths = await listStoragePaths(session, prefix, true);
    const deletedCount = paths.filter((item) => !item.isDirectory).length;
    await session.storage.fileSystemClient.getDirectoryClient(normalizedPrefix).delete(true);
    return deletedCount;
  }

  if (session.storage.provider === "gcs") {
    const files = await listStoragePaths(session, prefix, true);
    await Promise.all(files.map((file) => file.delete()));
    return files.length;
  }

  let continuationToken;
  let deletedCount = 0;

  do {
    const listResult = await session.storage.client.send(
      new ListObjectsV2Command({
        Bucket: session.storage.bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      }),
    );

    const objects =
      listResult.Contents?.map((item) => item.Key).filter((key) => typeof key === "string" && key.length) ?? [];

    if (objects.length) {
      await session.storage.client.send(
        new DeleteObjectsCommand({
          Bucket: session.storage.bucket,
          Delete: {
            Objects: objects.map((key) => ({ Key: key })),
            Quiet: true,
          },
        }),
      );
      deletedCount += objects.length;
    }

    continuationToken = listResult.IsTruncated ? listResult.NextContinuationToken : undefined;
  } while (continuationToken);

  return deletedCount;
}

async function getDownloadResponse(session, key) {
  if (session.storage.provider === "adls") {
    const fileClient = session.storage.fileSystemClient.getFileClient(key);
    const response = await fileClient.read();
    return {
      contentType: response.contentType,
      contentLength: response.contentLength,
      body: response.readableStreamBody,
    };
  }

  if (session.storage.provider === "gcs") {
    const file = session.storage.bucket.file(key);
    const [metadata] = await file.getMetadata();

    return {
      contentType: metadata.contentType,
      contentLength: Number(metadata.size ?? 0),
      body: file.createReadStream(),
    };
  }

  const response = await session.storage.client.send(
    new GetObjectCommand({
      Bucket: session.storage.bucket,
      Key: key,
    }),
  );

  return {
    contentType: response.ContentType,
    contentLength: response.ContentLength,
    body: response.Body,
  };
}

function normalizeAdlsDirectory(prefix) {
  return prefix.replace(/\/+$/, "");
}

function ensureTrailingSlash(value) {
  return value.endsWith("/") ? value : `${value}/`;
}

async function readObjectText(session, key, gzip = false) {
  const buffer = await readObjectBuffer(session, key, gzip);
  const text = buffer.toString("utf-8");

  if (!text) {
    throw new Error("The file was empty.");
  }

  return text;
}

async function readObjectBuffer(session, key, gzip = false) {
  const buffer = await readObjectBufferRaw(session, key);

  if (!buffer.length) {
    throw new Error("The file was empty.");
  }

  return gzip ? gunzipSync(buffer) : buffer;
}

async function readObjectBufferRaw(session, key) {
  if (session.storage.provider === "adls") {
    const fileClient = session.storage.fileSystemClient.getFileClient(key);
    return fileClient.readToBuffer();
  }

  if (session.storage.provider === "gcs") {
    const [buffer] = await session.storage.bucket.file(key).download();
    return buffer;
  }

  const response = await session.storage.client.send(
    new GetObjectCommand({
      Bucket: session.storage.bucket,
      Key: key,
    }),
  );
  const bytes = await response.Body?.transformToByteArray();
  return bytes?.length ? Buffer.from(bytes) : Buffer.alloc(0);
}

async function getObjectReadableStream(session, key) {
  if (session.storage.provider === "adls") {
    const fileClient = session.storage.fileSystemClient.getFileClient(key);
    const response = await fileClient.read();
    return response.readableStreamBody ?? null;
  }

  if (session.storage.provider === "gcs") {
    return session.storage.bucket.file(key).createReadStream();
  }

  const response = await session.storage.client.send(
    new GetObjectCommand({
      Bucket: session.storage.bucket,
      Key: key,
    }),
  );

  return response.Body ?? null;
}

async function getObjectContentLength(session, key) {
  if (session.storage.provider === "adls") {
    const response = await session.storage.fileSystemClient.getFileClient(key).getProperties();
    return Number(response.contentLength ?? 0);
  }

  if (session.storage.provider === "gcs") {
    const [metadata] = await session.storage.bucket.file(key).getMetadata();
    return Number(metadata.size ?? 0);
  }

  const response = await session.storage.client.send(
    new HeadObjectCommand({
      Bucket: session.storage.bucket,
      Key: key,
    }),
  );

  return Number(response.ContentLength ?? 0);
}

async function readObjectRangeBuffer(session, key, start, end) {
  const count = end - start + 1;

  if (session.storage.provider === "adls") {
    const fileClient = session.storage.fileSystemClient.getFileClient(key);
    return fileClient.readToBuffer(start, count);
  }

  if (session.storage.provider === "gcs") {
    const [buffer] = await session.storage.bucket.file(key).download({ start, end });
    return buffer;
  }

  const response = await session.storage.client.send(
    new GetObjectCommand({
      Bucket: session.storage.bucket,
      Key: key,
      Range: `bytes=${start}-${end}`,
    }),
  );
  const bytes = await response.Body?.transformToByteArray();
  return bytes?.length ? Buffer.from(bytes) : Buffer.alloc(0);
}

function getSession(sessionId) {
  if (!sessionId) {
    throw new Error("Session not provided.");
  }

  const session = sessions.get(sessionId);

  if (!session) {
    throw new Error("Invalid or expired session. Connect again.");
  }

  if (Date.now() - session.createdAt > SESSION_TTL_MS) {
    sessions.delete(sessionId);
    throw new Error("Session expired. Connect again.");
  }

  return session;
}

function pruneSessions() {
  const now = Date.now();

  for (const [sessionId, session] of sessions.entries()) {
    if (now - session.createdAt > SESSION_TTL_MS) {
      sessions.delete(sessionId);
    }
  }
}

function trimCurrentPrefix(key, prefix) {
  return prefix && key.startsWith(prefix) ? key.slice(prefix.length) : key;
}

function readJsonBody(request) {
  return new Promise((resolve, reject) => {
    let body = "";

    request.on("data", (chunk) => {
      body += chunk;
    });

    request.on("end", () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch {
        reject(new Error("Invalid JSON in request body."));
      }
    });

    request.on("error", reject);
  });
}

function normalizeConnection(body) {
  const provider = ["adls", "gcs", "minio"].includes(body.provider) ? body.provider : "s3";

  return {
    provider,
    region:
      provider === "minio"
        ? typeof body.minioRegion === "string"
          ? body.minioRegion.trim()
          : ""
        : typeof body.region === "string"
          ? body.region.trim()
          : "",
    bucket:
      provider === "gcs"
        ? normalizeGcsBucketName(body.gcsBucket)
        : provider === "minio"
          ? typeof body.minioBucket === "string"
            ? body.minioBucket.trim()
            : ""
          : typeof body.bucket === "string"
            ? body.bucket.trim()
            : "",
    accessKeyId:
      provider === "minio"
        ? typeof body.minioAccessKeyId === "string"
          ? body.minioAccessKeyId.trim()
          : ""
        : typeof body.accessKeyId === "string"
          ? body.accessKeyId.trim()
          : "",
    secretAccessKey:
      provider === "minio"
        ? typeof body.minioSecretAccessKey === "string"
          ? body.minioSecretAccessKey.trim()
          : ""
        : typeof body.secretAccessKey === "string"
          ? body.secretAccessKey.trim()
          : "",
    sessionToken: typeof body.sessionToken === "string" ? body.sessionToken.trim() : "",
    accountName: typeof body.accountName === "string" ? body.accountName.trim() : "",
    fileSystem: typeof body.fileSystem === "string" ? body.fileSystem.trim() : "",
    accountKey: typeof body.accountKey === "string" ? body.accountKey.trim() : "",
    endpoint: typeof body.endpoint === "string" ? body.endpoint.trim() : "",
    ignoreTlsErrors: body.ignoreTlsErrors === true,
    projectId: typeof body.projectId === "string" ? body.projectId.trim() : "",
    serviceAccountJson: typeof body.serviceAccountJson === "string" ? body.serviceAccountJson.trim() : "",
  };
}

function validateConnection(connection) {
  if (connection.provider === "adls") {
    if (!connection.accountName || !connection.fileSystem || !connection.accountKey) {
      throw new Error("Fill in account name, file system, and account key.");
    }
    return;
  }

  if (connection.provider === "gcs") {
    if (!connection.bucket || !connection.serviceAccountJson) {
      throw new Error("Fill in bucket, and service account JSON.");
    }

    try {
      JSON.parse(connection.serviceAccountJson);
    } catch {
      throw new Error("Service account JSON must be valid JSON.");
    }

    return;
  }

  if (connection.provider === "minio") {
    if (!connection.endpoint || !connection.bucket || !connection.accessKeyId || !connection.secretAccessKey) {
      throw new Error("Fill in endpoint, bucket, access key ID, and secret access key.");
    }

    return;
  }

  if (!connection.region || !connection.bucket || !connection.accessKeyId || !connection.secretAccessKey) {
    throw new Error("Fill in region, bucket, and credentials.");
  }
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
  });
  response.end(JSON.stringify(payload));
}

function getErrorMessage(error) {
  if (typeof error === "string" && error) {
    return error;
  }

  if (error && typeof error === "object") {
    const message = Reflect.get(error, "message");
    if (typeof message === "string" && message) {
      return message;
    }
  }

  return "Internal server error.";
}

function parsePreviewLimit(value) {
  if (value === "all") {
    return null;
  }

  const parsedValue = Number.parseInt(value ?? "10", 10);

  if (!Number.isFinite(parsedValue) || parsedValue <= 0) {
    return 10;
  }

  return parsedValue;
}

function parsePreviewOrder(value) {
  return value === "reverse" ? "reverse" : "normal";
}

function parsePreviewMode(value) {
  return value === "raw" ? "raw" : "table";
}

async function loadPreviewRows(session, key, limit, order, formatOptions) {
  if (isGzipKey(key)) {
    const csvText = await readObjectText(session, key, true);

    return parseCsv(csvText, limit, {
      fieldDelimiter: formatOptions.fieldDelimiter,
      quoteChar: formatOptions.quoteChar,
      order,
    });
  }

  if (order === "reverse" && limit !== null) {
    const tailText = await loadCsvTailText(session, key, limit, formatOptions.recordDelimiter);

    if (!tailText) {
      throw new Error("The file was empty.");
    }

    return parseCsv(tailText, null, {
      fieldDelimiter: formatOptions.fieldDelimiter,
      quoteChar: formatOptions.quoteChar,
      order: "reverse",
    });
  }

  if (order === "normal" && limit !== null) {
    return loadCsvHeadRows(session, key, limit, formatOptions);
  }

  const csvText = await readObjectText(session, key);

  if (!csvText) {
    throw new Error("The file was empty.");
  }

  return parseCsv(csvText, limit, {
    fieldDelimiter: formatOptions.fieldDelimiter,
    quoteChar: formatOptions.quoteChar,
    order,
  });
}

async function loadPreviewData(session, key, limit, order, mode) {
  const previewTarget = analyzePreviewTarget(key);
  const extension = previewTarget.extension;
  const isGzip = previewTarget.isGzip;

  if (extension === ".csv") {
    const dfmMatch = await loadDfmMetadata(session, previewTarget.metadataKey);
    const dfmMetadata = dfmMatch?.metadata ?? null;
    const formatOptions = {
      fieldDelimiter: dfmMetadata?.formatInfo?.options?.fieldDelimiter,
      quoteChar: dfmMetadata?.formatInfo?.options?.quoteChar,
      recordDelimiter: dfmMetadata?.formatInfo?.options?.recordDelimiter,
    };
    const rows = await loadPreviewRows(session, key, limit, order, formatOptions);

    return {
      rows,
      metadataColumns: extractMetadataColumns(dfmMetadata),
      dfmKey: dfmMatch?.key ?? null,
      previewFormat: isGzip ? "csv.gz" : "csv",
      previewMode: "table",
      rawText: null,
      lineCount: countPreviewLines(rows),
    };
  }

  if (extension === ".json" || extension === ".jsonl" || extension === ".ndjson" || extension === ".dfm") {
    if (mode === "raw") {
      const rawText = await loadJsonRawPreview(session, key, limit, order, isGzip, extension);
      return {
        rows: [],
        metadataColumns: [],
        dfmKey: null,
        previewFormat: isGzip ? `${extension.slice(1)}.gz` : extension.slice(1),
        previewMode: "raw",
        rawText,
        lineCount: countRawPreviewLines(rawText),
      };
    }

    if (extension === ".dfm") {
      const rawText = await loadJsonRawPreview(session, key, limit, order, isGzip, extension);
      return {
        rows: [],
        metadataColumns: [],
        dfmKey: null,
        previewFormat: isGzip ? "dfm.gz" : "dfm",
        previewMode: "raw",
        rawText,
        lineCount: countRawPreviewLines(rawText),
      };
    }

    const records = await loadJsonPreviewRecords(session, key, limit, order, isGzip, extension);
    const normalized = normalizePreviewRecords(records);
    return {
      rows: normalized.rows,
      metadataColumns: normalized.columns,
      dfmKey: null,
      previewFormat: isGzip ? `${extension.slice(1)}.gz` : extension.slice(1),
      previewMode: "table",
      rawText: null,
      lineCount: normalized.rows.length,
    };
  }

  if (extension === ".parquet" || extension === ".parq") {
    const records = await loadParquetPreviewRecords(session, key, limit, order, isGzip);
    const normalized = normalizePreviewRecords(records);
    return {
      rows: normalized.rows,
      metadataColumns: normalized.columns,
      dfmKey: null,
      previewFormat: isGzip ? "parquet.gz" : "parquet",
      previewMode: "table",
      rawText: null,
      lineCount: normalized.rows.length,
    };
  }

  throw new Error("Unsupported preview format.");
}

async function loadDfmMetadata(session, csvKey) {
  const dfmKeys = buildDfmCandidateKeys(csvKey);

  if (!dfmKeys.length) {
    return null;
  }

  for (const dfmKey of dfmKeys) {
    const exactMatch = await loadJsonObject(session, dfmKey);

    if (exactMatch) {
      return {
        key: dfmKey,
        metadata: exactMatch,
      };
    }
  }

  return findMatchingDfmMetadata(session, csvKey);
}

function extractMetadataColumns(dfmMetadata) {
  const columns = dfmMetadata?.dataInfo?.columns;

  if (!Array.isArray(columns) || !columns.length) {
    return [];
  }

  return [...columns]
    .sort((left, right) => (left.ordinal ?? 0) - (right.ordinal ?? 0))
    .map((column, index) => column?.name || `column_${index + 1}`);
}

function replaceExtension(key, newExtension) {
  const extension = path.extname(key);

  if (!extension) {
    return key;
  }

  return `${key.slice(0, -extension.length)}${newExtension}`;
}

function buildDfmCandidateKeys(csvKey) {
  const normalizedKey = stripGzipExtension(csvKey);
  const extension = path.extname(normalizedKey);

  if (!extension) {
    return [];
  }

  return [".dfm", ".DFM"].map((dfmExtension) => `${normalizedKey.slice(0, -extension.length)}${dfmExtension}`);
}

async function findMatchingDfmMetadata(session, csvKey) {
  const normalizedKey = stripGzipExtension(csvKey);
  const directoryPrefix = getDirectoryPrefix(normalizedKey);
  const csvBaseName = path.basename(normalizedKey, path.extname(normalizedKey));
  const listResult = await listStoragePaths(session, directoryPrefix, true);

  const dfmKeys = listResult
    .map((item) => ("name" in item ? item.name : item.Key))
    .filter((key) => typeof key === "string" && key.toLowerCase().endsWith(".dfm"));

  const basenameMatch = dfmKeys.find((key) => {
    const dfmBaseName = path.basename(key, path.extname(key));
    return csvBaseName === dfmBaseName || csvBaseName.startsWith(dfmBaseName);
  });

  if (basenameMatch) {
    const metadata = await loadJsonObject(session, basenameMatch);

    if (metadata) {
      return {
        key: basenameMatch,
        metadata,
      };
    }
  }

  for (const key of dfmKeys) {
    const metadata = await loadJsonObject(session, key);

    if (!metadata) {
      continue;
    }

    const metadataName = typeof metadata?.fileInfo?.name === "string" ? metadata.fileInfo.name.trim() : "";

    if (metadataName && (csvBaseName === metadataName || csvBaseName.startsWith(metadataName))) {
      return {
        key,
        metadata,
      };
    }
  }

  return null;
}

function getDirectoryPrefix(key) {
  const lastSlashIndex = key.lastIndexOf("/");
  return lastSlashIndex >= 0 ? key.slice(0, lastSlashIndex + 1) : "";
}

async function loadJsonObject(session, key) {
  try {
    const jsonText = await readObjectText(session, key);

    if (!jsonText) {
      return null;
    }

    return JSON.parse(jsonText);
  } catch {
    return null;
  }
}

async function loadJsonPreviewRecords(session, key, limit, order, gzip = false, extension = ".json") {
  const isLineDelimitedJson = extension === ".jsonl" || extension === ".ndjson";

  if (gzip && limit !== null) {
    const records = await tryLoadGzipJsonRecords(session, key, limit, order);

    if (records) {
      return records;
    }
  }

  if (!gzip && isLineDelimitedJson && order === "normal" && limit !== null) {
    const lines = await loadDelimitedHeadRecords(session, key, limit);
    return lines.map((line) => JSON.parse(line));
  }

  if (!gzip && isLineDelimitedJson && order === "reverse" && limit !== null) {
    const lines = await loadDelimitedTailRecords(session, key, limit, "\n");
    return lines.map((line) => JSON.parse(line)).reverse();
  }

  const text = await readObjectText(session, key, gzip);
  const trimmed = text.trim();
  let records = [];

  if (!trimmed) {
    throw new Error("The file was empty.");
  }

  try {
    const parsed = JSON.parse(trimmed);
    if (Array.isArray(parsed)) {
      records = parsed;
    } else {
      records = [parsed];
    }
  } catch {
    records = trimmed
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  }

  return slicePreviewRecords(records, limit, order);
}

async function loadJsonRawPreview(session, key, limit, order, gzip = false, extension = ".json") {
  const isLineDelimitedJson = extension === ".jsonl" || extension === ".ndjson";

  if (limit !== null && isLineDelimitedJson) {
    if (gzip) {
      const lines = await tryLoadGzipDelimitedLines(session, key, limit, order);

      if (lines) {
        return formatRawJsonLines(order === "reverse" ? [...lines].reverse() : lines);
      }
    } else if (order === "normal") {
      return formatRawJsonLines(await loadDelimitedHeadRecords(session, key, limit));
    } else {
      return formatRawJsonLines((await loadDelimitedTailRecords(session, key, limit, "\n")).reverse());
    }
  }

  const text = await readObjectText(session, key, gzip);
  const trimmed = text.trim();

  if (!trimmed) {
    throw new Error("The file was empty.");
  }

  if (isLineDelimitedJson) {
    return formatRawJsonLines(slicePreviewRecords(splitJsonLines(trimmed), limit, order));
  }

  try {
    return JSON.stringify(JSON.parse(trimmed), null, 2);
  } catch {
    const lines = splitJsonLines(trimmed);

    if (lines.length > 1) {
      return formatRawJsonLines(slicePreviewRecords(lines, limit, order));
    }

    return trimmed;
  }
}

function formatRawJsonLines(lines) {
  return lines
    .map((line) => {
      try {
        return JSON.stringify(JSON.parse(line), null, 2);
      } catch {
        return line;
      }
    })
    .join("\n\n");
}

function splitJsonLines(text) {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
}

async function tryLoadGzipJsonRecords(session, key, limit, order) {
  const lines = await tryLoadGzipDelimitedLines(session, key, limit, order);

  if (!lines) {
    return null;
  }

  try {
    return order === "reverse"
      ? [...lines].reverse().map((line) => JSON.parse(line))
      : lines.map((line) => JSON.parse(line));
  } catch {
    return null;
  }
}

async function tryLoadGzipDelimitedLines(session, key, limit, order) {
  const body = await getObjectReadableStream(session, key);

  if (!body) {
      throw new Error("The file was empty.");
  }

  const source = body instanceof Readable ? body : Readable.from(body);
  const gunzip = createGunzip();
  let buffer = "";
  const lines = [];

  source.pipe(gunzip);

  try {
    for await (const chunk of gunzip) {
      buffer += chunk.toString("utf-8");
      const parts = buffer.split(/\r?\n/);
      buffer = parts.pop() ?? "";

      for (const part of parts) {
        const normalized = part.trim();

        if (!normalized) {
          continue;
        }

        if (order === "reverse") {
          if (lines.length === limit) {
            lines.shift();
          }
          lines.push(normalized);
          continue;
        }

        lines.push(normalized);

        if (lines.length >= limit) {
          source.destroy();
          gunzip.destroy();
          return lines;
        }
      }
    }
  } catch (error) {
    if (lines.length >= limit && (error?.code === "ERR_STREAM_PREMATURE_CLOSE" || error?.code === "ABORT_ERR")) {
      return lines;
    }

    return null;
  }

  if (buffer.trim()) {
    if (order === "reverse") {
      if (lines.length === limit) {
        lines.shift();
      }
      lines.push(buffer.trim());
    } else {
      lines.push(buffer.trim());
    }
  }

  return order === "reverse" ? lines : lines.slice(0, limit);
}

async function loadParquetPreviewRecords(session, key, limit, order, gzip = false) {
  try {
    const fileBuffer = await readObjectBuffer(session, key, gzip);
    const file = toArrayBuffer(fileBuffer);
    const metadata = await parquetMetadataAsync(file, { compressors: hyparquetCompressors });
    const totalRows = Number(metadata.num_rows ?? 0);
    const effectiveLimit = limit ?? totalRows;
    const rowStart =
      order === "reverse" ? Math.max(totalRows - effectiveLimit, 0) : 0;
    const rowEnd =
      limit === null ? totalRows : Math.min(rowStart + effectiveLimit, totalRows);

    const records = await parquetReadObjects({
      file,
      compressors: hyparquetCompressors,
      rowStart,
      rowEnd,
    });

    return order === "reverse" ? [...records].reverse() : records;
  } catch (error) {
    throw new Error(`Failed to read Parquet preview: ${getErrorMessage(error)}`);
  }
}

function slicePreviewRecords(records, limit, order) {
  if (order === "reverse") {
    const reversed = limit === null ? [...records].reverse() : records.slice(Math.max(records.length - limit, 0)).reverse();
    return reversed;
  }

  return limit === null ? records : records.slice(0, limit);
}

function countPreviewLines(rows) {
  return Array.isArray(rows) ? rows.length : 0;
}

function countRawPreviewLines(rawText) {
  if (!rawText) {
    return 0;
  }

  return rawText.split(/\r?\n/).length;
}

function normalizePreviewRecords(records) {
  const normalizedRecords = records.map((record) => normalizePreviewRecord(record));
  const columns = [];

  for (const record of normalizedRecords) {
    for (const key of Object.keys(record)) {
      if (!columns.includes(key)) {
        columns.push(key);
      }
    }
  }

  return {
    columns: columns.length ? columns : ["value"],
    rows: normalizedRecords.map((record) =>
      (columns.length ? columns : ["value"]).map((column) => stringifyPreviewValue(record[column])),
    ),
  };
}

function normalizePreviewRecord(record) {
  if (record && typeof record === "object" && !Array.isArray(record)) {
    return record;
  }

  if (Array.isArray(record)) {
    return Object.fromEntries(record.map((value, index) => [`column_${index + 1}`, value]));
  }

  return { value: record };
}

function stringifyPreviewValue(value) {
  if (value === null || value === undefined) {
    return "";
  }

  if (typeof value === "object") {
    return JSON.stringify(value);
  }

  return String(value);
}

function analyzePreviewTarget(key) {
  const normalizedKey = stripGzipExtension(key);

  return {
    extension: path.extname(normalizedKey).toLowerCase(),
    isGzip: isGzipKey(key),
    metadataKey: normalizedKey,
  };
}

function stripGzipExtension(key) {
  const normalizedKey = key.toLowerCase();

  if (normalizedKey.endsWith(".gzip.parquet")) {
    return key;
  }

  if (normalizedKey.endsWith(".gz.parquet")) {
    return key;
  }

  if (normalizedKey.endsWith(".gzip.parq")) {
    return key;
  }

  if (normalizedKey.endsWith(".gz.parq")) {
    return key;
  }

  return isGzipKey(key) ? key.slice(0, -3) : key;
}

function isGzipKey(key) {
  return key.toLowerCase().endsWith(".gz");
}

function toArrayBuffer(buffer) {
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
}

async function loadDelimitedHeadRecords(session, key, limit) {
  const body = await getObjectReadableStream(session, key);

  if (!body) {
    throw new Error("The file was empty.");
  }

  const decoder = new TextDecoder("utf-8");
  let buffer = "";
  const records = [];

  for await (const chunk of body) {
    buffer += decoder.decode(chunk, { stream: true });
    const parts = buffer.split(/\r?\n/);
    buffer = parts.pop() ?? "";

    for (const part of parts) {
      const normalized = part.trim();

      if (!normalized) {
        continue;
      }

      records.push(normalized);

      if (records.length >= limit) {
        if (typeof body.destroy === "function") {
          body.destroy();
        }
        return records;
      }
    }
  }

  buffer += decoder.decode();

  if (buffer.trim()) {
    records.push(buffer.trim());
  }

  return records.slice(0, limit);
}

async function loadDelimitedTailRecords(session, key, limit, recordDelimiter = "\n") {
  const tailText = await loadTextTailChunk(session, key, limit, recordDelimiter);
  return tailText
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(-limit);
}

async function loadTextTailChunk(session, key, limit, recordDelimiter) {
  const delimiter = typeof recordDelimiter === "string" && recordDelimiter.length ? recordDelimiter : "\n";
  const contentLength = await getObjectContentLength(session, key);

  if (!contentLength) {
    return "";
  }

  const decoder = new TextDecoder("utf-8");
  const chunkSize = 256 * 1024;
  let position = contentLength;
  let buffer = "";

  while (position > 0) {
    const start = Math.max(0, position - chunkSize);
    const end = position - 1;
    const bytes = await readObjectRangeBuffer(session, key, start, end);
    const chunkText = bytes.length ? decoder.decode(bytes) : "";

    buffer = chunkText + buffer;
    position = start;

    const records = extractRecordsFromTailBuffer(buffer, delimiter, position === 0);

    if (records.length >= limit || position === 0) {
      return records.slice(-limit).join(delimiter);
    }
  }

  return buffer;
}

async function loadCsvTailText(session, key, limit, recordDelimiter) {
  return loadTextTailChunk(session, key, limit, recordDelimiter);
}

async function loadCsvHeadRows(session, key, limit, formatOptions) {
  const body = await getObjectReadableStream(session, key);

  if (!body) {
    throw new Error("The file was empty.");
  }

  const collector = createCsvCollector(limit, {
    fieldDelimiter: formatOptions.fieldDelimiter,
    quoteChar: formatOptions.quoteChar,
    order: "normal",
  });
  const decoder = new TextDecoder("utf-8");

  for await (const chunk of body) {
    const chunkText = decoder.decode(chunk, { stream: true });
    consumeCsvText(collector, chunkText);

    if (collector.targetRows !== null && collector.rows.length >= collector.targetRows) {
      if (typeof body.destroy === "function") {
        body.destroy();
      }
      break;
    }
  }

  consumeCsvText(collector, decoder.decode());
  finalizeCsvCollector(collector);

  if (!collector.rows.length) {
    throw new Error("The file was empty.");
  }

  return buildCsvResult(collector);
}

function extractRecordsFromTailBuffer(buffer, delimiter, reachedFileStart) {
  const parts = buffer.split(delimiter);

  if (parts.length && parts.at(-1) === "") {
    parts.pop();
  }

  if (!reachedFileStart && parts.length) {
    parts.shift();
  }

  return parts;
}

function parseCsv(text, maxRows, options = {}) {
  const collector = createCsvCollector(maxRows, options);
  consumeCsvText(collector, text);
  finalizeCsvCollector(collector);
  return buildCsvResult(collector);
}

function createCsvCollector(maxRows, options = {}) {
  const delimiter = typeof options.fieldDelimiter === "string" && options.fieldDelimiter.length
    ? options.fieldDelimiter
    : ",";
  const quoteChar = typeof options.quoteChar === "string" && options.quoteChar.length
    ? options.quoteChar
    : '"';
  const order = options.order === "reverse" ? "reverse" : "normal";
  const targetRows = order === "normal" && maxRows !== null ? maxRows + 1 : null;

  return {
    delimiter,
    quoteChar,
    order,
    targetRows,
    rows: [],
    row: [],
    field: "",
    insideQuotes: false,
  };
}

function consumeCsvText(collector, text) {
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];

    if (char === collector.quoteChar) {
      if (collector.insideQuotes && next === collector.quoteChar) {
        collector.field += collector.quoteChar;
        index += 1;
      } else {
        collector.insideQuotes = !collector.insideQuotes;
      }
      continue;
    }

    if (!collector.insideQuotes && char === collector.delimiter) {
      collector.row.push(collector.field);
      collector.field = "";
      continue;
    }

    if (!collector.insideQuotes && (char === "\n" || char === "\r")) {
      if (char === "\r" && next === "\n") {
        index += 1;
      }

      collector.row.push(collector.field);
      collector.rows.push(collector.row);
      collector.field = "";
      collector.row = [];

      if (collector.targetRows !== null && collector.rows.length >= collector.targetRows) {
        break;
      }
      continue;
    }

    collector.field += char;
  }
}

function finalizeCsvCollector(collector) {
  if (collector.field || collector.row.length) {
    collector.row.push(collector.field);
    collector.rows.push(collector.row);
  }
}

function buildCsvResult(collector) {
  if (collector.order === "reverse") {
    if (collector.targetRows === null) {
      return collector.rows.reverse();
    }
  }

  return collector.rows;
}
