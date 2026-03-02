import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { Readable } from "node:stream";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGunzip, gunzipSync } from "node:zlib";
import parquet from "parquetjs-lite";
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

    sendJson(response, 404, { error: "Rota não encontrada." });
  } catch (error) {
    sendJson(response, 500, { error: getErrorMessage(error) });
  }
});

server.listen(PORT, () => {
  console.log(`MultiBucket Explorer disponível em http://localhost:${PORT}`);
});

async function handleConnect(request, response) {
  const body = await readJsonBody(request);
  const connection = normalizeConnection(body);
  validateConnection(connection);

  const client = createS3Client(connection);
  await client.send(
    new ListObjectsV2Command({
      Bucket: connection.bucket,
      MaxKeys: 1,
    }),
  );

  const sessionId = randomUUID();
  sessions.set(sessionId, {
    ...connection,
    client,
    createdAt: Date.now(),
  });
  pruneSessions();

  sendJson(response, 200, {
    sessionId,
    bucket: connection.bucket,
    region: connection.region,
  });
}

async function handleListObjects(url, response) {
  const prefix = url.searchParams.get("prefix") ?? "";
  const session = getSession(url.searchParams.get("sessionId"));

  const result = await session.client.send(
    new ListObjectsV2Command({
      Bucket: session.bucket,
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
    throw new Error("Parâmetro key é obrigatório.");
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
    throw new Error("Parâmetro key é obrigatório.");
  }

  const result = await session.client.send(
    new GetObjectCommand({
      Bucket: session.bucket,
      Key: key,
    }),
  );

  const contentType = result.ContentType || "application/octet-stream";
  const fileName = path.basename(key);

  response.writeHead(200, {
    "Content-Type": contentType,
    "Content-Disposition": `attachment; filename="${encodeURIComponent(fileName)}"`,
    ...(result.ContentLength ? { "Content-Length": String(result.ContentLength) } : {}),
  });

  if (!result.Body) {
    response.end();
    return;
  }

  for await (const chunk of result.Body) {
    response.write(chunk);
  }

  response.end();
}

async function handleDeletePrefix(request, response) {
  const body = await readJsonBody(request);
  const session = getSession(typeof body.sessionId === "string" ? body.sessionId : "");
  const prefix = typeof body.prefix === "string" ? body.prefix.trim() : "";

  if (!prefix) {
    throw new Error("Por segurança, a limpeza da raiz não é permitida.");
  }

  let continuationToken;
  let deletedCount = 0;

  do {
    const listResult = await session.client.send(
      new ListObjectsV2Command({
        Bucket: session.bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      }),
    );

    const objects =
      listResult.Contents?.map((item) => item.Key).filter((key) => typeof key === "string" && key.length) ?? [];

    if (objects.length) {
      await session.client.send(
        new DeleteObjectsCommand({
          Bucket: session.bucket,
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

  sendJson(response, 200, {
    deletedCount,
    prefix,
  });
}

async function serveStatic(requestPath, response) {
  const safePath = requestPath === "/" ? "/index.html" : requestPath;
  const resolvedPath = path.resolve(__dirname, `.${safePath}`);

  if (!resolvedPath.startsWith(__dirname)) {
    sendJson(response, 403, { error: "Acesso negado." });
    return;
  }

  let fileStat;
  try {
    fileStat = await stat(resolvedPath);
  } catch {
    sendJson(response, 404, { error: "Arquivo não encontrado." });
    return;
  }

  if (!fileStat.isFile()) {
    sendJson(response, 404, { error: "Arquivo não encontrado." });
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

function getSession(sessionId) {
  if (!sessionId) {
    throw new Error("Sessão não informada.");
  }

  const session = sessions.get(sessionId);

  if (!session) {
    throw new Error("Sessão inválida ou expirada. Conecte novamente.");
  }

  if (Date.now() - session.createdAt > SESSION_TTL_MS) {
    sessions.delete(sessionId);
    throw new Error("Sessão expirada. Conecte novamente.");
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
        reject(new Error("JSON inválido no corpo da requisição."));
      }
    });

    request.on("error", reject);
  });
}

function normalizeConnection(body) {
  return {
    region: typeof body.region === "string" ? body.region.trim() : "",
    bucket: typeof body.bucket === "string" ? body.bucket.trim() : "",
    accessKeyId: typeof body.accessKeyId === "string" ? body.accessKeyId.trim() : "",
    secretAccessKey: typeof body.secretAccessKey === "string" ? body.secretAccessKey.trim() : "",
    sessionToken: typeof body.sessionToken === "string" ? body.sessionToken.trim() : "",
  };
}

function validateConnection(connection) {
  if (!connection.region || !connection.bucket || !connection.accessKeyId || !connection.secretAccessKey) {
    throw new Error("Preencha região, bucket e credenciais.");
  }
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
  });
  response.end(JSON.stringify(payload));
}

function getErrorMessage(error) {
  if (error && typeof error === "object") {
    const message = Reflect.get(error, "message");
    if (typeof message === "string" && message) {
      return message;
    }
  }

  return "Erro interno do servidor.";
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
    const csvText = await loadObjectText(session, key, true);

    return parseCsv(csvText, limit, {
      fieldDelimiter: formatOptions.fieldDelimiter,
      quoteChar: formatOptions.quoteChar,
      order,
    });
  }

  if (order === "reverse" && limit !== null) {
    const tailText = await loadCsvTailText(session, key, limit, formatOptions.recordDelimiter);

    if (!tailText) {
      throw new Error("O arquivo retornou vazio.");
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

  const result = await session.client.send(
    new GetObjectCommand({
      Bucket: session.bucket,
      Key: key,
    }),
  );

  const csvText = await result.Body?.transformToString();

  if (!csvText) {
    throw new Error("O arquivo retornou vazio.");
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

  if (extension === ".json" || extension === ".jsonl" || extension === ".ndjson") {
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

  throw new Error("Formato não suportado para preview.");
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
    .map((column, index) => column?.name || `coluna_${index + 1}`);
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
  const listResult = await session.client.send(
    new ListObjectsV2Command({
      Bucket: session.bucket,
      Prefix: directoryPrefix,
    }),
  );

  const dfmKeys =
    listResult.Contents?.map((item) => item.Key).filter((key) => typeof key === "string" && key.toLowerCase().endsWith(".dfm")) ??
    [];

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
    const jsonText = await loadObjectText(session, key);

    if (!jsonText) {
      return null;
    }

    return JSON.parse(jsonText);
  } catch {
    return null;
  }
}

async function loadObjectText(session, key, gzip = false) {
  const response = await session.client.send(
    new GetObjectCommand({
      Bucket: session.bucket,
      Key: key,
    }),
  );
  let text = "";

  if (gzip) {
    const bytes = await response.Body?.transformToByteArray();
    text = bytes?.length ? gunzipSync(Buffer.from(bytes)).toString("utf-8") : "";
  } else {
    text = await response.Body?.transformToString();
  }

  if (!text) {
    throw new Error("O arquivo retornou vazio.");
  }

  return text;
}

async function loadObjectBuffer(session, key, gzip = false) {
  const response = await session.client.send(
    new GetObjectCommand({
      Bucket: session.bucket,
      Key: key,
    }),
  );
  const bytes = await response.Body?.transformToByteArray();

  if (!bytes?.length) {
    throw new Error("O arquivo retornou vazio.");
  }

  return gzip ? gunzipSync(Buffer.from(bytes)) : Buffer.from(bytes);
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

  const text = await loadObjectText(session, key, gzip);
  const trimmed = text.trim();
  let records = [];

  if (!trimmed) {
    throw new Error("O arquivo retornou vazio.");
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

  if (limit !== null && (isLineDelimitedJson || extension === ".json")) {
    if (gzip) {
      const lines = await tryLoadGzipDelimitedLines(session, key, limit, order);

      if (lines) {
        return order === "reverse" ? [...lines].reverse().join("\n") : lines.join("\n");
      }
    } else if (order === "normal") {
      return (await loadDelimitedHeadRecords(session, key, limit)).join("\n");
    } else {
      return (await loadDelimitedTailRecords(session, key, limit, "\n")).reverse().join("\n");
    }
  }

  const text = await loadObjectText(session, key, gzip);
  const trimmed = text.trim();

  if (!trimmed) {
    throw new Error("O arquivo retornou vazio.");
  }

  if (isLineDelimitedJson) {
    return trimmed;
  }

  try {
    return JSON.stringify(JSON.parse(trimmed), null, 2);
  } catch {
    return trimmed;
  }
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
  const response = await session.client.send(
    new GetObjectCommand({
      Bucket: session.bucket,
      Key: key,
    }),
  );

  if (!response.Body) {
      throw new Error("O arquivo retornou vazio.");
  }

  const source = response.Body instanceof Readable ? response.Body : Readable.from(response.Body);
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
  if (gzip) {
    const fileBuffer = await loadObjectBuffer(session, key, true);
    const reader = await parquet.ParquetReader.openBuffer(fileBuffer);

    try {
      const cursor = reader.getCursor();
      const records = [];
      let record = null;

      while ((record = await cursor.next())) {
        if (order === "reverse" && limit !== null) {
          if (records.length === limit) {
            records.shift();
          }
          records.push(record);
          continue;
        }

        records.push(record);

        if (order === "normal" && limit !== null && records.length >= limit) {
          break;
        }
      }

      return slicePreviewRecords(records, limit, order);
    } finally {
      await reader.close();
    }
  }

  const reader = await parquet.ParquetReader.openS3(
    createParquetS3Adapter(session.client),
    {
      Bucket: session.bucket,
      Key: key,
    },
  );

  try {
    const cursor = reader.getCursor();
    const records = [];
    let record = null;

    while ((record = await cursor.next())) {
      if (order === "reverse" && limit !== null) {
        if (records.length === limit) {
          records.shift();
        }
        records.push(record);
        continue;
      }

      records.push(record);

      if (order === "normal" && limit !== null && records.length >= limit) {
        break;
      }
    }

    return slicePreviewRecords(records, limit, order);
  } finally {
    await reader.close();
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
    return Object.fromEntries(record.map((value, index) => [`coluna_${index + 1}`, value]));
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
  return isGzipKey(key) ? key.slice(0, -3) : key;
}

function isGzipKey(key) {
  return key.toLowerCase().endsWith(".gz");
}

function createParquetS3Adapter(s3Client) {
  return {
    headObject(params) {
      return {
        promise: async () => {
          const result = await s3Client.send(new HeadObjectCommand(params));
          return {
            ContentLength: result.ContentLength ?? 0,
          };
        },
      };
    },
    getObject(params) {
      return {
        promise: async () => {
          const result = await s3Client.send(new GetObjectCommand(params));
          const body = await result.Body?.transformToByteArray();
          return {
            Body: body ? Buffer.from(body) : Buffer.alloc(0),
          };
        },
      };
    },
  };
}

async function loadDelimitedHeadRecords(session, key, limit) {
  const result = await session.client.send(
    new GetObjectCommand({
      Bucket: session.bucket,
      Key: key,
    }),
  );

  if (!result.Body) {
    throw new Error("O arquivo retornou vazio.");
  }

  const decoder = new TextDecoder("utf-8");
  let buffer = "";
  const records = [];

  for await (const chunk of result.Body) {
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
        if (typeof result.Body.destroy === "function") {
          result.Body.destroy();
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
  const headResult = await session.client.send(
    new HeadObjectCommand({
      Bucket: session.bucket,
      Key: key,
    }),
  );
  const contentLength = headResult.ContentLength ?? 0;

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
    const result = await session.client.send(
      new GetObjectCommand({
        Bucket: session.bucket,
        Key: key,
        Range: `bytes=${start}-${end}`,
      }),
    );
    const bytes = await result.Body?.transformToByteArray();
    const chunkText = bytes ? decoder.decode(bytes) : "";

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
  const result = await session.client.send(
    new GetObjectCommand({
      Bucket: session.bucket,
      Key: key,
    }),
  );

  if (!result.Body) {
    throw new Error("O arquivo retornou vazio.");
  }

  const collector = createCsvCollector(limit, {
    fieldDelimiter: formatOptions.fieldDelimiter,
    quoteChar: formatOptions.quoteChar,
    order: "normal",
  });
  const decoder = new TextDecoder("utf-8");

  for await (const chunk of result.Body) {
    const chunkText = decoder.decode(chunk, { stream: true });
    consumeCsvText(collector, chunkText);

    if (collector.targetRows !== null && collector.rows.length >= collector.targetRows) {
      if (typeof result.Body.destroy === "function") {
        result.Body.destroy();
      }
      break;
    }
  }

  consumeCsvText(collector, decoder.decode());
  finalizeCsvCollector(collector);

  if (!collector.rows.length) {
    throw new Error("O arquivo retornou vazio.");
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
