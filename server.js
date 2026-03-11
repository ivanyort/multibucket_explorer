import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { createReadStream, createWriteStream, existsSync, readFileSync } from "node:fs";
import { cp, mkdir, mkdtemp, readdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import https from "node:https";
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { Readable } from "node:stream";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGunzip, gunzipSync } from "node:zlib";
import AdmZip from "adm-zip";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { DataLakeServiceClient, StorageSharedKeyCredential } from "@azure/storage-file-datalake";
import { Storage as GoogleCloudStorage } from "@google-cloud/storage";
import duckdb from "@duckdb/node-api";
import avro from "avsc";
import { parquetMetadataAsync, parquetReadObjects } from "hyparquet";
import { compressors as hyparquetCompressors } from "hyparquet-compressors";
import snappy from "snappyjs";
import {
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PACKAGE_VERSION = JSON.parse(readFileSync(path.join(__dirname, "package.json"), "utf-8")).version ?? "0.0.0";
const APP_VERSION = typeof process.env.APP_VERSION === "string" && process.env.APP_VERSION.trim()
  ? process.env.APP_VERSION.trim()
  : PACKAGE_VERSION;
const PORT = Number.parseInt(process.env.PORT ?? "8086", 10);
const IS_DOCKER = detectDockerEnvironment();
const DEV_FEATURES_ENABLED = process.env.NODE_ENV !== "production" && !IS_DOCKER;
const SESSION_TTL_MS = 12 * 60 * 60 * 1000;
const DESTRUCTIVE_OPERATIONS_ENABLED = !isTruthyEnv(process.env.DISABLE_DESTRUCTIVE_OPERATIONS);
const ORC_TOOLS_VERSION = "2.3.0";
const ORC_TOOLS_FILE_NAME = `orc-tools-${ORC_TOOLS_VERSION}-uber.jar`;
const ORC_TOOLS_URL =
  `https://repo1.maven.org/maven2/org/apache/orc/orc-tools/${ORC_TOOLS_VERSION}/${ORC_TOOLS_FILE_NAME}`;
const ORC_TOOLS_CACHE_DIR = path.join(__dirname, ".cache", "orc-tools");
const ICEBERG_SAMPLE_PREFIX_SUFFIX = "_sample_data/iceberg/";
const ICEBERG_SAMPLE_ARCHIVE_URL = "https://duckdb.org/data/iceberg_data.zip";
const ICEBERG_SAMPLE_CACHE_DIR = path.join(__dirname, ".cache", "iceberg-samples");
const ICEBERG_SAMPLE_ARCHIVE_PATH = path.join(ICEBERG_SAMPLE_CACHE_DIR, "iceberg_data.zip");
const ICEBERG_SAMPLE_EXTRACTED_DIR = path.join(ICEBERG_SAMPLE_CACHE_DIR, "iceberg_data");
const ICEBERG_BASE_FIXTURE_DIR_NAME = "lineitem_iceberg";
const ICEBERG_FIXTURE_TABLES = [
  { name: "orders_parquet_basic", format: "parquet", notes: "Real parquet-backed Iceberg fixture copied from the official DuckDB sample." },
  { name: "orders_avro_basic", format: "parquet", notes: "Temporary alias of the official parquet-backed Iceberg fixture until an Avro-backed Iceberg writer is available." },
  { name: "orders_orc_basic", format: "parquet", notes: "Temporary alias of the official parquet-backed Iceberg fixture until an ORC-backed Iceberg writer is available." },
  { name: "orders_with_position_deletes", format: "parquet", notes: "Temporary alias of the official multi-snapshot Iceberg fixture; row-level position delete generation is not emitted by DuckDB directly here." },
  { name: "orders_with_equality_deletes", format: "parquet", notes: "Temporary alias of the official multi-snapshot Iceberg fixture; equality delete generation is not emitted by DuckDB directly here." },
  { name: "orders_multi_snapshot", format: "parquet", notes: "Real Iceberg fixture with multiple metadata versions from the official DuckDB sample." },
  { name: "orders_empty", format: "parquet", notes: "Temporary alias of the official parquet-backed Iceberg fixture until an empty Iceberg table generator is available." },
];
const AVRO_LONG_AS_STRING_TYPE = avro.types.LongType.__with({
  fromBuffer(buffer) {
    return buffer.readBigInt64LE().toString();
  },
  toBuffer(value) {
    const normalized = normalizeAvroLongValue(value);
    const buffer = Buffer.alloc(8);
    buffer.writeBigInt64LE(normalized);
    return buffer;
  },
  fromJSON(value) {
    return normalizeAvroLongValue(value).toString();
  },
  toJSON(value) {
    return normalizeAvroLongValue(value).toString();
  },
  isValid(value) {
    try {
      normalizeAvroLongValue(value);
      return true;
    } catch {
      return false;
    }
  },
  compare(left, right) {
    const leftValue = normalizeAvroLongValue(left);
    const rightValue = normalizeAvroLongValue(right);
    return leftValue === rightValue ? 0 : leftValue < rightValue ? -1 : 1;
  },
});
const sessions = new Map();

const MIME_TYPES = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
};

const SERVER_TRANSLATIONS = {
  en: {
    route_not_found: "Route not found.",
    internal_server_error: "Internal server error.",
    key_required: "The key parameter is required.",
    delete_root_forbidden: "For safety, deleting the storage root is not allowed.",
    delete_file_target_invalid: "Deleting a single file requires a file key, not a folder or root.",
    destructive_operations_disabled: "Destructive operations are disabled by the server configuration.",
    access_denied: "Access denied.",
    file_not_found: "File not found.",
    file_empty: "The file was empty.",
    session_not_provided: "Session not provided.",
    session_invalid: "Invalid or expired session. Connect again.",
    session_expired: "Session expired. Connect again.",
    invalid_json_body: "Invalid JSON in request body.",
    fill_adls: "Fill in account name, file system, and account key.",
    fill_gcs: "Fill in bucket, and service account JSON.",
    invalid_service_account_json: "Service account JSON must be valid JSON.",
    fill_minio: "Fill in endpoint, bucket, access key ID, and secret access key.",
    fill_s3: "Fill in region, bucket, and credentials.",
    unsupported_preview_format: "Unsupported preview format.",
    iceberg_not_detected: "The selected prefix is not an Iceberg table root.",
    iceberg_no_snapshot: "The Iceberg table has no current snapshot.",
    iceberg_delete_files_unsupported: "Iceberg preview does not support snapshots with active delete files yet.",
    iceberg_data_format_unsupported: "Unsupported Iceberg data file format: {format}",
    iceberg_preview_failed: "Failed to read Iceberg preview: {message}",
    iceberg_seed_prefix_invalid: "The sample prefix is required.",
    iceberg_seed_conflict: "The sample prefix already contains data. Remove {prefix} before generating fixtures again.",
    iceberg_seed_failed: "Failed to generate Iceberg sample data: {message}",
    iceberg_seed_warning_aliases: "Avro, ORC, delete-heavy, and empty table variants are temporary aliases of the parquet-backed sample until native fixture generation is expanded.",
    iceberg_seed_warning_template: "The current seed tool stages the official DuckDB Iceberg sample under several table names to keep the test prefix deterministic.",
    avro_preview_failed: "Failed to read Avro preview: {message}",
    parquet_preview_failed: "Failed to read Parquet preview: {message}",
    orc_preview_failed: "Failed to read ORC preview: {message}",
    orc_java_missing: "ORC preview requires Java in PATH on the backend host.",
    orc_tools_download_failed: "Failed to prepare ORC preview tooling: {message}",
  },
  "pt-BR": {
    route_not_found: "Rota nao encontrada.",
    internal_server_error: "Erro interno do servidor.",
    key_required: "O parametro key e obrigatorio.",
    delete_root_forbidden: "Por seguranca, nao e permitido apagar a raiz do storage.",
    delete_file_target_invalid: "Apagar um arquivo individual exige a chave de um arquivo, nao de uma pasta ou da raiz.",
    destructive_operations_disabled: "As operacoes destrutivas estao desativadas pela configuracao do servidor.",
    access_denied: "Acesso negado.",
    file_not_found: "Arquivo nao encontrado.",
    file_empty: "O arquivo esta vazio.",
    session_not_provided: "Sessao nao informada.",
    session_invalid: "Sessao invalida ou expirada. Conecte-se novamente.",
    session_expired: "Sessao expirada. Conecte-se novamente.",
    invalid_json_body: "JSON invalido no corpo da requisicao.",
    fill_adls: "Preencha nome da conta, file system e account key.",
    fill_gcs: "Preencha bucket e service account JSON.",
    invalid_service_account_json: "O service account JSON deve ser um JSON valido.",
    fill_minio: "Preencha endpoint, bucket, access key ID e secret access key.",
    fill_s3: "Preencha regiao, bucket e credenciais.",
    unsupported_preview_format: "Formato de pre-visualizacao nao suportado.",
    iceberg_not_detected: "O prefixo selecionado nao e a raiz de uma tabela Iceberg.",
    iceberg_no_snapshot: "A tabela Iceberg nao possui snapshot atual.",
    iceberg_delete_files_unsupported: "A pre-visualizacao Iceberg ainda nao suporta snapshots com delete files ativos.",
    iceberg_data_format_unsupported: "Formato de arquivo Iceberg nao suportado: {format}",
    iceberg_preview_failed: "Falha ao ler a pre-visualizacao Iceberg: {message}",
    iceberg_seed_prefix_invalid: "O prefixo de amostra e obrigatorio.",
    iceberg_seed_conflict: "O prefixo de amostras ja contem dados. Remova {prefix} antes de gerar os fixtures novamente.",
    iceberg_seed_failed: "Falha ao gerar os dados de exemplo Iceberg: {message}",
    iceberg_seed_warning_aliases: "As variantes Avro, ORC, com deletes e vazia usam temporariamente o fixture em Parquet ate a geracao nativa desses cenarios ser ampliada.",
    iceberg_seed_warning_template: "A ferramenta atual publica o sample oficial Iceberg do DuckDB sob varios nomes de tabela para manter o prefixo de teste deterministico.",
    avro_preview_failed: "Falha ao ler a pre-visualizacao do Avro: {message}",
    parquet_preview_failed: "Falha ao ler a pre-visualizacao do Parquet: {message}",
    orc_preview_failed: "Falha ao ler a pre-visualizacao do ORC: {message}",
    orc_java_missing: "A pre-visualizacao de ORC exige Java no PATH do backend.",
    orc_tools_download_failed: "Falha ao preparar a ferramenta de pre-visualizacao ORC: {message}",
  },
  es: {
    route_not_found: "Ruta no encontrada.",
    internal_server_error: "Error interno del servidor.",
    key_required: "El parametro key es obligatorio.",
    delete_root_forbidden: "Por seguridad, no se permite borrar la raiz del storage.",
    delete_file_target_invalid: "Borrar un archivo individual requiere la clave de un archivo, no de una carpeta ni de la raiz.",
    destructive_operations_disabled: "Las operaciones destructivas estan deshabilitadas por la configuracion del servidor.",
    access_denied: "Acceso denegado.",
    file_not_found: "Archivo no encontrado.",
    file_empty: "El archivo esta vacio.",
    session_not_provided: "Sesion no informada.",
    session_invalid: "Sesion invalida o expirada. Conectate de nuevo.",
    session_expired: "Sesion expirada. Conectate de nuevo.",
    invalid_json_body: "JSON invalido en el cuerpo de la solicitud.",
    fill_adls: "Completa nombre de la cuenta, file system y account key.",
    fill_gcs: "Completa bucket y service account JSON.",
    invalid_service_account_json: "El service account JSON debe ser un JSON valido.",
    fill_minio: "Completa endpoint, bucket, access key ID y secret access key.",
    fill_s3: "Completa region, bucket y credenciales.",
    unsupported_preview_format: "Formato de vista previa no compatible.",
    iceberg_not_detected: "El prefijo seleccionado no es la raiz de una tabla Iceberg.",
    iceberg_no_snapshot: "La tabla Iceberg no tiene snapshot actual.",
    iceberg_delete_files_unsupported: "La vista previa Iceberg todavia no admite snapshots con delete files activos.",
    iceberg_data_format_unsupported: "Formato de archivo Iceberg no compatible: {format}",
    iceberg_preview_failed: "Error al leer la vista previa Iceberg: {message}",
    iceberg_seed_prefix_invalid: "El prefijo de muestras es obligatorio.",
    iceberg_seed_conflict: "El prefijo de muestras ya contiene datos. Elimina {prefix} antes de generar los fixtures nuevamente.",
    iceberg_seed_failed: "Error al generar los datos de ejemplo Iceberg: {message}",
    iceberg_seed_warning_aliases: "Las variantes Avro, ORC, con deletes y vacia son alias temporales del sample respaldado por Parquet hasta ampliar la generacion nativa de fixtures.",
    iceberg_seed_warning_template: "La herramienta actual publica el sample oficial Iceberg de DuckDB bajo varios nombres de tabla para mantener deterministico el prefijo de prueba.",
    avro_preview_failed: "Error al leer la vista previa de Avro: {message}",
    parquet_preview_failed: "Error al leer la vista previa de Parquet: {message}",
    orc_preview_failed: "Error al leer la vista previa de ORC: {message}",
    orc_java_missing: "La vista previa de ORC requiere Java en el PATH del backend.",
    orc_tools_download_failed: "Error al preparar la herramienta de vista previa ORC: {message}",
  },
  it: {
    route_not_found: "Percorso non trovato.",
    internal_server_error: "Errore interno del server.",
    key_required: "Il parametro key e obbligatorio.",
    delete_root_forbidden: "Per sicurezza, non e consentito eliminare la radice dello storage.",
    delete_file_target_invalid: "Per eliminare un singolo file serve la chiave di un file, non di una cartella o della radice.",
    destructive_operations_disabled: "Le operazioni distruttive sono disabilitate dalla configurazione del server.",
    access_denied: "Accesso negato.",
    file_not_found: "File non trovato.",
    file_empty: "Il file e vuoto.",
    session_not_provided: "Sessione non fornita.",
    session_invalid: "Sessione non valida o scaduta. Connettiti di nuovo.",
    session_expired: "Sessione scaduta. Connettiti di nuovo.",
    invalid_json_body: "JSON non valido nel corpo della richiesta.",
    fill_adls: "Compila nome account, file system e account key.",
    fill_gcs: "Compila bucket e service account JSON.",
    invalid_service_account_json: "Il service account JSON deve essere un JSON valido.",
    fill_minio: "Compila endpoint, bucket, access key ID e secret access key.",
    fill_s3: "Compila regione, bucket e credenziali.",
    unsupported_preview_format: "Formato di anteprima non supportato.",
    iceberg_not_detected: "Il prefisso selezionato non e la radice di una tabella Iceberg.",
    iceberg_no_snapshot: "La tabella Iceberg non ha uno snapshot corrente.",
    iceberg_delete_files_unsupported: "L'anteprima Iceberg non supporta ancora snapshot con delete file attivi.",
    iceberg_data_format_unsupported: "Formato file Iceberg non supportato: {format}",
    iceberg_preview_failed: "Errore durante la lettura dell'anteprima Iceberg: {message}",
    iceberg_seed_prefix_invalid: "Il prefisso di esempio e obbligatorio.",
    iceberg_seed_conflict: "Il prefisso di esempio contiene gia dati. Rimuovi {prefix} prima di generare di nuovo i fixture.",
    iceberg_seed_failed: "Errore durante la generazione dei dati di esempio Iceberg: {message}",
    iceberg_seed_warning_aliases: "Le varianti Avro, ORC, con delete e vuota usano temporaneamente il fixture Parquet finche la generazione nativa non verra ampliata.",
    iceberg_seed_warning_template: "Lo strumento corrente pubblica il sample Iceberg ufficiale di DuckDB con vari nomi di tabella per mantenere deterministico il prefisso di test.",
    avro_preview_failed: "Errore durante la lettura dell'anteprima Avro: {message}",
    parquet_preview_failed: "Errore durante la lettura dell'anteprima Parquet: {message}",
    orc_preview_failed: "Errore durante la lettura dell'anteprima ORC: {message}",
    orc_java_missing: "L'anteprima ORC richiede Java nel PATH del backend.",
    orc_tools_download_failed: "Errore durante la preparazione dello strumento di anteprima ORC: {message}",
  },
};

class LocalizedError extends Error {
  constructor(key, variables = {}) {
    super(key);
    this.translationKey = key;
    this.translationVariables = variables;
  }
}

const server = createServer(async (request, response) => {
  const locale = getRequestLocale(request);

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

    if (request.method === "GET" && url.pathname === "/api/app-info") {
      handleAppInfo(response);
      return;
    }

    if (request.method === "GET" && url.pathname === "/api/preview") {
      await handlePreview(url, response, locale);
      return;
    }

    if (request.method === "GET" && url.pathname === "/api/iceberg/inspect") {
      await handleIcebergInspect(url, response, locale);
      return;
    }

    if (request.method === "GET" && url.pathname === "/api/iceberg/preview") {
      await handleIcebergPreview(url, response, locale);
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

    if (request.method === "POST" && url.pathname === "/api/delete-file") {
      await handleDeleteFile(request, response);
      return;
    }

    if (request.method === "POST" && url.pathname === "/api/dev/seed-iceberg") {
      await handleSeedIceberg(request, response, locale);
      return;
    }

    if (request.method === "GET") {
      await serveStatic(url.pathname, response, locale);
      return;
    }

    sendJson(response, 404, { error: localizeError(locale, new LocalizedError("route_not_found")) });
  } catch (error) {
    console.error("[server] request failed", {
      method: request.method,
      url: request.url,
      message: getErrorMessage(error, locale),
      code: typeof error?.code === "string" ? error.code : undefined,
      statusCode: typeof error?.statusCode === "number" ? error.statusCode : undefined,
    });
    sendJson(response, 500, { error: localizeError(locale, error) });
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
    destructiveOperationsEnabled: DESTRUCTIVE_OPERATIONS_ENABLED,
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

function handleAppInfo(response) {
  sendJson(response, 200, {
    version: APP_VERSION,
    devFeatures: {
      seedIceberg: DEV_FEATURES_ENABLED,
    },
  });
}

function detectDockerEnvironment() {
  if (isTruthyEnv(process.env.CODESPACES) || isTruthyEnv(process.env.GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN)) {
    return false;
  }

  if (existsSync("/.dockerenv")) {
    return true;
  }

  try {
    const cgroup = readFileSync("/proc/1/cgroup", "utf-8");
    return /docker|containerd|kubepods|podman/i.test(cgroup);
  } catch {
    return false;
  }
}

async function handlePreview(url, response, locale) {
  const key = url.searchParams.get("key") ?? "";
  const limit = parsePreviewLimit(url.searchParams.get("limit"));
  const order = parsePreviewOrder(url.searchParams.get("order"));
  const mode = parsePreviewMode(url.searchParams.get("mode"));
  const session = getSession(url.searchParams.get("sessionId"));

  if (!key) {
    throw new LocalizedError("key_required");
  }

  const previewData = await loadPreviewData(session, key, limit, order, mode, locale);

  sendJson(response, 200, {
    rows: previewData.rows,
    metadataColumns: previewData.metadataColumns,
    dfmKey: previewData.dfmKey,
    order: previewData.order ?? order,
    previewFormat: previewData.previewFormat,
    previewMode: previewData.previewMode,
    rawText: previewData.rawText,
    lineCount: previewData.lineCount,
  });
}

async function handleIcebergInspect(url, response, locale) {
  const prefix = ensureTrailingSlash(url.searchParams.get("prefix") ?? "");
  const requestedSnapshotId = url.searchParams.get("snapshotId");
  const session = getSession(url.searchParams.get("sessionId"));

  if (!prefix) {
    sendJson(response, 200, { isIceberg: false });
    return;
  }

  const table = await inspectIcebergTable(session, prefix, locale, requestedSnapshotId);

  if (!table) {
    sendJson(response, 200, { isIceberg: false });
    return;
  }

  sendJson(response, 200, {
    isIceberg: true,
    tablePrefix: table.tablePrefix,
    metadataFile: table.metadataKey,
    currentSnapshotId: table.currentSnapshotId,
    snapshotId: table.snapshotId,
    snapshots: table.snapshots,
    schemaColumns: table.schemaColumns,
    dataFileCount: table.dataFiles.length,
    dataFormat: table.dataFormat,
    deleteFileCount: table.deleteFileCount,
  });
}

async function handleIcebergPreview(url, response, locale) {
  const prefix = ensureTrailingSlash(url.searchParams.get("prefix") ?? "");
  const limit = parsePreviewLimit(url.searchParams.get("limit"));
  const order = parsePreviewOrder(url.searchParams.get("order"));
  const requestedSnapshotId = url.searchParams.get("snapshotId");
  const session = getSession(url.searchParams.get("sessionId"));

  const table = await inspectIcebergTable(session, prefix, locale, requestedSnapshotId);

  if (!table) {
    throw new LocalizedError("iceberg_not_detected");
  }

  const preview = await loadIcebergPreviewData(session, table, limit, order, locale);

  sendJson(response, 200, {
    rows: preview.rows,
    metadataColumns: preview.metadataColumns,
    previewFormat: "iceberg",
    previewMode: "table",
    lineCount: preview.rows.length,
    icebergMeta: {
      tablePrefix: table.tablePrefix,
      metadataFile: table.metadataKey,
      currentSnapshotId: table.currentSnapshotId,
      snapshotId: table.snapshotId,
      snapshots: table.snapshots,
      dataFileCount: table.dataFiles.length,
      deleteFileCount: table.deleteFileCount,
      dataFormat: table.dataFormat,
    },
  });
}

async function handleDownload(url, response) {
  const key = url.searchParams.get("key") ?? "";
  const session = getSession(url.searchParams.get("sessionId"));

  if (!key) {
    throw new LocalizedError("key_required");
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
  ensureDestructiveOperationsEnabled();
  const body = await readJsonBody(request);
  const session = getSession(typeof body.sessionId === "string" ? body.sessionId : "");
  const prefix = typeof body.prefix === "string" ? body.prefix.trim() : "";

  if (!prefix) {
    throw new LocalizedError("delete_root_forbidden");
  }

  const deletedCount = await deleteStoragePrefix(session, prefix);

  sendJson(response, 200, {
    deletedCount,
    prefix,
  });
}

async function handleDeleteFile(request, response) {
  ensureDestructiveOperationsEnabled();
  const body = await readJsonBody(request);
  const session = getSession(typeof body.sessionId === "string" ? body.sessionId : "");
  const key = typeof body.key === "string" ? body.key.trim() : "";

  if (!key || key.endsWith("/")) {
    throw new LocalizedError("delete_file_target_invalid");
  }

  await deleteStorageFile(session, key);

  sendJson(response, 200, {
    deletedCount: 1,
    key,
  });
}

async function handleSeedIceberg(request, response, locale) {
  const body = await readJsonBody(request);
  const session = getSession(typeof body.sessionId === "string" ? body.sessionId : "");
  const targetPrefix = buildIcebergSamplePrefix(typeof body.targetPrefix === "string" ? body.targetPrefix : "");
  console.log(`[seed] start provider=${session.storage.provider} prefix=${targetPrefix}`);
  const result = await seedIcebergFixtures(session, locale, targetPrefix);
  console.log(`[seed] completed provider=${session.storage.provider} prefix=${targetPrefix} tables=${result.createdTables?.length ?? 0}`);
  sendJson(response, 200, result);
}

async function serveStatic(requestPath, response, locale) {
  const safePath = requestPath === "/" ? "/index.html" : requestPath;
  const resolvedPath = path.resolve(__dirname, `.${safePath}`);

  if (!resolvedPath.startsWith(__dirname)) {
    sendJson(response, 403, { error: translateServer(locale, "access_denied") });
    return;
  }

  let fileStat;
  try {
    fileStat = await stat(resolvedPath);
  } catch {
    sendJson(response, 404, { error: translateServer(locale, "file_not_found") });
    return;
  }

  if (!fileStat.isFile()) {
    sendJson(response, 404, { error: translateServer(locale, "file_not_found") });
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

function getRequestLocale(request) {
  const explicit = typeof request.headers["x-app-language"] === "string" ? request.headers["x-app-language"] : "";
  const accepted = typeof request.headers["accept-language"] === "string" ? request.headers["accept-language"] : "";
  return normalizeLocale(explicit || accepted);
}

function normalizeLocale(value) {
  if (typeof value !== "string") {
    return "en";
  }

  if (value.startsWith("pt-BR") || value.startsWith("pt")) {
    return "pt-BR";
  }

  if (value.startsWith("es")) {
    return "es";
  }

  if (value.startsWith("it")) {
    return "it";
  }

  return "en";
}

function translateServer(locale, key, variables = {}) {
  const dictionary = SERVER_TRANSLATIONS[locale] ?? SERVER_TRANSLATIONS.en;
  const template = dictionary[key] ?? SERVER_TRANSLATIONS.en[key] ?? key;
  return template.replace(/\{(\w+)\}/g, (_, name) => `${variables[name] ?? ""}`);
}

function localizeError(locale, error) {
  if (error instanceof LocalizedError) {
    return translateServer(locale, error.translationKey, error.translationVariables);
  }

  return getErrorMessage(error, locale);
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

    const resolvedName = resolveAdlsListPath(path, item.name);
    const normalizedName = item.isDirectory ? ensureTrailingSlash(resolvedName) : resolvedName;

    if (normalizedName === prefix || resolvedName === path) {
      continue;
    }

    items.push({
      type: item.isDirectory ? "folder" : "file",
      key: item.isDirectory ? ensureTrailingSlash(resolvedName) : resolvedName,
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
        paths.push({
          ...item,
          name: resolveAdlsListPath(path, item.name),
        });
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

function toStoragePathItem(item) {
  if ("metadata" in item && "name" in item) {
    return {
      key: item.name,
      lastModified: item.metadata?.updated ?? null,
      type: "file",
    };
  }

  if ("name" in item) {
    return {
      key: item.isDirectory ? ensureTrailingSlash(item.name) : item.name,
      lastModified: item.lastModified ? new Date(item.lastModified).toISOString() : null,
      type: item.isDirectory ? "folder" : "file",
    };
  }

  return {
    key: item.Key ?? "",
    lastModified: item.LastModified?.toISOString?.() ?? null,
    type: "file",
  };
}

async function deleteStoragePrefix(session, prefix) {
  if (session.storage.provider === "adls") {
    const normalizedPrefix = normalizeAdlsDirectory(prefix);

    if (!normalizedPrefix) {
      throw new LocalizedError("delete_root_forbidden");
    }

    const paths = await listStoragePaths(session, prefix, true);
    const childPaths = paths.filter((item) => item.name && item.name !== normalizedPrefix);
    const files = childPaths.filter((item) => !item.isDirectory);
    const directories = childPaths
      .filter((item) => item.isDirectory)
      .sort((left, right) => getPathDepth(right.name) - getPathDepth(left.name));

    await Promise.all(files.map((item) => session.storage.fileSystemClient.getFileClient(item.name).delete()));

    for (const directory of directories) {
      await session.storage.fileSystemClient.getDirectoryClient(directory.name).delete();
    }

    const deletedCount = files.length;
    return deletedCount;
  }

  if (session.storage.provider === "gcs") {
    const normalizedPrefix = ensureTrailingSlash(prefix);
    const files = await listStoragePaths(session, normalizedPrefix, true);
    const childFiles = files.filter((file) => file.name !== normalizedPrefix);

    await Promise.all(childFiles.map((file) => file.delete()));
    await ensurePrefixMarkerObject(session, normalizedPrefix);
    return childFiles.length;
  }

  const normalizedPrefix = ensureTrailingSlash(prefix);
  let continuationToken;
  let deletedCount = 0;

  do {
    const listResult = await session.storage.client.send(
      new ListObjectsV2Command({
        Bucket: session.storage.bucket,
        Prefix: normalizedPrefix,
        ContinuationToken: continuationToken,
      }),
    );

    const objects =
      listResult.Contents?.map((item) => item.Key)
        .filter((key) => typeof key === "string" && key.length && key !== normalizedPrefix) ?? [];

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

  await ensurePrefixMarkerObject(session, normalizedPrefix);
  return deletedCount;
}

async function deleteStorageFile(session, key) {
  if (session.storage.provider === "adls") {
    await session.storage.fileSystemClient.getFileClient(key).delete();
    return;
  }

  if (session.storage.provider === "gcs") {
    await session.storage.bucket.file(key).delete();
    return;
  }

  await session.storage.client.send(
    new DeleteObjectCommand({
      Bucket: session.storage.bucket,
      Key: key,
    }),
  );
}

async function seedIcebergFixtures(session, locale, targetPrefix) {
  await ensureIcebergSamplePrefixEmpty(session, targetPrefix);

  let tempDir;

  try {
    const baseFixtureDir = await ensureIcebergSampleTemplate(locale);
    tempDir = await mkdtemp(path.join(tmpdir(), "multibucket-explorer-iceberg-seed-"));

    const stageRoot = path.join(tempDir, "stage");
    const stageBasePrefixDir = path.join(stageRoot, ...targetPrefix.split("/").filter(Boolean));
    await mkdir(stageBasePrefixDir, { recursive: true });

    const createdTables = [];

    for (const fixture of ICEBERG_FIXTURE_TABLES) {
      const tableDir = path.join(stageBasePrefixDir, fixture.name);
      await cp(baseFixtureDir, tableDir, { recursive: true });

      const summary = await summarizeSeededIcebergTable(tableDir, fixture, targetPrefix);
      createdTables.push(summary);
    }

    await uploadDirectoryToStorage(session, stageBasePrefixDir, targetPrefix);

    const warnings = [
      translateServer(locale, "iceberg_seed_warning_template"),
      translateServer(locale, "iceberg_seed_warning_aliases"),
    ];

    return {
      basePrefix: targetPrefix,
      createdTables,
      warnings,
    };
  } catch (error) {
    if (error instanceof LocalizedError) {
      throw error;
    }

    throw new LocalizedError("iceberg_seed_failed", {
      message: localizeError(locale, error),
    });
  } finally {
    if (tempDir) {
      await rm(tempDir, { recursive: true, force: true });
    }
  }
}

async function ensureIcebergSamplePrefixEmpty(session, targetPrefix) {
  let existingPaths = [];

  try {
    existingPaths = await listStoragePaths(session, targetPrefix, true);
  } catch (error) {
    if (!isStoragePathMissingError(error)) {
      throw error;
    }
  }

  if (existingPaths.length > 0) {
    throw new LocalizedError("iceberg_seed_conflict", {
      prefix: targetPrefix,
    });
  }
}

async function ensureIcebergSampleTemplate(locale) {
  const baseFixtureDir = path.join(ICEBERG_SAMPLE_EXTRACTED_DIR, "data", "iceberg", ICEBERG_BASE_FIXTURE_DIR_NAME);

  try {
    await stat(baseFixtureDir);
    return baseFixtureDir;
  } catch {
    // Cache miss.
  }

  try {
    await mkdir(ICEBERG_SAMPLE_CACHE_DIR, { recursive: true });

    try {
      await stat(ICEBERG_SAMPLE_ARCHIVE_PATH);
    } catch {
      await downloadFile(ICEBERG_SAMPLE_ARCHIVE_URL, ICEBERG_SAMPLE_ARCHIVE_PATH);
    }

    await rm(ICEBERG_SAMPLE_EXTRACTED_DIR, { recursive: true, force: true });
    const zip = new AdmZip(ICEBERG_SAMPLE_ARCHIVE_PATH);
    zip.extractAllTo(ICEBERG_SAMPLE_EXTRACTED_DIR, true);

    return baseFixtureDir;
  } catch (error) {
    throw new LocalizedError("iceberg_seed_failed", {
      message: localizeError(locale, error),
    });
  }
}

async function summarizeSeededIcebergTable(tableDir, fixture, targetPrefix) {
  const metadataPath = path.join(tableDir, "metadata", "v2.metadata.json");
  const metadata = JSON.parse(await readFile(metadataPath, "utf-8"));
  const currentSnapshotId = metadata["current-snapshot-id"] ?? null;
  const rowCount = await countSeedRowsWithDuckDb(path.join(tableDir, "data", "*.parquet"));
  const fileCount = (await listLocalFiles(tableDir)).length;

  return {
    name: fixture.name,
    prefix: `${targetPrefix}${fixture.name}/`,
    currentSnapshotId: currentSnapshotId === null ? null : String(currentSnapshotId),
    format: fixture.format,
    notes: fixture.notes,
    fileCount,
    rowCount,
  };
}

function buildIcebergSamplePrefix(prefix) {
  const normalizedPrefix = typeof prefix === "string" ? prefix.trim() : "";

  if (!normalizedPrefix || /^\/+$/.test(normalizedPrefix)) {
    throw new LocalizedError("iceberg_seed_prefix_invalid");
  }

  return ensureTrailingSlash(normalizedPrefix);
}

async function countSeedRowsWithDuckDb(parquetGlobPath) {
  const instance = await duckdb.DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  const normalizedGlob = parquetGlobPath.replace(/\\/g, "/").replace(/'/g, "''");

  try {
    const reader = await connection.runAndReadAll(
      `SELECT COUNT(*)::BIGINT AS row_count FROM read_parquet('${normalizedGlob}')`,
    );
    const rows = reader.getRowObjectsJson();
    const rowCountValue = rows[0]?.row_count ?? 0;
    return Number.parseInt(String(rowCountValue), 10) || 0;
  } finally {
    connection.closeSync?.();
  }
}

async function uploadDirectoryToStorage(session, localRootDir, remotePrefix) {
  const files = await listLocalFiles(localRootDir);

  for (const filePath of files) {
    const relativePath = path.relative(localRootDir, filePath).split(path.sep).join("/");
    const key = `${remotePrefix}${relativePath}`;
    const buffer = await readFile(filePath);
    await putStorageObject(session, key, buffer, getContentTypeForKey(key));
  }
}

async function listLocalFiles(rootDir) {
  const entries = await readdir(rootDir, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    if (entry.name.startsWith(".")) {
      continue;
    }

    const fullPath = path.join(rootDir, entry.name);

    if (entry.isDirectory()) {
      files.push(...(await listLocalFiles(fullPath)));
      continue;
    }

    if (entry.isFile()) {
      files.push(fullPath);
    }
  }

  return files;
}

async function putStorageObject(session, key, buffer, contentType = "application/octet-stream") {
  if (session.storage.provider === "adls") {
    await ensureAdlsParentDirectories(session.storage.fileSystemClient, key);
    const fileClient = session.storage.fileSystemClient.getFileClient(key);
    await fileClient.upload(buffer, {
      pathHttpHeaders: {
        contentType,
      },
    });
    return;
  }

  if (session.storage.provider === "gcs") {
    await session.storage.bucket.file(key).save(buffer, {
      contentType,
      resumable: false,
    });
    return;
  }

  await session.storage.client.send(
    new PutObjectCommand({
      Bucket: session.storage.bucket,
      Key: key,
      Body: buffer,
      ContentType: contentType,
    }),
  );
}

async function ensurePrefixMarkerObject(session, prefix) {
  if (!prefix || session.storage.provider === "adls") {
    return;
  }

  await putStorageObject(session, prefix, Buffer.alloc(0), "application/x-directory");
}

async function ensureAdlsParentDirectories(fileSystemClient, key) {
  const parentSegments = key.split("/").slice(0, -1);
  let currentPath = "";

  for (const segment of parentSegments) {
    if (!segment) {
      continue;
    }

    currentPath = currentPath ? `${currentPath}/${segment}` : segment;
    await fileSystemClient.getDirectoryClient(currentPath).createIfNotExists();
  }
}

function getContentTypeForKey(key) {
  const extension = path.extname(key).toLowerCase();
  const contentTypes = {
    ".avro": "application/avro",
    ".json": "application/json; charset=utf-8",
    ".md": "text/markdown; charset=utf-8",
    ".parquet": "application/octet-stream",
    ".text": "text/plain; charset=utf-8",
    ".txt": "text/plain; charset=utf-8",
  };

  return contentTypes[extension] ?? "application/octet-stream";
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

async function inspectIcebergTable(session, prefix, locale = "en", requestedSnapshotId = null) {
  try {
    const metadataPrefix = `${prefix}metadata/`;
    const metadataPaths = await listStoragePaths(session, metadataPrefix, true);
    const metadataFiles = metadataPaths
      .map((item) => toStoragePathItem(item))
      .filter((item) => item.type === "file" && item.key.toLowerCase().endsWith(".metadata.json"));

    if (!metadataFiles.length) {
      return null;
    }

    const metadataFile = pickLatestIcebergMetadataFile(metadataFiles);
    const metadata = JSON.parse(await readObjectText(session, metadataFile.key));
    const currentSnapshotId = metadata["current-snapshot-id"] ?? null;
    const snapshotId = requestedSnapshotId ?? currentSnapshotId;

    if (snapshotId === null || snapshotId === undefined) {
      throw new LocalizedError("iceberg_no_snapshot");
    }

    const snapshots = extractIcebergSnapshots(metadata, currentSnapshotId);
    const currentSnapshot = (metadata.snapshots ?? []).find((snapshot) => String(snapshot["snapshot-id"]) === String(snapshotId));

    if (!currentSnapshot) {
      throw new LocalizedError("iceberg_no_snapshot");
    }

    const deleteFileCount = Number(currentSnapshot?.summary?.["total-delete-files"] ?? 0);
    const manifestListPath = resolveIcebergStorageKey(prefix, currentSnapshot["manifest-list"], "metadata");
    const manifestListRecords = await loadAvroRecords(session, manifestListPath, null, "normal", locale);
    const dataManifestPaths = manifestListRecords
      .filter((record) => Number(record?.content ?? 0) === 0)
      .map((record) => resolveIcebergStorageKey(prefix, record?.manifest_path, "metadata"))
      .filter(Boolean);

    const dataFiles = [];

    for (const manifestPath of dataManifestPaths) {
      const manifestEntries = await loadAvroRecords(session, manifestPath, null, "normal", locale);

      for (const entry of manifestEntries) {
        if (Number(entry?.status ?? 0) === 2) {
          continue;
        }

        const dataFile = entry?.data_file;

        if (!dataFile || Number(dataFile.content ?? 0) !== 0) {
          continue;
        }

        dataFiles.push({
          key: resolveIcebergStorageKey(prefix, dataFile.file_path, "data"),
          format: String(dataFile.file_format ?? "").toLowerCase(),
        });
      }
    }

    const schemaColumns = extractIcebergSchemaColumns(metadata);
    const dataFormat = summarizeIcebergDataFormats(dataFiles);

    return {
      tablePrefix: prefix,
      metadataKey: metadataFile.key,
      metadata,
      currentSnapshotId: currentSnapshotId === null || currentSnapshotId === undefined ? null : String(currentSnapshotId),
      snapshotId: String(snapshotId),
      snapshots,
      schemaColumns,
      dataFiles,
      dataFormat,
      deleteFileCount,
    };
  } catch (error) {
    if (error instanceof LocalizedError) {
      throw error;
    }

    if (isStoragePathMissingError(error)) {
      return null;
    }

    throw new LocalizedError("iceberg_preview_failed", {
      message: localizeError(locale, error),
    });
  }
}

function pickLatestIcebergMetadataFile(metadataFiles) {
  return [...metadataFiles].sort((left, right) => {
    const leftVersion = extractIcebergMetadataVersion(left.key);
    const rightVersion = extractIcebergMetadataVersion(right.key);

    if (leftVersion !== rightVersion) {
      return rightVersion - leftVersion;
    }

    return new Date(right.lastModified ?? 0).getTime() - new Date(left.lastModified ?? 0).getTime();
  })[0];
}

function extractIcebergMetadataVersion(key) {
  const match = path.basename(key).match(/(?:^v|^)(\d+)\.metadata\.json$/i);
  return match ? Number.parseInt(match[1], 10) : -1;
}

function extractIcebergSchemaColumns(metadata) {
  const currentSchemaId = metadata["current-schema-id"];
  const schema = (metadata.schemas ?? []).find((item) => Number(item["schema-id"]) === Number(currentSchemaId))
    ?? metadata.schema
    ?? null;

  return Array.isArray(schema?.fields)
    ? schema.fields.map((field) => String(field?.name ?? "")).filter(Boolean)
    : [];
}

function extractIcebergSnapshots(metadata, currentSnapshotId) {
  return [...(metadata.snapshots ?? [])]
    .map((snapshot) => ({
      snapshotId: String(snapshot?.["snapshot-id"] ?? ""),
      committedAt: Number(snapshot?.["timestamp-ms"] ?? 0) || null,
      operation: String(snapshot?.summary?.operation ?? snapshot?.operation ?? ""),
      isCurrent: String(snapshot?.["snapshot-id"] ?? "") === String(currentSnapshotId ?? ""),
    }))
    .filter((snapshot) => snapshot.snapshotId)
    .sort((left, right) => {
      if ((right.committedAt ?? 0) !== (left.committedAt ?? 0)) {
        return (right.committedAt ?? 0) - (left.committedAt ?? 0);
      }

      return right.snapshotId.localeCompare(left.snapshotId, "en", { numeric: true });
    });
}

function summarizeIcebergDataFormats(dataFiles) {
  const formats = [...new Set(dataFiles.map((file) => file.format).filter(Boolean))];

  if (!formats.length) {
    return "unknown";
  }

  return formats.join(", ");
}

async function loadIcebergPreviewData(session, table, limit, order, locale = "en") {
  if (table.deleteFileCount > 0) {
    throw new LocalizedError("iceberg_delete_files_unsupported");
  }

  try {
    const effectiveFiles = order === "reverse" ? [...table.dataFiles].reverse() : table.dataFiles;
    const records = [];

    for (const file of effectiveFiles) {
      const remaining = limit === null ? null : Math.max(limit - records.length, 0);

      if (remaining === 0) {
        break;
      }

      const fileRecords = await loadIcebergDataFileRecords(session, file.key, file.format, remaining, order, locale);
      records.push(...fileRecords);
    }

    const normalized = normalizePreviewRecords(records);

    return {
      rows: normalized.rows,
      metadataColumns: table.schemaColumns.length ? table.schemaColumns : normalized.columns,
    };
  } catch (error) {
    if (error instanceof LocalizedError) {
      throw error;
    }

    throw new LocalizedError("iceberg_preview_failed", {
      message: localizeError(locale, error),
    });
  }
}

async function loadIcebergDataFileRecords(session, key, format, limit, order, locale = "en") {
  if (format === "parquet") {
    return loadParquetPreviewRecords(session, key, limit, order, "none", locale);
  }

  if (format === "avro") {
    return loadAvroRecords(session, key, limit, order, locale);
  }

  if (format === "orc") {
    return loadOrcPreviewRecords(session, key, limit, order, "none", locale);
  }

  throw new LocalizedError("iceberg_data_format_unsupported", {
    format: format || "unknown",
  });
}

function resolveIcebergStorageKey(tablePrefix, sourcePath, segmentHint = "") {
  if (typeof sourcePath !== "string" || !sourcePath.trim()) {
    return "";
  }

  const trimmedPath = sourcePath.trim().replace(/\\/g, "/");

  if (trimmedPath.startsWith(tablePrefix)) {
    return trimmedPath;
  }

  const uriMatch = trimmedPath.match(/^[a-z]+:\/\/[^/]+\/(.+)$/i);

  if (uriMatch?.[1]) {
    return uriMatch[1];
  }

  if (trimmedPath.startsWith("./")) {
    const relativePath = trimmedPath.slice(2);
    const relativeSegments = relativePath.split("/").filter(Boolean);
    if (relativeSegments.length > 1) {
      return `${tablePrefix}${relativeSegments.slice(1).join("/")}`;
    }
  }

  const marker = segmentHint ? `/${segmentHint}/` : null;
  const markerIndex = marker ? trimmedPath.lastIndexOf(marker) : -1;

  if (markerIndex >= 0) {
    return `${tablePrefix}${trimmedPath.slice(markerIndex + 1)}`;
  }

  return `${tablePrefix}${path.basename(trimmedPath)}`;
}

function normalizeAdlsDirectory(prefix) {
  return prefix.replace(/\/+$/, "");
}

function resolveAdlsListPath(prefix, itemName) {
  if (typeof itemName !== "string") {
    return "";
  }

  const normalizedItemName = itemName.replace(/^\/+/, "");
  const normalizedPrefix = normalizeAdlsDirectory(prefix).replace(/^\/+/, "");

  if (!normalizedPrefix || !normalizedItemName || normalizedItemName === normalizedPrefix) {
    return normalizedItemName;
  }

  if (normalizedItemName.startsWith(`${normalizedPrefix}/`)) {
    return normalizedItemName;
  }

  return `${normalizedPrefix}/${normalizedItemName}`;
}

function getPathDepth(value) {
  if (typeof value !== "string") {
    return 0;
  }

  return value.split("/").filter(Boolean).length;
}

function isStoragePathMissingError(error) {
  if (!error || typeof error !== "object") {
    return false;
  }

  const code = typeof error.code === "string" ? error.code : "";
  const statusCode = typeof error.statusCode === "number" ? error.statusCode : null;
  const details =
    error.details && typeof error.details === "object" && typeof error.details.errorCode === "string"
      ? error.details.errorCode
      : "";

  return (
    code === "PathNotFound" ||
    code === "ResourceNotFound" ||
    details === "PathNotFound" ||
    details === "ResourceNotFound" ||
    statusCode === 404
  );
}

function ensureTrailingSlash(value) {
  return value.endsWith("/") ? value : `${value}/`;
}

async function readObjectText(session, key, compression = "none") {
  const buffer = await readObjectBuffer(session, key, compression);
  const text = buffer.toString("utf-8");

  if (!text) {
    throw new LocalizedError("file_empty");
  }

  return text;
}

async function readObjectBuffer(session, key, compression = "none") {
  const buffer = await readObjectBufferRaw(session, key);

  if (!buffer.length) {
    throw new LocalizedError("file_empty");
  }

  return decompressBuffer(buffer, compression);
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
    throw new LocalizedError("session_not_provided");
  }

  const session = sessions.get(sessionId);

  if (!session) {
    throw new LocalizedError("session_invalid");
  }

  if (Date.now() - session.createdAt > SESSION_TTL_MS) {
    sessions.delete(sessionId);
    throw new LocalizedError("session_expired");
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

function ensureDestructiveOperationsEnabled() {
  if (!DESTRUCTIVE_OPERATIONS_ENABLED) {
    throw new LocalizedError("destructive_operations_disabled");
  }
}

function isTruthyEnv(value) {
  if (typeof value !== "string") {
    return false;
  }

  return ["1", "true", "yes", "on"].includes(value.trim().toLowerCase());
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
        reject(new LocalizedError("invalid_json_body"));
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
      throw new LocalizedError("fill_adls");
    }
    return;
  }

  if (connection.provider === "gcs") {
    if (!connection.bucket || !connection.serviceAccountJson) {
      throw new LocalizedError("fill_gcs");
    }

    try {
      JSON.parse(connection.serviceAccountJson);
    } catch {
      throw new LocalizedError("invalid_service_account_json");
    }

    return;
  }

  if (connection.provider === "minio") {
    if (!connection.endpoint || !connection.bucket || !connection.accessKeyId || !connection.secretAccessKey) {
      throw new LocalizedError("fill_minio");
    }

    return;
  }

  if (!connection.region || !connection.bucket || !connection.accessKeyId || !connection.secretAccessKey) {
    throw new LocalizedError("fill_s3");
  }
}

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
  });
  response.end(JSON.stringify(payload));
}

function getErrorMessage(error, locale = "en") {
  if (typeof error === "string" && error) {
    return error;
  }

  if (error instanceof LocalizedError) {
    return translateServer(locale, error.translationKey, error.translationVariables);
  }

  if (error && typeof error === "object") {
    const message = Reflect.get(error, "message");
    if (typeof message === "string" && message) {
      const code = typeof Reflect.get(error, "code") === "string" ? Reflect.get(error, "code") : "";
      const statusCode = typeof Reflect.get(error, "statusCode") === "number" ? Reflect.get(error, "statusCode") : null;
      const parts = [message.trim()];

      if (code) {
        parts.push(`code=${code}`);
      }

      if (statusCode !== null) {
        parts.push(`status=${statusCode}`);
      }

      return parts.join(" ");
    }
  }

  return translateServer(locale, "internal_server_error");
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
  const compression = getCompressionKind(key);

  if (compression !== "none") {
    const csvText = await readObjectText(session, key, compression);

    return parseCsv(csvText, limit, {
      fieldDelimiter: formatOptions.fieldDelimiter,
      quoteChar: formatOptions.quoteChar,
      order,
    });
  }

  if (order === "reverse" && limit !== null) {
    const tailText = await loadCsvTailText(session, key, limit, formatOptions.recordDelimiter);

    if (!tailText) {
      throw new LocalizedError("file_empty");
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
    throw new LocalizedError("file_empty");
  }

  return parseCsv(csvText, limit, {
    fieldDelimiter: formatOptions.fieldDelimiter,
    quoteChar: formatOptions.quoteChar,
    order,
  });
}

async function loadPreviewData(session, key, limit, order, mode, locale) {
  const previewTarget = analyzePreviewTarget(key);
  const extension = previewTarget.extension;
  const compression = previewTarget.compression;

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
      previewFormat: formatPreviewExtension("csv", compression),
      previewMode: "table",
      rawText: null,
      lineCount: countPreviewLines(rows),
    };
  }

  if (extension === ".json" || extension === ".jsonl" || extension === ".ndjson" || extension === ".dfm") {
    if (mode === "raw") {
      const rawText = await loadJsonRawPreview(session, key, limit, order, compression, extension);
      return {
        rows: [],
        metadataColumns: [],
        dfmKey: null,
        previewFormat: formatPreviewExtension(extension.slice(1), compression),
        previewMode: "raw",
        rawText,
        lineCount: countRawPreviewLines(rawText),
      };
    }

    if (extension === ".dfm") {
      const rawText = await loadJsonRawPreview(session, key, limit, order, compression, extension);
      return {
        rows: [],
        metadataColumns: [],
        dfmKey: null,
        previewFormat: formatPreviewExtension("dfm", compression),
        previewMode: "raw",
        rawText,
        lineCount: countRawPreviewLines(rawText),
      };
    }

    const records = await loadJsonPreviewRecords(session, key, limit, order, compression, extension);
    const normalized = normalizePreviewRecords(records);
    return {
      rows: normalized.rows,
      metadataColumns: normalized.columns,
      dfmKey: null,
      previewFormat: formatPreviewExtension(extension.slice(1), compression),
      previewMode: "table",
      rawText: null,
      lineCount: normalized.rows.length,
    };
  }

  if (extension === ".md" || extension === ".txt") {
    const rawText = await loadTextRawPreview(session, key, limit, order, compression);
    return {
      rows: [],
      metadataColumns: [],
      dfmKey: null,
      previewFormat: formatPreviewExtension(extension.slice(1), compression),
      previewMode: "raw",
      rawText,
      lineCount: countRawPreviewLines(rawText),
    };
  }

  if (extension === ".parquet" || extension === ".parq") {
    const records = await loadParquetPreviewRecords(session, key, limit, order, compression, locale);
    const normalized = normalizePreviewRecords(records);
    return {
      rows: normalized.rows,
      metadataColumns: normalized.columns,
      dfmKey: null,
      previewFormat: formatPreviewExtension("parquet", compression),
      previewMode: "table",
      rawText: null,
      lineCount: normalized.rows.length,
    };
  }

  if (extension === ".avro") {
    const preview = await loadAvroPreviewData(session, key, limit, order, compression, locale);
    return {
      rows: preview.rows,
      metadataColumns: preview.columns,
      dfmKey: null,
      previewFormat: formatPreviewExtension("avro", compression),
      previewMode: "table",
      rawText: null,
      lineCount: preview.rows.length,
    };
  }

  if (extension === ".orc") {
    const preview = await loadOrcPreviewData(session, key, limit, order, compression, locale);
    return {
      rows: preview.rows,
      metadataColumns: preview.columns,
      dfmKey: null,
      previewFormat: formatPreviewExtension("orc", compression),
      previewMode: "table",
      rawText: null,
      lineCount: preview.rows.length,
      order: preview.order,
    };
  }

  throw new LocalizedError("unsupported_preview_format");
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
  const normalizedKey = stripCompressionSuffix(csvKey);
  const extension = path.extname(normalizedKey);

  if (!extension) {
    return [];
  }

  return [".dfm", ".DFM"].map((dfmExtension) => `${normalizedKey.slice(0, -extension.length)}${dfmExtension}`);
}

async function findMatchingDfmMetadata(session, csvKey) {
  const normalizedKey = stripCompressionSuffix(csvKey);
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

async function loadJsonPreviewRecords(session, key, limit, order, compression = "none", extension = ".json") {
  const isLineDelimitedJson = extension === ".jsonl" || extension === ".ndjson";

  if (compression === "gzip" && limit !== null) {
    const records = await tryLoadGzipJsonRecords(session, key, limit, order);

    if (records) {
      return records;
    }
  }

  if (compression === "none" && isLineDelimitedJson && order === "normal" && limit !== null) {
    const lines = await loadDelimitedHeadRecords(session, key, limit);
    return lines.map((line) => JSON.parse(line));
  }

  if (compression === "none" && isLineDelimitedJson && order === "reverse" && limit !== null) {
    const lines = await loadDelimitedTailRecords(session, key, limit, "\n");
    return lines.map((line) => JSON.parse(line)).reverse();
  }

  const text = await readObjectText(session, key, compression);
  const trimmed = text.trim();
  let records = [];

  if (!trimmed) {
    throw new LocalizedError("file_empty");
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

async function loadJsonRawPreview(session, key, limit, order, compression = "none", extension = ".json") {
  const isLineDelimitedJson = extension === ".jsonl" || extension === ".ndjson";

  if (limit !== null && isLineDelimitedJson) {
    if (compression === "gzip") {
      const lines = await tryLoadGzipDelimitedLines(session, key, limit, order);

      if (lines) {
        return formatRawJsonLines(order === "reverse" ? [...lines].reverse() : lines);
      }
    } else if (compression === "none" && order === "normal") {
      return formatRawJsonLines(await loadDelimitedHeadRecords(session, key, limit));
    } else if (compression === "none") {
      return formatRawJsonLines((await loadDelimitedTailRecords(session, key, limit, "\n")).reverse());
    }
  }

  const text = await readObjectText(session, key, compression);
  const trimmed = text.trim();

  if (!trimmed) {
    throw new LocalizedError("file_empty");
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

async function loadTextRawPreview(session, key, limit, order, compression = "none") {
  const text = await readObjectText(session, key, compression);

  if (!text.trim()) {
    throw new LocalizedError("file_empty");
  }

  if (limit === null) {
    return text;
  }

  const lines = text.split(/\r?\n/);
  const sliced = order === "reverse" ? lines.slice(-limit) : lines.slice(0, limit);
  return sliced.join("\n");
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
    throw new LocalizedError("file_empty");
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

async function loadParquetPreviewRecords(session, key, limit, order, compression = "none", locale = "en") {
  try {
    const fileBuffer = await readObjectBuffer(session, key, compression);
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
    throw new LocalizedError("parquet_preview_failed", {
      message: localizeError(locale, error),
    });
  }
}

async function loadAvroPreviewData(session, key, limit, order, compression = "none", locale = "en") {
  try {
    const preview = await loadAvroRecordsWithColumns(session, key, limit, order, compression, locale);
    const normalized = normalizePreviewRecords(preview.records);

    return {
      rows: normalized.rows,
      columns: preview.columns.length ? preview.columns : normalized.columns,
    };
  } catch (error) {
    throw new LocalizedError("avro_preview_failed", {
      message: localizeError(locale, error),
    });
  }
}

async function loadAvroRecords(session, key, limit, order, locale = "en") {
  const preview = await loadAvroRecordsWithColumns(session, key, limit, order, "none", locale);
  return preview.records;
}

async function loadAvroRecordsWithColumns(session, key, limit, order, compression = "none", locale = "en") {
  try {
    const fileBuffer = await readObjectBuffer(session, key, compression);
    return await collectAvroPreview(fileBuffer, limit, order);
  } catch (error) {
    throw new LocalizedError("avro_preview_failed", {
      message: localizeError(locale, error),
    });
  }
}

function collectAvroPreview(fileBuffer, limit, order) {
  return new Promise((resolve, reject) => {
    const records = [];
    let columns = [];
    let readerType = null;
    const decoder = new avro.streams.BlockDecoder({
      noDecode: true,
      codecs: createAvroCodecs(),
    });

    decoder.on("metadata", (_type, _codec, header) => {
      const schemaBuffer = header?.meta?.["avro.schema"];
      const schema = Buffer.isBuffer(schemaBuffer) ? JSON.parse(schemaBuffer.toString("utf-8")) : schemaBuffer;
      readerType = createAvroType(schema);
      columns = extractAvroColumns(readerType);
    });

    decoder.on("data", (recordBuffer) => {
      const record = readerType ? readerType.fromBuffer(recordBuffer) : recordBuffer;

      if (order === "reverse" && limit !== null) {
        if (records.length === limit) {
          records.shift();
        }
        records.push(record);
        return;
      }

      records.push(record);
    });

    decoder.on("end", () => {
      resolve({
        records: order === "reverse" ? records.reverse() : limit === null ? records : records.slice(0, limit),
        columns,
      });
    });

    decoder.on("error", reject);
    decoder.end(fileBuffer);
  });
}

function createAvroCodecs() {
  return {
    ...avro.streams.BlockDecoder.getDefaultCodecs(),
    snappy(buffer, callback) {
      try {
        if (buffer.length < 4) {
          throw new Error("Invalid Avro Snappy block.");
        }

        const compressed = buffer.subarray(0, buffer.length - 4);
        callback(null, Buffer.from(snappy.uncompress(compressed)));
      } catch (error) {
        callback(error);
      }
    },
  };
}

function extractAvroColumns(type) {
  if (typeof type?.getFields !== "function") {
    return [];
  }

  return type.getFields()
    .map((field) => field?.getName?.() ?? "")
    .filter((name) => typeof name === "string" && name.length);
}

async function loadOrcPreviewData(session, key, limit, order, compression = "none", locale = "en") {
  let tempDir;

  try {
    const jarPath = await ensureOrcToolsJar(locale);
    tempDir = await mkdtemp(path.join(tmpdir(), "multibucket-explorer-orc-"));
    const tempFilePath = path.join(tempDir, buildOrcTempFileName(key));
    const buffer = await readObjectBuffer(session, key, compression);

    await writeFile(tempFilePath, buffer);

    const columns = await loadOrcSchemaColumns(jarPath, tempFilePath, locale);
    const rows = await loadOrcRows(jarPath, tempFilePath, limit, order, locale);
    const normalized = normalizePreviewRecords(rows);

    return {
      rows: normalized.rows,
      columns: columns.length ? columns : normalized.columns,
      order,
    };
  } catch (error) {
    if (error instanceof LocalizedError) {
      throw error;
    }

    throw new LocalizedError("orc_preview_failed", {
      message: localizeError(locale, error),
    });
  } finally {
    if (tempDir) {
      await rm(tempDir, { recursive: true, force: true });
    }
  }
}

async function loadOrcPreviewRecords(session, key, limit, order, compression = "none", locale = "en") {
  let tempDir;

  try {
    const jarPath = await ensureOrcToolsJar(locale);
    tempDir = await mkdtemp(path.join(tmpdir(), "multibucket-explorer-orc-"));
    const tempFilePath = path.join(tempDir, buildOrcTempFileName(key));
    const buffer = await readObjectBuffer(session, key, compression);

    await writeFile(tempFilePath, buffer);
    return await loadOrcRows(jarPath, tempFilePath, limit, order, locale);
  } catch (error) {
    if (error instanceof LocalizedError) {
      throw error;
    }

    throw new LocalizedError("orc_preview_failed", {
      message: localizeError(locale, error),
    });
  } finally {
    if (tempDir) {
      await rm(tempDir, { recursive: true, force: true });
    }
  }
}

async function ensureOrcToolsJar(locale) {
  const jarPath = path.join(ORC_TOOLS_CACHE_DIR, ORC_TOOLS_FILE_NAME);

  try {
    await stat(jarPath);
    return jarPath;
  } catch {
    // Cache miss.
  }

  await assertJavaAvailable();

  try {
    await mkdir(ORC_TOOLS_CACHE_DIR, { recursive: true });
    await downloadFile(ORC_TOOLS_URL, jarPath);
    return jarPath;
  } catch (error) {
    throw new LocalizedError("orc_tools_download_failed", {
      message: localizeError(locale, error),
    });
  }
}

async function assertJavaAvailable() {
  try {
    await runProcess("java", ["-version"], {
      captureStdout: false,
      captureStderr: true,
    });
  } catch {
    throw new LocalizedError("orc_java_missing");
  }
}

async function loadOrcSchemaColumns(jarPath, filePath, locale) {
  const result = await runProcess("java", ["-jar", jarPath, "scan", "-s", filePath], {
    captureStderr: true,
  });

  const schemaText = extractJsonBlock(result.stdout);

  if (!schemaText) {
    throw new LocalizedError("orc_preview_failed", {
      message: localizeError(locale, new Error("ORC schema output was empty.")),
    });
  }

  let schema;

  try {
    schema = JSON.parse(schemaText);
  } catch (error) {
    throw new LocalizedError("orc_preview_failed", {
      message: localizeError(locale, error),
    });
  }

  if (schema?.category !== "struct" || !Array.isArray(schema.fields)) {
    return [];
  }

  return schema.fields
    .map((field) => Object.keys(field ?? {})[0] ?? "")
    .filter((name) => typeof name === "string" && name.length);
}

async function loadOrcRows(jarPath, filePath, limit, order, locale) {
  const args = ["-jar", jarPath, "data"];

  if (order !== "reverse" && limit !== null) {
    args.push("-n", String(limit));
  }

  args.push(filePath);

  try {
    return await collectOrcDataRows(args, limit, order);
  } catch (error) {
    throw new LocalizedError("orc_preview_failed", {
      message: localizeError(locale, error),
    });
  }
}

async function collectOrcDataRows(args, limit, order) {
  const child = spawn("java", args, {
    stdio: ["ignore", "pipe", "pipe"],
  });
  const reader = createInterface({ input: child.stdout });
  const stderrChunks = [];
  const rows = [];
  let exitCode = null;
  let spawnError = null;

  child.stderr.on("data", (chunk) => {
    stderrChunks.push(chunk.toString("utf-8"));
  });

  child.on("error", (error) => {
    spawnError = error;
  });

  child.on("close", (code) => {
    exitCode = code;
  });

  try {
    for await (const line of reader) {
      const trimmed = line.trim();

      if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
        continue;
      }

      const parsed = JSON.parse(trimmed);

      if (order === "reverse" && limit !== null) {
        if (rows.length === limit) {
          rows.shift();
        }
        rows.push(parsed);
        continue;
      }

      rows.push(parsed);
    }
  } finally {
    reader.close();
  }

  if (exitCode === null && !spawnError) {
    await new Promise((resolve) => child.once("close", resolve));
  }

  if (spawnError) {
    throw spawnError;
  }

  if (exitCode !== 0) {
    throw new Error(stderrChunks.join("").trim() || `java exited with code ${exitCode}`);
  }

  if (order === "reverse") {
    return rows.reverse();
  }

  return limit === null ? rows : rows.slice(0, limit);
}

function extractJsonBlock(text) {
  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");

  if (start < 0 || end < start) {
    return "";
  }

  return text.slice(start, end + 1);
}

function buildOrcTempFileName(key) {
  const baseName = path.basename(stripCompressionSuffix(key)) || "preview.orc";
  const sanitized = baseName.replace(/[^a-zA-Z0-9._-]+/g, "_");
  return sanitized.toLowerCase().endsWith(".orc") ? sanitized : `${sanitized}.orc`;
}

function downloadFile(url, destinationPath) {
  return new Promise((resolve, reject) => {
    const request = https.get(url, (response) => {
      if (
        response.statusCode &&
        response.statusCode >= 300 &&
        response.statusCode < 400 &&
        response.headers.location
      ) {
        response.resume();
        downloadFile(response.headers.location, destinationPath).then(resolve).catch(reject);
        return;
      }

      if (response.statusCode !== 200) {
        response.resume();
        reject(new Error(`HTTP ${response.statusCode ?? "unknown"} while downloading ${url}`));
        return;
      }

      const tempPath = `${destinationPath}.tmp`;
      const output = createWriteStream(tempPath);

      output.on("finish", () => {
        output.close(async (closeError) => {
          if (closeError) {
            reject(closeError);
            return;
          }

          try {
            await rename(tempPath, destinationPath);
            resolve();
          } catch (error) {
            reject(error);
          }
        });
      });

      output.on("error", async (error) => {
        response.destroy();
        await rm(tempPath, { force: true });
        reject(error);
      });

      response.on("error", async (error) => {
        await rm(tempPath, { force: true });
        reject(error);
      });

      response.pipe(output);
    });

    request.on("error", reject);
  });
}

function runProcess(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: [
        "ignore",
        options.captureStdout === false ? "ignore" : "pipe",
        options.captureStderr === false ? "ignore" : "pipe",
      ],
    });
    const stdoutChunks = [];
    const stderrChunks = [];

    child.on("error", reject);

    if (child.stdout) {
      child.stdout.on("data", (chunk) => stdoutChunks.push(chunk.toString("utf-8")));
    }

    if (child.stderr) {
      child.stderr.on("data", (chunk) => stderrChunks.push(chunk.toString("utf-8")));
    }

    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(stderrChunks.join("").trim() || `${command} exited with code ${code}`));
        return;
      }

      resolve({
        stdout: stdoutChunks.join(""),
        stderr: stderrChunks.join(""),
      });
    });
  });
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

function normalizeAvroLongValue(value) {
  if (typeof value === "bigint") {
    return value;
  }

  if (typeof value === "number") {
    if (!Number.isInteger(value)) {
      throw new Error("Avro long values must be integers.");
    }

    return BigInt(value);
  }

  if (typeof value === "string" && /^-?\d+$/.test(value.trim())) {
    return BigInt(value.trim());
  }

  throw new Error("Invalid Avro long value.");
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

function createAvroType(schema) {
  return avro.Type.forSchema(schema, {
    typeHook(currentSchema) {
      if (currentSchema === "long") {
        return AVRO_LONG_AS_STRING_TYPE;
      }

      if (
        currentSchema &&
        typeof currentSchema === "object" &&
        !Array.isArray(currentSchema) &&
        currentSchema.type === "long"
      ) {
        return AVRO_LONG_AS_STRING_TYPE;
      }

      return undefined;
    },
  });
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
  const normalizedKey = stripCompressionSuffix(key);
  const compression = getCompressionKind(key);

  return {
    extension: path.extname(normalizedKey).toLowerCase(),
    compression,
    metadataKey: normalizedKey,
  };
}

function stripCompressionSuffix(key) {
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

  if (normalizedKey.endsWith(".snappy.parquet")) {
    return key;
  }

  if (normalizedKey.endsWith(".snappy.parq")) {
    return key;
  }

  if (normalizedKey.endsWith(".gzip.avro")) {
    return key;
  }

  if (normalizedKey.endsWith(".gz.avro")) {
    return key;
  }

  if (normalizedKey.endsWith(".snappy.avro")) {
    return key;
  }

  if (normalizedKey.endsWith(".snappy.orc")) {
    return key;
  }

  const compression = getCompressionKind(key);

  if (compression === "gzip") {
    return key.slice(0, -3);
  }

  if (compression === "snappy") {
    return key.slice(0, -7);
  }

  return key;
}

function isGzipKey(key) {
  return key.toLowerCase().endsWith(".gz");
}

function isSnappyKey(key) {
  return key.toLowerCase().endsWith(".snappy");
}

function getCompressionKind(key) {
  if (isGzipKey(key)) {
    return "gzip";
  }

  if (isSnappyKey(key)) {
    return "snappy";
  }

  return "none";
}

function decompressBuffer(buffer, compression) {
  if (compression === "gzip") {
    return gunzipSync(buffer);
  }

  if (compression === "snappy") {
    return Buffer.from(snappy.uncompress(buffer));
  }

  return buffer;
}

function formatPreviewExtension(baseExtension, compression) {
  if (compression === "gzip") {
    return `${baseExtension}.gz`;
  }

  if (compression === "snappy") {
    return `${baseExtension}.snappy`;
  }

  return baseExtension;
}

function toArrayBuffer(buffer) {
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
}

async function loadDelimitedHeadRecords(session, key, limit) {
  const body = await getObjectReadableStream(session, key);

  if (!body) {
    throw new LocalizedError("file_empty");
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
    throw new LocalizedError("file_empty");
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
    throw new LocalizedError("file_empty");
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
