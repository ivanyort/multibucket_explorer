const state = {
  bucket: "",
  region: "",
  prefix: "",
  selectedKey: "",
  sessionId: "",
  objectItems: [],
  sort: {
    column: "name",
    direction: "asc",
  },
};

const STORAGE_KEY = "s3-browser-connection";

const elements = {
  connectionPanel: document.querySelector("#connectionPanel"),
  connectionSummaryText: document.querySelector("#connectionSummaryText"),
  credentialsForm: document.querySelector("#credentialsForm"),
  connectButton: document.querySelector("#connectButton"),
  connectionStatus: document.querySelector("#connectionStatus"),
  diagnosticBox: document.querySelector("#diagnosticBox"),
  objectList: document.querySelector("#objectList"),
  currentPrefix: document.querySelector("#currentPrefix"),
  refreshButton: document.querySelector("#refreshButton"),
  clearPrefixButton: document.querySelector("#clearPrefixButton"),
  upButton: document.querySelector("#upButton"),
  previewMeta: document.querySelector("#previewMeta"),
  previewTableWrap: document.querySelector("#previewTableWrap"),
  previewMode: document.querySelector("#previewMode"),
  previewRowLimit: document.querySelector("#previewRowLimit"),
  previewRowOrder: document.querySelector("#previewRowOrder"),
  downloadButton: document.querySelector("#downloadButton"),
};

elements.credentialsForm.addEventListener("submit", (event) => {
  event.preventDefault();
});
elements.credentialsForm.addEventListener("input", persistConnectionForm);
elements.connectButton.addEventListener("click", connectToBucket);
elements.refreshButton.addEventListener("click", () => loadObjects(state.prefix));
elements.clearPrefixButton.addEventListener("click", clearCurrentPrefix);
elements.upButton.addEventListener("click", goUpOneLevel);
elements.downloadButton.addEventListener("click", downloadSelectedObject);
elements.previewMode.addEventListener("change", () => {
  syncPreviewModeAvailability(state.selectedKey);
  if (state.selectedKey) {
    previewObject(state.selectedKey);
  }
});
elements.previewRowLimit.addEventListener("change", () => {
  if (state.selectedKey) {
    previewObject(state.selectedKey);
  }
});
elements.previewRowOrder.addEventListener("change", () => {
  if (state.selectedKey) {
    previewObject(state.selectedKey);
  }
});

restoreConnectionForm();
setStartupDiagnostic();
refreshConnectionSummary();
syncPreviewModeAvailability("");

async function connectToBucket() {
  const connection = getConnectionPayload();

  if (!connection.region || !connection.bucket || !connection.accessKeyId || !connection.secretAccessKey) {
    setConnectionStatus("Fill in region, bucket, and credentials.", true);
    return;
  }

  persistConnectionForm();
  state.bucket = connection.bucket;
  state.region = connection.region;
  state.prefix = "";
  state.selectedKey = "";
  state.sessionId = "";

  elements.refreshButton.disabled = true;
  elements.clearPrefixButton.disabled = true;
  elements.upButton.disabled = true;
  elements.downloadButton.disabled = true;
  syncPreviewModeAvailability("");
  renderObjectPlaceholder("Connecting...");
  resetPreview("Select a `.csv` file to preview.");
  setConnectionStatus(`Connecting to ${connection.bucket} (${connection.region})...`);
  setDiagnosticMessage("Calling the local backend to validate bucket access...");

  try {
    const response = await apiFetch("/api/connect", {
      method: "POST",
      body: JSON.stringify(connection),
    });

    state.sessionId = response.sessionId;
    elements.refreshButton.disabled = false;
    await loadObjects("");
    elements.connectionPanel.open = false;
    refreshConnectionSummary();
    setDiagnosticMessage(
      `Connection OK through the local backend.\nBucket: ${state.bucket}\nRegion: ${state.region}\nSession: ${state.sessionId}`,
    );
  } catch (error) {
    renderObjectPlaceholder("Failed to connect.");
    setConnectionStatus(getErrorMessage(error), true);
    setDiagnosticMessage(buildDiagnosticMessage(error));
  }
}

async function loadObjects(prefix) {
  if (!state.sessionId) {
    return;
  }

  state.prefix = prefix;
  elements.currentPrefix.textContent = prefix ? `/${prefix}` : "/";
  elements.upButton.disabled = !prefix;
  elements.clearPrefixButton.disabled = !prefix;
  renderObjectPlaceholder("Loading objects...");
  resetPreview("Select a compatible `.csv`, `.json`, `.dfm`, `.parquet`, or `.gz` file to preview.");

  try {
    const response = await apiFetch(
      `/api/objects?sessionId=${encodeURIComponent(state.sessionId)}&prefix=${encodeURIComponent(prefix)}`,
    );
    state.objectItems = response.items ?? [];
    renderObjectList();
    setConnectionStatus(
      `${response.summary?.folders ?? 0} folder(s) and ${response.summary?.files ?? 0} file(s) loaded from ${state.bucket}.`,
    );
  } catch (error) {
    renderObjectPlaceholder("Failed to list objects.");
    setConnectionStatus(getErrorMessage(error), true);
    setDiagnosticMessage(buildDiagnosticMessage(error));
  }
}

function renderObjectList() {
  if (!state.objectItems.length) {
    renderObjectPlaceholder("No items found in this prefix.");
    return;
  }

  const items = sortObjectItems(state.objectItems, state.sort);
  elements.objectList.className = "object-list";
  elements.objectList.innerHTML = "";
  const wrap = document.createElement("div");
  wrap.className = "object-grid-wrap";
  const table = document.createElement("table");
  table.className = "object-grid";
  const thead = document.createElement("thead");
  const tbody = document.createElement("tbody");
  const headerRow = document.createElement("tr");

  [
    { key: "name", label: "Name" },
    { key: "type", label: "Type" },
    { key: "size", label: "Size" },
    { key: "lastModified", label: "Date" },
  ].forEach((column) => {
    const th = document.createElement("th");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "sort-button";
    button.textContent = getSortLabel(column.label, column.key);
    button.addEventListener("click", () => toggleSort(column.key));
    th.appendChild(button);
    headerRow.appendChild(th);
  });

  thead.appendChild(headerRow);

  items.forEach((item) => {
    const row = document.createElement("tr");
    row.className = "object-grid-row";
    row.tabIndex = 0;

    const nameCell = document.createElement("td");
    const nameWrap = document.createElement("div");
    nameWrap.className = "object-name-cell";
    const icon = document.createElement("span");
    icon.className = item.type === "folder" ? "object-icon folder-icon" : "object-icon file-icon";
    icon.setAttribute("aria-hidden", "true");
    icon.innerHTML = item.type === "folder" ? folderIconSvg() : fileIconSvg();
    const nameText = document.createElement("strong");
    nameText.textContent = item.name || item.key;
    nameWrap.append(icon, nameText);
    nameCell.appendChild(nameWrap);

    const typeCell = document.createElement("td");
    typeCell.textContent = item.type === "folder" ? "Folder" : "File";

    const sizeCell = document.createElement("td");
    sizeCell.textContent = item.type === "folder" ? "—" : formatBytes(item.size);

    const dateCell = document.createElement("td");
    dateCell.textContent =
      item.type === "folder" || !item.lastModified
        ? "—"
        : new Date(item.lastModified).toLocaleString("en-US");

    row.append(nameCell, typeCell, sizeCell, dateCell);
    row.addEventListener("click", () => handleObjectSelection(item));
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        handleObjectSelection(item);
      }
    });
    tbody.appendChild(row);
  });

  table.append(thead, tbody);
  wrap.appendChild(table);
  elements.objectList.appendChild(wrap);
}

function handleObjectSelection(item) {
  if (item.type === "folder") {
    loadObjects(item.key);
    return;
  }

  previewObject(item.key);
}

async function previewObject(key) {
  state.selectedKey = key;
  elements.downloadButton.disabled = false;

  if (!isPreviewableFile(key)) {
    resetPreview(
      `Unsupported preview format: ${key}`,
      false,
      "Select a .csv, .json, .jsonl, .ndjson, .dfm, .parquet, .parq, or matching .gz file.",
    );
    return;
  }

  syncPreviewModeAvailability(key);
  elements.previewMeta.textContent = `Loading preview for ${key}...`;
  elements.previewTableWrap.className = "preview-table-wrap empty-state";
  elements.previewTableWrap.textContent = "Reading file...";

  try {
    const rowLimit = getPreviewRowLimit();
    const rowOrder = getPreviewRowOrder();
    const previewMode = getPreviewMode(key);
    const response = await apiFetch(
      `/api/preview?sessionId=${encodeURIComponent(state.sessionId)}&key=${encodeURIComponent(key)}&limit=${encodeURIComponent(rowLimit)}&order=${encodeURIComponent(rowOrder)}&mode=${encodeURIComponent(previewMode)}`,
    );

    if (response.previewMode === "raw") {
      renderPreviewRaw(response.rawText ?? "", response.previewFormat ?? "");
    } else {
      const preview = buildPreviewModel(
        response.rows ?? [],
        response.metadataColumns ?? [],
        response.order ?? rowOrder,
      );
      renderPreviewTable(preview);
    }
    const dfmSuffix =
      response.previewFormat === "csv" || response.previewFormat === "csv.gz"
        ? response.dfmKey
          ? ` · DFM: ${response.dfmKey} (${response.metadataColumns?.length ?? 0} columns)`
          : " · DFM not found"
        : "";
    const modeSuffix = response.previewMode === "raw" ? " · raw mode" : "";
    const formatSuffix = response.previewFormat ? ` · format ${response.previewFormat}` : "";
    const orderLabel = getPreviewRowOrder() === "reverse" ? " · reverse order" : "";
    elements.previewMeta.textContent =
      `${key} · showing ${response.lineCount ?? 0} sample row(s)${orderLabel}${modeSuffix}${formatSuffix}${dfmSuffix}`;
  } catch (error) {
    resetPreview(getErrorMessage(error), true, "Failed to load preview.");
  }
}

function renderPreviewTable(preview) {
  if (!preview.headerRow.length) {
    resetPreview("Empty file.", false, "The file has no rows to display.");
    return;
  }

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const tbody = document.createElement("tbody");
  const headerTr = document.createElement("tr");

  preview.headerRow.forEach((column, index) => {
    const th = document.createElement("th");
    th.textContent = column || `column_${index + 1}`;
    headerTr.appendChild(th);
  });

  thead.appendChild(headerTr);

  preview.bodyRows.forEach((row) => {
    const tr = document.createElement("tr");

    preview.headerRow.forEach((_, index) => {
      const td = document.createElement("td");
      td.textContent = row[index] ?? "";
      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });

  table.append(thead, tbody);
  elements.previewTableWrap.className = "preview-table-wrap";
  elements.previewTableWrap.innerHTML = "";
  elements.previewTableWrap.appendChild(table);
}

function renderPreviewRaw(rawText, previewFormat = "") {
  elements.previewTableWrap.className = "preview-table-wrap";
  elements.previewTableWrap.innerHTML = "";

  if (shouldRenderJsonTree(previewFormat, rawText)) {
    try {
      const parsed = parseRawJsonPreview(rawText, previewFormat);
      const tree = document.createElement("div");
      tree.className = "json-tree";
      tree.appendChild(renderJsonTreeNode(parsed, 0));
      elements.previewTableWrap.appendChild(tree);
      return;
    } catch {
      // Fall back to raw text when the payload is not a single JSON document.
    }
  }

  const pre = document.createElement("pre");
  pre.className = "preview-raw";
  pre.textContent = rawText || "";
  elements.previewTableWrap.appendChild(pre);
}

function shouldRenderJsonTree(previewFormat, rawText) {
  if (!rawText) {
    return false;
  }

  return ["dfm", "dfm.gz"].includes(previewFormat);
}

function parseRawJsonPreview(rawText, previewFormat) {
  if (["jsonl", "jsonl.gz", "ndjson", "ndjson.gz"].includes(previewFormat)) {
    return parseJsonLines(rawText);
  }

  try {
    return JSON.parse(rawText);
  } catch {
    const lines = splitJsonLines(rawText);

    if (lines.length > 1) {
      return lines.map((line) => JSON.parse(line));
    }

    throw new Error("Raw preview is not valid JSON.");
  }
}

function parseJsonLines(rawText) {
  return splitJsonLines(rawText).map((line) => JSON.parse(line));
}

function splitJsonLines(rawText) {
  return rawText
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
}

function renderJsonTreeNode(value, depth, keyName = "") {
  if (Array.isArray(value)) {
    return renderJsonBranch({
      keyName,
      label: `Array(${value.length})`,
      openToken: "[",
      closeToken: "]",
      items: value.map((item, index) => ({ keyName: String(index), value: item })),
      depth,
    });
  }

  if (value && typeof value === "object") {
    const entries = Object.entries(value);
    return renderJsonBranch({
      keyName,
      label: `Object(${entries.length})`,
      openToken: "{",
      closeToken: "}",
      items: entries.map(([entryKey, entryValue]) => ({ keyName: entryKey, value: entryValue })),
      depth,
    });
  }

  return renderJsonLeaf(keyName, value);
}

function renderJsonBranch({ keyName, label, openToken, closeToken, items, depth }) {
  const branch = document.createElement("div");
  branch.className = "json-branch";

  if (!items.length) {
    const empty = document.createElement("div");
    empty.className = "json-leaf";
    empty.append(renderJsonKey(keyName), renderJsonValue(`${openToken}${closeToken}`, "json-empty"));
    branch.appendChild(empty);
    return branch;
  }

  const details = document.createElement("details");
  details.className = "json-details";
  details.open = depth < 2;

  const summary = document.createElement("summary");
  summary.className = "json-summary";
  summary.append(renderJsonKey(keyName), renderJsonValue(`${openToken} ${label}`, "json-meta"));

  const children = document.createElement("div");
  children.className = "json-children";

  items.forEach((item) => {
    children.appendChild(renderJsonTreeNode(item.value, depth + 1, item.keyName));
  });

  const closing = document.createElement("div");
  closing.className = "json-closing";
  closing.textContent = closeToken;

  details.append(summary, children, closing);
  branch.appendChild(details);
  return branch;
}

function renderJsonLeaf(keyName, value) {
  const leaf = document.createElement("div");
  leaf.className = "json-leaf";
  leaf.append(renderJsonKey(keyName), renderJsonPrimitive(value));
  return leaf;
}

function renderJsonKey(keyName) {
  const key = document.createElement("span");
  key.className = "json-key";
  key.textContent = keyName ? `"${keyName}": ` : "";
  return key;
}

function renderJsonPrimitive(value) {
  if (value === null) {
    return renderJsonValue("null", "json-null");
  }

  switch (typeof value) {
    case "string":
      return renderJsonValue(JSON.stringify(value), "json-string");
    case "number":
      return renderJsonValue(String(value), "json-number");
    case "boolean":
      return renderJsonValue(String(value), "json-boolean");
    default:
      return renderJsonValue(String(value), "json-meta");
  }
}

function renderJsonValue(text, className) {
  const value = document.createElement("span");
  value.className = className;
  value.textContent = text;
  return value;
}

function buildPreviewModel(rows, metadataColumns = [], rowOrder = "normal") {
  if (!rows.length) {
    return {
      headerRow: [],
      bodyRows: [],
    };
  }

  const maxColumns = Math.max(
    rows.reduce((currentMax, row) => Math.max(currentMax, row.length), 0),
    metadataColumns.length,
  );

  if (metadataColumns.length) {
    return {
      headerRow: normalizeHeaderRow(metadataColumns, maxColumns),
      bodyRows: rows.map((row) => normalizeRowLength(row, maxColumns)),
    };
  }

  if (rows.length === 1) {
    return {
      headerRow: createGeneratedHeader(maxColumns),
      bodyRows: rows,
    };
  }

  const [firstRow, ...otherRows] = rows;
  const looksLikeHeader = firstRow.some((value) => /[a-zA-Z]/.test(value)) && otherRows.length > 0;

  if (looksLikeHeader) {
    return {
      headerRow: normalizeRowLength(firstRow, maxColumns).map((value, index) => value || `column_${index + 1}`),
      bodyRows: otherRows.map((row) => normalizeRowLength(row, maxColumns)),
    };
  }

  return {
    headerRow: createGeneratedHeader(maxColumns),
    bodyRows: rows.map((row) => normalizeRowLength(row, maxColumns)),
  };
}

function createGeneratedHeader(columnCount) {
  return Array.from({ length: columnCount }, (_, index) => `column_${index + 1}`);
}

function normalizeHeaderRow(headerRow, columnCount) {
  return Array.from({ length: columnCount }, (_, index) => headerRow[index] || `column_${index + 1}`);
}

function normalizeRowLength(row, columnCount) {
  return Array.from({ length: columnCount }, (_, index) => row[index] ?? "");
}

function resetPreview(message, isError = false, bodyMessage) {
  elements.previewMeta.className = isError ? "preview-meta error-text" : "preview-meta muted";
  elements.previewMeta.textContent = message;
  elements.previewTableWrap.className = "preview-table-wrap empty-state";
  elements.previewTableWrap.textContent =
    bodyMessage ?? (isError ? "Failed to load preview." : "No file selected.");
}

function renderObjectPlaceholder(message) {
  elements.objectList.className = "object-list empty-state";
  elements.objectList.textContent = message;
}

function toggleSort(column) {
  if (state.sort.column === column) {
    state.sort.direction = state.sort.direction === "asc" ? "desc" : "asc";
  } else {
    state.sort.column = column;
    state.sort.direction = column === "lastModified" || column === "size" ? "desc" : "asc";
  }

  renderObjectList();
}

function getSortLabel(label, column) {
  if (state.sort.column !== column) {
    return label;
  }

  return `${label} ${state.sort.direction === "asc" ? "↑" : "↓"}`;
}

function sortObjectItems(items, sort) {
  return [...items].sort((left, right) => {
    const direction = sort.direction === "asc" ? 1 : -1;
    const leftFolderBias = left.type === "folder" ? -1 : 1;
    const rightFolderBias = right.type === "folder" ? -1 : 1;

    if (leftFolderBias !== rightFolderBias) {
      return leftFolderBias - rightFolderBias;
    }

    switch (sort.column) {
      case "type":
        return left.type.localeCompare(right.type) * direction || left.name.localeCompare(right.name);
      case "size":
        return ((left.size ?? 0) - (right.size ?? 0)) * direction || left.name.localeCompare(right.name);
      case "lastModified":
        return (
          (new Date(left.lastModified ?? 0).getTime() - new Date(right.lastModified ?? 0).getTime()) *
            direction ||
          left.name.localeCompare(right.name)
        );
      case "name":
      default:
        return left.name.localeCompare(right.name, "en", { numeric: true }) * direction;
    }
  });
}

function setConnectionStatus(message, isError = false) {
  elements.connectionStatus.className = isError ? "status-text error-text" : "status-text muted";
  elements.connectionStatus.textContent = message;
  refreshConnectionSummary();
}

function setDiagnosticMessage(message) {
  elements.diagnosticBox.textContent = message;
}

function goUpOneLevel() {
  if (!state.prefix) {
    return;
  }

  const trimmed = state.prefix.endsWith("/") ? state.prefix.slice(0, -1) : state.prefix;
  const parts = trimmed.split("/").filter(Boolean);
  parts.pop();
  const nextPrefix = parts.length ? `${parts.join("/")}/` : "";
  loadObjects(nextPrefix);
}

function formatBytes(bytes) {
  if (!bytes) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB", "TB"];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const value = bytes / 1024 ** index;
  return `${value.toFixed(value >= 10 || index === 0 ? 0 : 1)} ${units[index]}`;
}

function getErrorMessage(error) {
  if (error instanceof Error && error.message) {
    return error.message;
  }

  return "Unexpected error while accessing the bucket.";
}

function buildDiagnosticMessage(error) {
  if (error instanceof Error) {
    return `Error returned by the local backend:\n${error.message}`;
  }

  return "Failure without additional details.";
}

async function clearCurrentPrefix() {
  if (!state.sessionId || !state.prefix) {
    return;
  }

  const confirmation = window.prompt(
    `Type the prefix below exactly to delete all objects recursively:\n${state.prefix}`,
    "",
  );

  if (confirmation === null) {
    return;
  }

  if (confirmation.trim() !== state.prefix) {
    setConnectionStatus("Invalid confirmation. No objects were deleted.", true);
    return;
  }

  elements.clearPrefixButton.disabled = true;
  setConnectionStatus(`Deleting all objects under ${state.prefix}...`);

  try {
    const response = await apiFetch("/api/delete-prefix", {
      method: "POST",
      body: JSON.stringify({
        sessionId: state.sessionId,
        prefix: state.prefix,
      }),
    });

    state.selectedKey = "";
    elements.downloadButton.disabled = true;
    resetPreview("Select a `.csv` file to preview.");
    setConnectionStatus(
      `${response.deletedCount ?? 0} object(s) deleted from ${state.prefix}.`,
    );
    await loadObjects(state.prefix);
  } catch (error) {
    setConnectionStatus(getErrorMessage(error), true);
    setDiagnosticMessage(buildDiagnosticMessage(error));
    elements.clearPrefixButton.disabled = false;
  }
}

function setStartupDiagnostic() {
  setDiagnosticMessage(
    "Frontend configured to use the local API.\nStart the Node server and open the application at http://localhost:8086.",
  );
}

function persistConnectionForm() {
  const payload = getConnectionPayload();
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
  refreshConnectionSummary();
}

function restoreConnectionForm() {
  const rawValue = window.localStorage.getItem(STORAGE_KEY);

  if (!rawValue) {
    return;
  }

  try {
    const payload = JSON.parse(rawValue);
    setInputValue("region", payload.region);
    setInputValue("bucket", payload.bucket);
    setInputValue("accessKeyId", payload.accessKeyId);
    setInputValue("secretAccessKey", payload.secretAccessKey);
    setConnectionStatus("Connection data restored from the browser.");
    refreshConnectionSummary();
  } catch {
    window.localStorage.removeItem(STORAGE_KEY);
  }
}

function setInputValue(name, value) {
  const input = elements.credentialsForm.elements.namedItem(name);

  if (input instanceof HTMLInputElement) {
    input.value = typeof value === "string" ? value : "";
  }
}

function getConnectionPayload() {
  const formData = new FormData(elements.credentialsForm);

  return {
    region: formData.get("region")?.toString().trim() ?? "",
    bucket: formData.get("bucket")?.toString().trim() ?? "",
    accessKeyId: formData.get("accessKeyId")?.toString().trim() ?? "",
    secretAccessKey: formData.get("secretAccessKey")?.toString().trim() ?? "",
  };
}

function getPreviewRowLimit() {
  return elements.previewRowLimit.value || "10";
}

function getPreviewRowOrder() {
  return elements.previewRowOrder.value || "normal";
}

function getPreviewMode(key) {
  return isJsonPreviewFile(key) && elements.previewMode.value === "raw" ? "raw" : "table";
}

function isPreviewableFile(key) {
  const normalizedKey = key.toLowerCase();
  return (
    normalizedKey.endsWith(".csv") ||
    normalizedKey.endsWith(".csv.gz") ||
    normalizedKey.endsWith(".dfm") ||
    normalizedKey.endsWith(".dfm.gz") ||
    normalizedKey.endsWith(".json") ||
    normalizedKey.endsWith(".json.gz") ||
    normalizedKey.endsWith(".jsonl") ||
    normalizedKey.endsWith(".jsonl.gz") ||
    normalizedKey.endsWith(".ndjson") ||
    normalizedKey.endsWith(".ndjson.gz") ||
    normalizedKey.endsWith(".parquet") ||
    normalizedKey.endsWith(".parquet.gz") ||
    normalizedKey.endsWith(".gzip.parquet") ||
    normalizedKey.endsWith(".gz.parquet") ||
    normalizedKey.endsWith(".parq") ||
    normalizedKey.endsWith(".parq.gz") ||
    normalizedKey.endsWith(".gzip.parq") ||
    normalizedKey.endsWith(".gz.parq")
  );
}

function isJsonPreviewFile(key) {
  const normalizedKey = key.toLowerCase();
  return (
    normalizedKey.endsWith(".dfm") ||
    normalizedKey.endsWith(".dfm.gz") ||
    normalizedKey.endsWith(".json") ||
    normalizedKey.endsWith(".json.gz") ||
    normalizedKey.endsWith(".jsonl") ||
    normalizedKey.endsWith(".jsonl.gz") ||
    normalizedKey.endsWith(".ndjson") ||
    normalizedKey.endsWith(".ndjson.gz")
  );
}

function syncPreviewModeAvailability(key) {
  const allowRaw = isJsonPreviewFile(key);
  elements.previewMode.disabled = !allowRaw;

  if (!allowRaw) {
    elements.previewMode.value = "table";
  }
}

function refreshConnectionSummary() {
  const connection = getConnectionPayload();
  const parts = [];

  if (connection.bucket) {
    parts.push(connection.bucket);
  }

  if (connection.region) {
    parts.push(connection.region);
  }

  elements.connectionSummaryText.textContent = parts.length
    ? parts.join(" · ")
    : "Connection settings";
}

async function apiFetch(url, init = {}) {
  const response = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      ...(init.headers ?? {}),
    },
    ...init,
  });

  let payload = null;
  const contentType = response.headers.get("content-type") ?? "";

  if (contentType.includes("application/json")) {
    payload = await response.json();
  } else {
    const text = await response.text();
    payload = text ? { error: text } : null;
  }

  if (!response.ok) {
    throw new Error(payload?.error || `HTTP failure ${response.status}`);
  }

  return payload;
}

function downloadSelectedObject() {
  if (!state.sessionId || !state.selectedKey) {
    return;
  }

  const url = `/api/download?sessionId=${encodeURIComponent(state.sessionId)}&key=${encodeURIComponent(state.selectedKey)}`;
  window.open(url, "_blank", "noopener,noreferrer");
}

function folderIconSvg() {
  return `
    <svg viewBox="0 0 24 24" focusable="false">
      <path d="M3 6.75A2.25 2.25 0 0 1 5.25 4.5h4.14c.6 0 1.17.24 1.59.66l1.11 1.09h6.66A2.25 2.25 0 0 1 21 8.5v8.25A2.25 2.25 0 0 1 18.75 19H5.25A2.25 2.25 0 0 1 3 16.75z"></path>
    </svg>
  `;
}

function fileIconSvg() {
  return `
    <svg viewBox="0 0 24 24" focusable="false">
      <path d="M7.5 3.75A2.25 2.25 0 0 0 5.25 6v12A2.25 2.25 0 0 0 7.5 20.25h9A2.25 2.25 0 0 0 18.75 18V8.56a2.25 2.25 0 0 0-.66-1.59l-2.81-2.81a2.25 2.25 0 0 0-1.59-.66zm6 1.8 3.45 3.45H13.5z"></path>
    </svg>
  `;
}
