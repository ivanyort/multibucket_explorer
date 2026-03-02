const state = {
  provider: "s3",
  targetName: "",
  locationName: "",
  prefix: "",
  selectedKey: "",
  sessionId: "",
  objectItems: [],
  sort: {
    column: "name",
    direction: "asc",
  },
};

const STORAGE_KEY = "multibucket-explorer-connection";

const elements = {
  connectionPanel: document.querySelector("#connectionPanel"),
  connectionSummaryText: document.querySelector("#connectionSummaryText"),
  credentialsForm: document.querySelector("#credentialsForm"),
  provider: document.querySelector("#provider"),
  providerCards: document.querySelectorAll("[data-provider-card]"),
  s3Fields: document.querySelector("#s3Fields"),
  adlsFields: document.querySelector("#adlsFields"),
  gcsFields: document.querySelector("#gcsFields"),
  minioFields: document.querySelector("#minioFields"),
  connectButton: document.querySelector("#connectButton"),
  connectionStatus: document.querySelector("#connectionStatus"),
  diagnosticBox: document.querySelector("#diagnosticBox"),
  objectList: document.querySelector("#objectList"),
  currentPrefix: document.querySelector("#currentPrefix"),
  refreshButton: document.querySelector("#refreshButton"),
  clearPrefixButton: document.querySelector("#clearPrefixButton"),
  previewMeta: document.querySelector("#previewMeta"),
  previewTableWrap: document.querySelector("#previewTableWrap"),
  previewMode: document.querySelector("#previewMode"),
  previewRowLimit: document.querySelector("#previewRowLimit"),
  previewRowOrder: document.querySelector("#previewRowOrder"),
  downloadButton: document.querySelector("#downloadButton"),
  confirmModal: document.querySelector("#confirmModal"),
  confirmModalPrefix: document.querySelector("#confirmModalPrefix"),
  confirmModalCancel: document.querySelector("#confirmModalCancel"),
  confirmModalConfirm: document.querySelector("#confirmModalConfirm"),
  deleteProgressModal: document.querySelector("#deleteProgressModal"),
  deleteProgressPrefix: document.querySelector("#deleteProgressPrefix"),
};

elements.credentialsForm.addEventListener("submit", (event) => {
  event.preventDefault();
});
elements.credentialsForm.addEventListener("input", persistConnectionForm);
elements.credentialsForm.addEventListener("change", persistConnectionForm);
const serviceAccountJsonField = elements.credentialsForm.elements.namedItem("serviceAccountJson");
if (serviceAccountJsonField instanceof HTMLTextAreaElement) {
  serviceAccountJsonField.addEventListener("input", syncProjectIdFromServiceAccountJson);
  serviceAccountJsonField.addEventListener("change", syncProjectIdFromServiceAccountJson);
}
elements.providerCards.forEach((card) => {
  card.addEventListener("click", () => {
    const provider = ["adls", "gcs", "minio"].includes(card.dataset.providerCard) ? card.dataset.providerCard : "s3";
    elements.provider.value = provider;
    syncProviderFields();
    persistConnectionForm();
  });
});
elements.connectButton.addEventListener("click", connectToBucket);
elements.refreshButton.addEventListener("click", () => loadObjects(state.prefix));
elements.clearPrefixButton.addEventListener("click", clearCurrentPrefix);
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
syncProviderFields();
setStartupDiagnostic();
refreshConnectionSummary();
syncPreviewModeAvailability("");

async function connectToBucket() {
  const connection = getConnectionPayload();

  const validationError = validateConnectionPayload(connection);

  if (validationError) {
    setConnectionStatus(validationError, true);
    return;
  }

  persistConnectionForm();
  state.provider = connection.provider;
  state.targetName = getConnectionTargetName(connection);
  state.locationName = getConnectionLocationName(connection);
  state.prefix = "";
  state.selectedKey = "";
  state.sessionId = "";

  elements.refreshButton.disabled = true;
  elements.clearPrefixButton.disabled = true;
  elements.downloadButton.disabled = true;
  syncPreviewModeAvailability("");
  renderObjectPlaceholder("Connecting...");
  resetPreview("Select a `.csv` file to preview.");
  setConnectionStatus(`Connecting to ${state.targetName} (${state.locationName})...`);
  setDiagnosticMessage("Calling the local backend to validate storage access...");

  try {
    const response = await apiFetch("/api/connect", {
      method: "POST",
      body: JSON.stringify(connection),
    });

    state.sessionId = response.sessionId;
    state.provider = response.provider ?? connection.provider;
    state.targetName = response.targetName ?? state.targetName;
    state.locationName = response.locationName ?? state.locationName;
    elements.refreshButton.disabled = false;
    await loadObjects("");
    elements.connectionPanel.open = false;
    refreshConnectionSummary();
    setDiagnosticMessage(
      `Connection OK through the local backend.\nProvider: ${state.provider.toUpperCase()}\nTarget: ${state.targetName}\nLocation: ${state.locationName}\nSession: ${state.sessionId}`,
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
  renderCurrentPrefix();
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
      `${response.summary?.folders ?? 0} folder(s) and ${response.summary?.files ?? 0} file(s) loaded from ${state.targetName}.`,
    );
  } catch (error) {
    renderObjectPlaceholder("Failed to list objects.");
    setConnectionStatus(getErrorMessage(error), true);
    setDiagnosticMessage(buildDiagnosticMessage(error));
  }
}

function renderCurrentPrefix() {
  elements.currentPrefix.innerHTML = "";

  const rootButton = document.createElement("button");
  rootButton.type = "button";
  rootButton.className = "prefix-crumb";
  rootButton.textContent = "/";
  rootButton.disabled = !state.prefix;
  rootButton.setAttribute("aria-current", state.prefix ? "false" : "page");
  rootButton.addEventListener("click", () => loadObjects(""));
  elements.currentPrefix.appendChild(rootButton);

  if (!state.prefix) {
    return;
  }

  const segments = state.prefix.split("/").filter(Boolean);
  let nextPrefix = "";

  segments.forEach((segment, index) => {
    if (index > 0) {
      const separator = document.createElement("span");
      separator.className = "prefix-separator";
      separator.textContent = "/";
      separator.setAttribute("aria-hidden", "true");
      elements.currentPrefix.appendChild(separator);
    }

    nextPrefix = `${nextPrefix}${segment}/`;
    const targetPrefix = nextPrefix;

    const crumb = document.createElement("button");
    crumb.type = "button";
    crumb.className = "prefix-crumb";
    crumb.textContent = segment;
    crumb.setAttribute("aria-current", index === segments.length - 1 ? "page" : "false");
    crumb.addEventListener("click", () => loadObjects(targetPrefix));
    elements.currentPrefix.appendChild(crumb);
  });
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

  return "Unexpected error while accessing storage.";
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

  const confirmation = await confirmPrefixDeletion(state.prefix);

  if (!confirmation) {
    setConnectionStatus("Prefix deletion cancelled.");
    return;
  }

  elements.clearPrefixButton.disabled = true;
  showDeleteProgress(state.prefix);
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
  } finally {
    hideDeleteProgress();
  }
}

function confirmPrefixDeletion(prefix) {
  return new Promise((resolve) => {
    const previousActiveElement = document.activeElement;

    const close = (confirmed) => {
      elements.confirmModal.hidden = true;
      document.body.style.overflow = "";
      elements.confirmModal.removeEventListener("click", handleShellClick);
      document.removeEventListener("keydown", handleKeydown);
      elements.confirmModalCancel.removeEventListener("click", handleCancel);
      elements.confirmModalConfirm.removeEventListener("click", handleConfirm);
      if (previousActiveElement instanceof HTMLElement) {
        previousActiveElement.focus();
      }
      resolve(confirmed);
    };

    const handleCancel = () => close(false);
    const handleConfirm = () => close(true);
    const handleShellClick = (event) => {
      const target = event.target;
      if (target instanceof HTMLElement && target.hasAttribute("data-modal-close")) {
        close(false);
      }
    };
    const handleKeydown = (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        close(false);
      }
    };

    elements.confirmModalPrefix.textContent = prefix;
    elements.confirmModal.hidden = false;
    document.body.style.overflow = "hidden";
    elements.confirmModal.addEventListener("click", handleShellClick);
    document.addEventListener("keydown", handleKeydown);
    elements.confirmModalCancel.addEventListener("click", handleCancel);
    elements.confirmModalConfirm.addEventListener("click", handleConfirm);
    elements.confirmModalCancel.focus();
  });
}

function showDeleteProgress(prefix) {
  elements.deleteProgressPrefix.textContent = prefix;
  elements.deleteProgressModal.hidden = false;
  document.body.style.overflow = "hidden";
}

function hideDeleteProgress() {
  elements.deleteProgressModal.hidden = true;
  document.body.style.overflow = "";
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
    setInputValue("provider", payload.provider || "s3");
    setInputValue("region", payload.region);
    setInputValue("bucket", payload.bucket);
    setInputValue("accessKeyId", payload.accessKeyId);
    setInputValue("secretAccessKey", payload.secretAccessKey);
    setInputValue("accountName", payload.accountName);
    setInputValue("fileSystem", payload.fileSystem);
    setInputValue("accountKey", payload.accountKey);
    setInputValue("gcsBucket", payload.gcsBucket);
    setInputValue("projectId", payload.projectId);
    setInputValue("serviceAccountJson", payload.serviceAccountJson);
    setInputValue("endpoint", payload.endpoint);
    setInputValue("minioBucket", payload.minioBucket);
    setInputValue("minioRegion", payload.minioRegion);
    setInputValue("minioAccessKeyId", payload.minioAccessKeyId);
    setInputValue("minioSecretAccessKey", payload.minioSecretAccessKey);
    setCheckboxValue("ignoreTlsErrors", payload.ignoreTlsErrors === true);
    syncProviderFields();
    setConnectionStatus("Connection data restored from the browser.");
    refreshConnectionSummary();
  } catch {
    window.localStorage.removeItem(STORAGE_KEY);
  }
}

function setInputValue(name, value) {
  const input = elements.credentialsForm.elements.namedItem(name);

  if (input instanceof HTMLInputElement || input instanceof HTMLSelectElement || input instanceof HTMLTextAreaElement) {
    input.value = typeof value === "string" ? value : "";
  }
}

function setCheckboxValue(name, checked) {
  const input = elements.credentialsForm.elements.namedItem(name);

  if (input instanceof HTMLInputElement && input.type === "checkbox") {
    input.checked = checked;
  }
}

function syncProjectIdFromServiceAccountJson() {
  const projectIdField = elements.credentialsForm.elements.namedItem("projectId");
  const serviceAccountJsonField = elements.credentialsForm.elements.namedItem("serviceAccountJson");

  if (!(projectIdField instanceof HTMLInputElement) || !(serviceAccountJsonField instanceof HTMLTextAreaElement)) {
    return;
  }

  if (projectIdField.value.trim()) {
    return;
  }

  const projectId = extractProjectIdFromServiceAccountJson(serviceAccountJsonField.value);

  if (!projectId) {
    return;
  }

  projectIdField.value = projectId;
  persistConnectionForm();
}

function extractProjectIdFromServiceAccountJson(value) {
  if (typeof value !== "string" || !value.trim()) {
    return "";
  }

  try {
    const parsed = JSON.parse(value);
    return typeof parsed.project_id === "string" ? parsed.project_id.trim() : "";
  } catch {
    return "";
  }
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

function getConnectionPayload() {
  const formData = new FormData(elements.credentialsForm);

  return {
    provider: ["adls", "gcs", "minio"].includes(formData.get("provider")?.toString().trim())
      ? formData.get("provider")?.toString().trim()
      : "s3",
    region: formData.get("region")?.toString().trim() ?? "",
    bucket: formData.get("bucket")?.toString().trim() ?? "",
    accessKeyId: formData.get("accessKeyId")?.toString().trim() ?? "",
    secretAccessKey: formData.get("secretAccessKey")?.toString().trim() ?? "",
    accountName: formData.get("accountName")?.toString().trim() ?? "",
    fileSystem: formData.get("fileSystem")?.toString().trim() ?? "",
    accountKey: formData.get("accountKey")?.toString().trim() ?? "",
    gcsBucket: normalizeGcsBucketName(formData.get("gcsBucket")?.toString() ?? ""),
    projectId: formData.get("projectId")?.toString().trim() ?? "",
    serviceAccountJson: formData.get("serviceAccountJson")?.toString().trim() ?? "",
    endpoint: formData.get("endpoint")?.toString().trim() ?? "",
    minioBucket: formData.get("minioBucket")?.toString().trim() ?? "",
    minioRegion: formData.get("minioRegion")?.toString().trim() ?? "",
    minioAccessKeyId: formData.get("minioAccessKeyId")?.toString().trim() ?? "",
    minioSecretAccessKey: formData.get("minioSecretAccessKey")?.toString().trim() ?? "",
    ignoreTlsErrors: elements.credentialsForm.elements.namedItem("ignoreTlsErrors") instanceof HTMLInputElement
      ? elements.credentialsForm.elements.namedItem("ignoreTlsErrors").checked
      : false,
  };
}

function validateConnectionPayload(connection) {
  if (connection.provider === "adls") {
    if (!connection.accountName || !connection.fileSystem || !connection.accountKey) {
      return "Fill in account name, container name, and access key.";
    }

    return "";
  }

  if (connection.provider === "gcs") {
    if (!connection.gcsBucket || !connection.serviceAccountJson) {
      return "Fill in bucket and service account JSON.";
    }

    try {
      JSON.parse(connection.serviceAccountJson);
    } catch {
      return "Service account JSON must be valid JSON.";
    }

    return "";
  }

  if (connection.provider === "minio") {
    if (!connection.endpoint || !connection.minioBucket || !connection.minioAccessKeyId || !connection.minioSecretAccessKey) {
      return "Fill in endpoint, bucket, access key ID, and secret access key.";
    }

    return "";
  }

  if (!connection.region || !connection.bucket || !connection.accessKeyId || !connection.secretAccessKey) {
    return "Fill in region, bucket, and credentials.";
  }

  return "";
}

function getConnectionTargetName(connection) {
  if (connection.provider === "adls") {
    return connection.fileSystem;
  }

  if (connection.provider === "gcs") {
    return connection.gcsBucket;
  }

  if (connection.provider === "minio") {
    return connection.minioBucket;
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

function syncProviderFields() {
  const provider = ["adls", "gcs", "minio"].includes(elements.provider.value) ? elements.provider.value : "s3";
  const useS3 = provider === "s3";
  const useAdls = provider === "adls";
  const useGcs = provider === "gcs";
  const useMinio = provider === "minio";

  elements.providerCards.forEach((card) => {
    const isActive = card.dataset.providerCard === provider;
    card.classList.toggle("is-active", isActive);
    card.setAttribute("aria-pressed", isActive ? "true" : "false");
  });

  elements.s3Fields.hidden = !useS3;
  elements.adlsFields.hidden = !useAdls;
  elements.gcsFields.hidden = !useGcs;
  elements.minioFields.hidden = !useMinio;
  elements.s3Fields.setAttribute("aria-hidden", useS3 ? "false" : "true");
  elements.adlsFields.setAttribute("aria-hidden", useAdls ? "false" : "true");
  elements.gcsFields.setAttribute("aria-hidden", useGcs ? "false" : "true");
  elements.minioFields.setAttribute("aria-hidden", useMinio ? "false" : "true");

  setFieldRequired(["region", "bucket", "accessKeyId", "secretAccessKey"], provider === "s3");
  setFieldRequired(["accountName", "fileSystem", "accountKey"], useAdls);
  setFieldRequired(["gcsBucket", "serviceAccountJson"], useGcs);
  setFieldRequired(["projectId"], false);
  setFieldRequired(["endpoint", "minioBucket", "minioAccessKeyId", "minioSecretAccessKey"], useMinio);
  setFieldRequired(["minioRegion"], false);
  refreshConnectionSummary();
}

function setFieldRequired(names, required) {
  names.forEach((name) => {
    const input = elements.credentialsForm.elements.namedItem(name);

    if (input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement) {
      input.required = required;
    }
  });
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

  parts.push(connection.provider.toUpperCase());

  const targetName = getConnectionTargetName(connection);
  const locationName = getConnectionLocationName(connection);

  if (targetName) {
    parts.push(targetName);
  }

  if (locationName) {
    parts.push(locationName);
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
