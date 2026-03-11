const state = {
  provider: "s3",
  language: "en",
  targetName: "",
  locationName: "",
  prefix: "",
  selectedKey: "",
  sessionId: "",
  browseMode: "raw",
  icebergTable: null,
  icebergAvailable: false,
  icebergSnapshotId: "",
  seedIcebergEnabled: false,
  destructiveOperationsEnabled: true,
  objectItems: [],
  sort: {
    column: "name",
    direction: "asc",
  },
};

const STORAGE_KEY = "multibucket-explorer-connection";
const LANGUAGE_STORAGE_KEY = "multibucket-explorer-language";
const SUPPORTED_LANGUAGES = ["en", "pt-BR", "es", "it"];
const DATE_LOCALES = {
  en: "en-US",
  "pt-BR": "pt-BR",
  es: "es-ES",
  it: "it-IT",
};
const translations = {
  en: {
    language: { label: "Language" },
    connection: { kicker: "Connection", title: "Storage Access", settings: "Connection settings", connect: "Connect" },
    providers: {
      kicker: "Providers",
      ariaLabel: "Storage provider",
      s3: "AWS buckets with region and access keys.",
      adls: "Azure Data Lake Storage Gen2 with account and container name.",
      gcs: "Google Cloud Storage with bucket and service account JSON.",
      minio: "S3-compatible storage with custom endpoint and access keys.",
    },
    s3: { ariaLabel: "S3 connection settings", kicker: "S3 Connection", copy: "Use AWS bucket credentials for this connection." },
    adls: { ariaLabel: "ADLS connection settings", kicker: "ADLS Connection", copy: "Use Azure Data Lake Storage Gen2 account credentials and container name." },
    gcs: { ariaLabel: "GCS connection settings", kicker: "GCS Connection", copy: "Use a Google Cloud Storage bucket and a service account JSON key." },
    minio: {
      ariaLabel: "MinIO connection settings",
      kicker: "MinIO Connection",
      copy: "Use a custom MinIO endpoint with S3-compatible access keys.",
      ignoreTlsErrors: "Ignore HTTPS certificate errors for this MinIO connection",
    },
    fields: {
      region: "Region", bucket: "Bucket", accessKeyId: "Access Key ID", secretAccessKey: "Secret Access Key",
      accountName: "Account Name", containerName: "Container Name", accessKey: "Access Key",
      bucketOrUrl: "Bucket or URL", projectId: "Project ID (auto-filled from the JSON when present)",
      serviceAccountJson: "Service Account JSON", endpoint: "Endpoint",
    },
    placeholders: {
      region: "us-east-1", bucket: "my-bucket", accessKeyId: "AKIA...", secretAccessKey: "********",
      accountName: "myaccount", fileSystem: "my-filesystem", gcsBucket: "gs://my-bucket or my-bucket",
      projectId: "my-project-id", serviceAccountJson: '{"type":"service_account", ...}', endpoint: "http://localhost:9000",
      minioAccessKeyId: "minioadmin",
    },
    browser: {
      kicker: "Object Browser", title: "Objects", currentPrefix: "Current prefix", refresh: "Refresh",
      clearPrefix: "Clear prefix", connectToList: "Connect to list storage objects.",
      noItems: "No items found in this prefix.", loading: "Loading objects...", failed: "Failed to list objects.",
      folder: "Folder", file: "File", actions: "Actions", deleteFile: "Delete file",
      destructiveDisabled: "Delete actions are disabled by the server.",
      openIceberg: "Open as Iceberg",
      openFolders: "View folders",
      icebergSummary: "Iceberg table detected. Latest snapshot: {snapshotId}. Data files: {dataFileCount}. Format: {dataFormat}.",
    },
    preview: {
      kicker: "File Preview", title: "Preview", view: "View", table: "Table", raw: "Raw", rows: "Rows", all: "All",
      order: "Order", normal: "Normal", reverse: "Newest first", snapshot: "Snapshot", download: "Download file",
      selectCompatible: "Select a compatible `.csv`, `.json`, `.dfm`, `.md`, `.txt`, `.parquet`, `.avro`, `.orc`, `.gz`, or `.snappy` file to preview.",
      noFileSelected: "No file selected.", reading: "Reading file...", loadingFor: "Loading preview for {key}...",
      unsupportedFormat: "Unsupported preview format: {key}",
      unsupportedBody: "Select a .csv, .json, .jsonl, .ndjson, .dfm, .md, .txt, .parquet, .parq, .avro, .orc, or matching .gz/.snappy file.",
      emptyFile: "Empty file.", emptyBody: "The file has no rows to display.", failed: "Failed to load preview.",
      rawModeSuffix: "raw mode", formatSuffix: "format {format}", reverseOrder: "reverse order",
      showingRows: "{key} · showing {count} sample row(s){orderSuffix}{modeSuffix}{formatSuffix}{dfmSuffix}",
      snapshotSuffix: " · snapshot {snapshotId}",
      dfmFound: " · DFM: {dfmKey} ({count} columns)", dfmNotFound: " · DFM not found",
    },
    delete: {
      confirmKicker: "Destructive Action", confirmTitle: "Delete prefix",
      confirmBody: "Are you absolutely sure you want to delete all files under this prefix?",
      confirmAction: "Delete files", progressKicker: "Delete In Progress", progressTitle: "Deleting files",
      progressBody: "The selected prefix is being deleted. This can take a while when many files are involved.",
      cancelled: "Prefix deletion cancelled.", deleting: "Deleting all objects under {prefix}...", deleted: "{count} object(s) deleted from {prefix}.",
      confirmFileTitle: "Delete file", confirmFileBody: "Are you absolutely sure you want to delete this file?",
      confirmFileAction: "Delete file", fileCancelled: "File deletion cancelled.", deletingFile: "Deleting {key}...",
      fileDeleted: "{key} deleted.", progressFileTitle: "Deleting file",
      progressFileBody: "The selected file is being deleted.",
    },
    seed: {
      button: "Create Iceberg samples", kicker: "Sample Data", confirmTitle: "Create Iceberg sample tables",
      confirmBody: "This will create development Iceberg fixtures in the connected storage target under a fixed prefix.",
      confirmAction: "Create samples", progressTitle: "Creating sample tables",
      progressBody: "The backend is preparing sample Iceberg tables and uploading them to the connected storage target.",
      resultTitle: "Sample tables created", resultBody: "The Iceberg sample tables are now available in the connected storage target.",
      navigateAction: "Open sample prefix", createdTablesTitle: "Created tables", warningsTitle: "Warnings",
      generating: "Creating Iceberg sample tables under {prefix}...", generatedStatus: "Iceberg sample tables created under {prefix}.",
      prefixRequired: "Seed prefix is required.",
    },
    common: { cancel: "Cancel", close: "Close" },
    messages: {
      enterCredentials: "Enter credentials and click connect.", startupDiagnostic: "Frontend configured to use the local API.\nStart the Node server and open the application at http://localhost:8086.",
      connecting: "Connecting...", connectingTo: "Connecting to {target} ({location})...", validateAccess: "Calling the local backend to validate storage access...",
      connectionOk: "Connection OK through the local backend.\nProvider: {provider}\nTarget: {target}\nLocation: {location}\nSession: {session}",
      destructiveDisabled: "Destructive operations are disabled by the server configuration.",
      failedToConnect: "Failed to connect.", restored: "Connection data restored from the browser.",
      loadedSummary: "{folders} folder(s) and {files} file(s) loaded from {target}.",
      unexpectedStorageError: "Unexpected error while accessing storage.", backendError: "Error returned by the local backend:\n{message}",
      noFailureDetails: "Failure without additional details.",
      fillAdls: "Fill in account name, container name, and access key.", fillGcs: "Fill in bucket and service account JSON.",
      invalidServiceAccountJson: "Service account JSON must be valid JSON.", fillMinio: "Fill in endpoint, bucket, access key ID, and secret access key.",
      fillS3: "Fill in region, bucket, and credentials.",
    },
    table: { name: "Name", type: "Type", size: "Size", date: "Date", column: "column_{index}" },
  },
  "pt-BR": {
    language: { label: "Idioma" },
    connection: { kicker: "Conexão", title: "Acesso ao Storage", settings: "Configurações da conexão", connect: "Conectar" },
    providers: {
      kicker: "Provedores", ariaLabel: "Provedor de storage",
      s3: "Buckets AWS com região e chaves de acesso.",
      adls: "Azure Data Lake Storage Gen2 com conta e nome do container.",
      gcs: "Google Cloud Storage com bucket e JSON de service account.",
      minio: "Storage compatível com S3 com endpoint customizado e chaves de acesso.",
    },
    s3: { ariaLabel: "Configurações de conexão S3", kicker: "Conexão S3", copy: "Use as credenciais do bucket AWS nesta conexão." },
    adls: { ariaLabel: "Configurações de conexão ADLS", kicker: "Conexão ADLS", copy: "Use credenciais da conta Azure Data Lake Storage Gen2 e o nome do container." },
    gcs: { ariaLabel: "Configurações de conexão GCS", kicker: "Conexão GCS", copy: "Use um bucket Google Cloud Storage e uma chave JSON de service account." },
    minio: { ariaLabel: "Configurações de conexão MinIO", kicker: "Conexão MinIO", copy: "Use um endpoint MinIO customizado com chaves compatíveis com S3.", ignoreTlsErrors: "Ignorar erros de certificado HTTPS nesta conexão MinIO" },
    fields: {
      region: "Região", bucket: "Bucket", accessKeyId: "Access Key ID", secretAccessKey: "Secret Access Key",
      accountName: "Nome da conta", containerName: "Nome do container", accessKey: "Access Key",
      bucketOrUrl: "Bucket ou URL", projectId: "Project ID (preenchido automaticamente a partir do JSON quando presente)",
      serviceAccountJson: "Service Account JSON", endpoint: "Endpoint",
    },
    placeholders: {
      region: "us-east-1", bucket: "meu-bucket", accessKeyId: "AKIA...", secretAccessKey: "********",
      accountName: "minhaconta", fileSystem: "meu-container", gcsBucket: "gs://meu-bucket ou meu-bucket",
      projectId: "meu-projeto", serviceAccountJson: '{"type":"service_account", ...}', endpoint: "http://localhost:9000",
      minioAccessKeyId: "minioadmin",
    },
    browser: {
      kicker: "Navegador de Objetos", title: "Objetos", currentPrefix: "Prefixo atual", refresh: "Atualizar",
      clearPrefix: "Limpar prefixo", connectToList: "Conecte-se para listar os objetos do storage.",
      noItems: "Nenhum item encontrado neste prefixo.", loading: "Carregando objetos...", failed: "Falha ao listar objetos.",
      folder: "Pasta", file: "Arquivo", actions: "Ações", deleteFile: "Apagar arquivo",
      destructiveDisabled: "As ações de exclusão estão desativadas pelo servidor.",
      openIceberg: "Abrir como Iceberg",
      openFolders: "Ver pastas",
      icebergSummary: "Tabela Iceberg detectada. Snapshot atual: {snapshotId}. Arquivos de dados: {dataFileCount}. Formato: {dataFormat}.",
    },
    preview: {
      kicker: "Pré-visualização de Arquivo", title: "Prévia", view: "Visualização", table: "Tabela", raw: "Bruto", rows: "Linhas", all: "Todas",
      order: "Ordem", normal: "Normal", reverse: "Mais novos primeiro", snapshot: "Snapshot", download: "Baixar arquivo",
      selectCompatible: "Selecione um arquivo compatível `.csv`, `.json`, `.dfm`, `.md`, `.txt`, `.parquet`, `.avro`, `.orc`, `.gz` ou `.snappy` para visualizar.",
      noFileSelected: "Nenhum arquivo selecionado.", reading: "Lendo arquivo...", loadingFor: "Carregando prévia de {key}...",
      unsupportedFormat: "Formato de prévia não suportado: {key}",
      unsupportedBody: "Selecione um arquivo .csv, .json, .jsonl, .ndjson, .dfm, .md, .txt, .parquet, .parq, .avro, .orc ou .gz/.snappy correspondente.",
      emptyFile: "Arquivo vazio.", emptyBody: "O arquivo não tem linhas para exibir.", failed: "Falha ao carregar a prévia.",
      rawModeSuffix: "modo bruto", formatSuffix: "formato {format}", reverseOrder: "ordem reversa",
      showingRows: "{key} · exibindo {count} linha(s) de amostra{orderSuffix}{modeSuffix}{formatSuffix}{dfmSuffix}",
      snapshotSuffix: " · snapshot {snapshotId}",
      dfmFound: " · DFM: {dfmKey} ({count} colunas)", dfmNotFound: " · DFM não encontrado",
    },
    delete: {
      confirmKicker: "Ação Destrutiva", confirmTitle: "Excluir prefixo",
      confirmBody: "Você tem certeza absoluta que deseja apagar todos os arquivos sob este prefixo?",
      confirmAction: "Apagar arquivos", progressKicker: "Exclusão em Andamento", progressTitle: "Apagando arquivos",
      progressBody: "O prefixo selecionado está sendo apagado. Isso pode demorar quando houver muitos arquivos envolvidos.",
      cancelled: "Exclusão do prefixo cancelada.", deleting: "Apagando todos os objetos sob {prefix}...", deleted: "{count} objeto(s) apagado(s) de {prefix}.",
      confirmFileTitle: "Apagar arquivo", confirmFileBody: "Você tem certeza absoluta que deseja apagar este arquivo?",
      confirmFileAction: "Apagar arquivo", fileCancelled: "Exclusão do arquivo cancelada.", deletingFile: "Apagando {key}...",
      fileDeleted: "{key} apagado.", progressFileTitle: "Apagando arquivo",
      progressFileBody: "O arquivo selecionado está sendo apagado.",
    },
    seed: {
      button: "Criar amostras Iceberg", kicker: "Dados de Exemplo", confirmTitle: "Criar tabelas Iceberg de exemplo",
      confirmBody: "Isso criará fixtures Iceberg de desenvolvimento no destino conectado usando um prefixo fixo.",
      confirmAction: "Criar amostras", progressTitle: "Criando tabelas de exemplo",
      progressBody: "O backend está preparando tabelas Iceberg de exemplo e enviando tudo para o storage conectado.",
      resultTitle: "Tabelas de exemplo criadas", resultBody: "As tabelas Iceberg de exemplo já estão disponíveis no storage conectado.",
      navigateAction: "Abrir prefixo de amostras", createdTablesTitle: "Tabelas criadas", warningsTitle: "Avisos",
      generating: "Criando tabelas Iceberg de exemplo em {prefix}...", generatedStatus: "Tabelas Iceberg de exemplo criadas em {prefix}.",
      prefixRequired: "O prefixo de seed e obrigatorio.",
    },
    common: { cancel: "Cancelar", close: "Fechar" },
    messages: {
      enterCredentials: "Preencha as credenciais e clique em conectar.", startupDiagnostic: "Frontend configurado para usar a API local.\nInicie o servidor Node e abra a aplicação em http://localhost:8086.",
      connecting: "Conectando...", connectingTo: "Conectando a {target} ({location})...", validateAccess: "Chamando o backend local para validar o acesso ao storage...",
      connectionOk: "Conexão OK pelo backend local.\nProvedor: {provider}\nDestino: {target}\nLocalização: {location}\nSessão: {session}",
      destructiveDisabled: "As operações destrutivas estão desativadas pela configuração do servidor.",
      failedToConnect: "Falha ao conectar.", restored: "Dados de conexão restaurados do navegador.",
      loadedSummary: "{folders} pasta(s) e {files} arquivo(s) carregados de {target}.",
      unexpectedStorageError: "Erro inesperado ao acessar o storage.", backendError: "Erro retornado pelo backend local:\n{message}",
      noFailureDetails: "Falha sem detalhes adicionais.",
      fillAdls: "Preencha nome da conta, nome do container e access key.", fillGcs: "Preencha bucket e service account JSON.",
      invalidServiceAccountJson: "O service account JSON deve ser um JSON válido.", fillMinio: "Preencha endpoint, bucket, access key ID e secret access key.",
      fillS3: "Preencha região, bucket e credenciais.",
    },
    table: { name: "Nome", type: "Tipo", size: "Tamanho", date: "Data", column: "coluna_{index}" },
  },
  es: {
    language: { label: "Idioma" },
    connection: { kicker: "Conexión", title: "Acceso al Storage", settings: "Configuración de la conexión", connect: "Conectar" },
    providers: {
      kicker: "Proveedores", ariaLabel: "Proveedor de storage",
      s3: "Buckets de AWS con región y claves de acceso.",
      adls: "Azure Data Lake Storage Gen2 con cuenta y nombre del contenedor.",
      gcs: "Google Cloud Storage con bucket y JSON de service account.",
      minio: "Storage compatible con S3 con endpoint personalizado y claves de acceso.",
    },
    s3: { ariaLabel: "Configuración de conexión S3", kicker: "Conexión S3", copy: "Usa las credenciales del bucket de AWS para esta conexión." },
    adls: { ariaLabel: "Configuración de conexión ADLS", kicker: "Conexión ADLS", copy: "Usa credenciales de la cuenta Azure Data Lake Storage Gen2 y el nombre del contenedor." },
    gcs: { ariaLabel: "Configuración de conexión GCS", kicker: "Conexión GCS", copy: "Usa un bucket de Google Cloud Storage y una clave JSON de service account." },
    minio: { ariaLabel: "Configuración de conexión MinIO", kicker: "Conexión MinIO", copy: "Usa un endpoint MinIO personalizado con claves compatibles con S3.", ignoreTlsErrors: "Ignorar errores de certificado HTTPS para esta conexión MinIO" },
    fields: {
      region: "Región", bucket: "Bucket", accessKeyId: "Access Key ID", secretAccessKey: "Secret Access Key",
      accountName: "Nombre de la cuenta", containerName: "Nombre del contenedor", accessKey: "Access Key",
      bucketOrUrl: "Bucket o URL", projectId: "Project ID (se completa automáticamente desde el JSON cuando está presente)",
      serviceAccountJson: "Service Account JSON", endpoint: "Endpoint",
    },
    placeholders: {
      region: "us-east-1", bucket: "mi-bucket", accessKeyId: "AKIA...", secretAccessKey: "********",
      accountName: "micuenta", fileSystem: "mi-contenedor", gcsBucket: "gs://mi-bucket o mi-bucket",
      projectId: "mi-proyecto", serviceAccountJson: '{"type":"service_account", ...}', endpoint: "http://localhost:9000",
      minioAccessKeyId: "minioadmin",
    },
    browser: {
      kicker: "Explorador de Objetos", title: "Objetos", currentPrefix: "Prefijo actual", refresh: "Actualizar",
      clearPrefix: "Borrar prefijo", connectToList: "Conéctate para listar los objetos del storage.",
      noItems: "No se encontraron elementos en este prefijo.", loading: "Cargando objetos...", failed: "Error al listar objetos.",
      folder: "Carpeta", file: "Archivo", actions: "Acciones", deleteFile: "Borrar archivo",
      destructiveDisabled: "Las acciones de borrado están deshabilitadas por el servidor.",
      openIceberg: "Abrir como Iceberg",
      openFolders: "Ver carpetas",
      icebergSummary: "Tabla Iceberg detectada. Snapshot actual: {snapshotId}. Archivos de datos: {dataFileCount}. Formato: {dataFormat}.",
    },
    preview: {
      kicker: "Vista Previa de Archivo", title: "Vista previa", view: "Vista", table: "Tabla", raw: "Raw", rows: "Filas", all: "Todas",
      order: "Orden", normal: "Normal", reverse: "Más nuevos primero", snapshot: "Snapshot", download: "Descargar archivo",
      selectCompatible: "Selecciona un archivo compatible `.csv`, `.json`, `.dfm`, `.md`, `.txt`, `.parquet`, `.avro`, `.orc`, `.gz` o `.snappy` para previsualizar.",
      noFileSelected: "Ningún archivo seleccionado.", reading: "Leyendo archivo...", loadingFor: "Cargando vista previa de {key}...",
      unsupportedFormat: "Formato de vista previa no compatible: {key}",
      unsupportedBody: "Selecciona un archivo .csv, .json, .jsonl, .ndjson, .dfm, .md, .txt, .parquet, .parq, .avro, .orc o un .gz/.snappy correspondiente.",
      emptyFile: "Archivo vacío.", emptyBody: "El archivo no tiene filas para mostrar.", failed: "Error al cargar la vista previa.",
      rawModeSuffix: "modo raw", formatSuffix: "formato {format}", reverseOrder: "orden inverso",
      showingRows: "{key} · mostrando {count} fila(s) de muestra{orderSuffix}{modeSuffix}{formatSuffix}{dfmSuffix}",
      snapshotSuffix: " · snapshot {snapshotId}",
      dfmFound: " · DFM: {dfmKey} ({count} columnas)", dfmNotFound: " · DFM no encontrado",
    },
    delete: {
      confirmKicker: "Acción Destructiva", confirmTitle: "Eliminar prefijo",
      confirmBody: "¿Estás absolutamente seguro de que deseas borrar todos los archivos bajo este prefijo?",
      confirmAction: "Borrar archivos", progressKicker: "Eliminación en Curso", progressTitle: "Borrando archivos",
      progressBody: "Se está borrando el prefijo seleccionado. Esto puede tardar cuando hay muchos archivos involucrados.",
      cancelled: "Eliminación del prefijo cancelada.", deleting: "Borrando todos los objetos bajo {prefix}...", deleted: "{count} objeto(s) eliminado(s) de {prefix}.",
      confirmFileTitle: "Borrar archivo", confirmFileBody: "¿Estás absolutamente seguro de que deseas borrar este archivo?",
      confirmFileAction: "Borrar archivo", fileCancelled: "Eliminación del archivo cancelada.", deletingFile: "Borrando {key}...",
      fileDeleted: "{key} eliminado.", progressFileTitle: "Borrando archivo",
      progressFileBody: "Se está borrando el archivo seleccionado.",
    },
    seed: {
      button: "Crear muestras Iceberg", kicker: "Datos de Muestra", confirmTitle: "Crear tablas Iceberg de ejemplo",
      confirmBody: "Esto creará fixtures Iceberg de desarrollo en el destino conectado bajo un prefijo fijo.",
      confirmAction: "Crear muestras", progressTitle: "Creando tablas de ejemplo",
      progressBody: "El backend está preparando tablas Iceberg de ejemplo y cargándolas en el storage conectado.",
      resultTitle: "Tablas de ejemplo creadas", resultBody: "Las tablas Iceberg de ejemplo ya están disponibles en el storage conectado.",
      navigateAction: "Abrir prefijo de muestras", createdTablesTitle: "Tablas creadas", warningsTitle: "Avisos",
      generating: "Creando tablas Iceberg de ejemplo en {prefix}...", generatedStatus: "Tablas Iceberg de ejemplo creadas en {prefix}.",
      prefixRequired: "El prefijo de seed es obligatorio.",
    },
    common: { cancel: "Cancelar", close: "Cerrar" },
    messages: {
      enterCredentials: "Ingresa las credenciales y haz clic en conectar.", startupDiagnostic: "Frontend configurado para usar la API local.\nInicia el servidor Node y abre la aplicación en http://localhost:8086.",
      connecting: "Conectando...", connectingTo: "Conectando a {target} ({location})...", validateAccess: "Llamando al backend local para validar el acceso al storage...",
      connectionOk: "Conexión OK a través del backend local.\nProveedor: {provider}\nDestino: {target}\nUbicación: {location}\nSesión: {session}",
      destructiveDisabled: "Las operaciones destructivas están deshabilitadas por la configuración del servidor.",
      failedToConnect: "Error al conectar.", restored: "Datos de conexión restaurados desde el navegador.",
      loadedSummary: "{folders} carpeta(s) y {files} archivo(s) cargados desde {target}.",
      unexpectedStorageError: "Error inesperado al acceder al storage.", backendError: "Error devuelto por el backend local:\n{message}",
      noFailureDetails: "Fallo sin detalles adicionales.",
      fillAdls: "Completa el nombre de la cuenta, el nombre del contenedor y la access key.", fillGcs: "Completa bucket y service account JSON.",
      invalidServiceAccountJson: "El service account JSON debe ser un JSON válido.", fillMinio: "Completa endpoint, bucket, access key ID y secret access key.",
      fillS3: "Completa región, bucket y credenciales.",
    },
    table: { name: "Nombre", type: "Tipo", size: "Tamaño", date: "Fecha", column: "columna_{index}" },
  },
  it: {
    language: { label: "Lingua" },
    connection: { kicker: "Connessione", title: "Accesso allo Storage", settings: "Impostazioni della connessione", connect: "Connetti" },
    providers: {
      kicker: "Provider", ariaLabel: "Provider di storage",
      s3: "Bucket AWS con regione e chiavi di accesso.",
      adls: "Azure Data Lake Storage Gen2 con account e nome del contenitore.",
      gcs: "Google Cloud Storage con bucket e JSON del service account.",
      minio: "Storage compatibile con S3 con endpoint personalizzato e chiavi di accesso.",
    },
    s3: { ariaLabel: "Impostazioni di connessione S3", kicker: "Connessione S3", copy: "Usa le credenziali del bucket AWS per questa connessione." },
    adls: { ariaLabel: "Impostazioni di connessione ADLS", kicker: "Connessione ADLS", copy: "Usa le credenziali dell'account Azure Data Lake Storage Gen2 e il nome del contenitore." },
    gcs: { ariaLabel: "Impostazioni di connessione GCS", kicker: "Connessione GCS", copy: "Usa un bucket Google Cloud Storage e una chiave JSON del service account." },
    minio: { ariaLabel: "Impostazioni di connessione MinIO", kicker: "Connessione MinIO", copy: "Usa un endpoint MinIO personalizzato con chiavi compatibili con S3.", ignoreTlsErrors: "Ignora gli errori del certificato HTTPS per questa connessione MinIO" },
    fields: {
      region: "Regione", bucket: "Bucket", accessKeyId: "Access Key ID", secretAccessKey: "Secret Access Key",
      accountName: "Nome account", containerName: "Nome contenitore", accessKey: "Access Key",
      bucketOrUrl: "Bucket o URL", projectId: "Project ID (compilato automaticamente dal JSON quando presente)",
      serviceAccountJson: "Service Account JSON", endpoint: "Endpoint",
    },
    placeholders: {
      region: "us-east-1", bucket: "mio-bucket", accessKeyId: "AKIA...", secretAccessKey: "********",
      accountName: "mioaccount", fileSystem: "mio-contenitore", gcsBucket: "gs://mio-bucket o mio-bucket",
      projectId: "mio-progetto", serviceAccountJson: '{"type":"service_account", ...}', endpoint: "http://localhost:9000",
      minioAccessKeyId: "minioadmin",
    },
    browser: {
      kicker: "Esploratore Oggetti", title: "Oggetti", currentPrefix: "Prefisso corrente", refresh: "Aggiorna",
      clearPrefix: "Cancella prefisso", connectToList: "Connettiti per elencare gli oggetti dello storage.",
      noItems: "Nessun elemento trovato in questo prefisso.", loading: "Caricamento oggetti...", failed: "Errore durante l'elenco degli oggetti.",
      folder: "Cartella", file: "File", actions: "Azioni", deleteFile: "Elimina file",
      destructiveDisabled: "Le azioni di eliminazione sono disabilitate dal server.",
      openIceberg: "Apri come Iceberg",
      openFolders: "Vedi cartelle",
      icebergSummary: "Tabella Iceberg rilevata. Snapshot corrente: {snapshotId}. File dati: {dataFileCount}. Formato: {dataFormat}.",
    },
    preview: {
      kicker: "Anteprima File", title: "Anteprima", view: "Vista", table: "Tabella", raw: "Raw", rows: "Righe", all: "Tutte",
      order: "Ordine", normal: "Normale", reverse: "Più recenti prima", snapshot: "Snapshot", download: "Scarica file",
      selectCompatible: "Seleziona un file compatibile `.csv`, `.json`, `.dfm`, `.md`, `.txt`, `.parquet`, `.avro`, `.orc`, `.gz` o `.snappy` da visualizzare.",
      noFileSelected: "Nessun file selezionato.", reading: "Lettura file...", loadingFor: "Caricamento anteprima di {key}...",
      unsupportedFormat: "Formato di anteprima non supportato: {key}",
      unsupportedBody: "Seleziona un file .csv, .json, .jsonl, .ndjson, .dfm, .md, .txt, .parquet, .parq, .avro, .orc o un .gz/.snappy corrispondente.",
      emptyFile: "File vuoto.", emptyBody: "Il file non contiene righe da mostrare.", failed: "Errore durante il caricamento dell'anteprima.",
      rawModeSuffix: "modalità raw", formatSuffix: "formato {format}", reverseOrder: "ordine inverso",
      showingRows: "{key} · visualizzazione di {count} righe di esempio{orderSuffix}{modeSuffix}{formatSuffix}{dfmSuffix}",
      snapshotSuffix: " · snapshot {snapshotId}",
      dfmFound: " · DFM: {dfmKey} ({count} colonne)", dfmNotFound: " · DFM non trovato",
    },
    delete: {
      confirmKicker: "Azione Distruttiva", confirmTitle: "Elimina prefisso",
      confirmBody: "Sei assolutamente sicuro di voler eliminare tutti i file sotto questo prefisso?",
      confirmAction: "Elimina file", progressKicker: "Eliminazione in corso", progressTitle: "Eliminazione file",
      progressBody: "Il prefisso selezionato è in fase di eliminazione. L'operazione può richiedere tempo se coinvolge molti file.",
      cancelled: "Eliminazione del prefisso annullata.", deleting: "Eliminazione di tutti gli oggetti sotto {prefix}...", deleted: "{count} oggetto/i eliminato/i da {prefix}.",
      confirmFileTitle: "Elimina file", confirmFileBody: "Sei assolutamente sicuro di voler eliminare questo file?",
      confirmFileAction: "Elimina file", fileCancelled: "Eliminazione del file annullata.", deletingFile: "Eliminazione di {key}...",
      fileDeleted: "{key} eliminato.", progressFileTitle: "Eliminazione file",
      progressFileBody: "Il file selezionato è in fase di eliminazione.",
    },
    seed: {
      button: "Crea campioni Iceberg", kicker: "Dati di Esempio", confirmTitle: "Crea tabelle Iceberg di esempio",
      confirmBody: "Questo creerà fixture Iceberg di sviluppo nella destinazione connessa usando un prefisso fisso.",
      confirmAction: "Crea campioni", progressTitle: "Creazione tabelle di esempio",
      progressBody: "Il backend sta preparando tabelle Iceberg di esempio e le sta caricando nello storage connesso.",
      resultTitle: "Tabelle di esempio create", resultBody: "Le tabelle Iceberg di esempio sono ora disponibili nello storage connesso.",
      navigateAction: "Apri prefisso campioni", createdTablesTitle: "Tabelle create", warningsTitle: "Avvisi",
      generating: "Creazione delle tabelle Iceberg di esempio in {prefix}...", generatedStatus: "Tabelle Iceberg di esempio create in {prefix}.",
      prefixRequired: "Il prefisso di seed e obbligatorio.",
    },
    common: { cancel: "Annulla", close: "Chiudi" },
    messages: {
      enterCredentials: "Inserisci le credenziali e fai clic su connetti.", startupDiagnostic: "Frontend configurato per usare l'API locale.\nAvvia il server Node e apri l'applicazione su http://localhost:8086.",
      connecting: "Connessione...", connectingTo: "Connessione a {target} ({location})...", validateAccess: "Chiamata al backend locale per validare l'accesso allo storage...",
      connectionOk: "Connessione OK tramite il backend locale.\nProvider: {provider}\nDestinazione: {target}\nPosizione: {location}\nSessione: {session}",
      destructiveDisabled: "Le operazioni distruttive sono disabilitate dalla configurazione del server.",
      failedToConnect: "Connessione non riuscita.", restored: "Dati di connessione ripristinati dal browser.",
      loadedSummary: "{folders} cartella/e e {files} file caricati da {target}.",
      unexpectedStorageError: "Errore imprevisto durante l'accesso allo storage.", backendError: "Errore restituito dal backend locale:\n{message}",
      noFailureDetails: "Errore senza dettagli aggiuntivi.",
      fillAdls: "Compila nome account, nome contenitore e access key.", fillGcs: "Compila bucket e service account JSON.",
      invalidServiceAccountJson: "Il service account JSON deve essere un JSON valido.", fillMinio: "Compila endpoint, bucket, access key ID e secret access key.",
      fillS3: "Compila regione, bucket e credenziali.",
    },
    table: { name: "Nome", type: "Tipo", size: "Dimensione", date: "Data", column: "colonna_{index}" },
  },
};

const elements = {
  appVersionText: document.querySelector("#appVersionText"),
  connectionPanel: document.querySelector("#connectionPanel"),
  languageSelect: document.querySelector("#languageSelect"),
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
  toggleIcebergModeButton: document.querySelector("#toggleIcebergModeButton"),
  seedIcebergButton: document.querySelector("#seedIcebergButton"),
  refreshButton: document.querySelector("#refreshButton"),
  clearPrefixButton: document.querySelector("#clearPrefixButton"),
  previewMeta: document.querySelector("#previewMeta"),
  previewTableWrap: document.querySelector("#previewTableWrap"),
  previewMode: document.querySelector("#previewMode"),
  previewRowLimit: document.querySelector("#previewRowLimit"),
  previewRowOrder: document.querySelector("#previewRowOrder"),
  icebergSnapshotControl: document.querySelector("#icebergSnapshotControl"),
  icebergSnapshotSelect: document.querySelector("#icebergSnapshotSelect"),
  downloadButton: document.querySelector("#downloadButton"),
  confirmModal: document.querySelector("#confirmModal"),
  confirmModalTitle: document.querySelector("#confirmModalTitle"),
  confirmModalBody: document.querySelector("#confirmModalBody"),
  confirmModalPrefix: document.querySelector("#confirmModalPrefix"),
  confirmModalCancel: document.querySelector("#confirmModalCancel"),
  confirmModalConfirm: document.querySelector("#confirmModalConfirm"),
  seedConfirmModal: document.querySelector("#seedConfirmModal"),
  seedConfirmTitle: document.querySelector("#seedConfirmTitle"),
  seedConfirmBody: document.querySelector("#seedConfirmBody"),
  seedConfirmPrefix: document.querySelector("#seedConfirmPrefix"),
  seedConfirmCancel: document.querySelector("#seedConfirmCancel"),
  seedConfirmConfirm: document.querySelector("#seedConfirmConfirm"),
  seedProgressModal: document.querySelector("#seedProgressModal"),
  seedProgressTitle: document.querySelector("#seedProgressTitle"),
  seedProgressBody: document.querySelector("#seedProgressBody"),
  seedProgressPrefix: document.querySelector("#seedProgressPrefix"),
  seedResultModal: document.querySelector("#seedResultModal"),
  seedResultTitle: document.querySelector("#seedResultTitle"),
  seedResultBody: document.querySelector("#seedResultBody"),
  seedResultPrefix: document.querySelector("#seedResultPrefix"),
  seedResultTables: document.querySelector("#seedResultTables"),
  seedResultWarnings: document.querySelector("#seedResultWarnings"),
  seedResultClose: document.querySelector("#seedResultClose"),
  seedResultNavigate: document.querySelector("#seedResultNavigate"),
  deleteProgressModal: document.querySelector("#deleteProgressModal"),
  deleteProgressTitle: document.querySelector("#deleteProgressTitle"),
  deleteProgressBody: document.querySelector("#deleteProgressBody"),
  deleteProgressPrefix: document.querySelector("#deleteProgressPrefix"),
};

state.language = restoreLanguage();
if (elements.languageSelect instanceof HTMLSelectElement) {
  elements.languageSelect.value = state.language;
  elements.languageSelect.addEventListener("change", () => {
    state.language = SUPPORTED_LANGUAGES.includes(elements.languageSelect.value) ? elements.languageSelect.value : "en";
    window.localStorage.setItem(LANGUAGE_STORAGE_KEY, state.language);
    applyLanguage();
  });
}

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
elements.toggleIcebergModeButton.addEventListener("click", toggleIcebergMode);
elements.seedIcebergButton.addEventListener("click", seedIcebergFixtures);
elements.refreshButton.addEventListener("click", () => loadObjects(state.prefix));
elements.clearPrefixButton.addEventListener("click", clearCurrentPrefix);
elements.downloadButton.addEventListener("click", downloadSelectedObject);
elements.previewMode.addEventListener("change", () => {
  syncPreviewModeAvailability(state.selectedKey);
  if (state.browseMode === "iceberg" && state.icebergTable) {
    previewIcebergTable();
  } else if (state.selectedKey) {
    previewObject(state.selectedKey);
  }
});
elements.previewRowLimit.addEventListener("change", () => {
  if (state.browseMode === "iceberg" && state.icebergTable) {
    previewIcebergTable();
  } else if (state.selectedKey) {
    previewObject(state.selectedKey);
  }
});
elements.previewRowOrder.addEventListener("change", () => {
  if (state.browseMode === "iceberg" && state.icebergTable) {
    previewIcebergTable();
  } else if (state.selectedKey) {
    previewObject(state.selectedKey);
  }
});
elements.icebergSnapshotSelect.addEventListener("change", () => {
  state.icebergSnapshotId = elements.icebergSnapshotSelect.value;
  if (state.browseMode === "iceberg" && state.icebergTable) {
    previewIcebergTable();
  }
});

restoreConnectionForm();
syncProviderFields();
applyLanguage();
loadAppVersion();
setStartupDiagnostic();
refreshConnectionSummary();
syncPreviewModeAvailability("");
syncSeedControls();

function restoreLanguage() {
  const stored = window.localStorage.getItem(LANGUAGE_STORAGE_KEY);
  return SUPPORTED_LANGUAGES.includes(stored) ? stored : "en";
}

function t(key, variables = {}) {
  const value = key.split(".").reduce((current, part) => current?.[part], translations[state.language] ?? translations.en);
  const fallback = key.split(".").reduce((current, part) => current?.[part], translations.en);
  const template = typeof value === "string" ? value : typeof fallback === "string" ? fallback : key;
  return template.replace(/\{(\w+)\}/g, (_, name) => `${variables[name] ?? ""}`);
}

function applyLanguage() {
  document.documentElement.lang = state.language;
  if (elements.languageSelect instanceof HTMLSelectElement) {
    elements.languageSelect.value = state.language;
    elements.languageSelect.setAttribute("aria-label", t("language.label"));
  }

  document.querySelectorAll("[data-i18n]").forEach((element) => {
    const key = element.getAttribute("data-i18n");
    if (key) {
      element.textContent = t(key);
    }
  });

  document.querySelectorAll("[data-i18n-placeholder]").forEach((element) => {
    const key = element.getAttribute("data-i18n-placeholder");
    if (key && (element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement)) {
      element.placeholder = t(key);
    }
  });

  document.querySelectorAll("[data-i18n-aria-label]").forEach((element) => {
    const key = element.getAttribute("data-i18n-aria-label");
    if (key) {
      element.setAttribute("aria-label", t(key));
    }
  });

  document.querySelectorAll("[data-i18n-title]").forEach((element) => {
    const key = element.getAttribute("data-i18n-title");
    if (key) {
      element.setAttribute("title", t(key));
    }
  });

  if (!elements.connectionStatus.textContent.trim()) {
    setConnectionStatus(t("messages.enterCredentials"));
  }
  if (!elements.diagnosticBox.textContent.trim()) {
    setDiagnosticMessage(t("messages.startupDiagnostic"));
  }
  if (!state.sessionId && !state.objectItems.length) {
    renderObjectPlaceholder(t("browser.connectToList"));
  }
  if (!state.selectedKey && elements.previewTableWrap.classList.contains("empty-state")) {
    resetPreview(t("preview.selectCompatible"), false, t("preview.noFileSelected"));
  }
  renderCurrentPrefix();
  syncIcebergModeToggle();
  syncIcebergSnapshotControl();
  if (state.objectItems.length) {
    renderObjectList();
  }
  syncDestructiveControls();
  if (state.selectedKey && state.sessionId) {
    previewObject(state.selectedKey);
  }
}

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
  state.browseMode = "raw";
  state.icebergTable = null;
  state.icebergAvailable = false;
  state.icebergSnapshotId = "";
  state.sessionId = "";

  elements.refreshButton.disabled = true;
  elements.clearPrefixButton.disabled = true;
  syncSeedControls();
  elements.downloadButton.disabled = true;
  syncPreviewModeAvailability("");
  renderObjectPlaceholder(t("messages.connecting"));
  resetPreview(t("preview.selectCompatible"));
  setConnectionStatus(t("messages.connectingTo", { target: state.targetName, location: state.locationName }));
  setDiagnosticMessage(t("messages.validateAccess"));

  try {
    const response = await apiFetch("/api/connect", {
      method: "POST",
      body: JSON.stringify(connection),
    });

    state.sessionId = response.sessionId;
    state.provider = response.provider ?? connection.provider;
    state.targetName = response.targetName ?? state.targetName;
    state.locationName = response.locationName ?? state.locationName;
    state.destructiveOperationsEnabled = response.destructiveOperationsEnabled !== false;
    elements.refreshButton.disabled = false;
    syncDestructiveControls();
    syncSeedControls();
    await loadObjects("");
    elements.connectionPanel.open = false;
    refreshConnectionSummary();
    setDiagnosticMessage(
      t("messages.connectionOk", {
        provider: state.provider.toUpperCase(),
        target: state.targetName,
        location: state.locationName,
        session: state.sessionId,
      }),
    );
    if (!state.destructiveOperationsEnabled) {
      setDiagnosticMessage(`${elements.diagnosticBox.textContent}\n\n${t("messages.destructiveDisabled")}`);
    }
  } catch (error) {
    renderObjectPlaceholder(t("messages.failedToConnect"));
    setConnectionStatus(getErrorMessage(error), true);
    setDiagnosticMessage(buildDiagnosticMessage(error));
    syncSeedControls();
  }
}

async function loadObjects(prefix) {
  if (!state.sessionId) {
    return;
  }

  state.prefix = prefix;
  state.browseMode = "raw";
  state.icebergTable = null;
  state.icebergAvailable = false;
  state.icebergSnapshotId = "";
  renderCurrentPrefix();
  syncIcebergModeToggle();
  syncIcebergSnapshotControl();
  syncDestructiveControls();
  renderObjectPlaceholder(t("browser.loading"));
  resetPreview(t("preview.selectCompatible"));

  try {
    const response = await apiFetch(
      `/api/objects?sessionId=${encodeURIComponent(state.sessionId)}&prefix=${encodeURIComponent(prefix)}`,
    );
    state.objectItems = response.items ?? [];
    renderObjectList();
    await refreshIcebergAvailability(prefix);
    setConnectionStatus(
      t("messages.loadedSummary", {
        folders: response.summary?.folders ?? 0,
        files: response.summary?.files ?? 0,
        target: state.targetName,
      }),
    );
  } catch (error) {
    renderObjectPlaceholder(t("browser.failed"));
    state.icebergAvailable = false;
    syncIcebergModeToggle();
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
    crumb.addEventListener("click", () => openPrefix(targetPrefix));
    elements.currentPrefix.appendChild(crumb);
  });
}

function renderObjectList() {
  if (!state.objectItems.length) {
    renderObjectPlaceholder(t("browser.noItems"));
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
    { key: "name", label: t("table.name") },
    { key: "type", label: t("table.type") },
    { key: "size", label: t("table.size") },
    { key: "lastModified", label: t("table.date") },
    { key: "actions", label: t("browser.actions"), sortable: false },
  ].forEach((column) => {
    const th = document.createElement("th");
    if (column.sortable === false) {
      th.textContent = column.label;
    } else {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "sort-button";
      button.textContent = getSortLabel(column.label, column.key);
      button.addEventListener("click", () => toggleSort(column.key));
      th.appendChild(button);
    }
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
    typeCell.textContent = item.type === "folder" ? t("browser.folder") : t("browser.file");

    const sizeCell = document.createElement("td");
    sizeCell.textContent = item.type === "folder" ? "—" : formatBytes(item.size);

    const dateCell = document.createElement("td");
    dateCell.textContent =
      item.type === "folder" || !item.lastModified
        ? "—"
        : new Date(item.lastModified).toLocaleString(DATE_LOCALES[state.language] ?? "en-US");

    const actionsCell = document.createElement("td");
    actionsCell.className = "object-actions-cell";
    if (item.type === "file" && state.destructiveOperationsEnabled) {
      const deleteButton = document.createElement("button");
      deleteButton.type = "button";
      deleteButton.className = "ghost-button danger-ghost-button icon-button row-action-icon-button";
      deleteButton.setAttribute("aria-label", t("browser.deleteFile"));
      deleteButton.setAttribute("title", t("browser.deleteFile"));
      deleteButton.innerHTML = trashIconSvg();
      deleteButton.addEventListener("click", (event) => {
        event.stopPropagation();
        deleteObjectFile(item.key);
      });
      actionsCell.appendChild(deleteButton);
    } else {
      actionsCell.textContent = "—";
    }

    row.append(nameCell, typeCell, sizeCell, dateCell, actionsCell);
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
    openPrefix(item.key);
    return;
  }

  previewObject(item.key);
}

async function openPrefix(prefix) {
  if (!state.sessionId) {
    return;
  }

  try {
    const response = await apiFetch(
      `/api/iceberg/inspect?sessionId=${encodeURIComponent(state.sessionId)}&prefix=${encodeURIComponent(prefix)}`,
    );

    if (response.isIceberg) {
      state.icebergAvailable = true;
      await openIcebergTable(response);
      return;
    }
    state.icebergAvailable = false;
    state.icebergSnapshotId = "";
    syncIcebergModeToggle();
    syncIcebergSnapshotControl();
  } catch (error) {
    setConnectionStatus(getErrorMessage(error), true);
    setDiagnosticMessage(buildDiagnosticMessage(error));
    return;
  }

  await loadObjects(prefix);
}

async function openIcebergTable(table) {
  state.prefix = table.tablePrefix ?? "";
  state.selectedKey = "";
  state.browseMode = "iceberg";
  state.icebergTable = table;
  state.icebergAvailable = true;
  state.icebergSnapshotId = resolveIcebergSnapshotId(table);
  elements.downloadButton.disabled = true;
  elements.previewMode.value = "table";
  elements.previewMode.disabled = true;
  renderCurrentPrefix();
  syncIcebergModeToggle();
  syncIcebergSnapshotControl();
  syncDestructiveControls();
  renderObjectPlaceholder(
    t("browser.icebergSummary", {
      snapshotId: state.icebergSnapshotId || table.snapshotId || "n/a",
      dataFileCount: table.dataFileCount ?? 0,
      dataFormat: table.dataFormat ?? "unknown",
    }),
  );
  setConnectionStatus(
    t("browser.icebergSummary", {
      snapshotId: state.icebergSnapshotId || table.snapshotId || "n/a",
      dataFileCount: table.dataFileCount ?? 0,
      dataFormat: table.dataFormat ?? "unknown",
    }),
  );
  await previewIcebergTable();
}

async function refreshIcebergAvailability(prefix) {
  if (!state.sessionId || !prefix) {
    state.icebergAvailable = false;
    syncIcebergModeToggle();
    return;
  }

  try {
    const response = await apiFetch(
      `/api/iceberg/inspect?sessionId=${encodeURIComponent(state.sessionId)}&prefix=${encodeURIComponent(prefix)}`,
    );
    state.icebergAvailable = response.isIceberg === true;
    if (state.icebergAvailable) {
      state.icebergTable = response;
      state.icebergSnapshotId = resolveIcebergSnapshotId(response);
    } else {
      state.icebergSnapshotId = "";
    }
  } catch {
    state.icebergAvailable = false;
    state.icebergSnapshotId = "";
  }

  syncIcebergModeToggle();
  syncIcebergSnapshotControl();
}

async function toggleIcebergMode() {
  if (!state.sessionId || !state.prefix || !state.icebergAvailable) {
    return;
  }

  if (state.browseMode === "iceberg") {
    await loadObjects(state.prefix);
    return;
  }

  if (state.icebergTable?.isIceberg) {
    await openIcebergTable(state.icebergTable);
    return;
  }

  await openPrefix(state.prefix);
}

async function previewObject(key) {
  state.selectedKey = key;
  elements.downloadButton.disabled = false;

  if (!isPreviewableFile(key)) {
    resetPreview(
      t("preview.unsupportedFormat", { key }),
      false,
      t("preview.unsupportedBody"),
    );
    return;
  }

  syncPreviewModeAvailability(key);
  elements.previewMeta.textContent = t("preview.loadingFor", { key });
  elements.previewTableWrap.className = "preview-table-wrap empty-state";
  elements.previewTableWrap.textContent = t("preview.reading");

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
      response.previewFormat === "csv" || response.previewFormat === "csv.gz" || response.previewFormat === "csv.snappy"
        ? response.dfmKey
          ? t("preview.dfmFound", { dfmKey: response.dfmKey, count: response.metadataColumns?.length ?? 0 })
          : t("preview.dfmNotFound")
        : "";
    const modeSuffix = response.previewMode === "raw" ? ` · ${t("preview.rawModeSuffix")}` : "";
    const formatSuffix = response.previewFormat ? ` · ${t("preview.formatSuffix", { format: response.previewFormat })}` : "";
    const orderLabel = getPreviewRowOrder() === "reverse" ? ` · ${t("preview.reverseOrder")}` : "";
    elements.previewMeta.textContent =
      t("preview.showingRows", {
        key,
        count: response.lineCount ?? 0,
        orderSuffix: orderLabel,
        modeSuffix,
        formatSuffix,
        dfmSuffix,
      });
  } catch (error) {
    resetPreview(getErrorMessage(error), true, t("preview.failed"));
  }
}

async function previewIcebergTable() {
  if (!state.sessionId || !state.icebergTable?.tablePrefix) {
    return;
  }

  const snapshotId = getSelectedIcebergSnapshotId();
  elements.previewMeta.textContent = t("preview.loadingFor", { key: state.icebergTable.tablePrefix });
  elements.previewTableWrap.className = "preview-table-wrap empty-state";
  elements.previewTableWrap.textContent = t("preview.reading");

  try {
    const rowLimit = getPreviewRowLimit();
    const rowOrder = getPreviewRowOrder();
    const response = await apiFetch(
      `/api/iceberg/preview?sessionId=${encodeURIComponent(state.sessionId)}&prefix=${encodeURIComponent(state.icebergTable.tablePrefix)}&limit=${encodeURIComponent(rowLimit)}&order=${encodeURIComponent(rowOrder)}&snapshotId=${encodeURIComponent(snapshotId)}`,
    );

    const preview = buildPreviewModel(
      response.rows ?? [],
      response.metadataColumns ?? [],
      response.order ?? rowOrder,
    );
    state.icebergTable = {
      ...state.icebergTable,
      ...response.icebergMeta,
      snapshots: response.icebergMeta?.snapshots ?? state.icebergTable.snapshots ?? [],
    };
    state.icebergSnapshotId = String(response.icebergMeta?.snapshotId ?? snapshotId);
    syncIcebergSnapshotControl();
    renderPreviewTable(preview);
    elements.previewMeta.textContent = t("preview.showingRows", {
      key: state.icebergTable.tablePrefix,
      count: response.lineCount ?? 0,
      orderSuffix: getPreviewRowOrder() === "reverse" ? ` · ${t("preview.reverseOrder")}` : "",
      modeSuffix: "",
      formatSuffix: ` · ${t("preview.formatSuffix", { format: "iceberg" })}${t("preview.snapshotSuffix", { snapshotId: state.icebergSnapshotId })}`,
      dfmSuffix: "",
    });
  } catch (error) {
    resetPreview(getErrorMessage(error), true, t("preview.failed"));
  }
}

function resolveIcebergSnapshotId(table) {
  const availableSnapshots = Array.isArray(table?.snapshots) ? table.snapshots : [];

  if (state.icebergSnapshotId && availableSnapshots.some((snapshot) => String(snapshot.snapshotId) === state.icebergSnapshotId)) {
    return state.icebergSnapshotId;
  }

  return String(table?.snapshotId ?? table?.currentSnapshotId ?? availableSnapshots[0]?.snapshotId ?? "");
}

function getSelectedIcebergSnapshotId() {
  const selectedValue = elements.icebergSnapshotSelect.value || state.icebergSnapshotId;
  return selectedValue || resolveIcebergSnapshotId(state.icebergTable);
}

function syncIcebergSnapshotControl() {
  const hasSnapshots = state.browseMode === "iceberg" && Array.isArray(state.icebergTable?.snapshots) && state.icebergTable.snapshots.length > 0;
  elements.icebergSnapshotControl.hidden = !hasSnapshots;

  if (!hasSnapshots) {
    elements.icebergSnapshotSelect.innerHTML = "";
    elements.icebergSnapshotSelect.disabled = true;
    return;
  }

  const selectedSnapshotId = resolveIcebergSnapshotId(state.icebergTable);
  elements.icebergSnapshotSelect.innerHTML = "";

  state.icebergTable.snapshots.forEach((snapshot) => {
    const option = document.createElement("option");
    option.value = String(snapshot.snapshotId);
    option.textContent = formatIcebergSnapshotOption(snapshot);
    option.selected = option.value === selectedSnapshotId;
    elements.icebergSnapshotSelect.appendChild(option);
  });

  state.icebergSnapshotId = selectedSnapshotId;
  elements.icebergSnapshotSelect.disabled = state.icebergTable.snapshots.length <= 1;
  elements.icebergSnapshotSelect.setAttribute("aria-label", t("preview.snapshot"));
}

function formatIcebergSnapshotOption(snapshot) {
  const parts = [String(snapshot.snapshotId)];

  if (snapshot?.committedAt) {
    parts.push(new Date(snapshot.committedAt).toLocaleString(DATE_LOCALES[state.language] ?? "en-US"));
  }

  if (snapshot?.operation) {
    parts.push(String(snapshot.operation));
  }

  return parts.join(" · ");
}

function renderPreviewTable(preview) {
  if (!preview.headerRow.length) {
    resetPreview(t("preview.emptyFile"), false, t("preview.emptyBody"));
    return;
  }

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const tbody = document.createElement("tbody");
  const headerTr = document.createElement("tr");

  preview.headerRow.forEach((column, index) => {
    const th = document.createElement("th");
    th.textContent = column || t("table.column", { index: index + 1 });
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

  return ["dfm", "dfm.gz", "dfm.snappy"].includes(previewFormat);
}

function parseRawJsonPreview(rawText, previewFormat) {
  if (["jsonl", "jsonl.gz", "jsonl.snappy", "ndjson", "ndjson.gz", "ndjson.snappy"].includes(previewFormat)) {
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
  return Array.from({ length: columnCount }, (_, index) => t("table.column", { index: index + 1 }));
}

function normalizeHeaderRow(headerRow, columnCount) {
  return Array.from({ length: columnCount }, (_, index) => headerRow[index] || t("table.column", { index: index + 1 }));
}

function normalizeRowLength(row, columnCount) {
  return Array.from({ length: columnCount }, (_, index) => row[index] ?? "");
}

function resetPreview(message, isError = false, bodyMessage) {
  elements.previewMeta.className = isError ? "preview-meta error-text" : "preview-meta muted";
  elements.previewMeta.textContent = message;
  elements.previewTableWrap.className = "preview-table-wrap empty-state";
  elements.previewTableWrap.textContent =
    bodyMessage ?? (isError ? t("preview.failed") : t("preview.noFileSelected"));
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

  return t("messages.unexpectedStorageError");
}

function buildDiagnosticMessage(error) {
  if (error instanceof Error) {
    return t("messages.backendError", { message: error.message });
  }

  return t("messages.noFailureDetails");
}

async function clearCurrentPrefix() {
  if (!state.sessionId || !state.prefix) {
    return;
  }

  if (!state.destructiveOperationsEnabled) {
    setConnectionStatus(t("messages.destructiveDisabled"), true);
    return;
  }

  const confirmation = await confirmDeletion({
    title: t("delete.confirmTitle"),
    body: t("delete.confirmBody"),
    actionLabel: t("delete.confirmAction"),
    target: state.prefix,
  });

  if (!confirmation) {
    setConnectionStatus(t("delete.cancelled"));
    return;
  }

  elements.clearPrefixButton.disabled = true;
  showDeleteProgress({
    title: t("delete.progressTitle"),
    body: t("delete.progressBody"),
    target: state.prefix,
  });
  setConnectionStatus(t("delete.deleting", { prefix: state.prefix }));

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
    resetPreview(t("preview.selectCompatible"));
    setConnectionStatus(
      t("delete.deleted", { count: response.deletedCount ?? 0, prefix: state.prefix }),
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

async function deleteObjectFile(key) {
  if (!state.sessionId || !key) {
    return;
  }

  if (!state.destructiveOperationsEnabled) {
    setConnectionStatus(t("messages.destructiveDisabled"), true);
    return;
  }

  const confirmation = await confirmDeletion({
    title: t("delete.confirmFileTitle"),
    body: t("delete.confirmFileBody"),
    actionLabel: t("delete.confirmFileAction"),
    target: key,
  });

  if (!confirmation) {
    setConnectionStatus(t("delete.fileCancelled"));
    return;
  }

  showDeleteProgress({
    title: t("delete.progressFileTitle"),
    body: t("delete.progressFileBody"),
    target: key,
  });
  setConnectionStatus(t("delete.deletingFile", { key }));

  try {
    await apiFetch("/api/delete-file", {
      method: "POST",
      body: JSON.stringify({
        sessionId: state.sessionId,
        key,
      }),
    });

    if (state.selectedKey === key) {
      state.selectedKey = "";
      elements.downloadButton.disabled = true;
      syncPreviewModeAvailability("");
      resetPreview(t("preview.selectCompatible"));
    }

    setConnectionStatus(t("delete.fileDeleted", { key }));
    await loadObjects(state.prefix);
  } catch (error) {
    setConnectionStatus(getErrorMessage(error), true);
    setDiagnosticMessage(buildDiagnosticMessage(error));
  } finally {
    hideDeleteProgress();
  }
}

async function seedIcebergFixtures() {
  if (!state.sessionId) {
    console.warn("[seed] skipped: no active session");
    return;
  }

  const suggestedPrefix = getSeedPrefix();
  console.log("[seed] open confirm", { suggestedPrefix, provider: state.provider, sessionId: state.sessionId });
  const confirmed = await confirmSeedGeneration(suggestedPrefix);

  if (!confirmed) {
    console.log("[seed] cancelled");
    return;
  }

  const prefix = normalizeSeedPrefix(elements.seedConfirmPrefix.value);

  if (!prefix) {
    console.warn("[seed] invalid prefix");
    setConnectionStatus(t("seed.prefixRequired"), true);
    return;
  }

  elements.seedIcebergButton.disabled = true;
  showSeedProgress(prefix);
  setConnectionStatus(t("seed.generating", { prefix }));
  setDiagnosticMessage(`Seed request started for ${prefix}`);
  console.log("[seed] request start", { prefix, provider: state.provider, sessionId: state.sessionId });

  try {
    const response = await apiFetch("/api/dev/seed-iceberg", {
      method: "POST",
      body: JSON.stringify({
        sessionId: state.sessionId,
        targetPrefix: prefix,
      }),
    });

    const basePrefix = response.basePrefix ?? prefix;
    console.log("[seed] request completed", response);
    setConnectionStatus(t("seed.generatedStatus", { prefix: basePrefix }));
    setDiagnosticMessage(buildSeedDiagnosticMessage(response));
    showSeedResult(response);
  } catch (error) {
    console.error("[seed] request failed", error);
    setConnectionStatus(getErrorMessage(error), true);
    setDiagnosticMessage(buildDiagnosticMessage(error));
  } finally {
    hideSeedProgress();
    syncSeedControls();
  }
}

function getSeedPrefix() {
  return `${state.prefix || ""}_sample_data/iceberg/`;
}

function normalizeSeedPrefix(value) {
  const trimmed = typeof value === "string" ? value.trim() : "";

  if (!trimmed) {
    return "";
  }

  return trimmed.endsWith("/") ? trimmed : `${trimmed}/`;
}

function buildSeedDiagnosticMessage(response) {
  const warnings = Array.isArray(response.warnings) ? response.warnings : [];

  if (!warnings.length) {
    return setDiagnosticForSeedTables(response.createdTables ?? []);
  }

  return `${setDiagnosticForSeedTables(response.createdTables ?? [])}\n\n${warnings.join("\n")}`;
}

function setDiagnosticForSeedTables(createdTables) {
  if (!Array.isArray(createdTables) || !createdTables.length) {
    return "";
  }

  return createdTables
    .map((table) => `${table.name} · ${table.prefix} · snapshot ${table.currentSnapshotId ?? "n/a"}`)
    .join("\n");
}

function confirmSeedGeneration(prefix) {
  return new Promise((resolve) => {
    const previousActiveElement = document.activeElement;

    const close = (confirmed) => {
      elements.seedConfirmModal.hidden = true;
      document.body.style.overflow = "";
      elements.seedConfirmModal.removeEventListener("click", handleShellClick);
      document.removeEventListener("keydown", handleKeydown);
      elements.seedConfirmCancel.removeEventListener("click", handleCancel);
      elements.seedConfirmConfirm.removeEventListener("click", handleConfirm);
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

    elements.seedConfirmTitle.textContent = t("seed.confirmTitle");
    elements.seedConfirmBody.textContent = t("seed.confirmBody");
    elements.seedConfirmConfirm.textContent = t("seed.confirmAction");
    elements.seedConfirmPrefix.value = prefix;
    elements.seedConfirmModal.hidden = false;
    document.body.style.overflow = "hidden";
    elements.seedConfirmModal.addEventListener("click", handleShellClick);
    document.addEventListener("keydown", handleKeydown);
    elements.seedConfirmCancel.addEventListener("click", handleCancel);
    elements.seedConfirmConfirm.addEventListener("click", handleConfirm);
    elements.seedConfirmCancel.focus();
  });
}

function showSeedProgress(prefix) {
  elements.seedProgressTitle.textContent = t("seed.progressTitle");
  elements.seedProgressBody.textContent = t("seed.progressBody");
  elements.seedProgressPrefix.textContent = prefix;
  elements.seedProgressModal.hidden = false;
  document.body.style.overflow = "hidden";
}

function hideSeedProgress() {
  elements.seedProgressModal.hidden = true;
  document.body.style.overflow = "";
}

function showSeedResult(response) {
  const previousActiveElement = document.activeElement;
  const basePrefix = response.basePrefix ?? getSeedPrefix();
  const createdTables = Array.isArray(response.createdTables) ? response.createdTables : [];
  const warnings = Array.isArray(response.warnings) ? response.warnings : [];

  const close = () => {
    elements.seedResultModal.hidden = true;
    document.body.style.overflow = "";
    elements.seedResultModal.removeEventListener("click", handleShellClick);
    document.removeEventListener("keydown", handleKeydown);
    elements.seedResultClose.removeEventListener("click", close);
    elements.seedResultNavigate.removeEventListener("click", handleNavigate);
    if (previousActiveElement instanceof HTMLElement) {
      previousActiveElement.focus();
    }
  };

  const handleNavigate = async () => {
    close();
    await loadObjects(basePrefix);
  };
  const handleShellClick = (event) => {
    const target = event.target;
    if (target instanceof HTMLElement && target.hasAttribute("data-seed-result-close")) {
      close();
    }
  };
  const handleKeydown = (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      close();
    }
  };

  elements.seedResultTitle.textContent = t("seed.resultTitle");
  elements.seedResultBody.textContent = t("seed.resultBody");
  elements.seedResultPrefix.textContent = basePrefix;
  elements.seedResultTables.innerHTML = "";
  createdTables.forEach((table) => {
    const item = document.createElement("li");
    item.textContent = `${table.name} · ${table.format} · ${table.prefix}`;
    elements.seedResultTables.appendChild(item);
  });
  elements.seedResultWarnings.innerHTML = "";
  if (warnings.length) {
    warnings.forEach((warning) => {
      const item = document.createElement("li");
      item.textContent = warning;
      elements.seedResultWarnings.appendChild(item);
    });
  } else {
    const item = document.createElement("li");
    item.textContent = "—";
    elements.seedResultWarnings.appendChild(item);
  }
  elements.seedResultModal.hidden = false;
  document.body.style.overflow = "hidden";
  elements.seedResultModal.addEventListener("click", handleShellClick);
  document.addEventListener("keydown", handleKeydown);
  elements.seedResultClose.addEventListener("click", close);
  elements.seedResultNavigate.addEventListener("click", handleNavigate);
  elements.seedResultNavigate.focus();
}

function confirmDeletion({ title, body, actionLabel, target }) {
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

    elements.confirmModalTitle.textContent = title;
    elements.confirmModalBody.textContent = body;
    elements.confirmModalConfirm.textContent = actionLabel;
    elements.confirmModalPrefix.textContent = target;
    elements.confirmModal.hidden = false;
    document.body.style.overflow = "hidden";
    elements.confirmModal.addEventListener("click", handleShellClick);
    document.addEventListener("keydown", handleKeydown);
    elements.confirmModalCancel.addEventListener("click", handleCancel);
    elements.confirmModalConfirm.addEventListener("click", handleConfirm);
    elements.confirmModalCancel.focus();
  });
}

function showDeleteProgress({ title, body, target }) {
  elements.deleteProgressTitle.textContent = title;
  elements.deleteProgressBody.textContent = body;
  elements.deleteProgressPrefix.textContent = target;
  elements.deleteProgressModal.hidden = false;
  document.body.style.overflow = "hidden";
}

function hideDeleteProgress() {
  elements.deleteProgressModal.hidden = true;
  document.body.style.overflow = "";
}

function syncDestructiveControls() {
  const canDeletePrefix =
    state.destructiveOperationsEnabled && state.browseMode !== "iceberg" && Boolean(state.prefix) && Boolean(state.sessionId);
  elements.clearPrefixButton.disabled = !canDeletePrefix;
  elements.clearPrefixButton.hidden = !state.destructiveOperationsEnabled || state.browseMode === "iceberg";
}

function syncIcebergModeToggle() {
  const available = Boolean(state.sessionId) && Boolean(state.prefix) && state.icebergAvailable;
  elements.toggleIcebergModeButton.hidden = !available;
  elements.toggleIcebergModeButton.disabled = !available;
  elements.toggleIcebergModeButton.textContent =
    state.browseMode === "iceberg" ? t("browser.openFolders") : t("browser.openIceberg");
}

function syncSeedControls() {
  const enabled = state.seedIcebergEnabled === true;
  elements.seedIcebergButton.hidden = !enabled;
  elements.seedIcebergButton.disabled = !enabled || !state.sessionId;
}

function setStartupDiagnostic() {
  setDiagnosticMessage(t("messages.startupDiagnostic"));
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
    setConnectionStatus(t("messages.restored"));
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
      return t("messages.fillAdls");
    }

    return "";
  }

  if (connection.provider === "gcs") {
    if (!connection.gcsBucket || !connection.serviceAccountJson) {
      return t("messages.fillGcs");
    }

    try {
      JSON.parse(connection.serviceAccountJson);
    } catch {
      return t("messages.invalidServiceAccountJson");
    }

    return "";
  }

  if (connection.provider === "minio") {
    if (!connection.endpoint || !connection.minioBucket || !connection.minioAccessKeyId || !connection.minioSecretAccessKey) {
      return t("messages.fillMinio");
    }

    return "";
  }

  if (!connection.region || !connection.bucket || !connection.accessKeyId || !connection.secretAccessKey) {
    return t("messages.fillS3");
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
    normalizedKey.endsWith(".csv.snappy") ||
    normalizedKey.endsWith(".dfm") ||
    normalizedKey.endsWith(".dfm.gz") ||
    normalizedKey.endsWith(".dfm.snappy") ||
    normalizedKey.endsWith(".md") ||
    normalizedKey.endsWith(".md.gz") ||
    normalizedKey.endsWith(".md.snappy") ||
    normalizedKey.endsWith(".txt") ||
    normalizedKey.endsWith(".txt.gz") ||
    normalizedKey.endsWith(".txt.snappy") ||
    normalizedKey.endsWith(".json") ||
    normalizedKey.endsWith(".json.gz") ||
    normalizedKey.endsWith(".json.snappy") ||
    normalizedKey.endsWith(".jsonl") ||
    normalizedKey.endsWith(".jsonl.gz") ||
    normalizedKey.endsWith(".jsonl.snappy") ||
    normalizedKey.endsWith(".ndjson") ||
    normalizedKey.endsWith(".ndjson.gz") ||
    normalizedKey.endsWith(".ndjson.snappy") ||
    normalizedKey.endsWith(".parquet") ||
    normalizedKey.endsWith(".parquet.gz") ||
    normalizedKey.endsWith(".parquet.snappy") ||
    normalizedKey.endsWith(".gzip.parquet") ||
    normalizedKey.endsWith(".gz.parquet") ||
    normalizedKey.endsWith(".snappy.parquet") ||
    normalizedKey.endsWith(".parq") ||
    normalizedKey.endsWith(".parq.gz") ||
    normalizedKey.endsWith(".parq.snappy") ||
    normalizedKey.endsWith(".gzip.parq") ||
    normalizedKey.endsWith(".gz.parq") ||
    normalizedKey.endsWith(".snappy.parq") ||
    normalizedKey.endsWith(".avro") ||
    normalizedKey.endsWith(".avro.gz") ||
    normalizedKey.endsWith(".avro.snappy") ||
    normalizedKey.endsWith(".gzip.avro") ||
    normalizedKey.endsWith(".gz.avro") ||
    normalizedKey.endsWith(".snappy.avro") ||
    normalizedKey.endsWith(".orc") ||
    normalizedKey.endsWith(".orc.gz") ||
    normalizedKey.endsWith(".orc.snappy") ||
    normalizedKey.endsWith(".snappy.orc")
  );
}

function isJsonPreviewFile(key) {
  const normalizedKey = key.toLowerCase();
  return (
    normalizedKey.endsWith(".dfm") ||
    normalizedKey.endsWith(".dfm.gz") ||
    normalizedKey.endsWith(".dfm.snappy") ||
    normalizedKey.endsWith(".json") ||
    normalizedKey.endsWith(".json.gz") ||
    normalizedKey.endsWith(".json.snappy") ||
    normalizedKey.endsWith(".jsonl") ||
    normalizedKey.endsWith(".jsonl.gz") ||
    normalizedKey.endsWith(".jsonl.snappy") ||
    normalizedKey.endsWith(".ndjson") ||
    normalizedKey.endsWith(".ndjson.gz") ||
    normalizedKey.endsWith(".ndjson.snappy")
  );
}

function syncPreviewModeAvailability(key) {
  const allowRaw = isJsonPreviewFile(key);
  const rawOnly = isRawOnlyPreviewFile(key);
  elements.previewMode.disabled = !allowRaw || rawOnly;

  if (rawOnly) {
    elements.previewMode.value = "raw";
    return;
  }

  if (!allowRaw) {
    elements.previewMode.value = "table";
  }
}

function isRawOnlyPreviewFile(key) {
  const normalizedKey = key.toLowerCase();
  return (
    normalizedKey.endsWith(".md") ||
    normalizedKey.endsWith(".md.gz") ||
    normalizedKey.endsWith(".md.snappy") ||
    normalizedKey.endsWith(".txt") ||
    normalizedKey.endsWith(".txt.gz") ||
    normalizedKey.endsWith(".txt.snappy")
  );
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
    : t("connection.settings");
}

async function loadAppVersion() {
  if (!(elements.appVersionText instanceof HTMLElement)) {
    return;
  }

  try {
    const response = await apiFetch("/api/app-info");
    const version = typeof response.version === "string" ? response.version.trim() : "";
    state.seedIcebergEnabled = response.devFeatures?.seedIceberg === true;
    syncSeedControls();

    if (!version) {
      elements.appVersionText.hidden = true;
      return;
    }

    elements.appVersionText.textContent = `v${version}`;
    elements.appVersionText.hidden = false;
  } catch {
    state.seedIcebergEnabled = false;
    syncSeedControls();
    elements.appVersionText.hidden = true;
  }
}

async function apiFetch(url, init = {}) {
  const response = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      "X-App-Language": state.language,
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

function trashIconSvg() {
  return `
    <svg viewBox="0 0 24 24" focusable="false">
      <path d="M9.75 3A2.25 2.25 0 0 0 7.5 5.25V6H4.75a.75.75 0 0 0 0 1.5h.58l.71 10.05A2.25 2.25 0 0 0 8.28 19.5h7.44a2.25 2.25 0 0 0 2.24-1.95l.71-10.05h.58a.75.75 0 0 0 0-1.5H16.5v-.75A2.25 2.25 0 0 0 14.25 3h-4.5Zm0 1.5h4.5a.75.75 0 0 1 .75.75V6H9V5.25a.75.75 0 0 1 .75-.75ZM7.54 7.5l.7 9.94a.75.75 0 0 0 .74.56h7.04a.75.75 0 0 0 .74-.56l.7-9.94H7.54Zm2.71 1.75a.75.75 0 0 1 .75.75v5a.75.75 0 0 1-1.5 0v-5a.75.75 0 0 1 .75-.75Zm3.5 0a.75.75 0 0 1 .75.75v5a.75.75 0 0 1-1.5 0v-5a.75.75 0 0 1 .75-.75Z"></path>
    </svg>
  `;
}
