# MultiBucket Explorer

Aplicação web com backend local para:

- navegar por prefixos de um bucket AWS S3
- listar pastas e arquivos
- pré-visualizar arquivos CSV, JSON e Parquet
- baixar arquivos via proxy local

## Como executar

Instale as dependências:

```bash
npm install
```

Suba o servidor:

```bash
npm start
```

Por padrão a aplicação sobe em `http://localhost:8086`.

## Como funciona

- o navegador fala apenas com o servidor local
- o servidor local acessa a AWS usando o SDK
- o bucket S3 não precisa responder ao navegador diretamente

Isso elimina o problema de CORS entre browser e S3 nesse fluxo.

## Campos de conexão

Preencha na interface:

- `Region`
- `Bucket`
- `Access Key ID`
- `Secret Access Key`

Os campos ficam persistidos no `localStorage` do navegador, incluindo a `Secret Access Key`.

## Permissões AWS

As credenciais precisam permitir pelo menos:

- `s3:ListBucket` no bucket
- `s3:GetObject` nos objetos

## Observações

- o backend mantém uma sessão em memória por 12 horas após conectar
- esta solução é adequada para uso local/interno
- para produção, o ideal é não enviar credenciais pelo frontend e usar autenticação do lado servidor
