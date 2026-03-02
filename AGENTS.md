# Objetivo
Este projeto e um explorador web local para buckets S3, com backend Node.js servindo a interface e atuando como proxy para AWS. O fluxo principal e:

1. receber credenciais AWS e dados do bucket pelo frontend
2. criar uma sessao local temporaria no backend
3. listar prefixos e objetos do bucket
4. fazer preview de arquivos suportados
5. baixar arquivos e remover todos os objetos de um prefixo

# Stack e estrutura
- `server.js`: servidor HTTP Node, endpoints `/api/*`, sessao em memoria e acesso ao S3
- `app.js`: logica da interface, estado local, chamadas ao backend e renderizacao
- `index.html`: estrutura da pagina
- `styles.css`: estilos da interface
- `start.sh`: bootstrap local com `npm install` automatico se faltar `node_modules`
- `samples/`: arquivos de amostra usados para desenvolvimento local

O projeto usa JavaScript ESM puro, sem framework frontend e sem Express no backend. Mantenha essa simplicidade, salvo instrucao explicita em contrario.

# Comandos
- instalar dependencias: `npm install`
- subir o servidor: `npm start`
- fluxo padrao local: `./start.sh`

Por padrao a aplicacao sobe em `http://localhost:8086`.

# Regras de desenvolvimento
1. Antes de alterar comportamento, leia `README.md`, `server.js` e `app.js` para preservar o fluxo atual.
2. Sempre que concluir uma entrega, avalie explicitamente se `AGENTS.md` precisa ser atualizado para preservar memoria de longo prazo do projeto.
3. Sempre que concluir uma mudanca relevante, atualize `README.md` e este `AGENTS.md` se houver nova convencao, comando, risco operacional ou decisao de arquitetura.
4. Evite adicionar dependencias novas sem necessidade clara. O projeto hoje e pequeno e intencionalmente direto.
5. Preserve compatibilidade com Node em modo ESM e com execucao local simples via `npm start`.
6. Se criar novos scripts operacionais, documente o uso no `README.md`.

# Regras para backend
1. Toda chamada ao S3 deve continuar passando pelo backend. Nao mover acesso AWS direto para o navegador.
2. Trate entradas de rota e query string como nao confiaveis. Valide `sessionId`, `prefix`, `key`, limites e modos de preview.
3. Ao mexer em sessoes, preserve a expiracao em memoria e revise riscos de vazamento de credenciais.
4. Mudancas no endpoint destrutivo `/api/delete-prefix` exigem cuidado extra. Nunca permitir limpeza da raiz do bucket.
5. Em respostas de erro, prefira mensagens uteis sem expor segredos, chaves ou stack traces desnecessarios.

# Regras para frontend
1. Preserve a interface em portugues, a menos que o usuario peça internacionalizacao.
2. Mantenha a experiencia atual de app local: formulario de conexao, browser de objetos e painel de preview.
3. Evite frameworks, bundlers ou etapas de build se o problema puder ser resolvido no HTML/CSS/JS atual.
4. Ao adicionar controles novos, conecte estado, feedback visual e tratamento de erro de forma consistente com o restante do `app.js`.

# Seguranca e dados sensiveis
1. As credenciais AWS sao informadas pela interface e hoje ficam persistidas no `localStorage`. Qualquer mudanca nessa area deve considerar impacto de seguranca e ser documentada.
2. Nunca registrar `secretAccessKey` em logs, mensagens de erro, dumps de estado ou documentacao.
3. Nao commitar credenciais reais, buckets privados ou dados sensiveis de clientes.
4. Se for necessario testar com AWS real, prefira validacoes minimamente invasivas e confirme claramente qualquer acao destrutiva.

# Validacao
1. Como nao ha suite de testes automatizados no repositorio hoje, valide manualmente o fluxo alterado sempre que possivel.
2. Para mudancas de UI, verifique ao menos: conectar, listar objetos, navegar em pastas, preview e download.
3. Para mudancas em delecao, valide primeiro com prefixos controlados e nunca assuma comportamento seguro sem executar o fluxo.

# Decisoes persistentes
1. Este arquivo deve registrar apenas diretrizes duraveis do projeto.
2. Detalhes temporarios de tarefa ou experimentos nao devem ficar aqui.
3. Se uma decisao mudar o modo de executar, desenvolver ou operar o app, registre a decisao neste arquivo.
