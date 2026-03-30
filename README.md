# Frutamina Multas

Aplicacao web para login interno, dashboard de multas ativas e sincronizacao com o portal da ANTT pelo proprio painel.

## Stack escolhida

Python foi a melhor escolha para este projeto porque:

- `playwright` e `pdfplumber` ja estao disponiveis no ambiente
- Java nao esta instalado nesta maquina
- a automacao de navegador e muito mais direta em Python neste cenario
- o backend web e leve o suficiente para rodar sem depender de frameworks externos

## Estrutura

- `app.py`: ponto de entrada da aplicacao
- `frutamina_app/web.py`: servidor web, login, sessoes e rotas
- `frutamina_app/scraper.py`: leitura de multas no portal ANTT
- `frutamina_app/sync_manager.py`: execucao embutida de sincronizacao para ambiente local
- `frutamina_app/store.py`: persistencia em JSON/CSV ou PostgreSQL
- `sync_agent.py`: agente local que executa a leitura real da ANTT e envia os resultados ao site
- `templates/`: HTML da tela de login e dashboard
- `static/`: CSS e JavaScript do frontend

## Como executar

```powershell
python .\app.py
```

Endereco padrao:

```text
http://127.0.0.1:8080/login
```

## Credenciais do dashboard

Defina no `.env` ou no ambiente:

```powershell
$env:DASHBOARD_USER = "admin"
$env:DASHBOARD_PASSWORD = "admin123"
```

## Deploy no Railway

Arquivos importantes para o deploy:

- `requirements.txt`
- `Procfile`

### Passo 1: adicionar PostgreSQL

No Railway:

1. Abra o projeto.
2. Clique em `New`.
3. Adicione um banco PostgreSQL.
4. Conecte o banco ao servico web para expor `DATABASE_URL` ou `DATABASE_PUBLIC_URL`.

### Passo 2: variaveis do servico web

Variaveis recomendadas no Railway:

```text
DASHBOARD_USER=admin
DASHBOARD_PASSWORD=admin123
SYNC_MODE=agent
SYNC_AGENT_TOKEN=crie-um-token-forte-aqui
MOCK_SYNC=0
```

Observacoes:

- para a aplicacao subir no Railway, ela precisa escutar em `0.0.0.0` e usar a variavel `PORT`; isso ja foi ajustado no projeto
- o Railway vai usar PostgreSQL automaticamente se `DATABASE_URL` ou `DATABASE_PUBLIC_URL` estiver presente
- o site no Railway passa a apenas registrar o pedido de leitura e armazenar os dados; a leitura real fica a cargo do agente local
- se nenhum banco estiver configurado, o app cai para arquivos locais e esses dados podem sumir em redeploy

## Agente local da ANTT

O agente local e a forma recomendada de fazer a leitura real da ANTT sem depender do Railway para abrir navegador ou resolver CAPTCHA.

### Variaveis do agente local

Defina na maquina que vai rodar o agente:

```powershell
$env:AGENT_SERVER_URL = "https://seu-app.up.railway.app"
$env:SYNC_AGENT_TOKEN = "o-mesmo-token-do-railway"
$env:SYNC_AGENT_NAME = "pc-escritorio"
$env:ANTT_CPF_CNPJ = "seu_cpf_ou_cnpj"
$env:ANTT_SENHA = "sua_senha"
$env:PLAYWRIGHT_HEADLESS = "0"
```

### Executar o agente

Modo continuo:

```powershell
python .\sync_agent.py
```

Modo pontual:

```powershell
python .\sync_agent.py --once
```

### Como funciona

1. O usuario clica no dashboard em "Solicitar leitura agora".
2. O site cria um job de sincronizacao no PostgreSQL.
3. O agente local consulta a fila.
4. O agente executa o Playwright localmente, faz a leitura da ANTT e envia o resultado de volta.
5. O dashboard atualiza status, cards e tabela.

## Credenciais da ANTT

As credenciais da ANTT devem ficar apenas na maquina do agente local ou em ambiente controlado:

```powershell
$env:ANTT_CPF_CNPJ = "seu_cpf_ou_cnpj"
$env:ANTT_SENHA = "sua_senha"
```

## Modo de teste

Se quiser testar a interface ou o agente sem acessar a ANTT:

```powershell
$env:MOCK_SYNC = "1"
```

Nesse modo, o job e processado normalmente, mas com multas de exemplo.

## Fluxo do sistema

1. O usuario entra no dashboard com login e senha do sistema.
2. No painel, clica em "Solicitar leitura agora".
3. O sistema registra um job no PostgreSQL.
4. O agente local consulta a fila e executa a leitura real.
5. O dashboard acompanha o status e atualiza os cards e a tabela.
6. Os dados sincronizados ficam salvos no PostgreSQL.

## Observacoes

- Na sincronizacao real, o navegador da ANTT abre em modo visivel na maquina do agente para permitir CAPTCHA e login quando necessario.
- O app web continua funcionando mesmo sem o agente conectado; nesse caso ele apenas deixa jobs pendentes.
- O CSV exportado pelo sistema reflete os dados persistidos no armazenamento atual.
