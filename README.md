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
- `frutamina_app/sync_manager.py`: execucao da sincronizacao em background
- `frutamina_app/store.py`: persistencia em JSON e CSV
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

## Credenciais da ANTT

Para a sincronizacao real:

```powershell
$env:ANTT_CPF_CNPJ = "seu_cpf_ou_cnpj"
$env:ANTT_SENHA = "sua_senha"
```

## Modo de teste

Se quiser testar a interface sem acessar a ANTT:

```powershell
$env:MOCK_SYNC = "1"
```

Nesse modo, o botao "Ler multas agora" gera dados de exemplo.

## Fluxo do sistema

1. O usuario entra no dashboard com login e senha do sistema.
2. No painel, clica em "Ler multas agora".
3. A sincronizacao roda em background.
4. O dashboard acompanha o status e atualiza os cards e a tabela.
5. Os dados sincronizados ficam salvos em `data/`.

## Observacoes

- Na sincronizacao real, o navegador da ANTT abre em modo visivel para permitir CAPTCHA e login quando necessario.
- Os PDFs baixados ficam em `downloads/pdfs/`.
- O CSV exportado pelo sistema fica em `data/multas_ativas.csv`.
