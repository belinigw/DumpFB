# Aplicativo de Migração Firebird → MSSQL

Este utilitário realiza o dump incremental de dados entre bancos Fortes AC.

## Como executar

1. Configure o `config.json` com os parâmetros dos bancos.
2. Instale as dependências:
```
pip install -r requirements.txt
```
3. Execute:
```
python main.py
```

## Funcionalidades
- Conexão com Firebird e MSSQL
- Dump por blocos (5.000 linhas)
- Log de progresso
- Retentativa em falhas
- Interface futura via Tkinter

## Requisitos
- Python 3.10+
- Firebird + MSSQL com drivers corretos instalados
