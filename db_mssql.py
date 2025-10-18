import pymssql

def conectar_mssql(config):
    cfg = config['mssql']
    return pymssql.connect(
        server=cfg['server'],
        user=cfg['user'],
        password=cfg['password'],
        database=cfg['database']
    )

def inserir_lote_mssql(con, tabela, colunas, dados):
    if not dados:
        return

    cursor = con.cursor()
    placeholders = ", ".join(["%s"] * len(colunas))
    colunas_str = ", ".join(colunas)
    insert_sql = f"INSERT INTO {tabela} ({colunas_str}) VALUES ({placeholders})"

    try:
        cursor.executemany(insert_sql, dados)
        con.commit()
    except Exception as e:
        con.rollback()
        raise RuntimeError(f"Erro ao inserir lote: {e}")
