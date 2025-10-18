import fdb


def conectar_firebird(config):
    cfg = config['firebird']
    con = fdb.connect(
        dsn=f"{cfg['host']}/{cfg['port']}:{cfg['database']}",
        user=cfg['user'],
        password=cfg['password']
    )
    return con


def listar_tabelas_firebird(con):
    cur = con.cursor()
    cur.execute(
        """
        SELECT TRIM(rdb$relation_name)
        FROM rdb$relations
        WHERE rdb$view_blr IS NULL
          AND (rdb$system_flag IS NULL OR rdb$system_flag = 0)
        ORDER BY rdb$relation_name
        """
    )
    return [row[0].strip() for row in cur.fetchall()]


def buscar_lotes_firebird(con, tabela, chunk_size=5000, offset=0):
    cur = con.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {tabela}")
    total = cur.fetchone()[0]

    while offset < total:
        cur.execute(f"SELECT FIRST {chunk_size} SKIP {offset} * FROM {tabela}")
        yield cur.fetchall()
        offset += chunk_size

