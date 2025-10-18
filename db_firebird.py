import fdb


def conectar_firebird(config):
    firebird_config = config["firebird"]
    connection = fdb.connect(
        dsn=f"{firebird_config['host']}/{firebird_config['port']}:{firebird_config['database']}",
        user=firebird_config["user"],
        password=firebird_config["password"],
    )
    return connection


def listar_tabelas_firebird(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT TRIM(rdb$relation_name)
        FROM rdb$relations
        WHERE rdb$view_blr IS NULL
          AND (rdb$system_flag IS NULL OR rdb$system_flag = 0)
        ORDER BY rdb$relation_name
        """
    )
    return [row[0].strip() for row in cursor.fetchall()]


def buscar_lotes_firebird(connection, tabela, chunk_size=5000, offset=0):
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
    total = cursor.fetchone()[0]


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
        cursor.execute(f"SELECT FIRST {chunk_size} SKIP {offset} * FROM {tabela}")
        yield cursor.fetchall()
        offset += chunk_size
