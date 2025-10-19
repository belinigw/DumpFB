from typing import Iterable, List, Sequence, Set

import fdb


def conectar_firebird(config: dict):
    connection = fdb.connect(
        dsn=f"{config['host']}/{config['port']}:{config['database']}",
        user=config["user"],
        password=config["password"],
    )
    return connection


def listar_tabelas_firebird(connection) -> List[str]:
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


def buscar_lotes_firebird(
    connection, tabela: str, chunk_size: int = 5000, offset: int = 0
):
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
    total = cursor.fetchone()[0]

    while offset < total:
        cursor.execute(f"SELECT FIRST {chunk_size} SKIP {offset} * FROM {tabela}")
        yield cursor.fetchall()
        offset += chunk_size


def limpar_tabela_firebird(connection, tabela: str, sql_logger=None) -> None:
    cursor = connection.cursor()
    comando = f"DELETE FROM {tabela}"
    if sql_logger:
        sql_logger(comando)
    cursor.execute(comando)
    connection.commit()


def inserir_lote_firebird(
    connection,
    tabela: str,
    colunas: Sequence[str],
    dados: Iterable[Sequence],
    sql_logger=None,
) -> None:
    dados = list(dados)
    if not dados:
        return

    cursor = connection.cursor()
    placeholders = ", ".join(["?"] * len(colunas))
    colunas_str = ", ".join(colunas)
    comando = f"INSERT INTO {tabela} ({colunas_str}) VALUES ({placeholders})"
    if sql_logger:
        sql_logger(comando)
    cursor.executemany(comando, dados)
    connection.commit()


def obter_versao_firebird(connection) -> str:
    cursor = connection.cursor()
    cursor.execute(
        "SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') FROM rdb$database"
    )
    resultado = cursor.fetchone()
    return resultado[0] if resultado else "Desconhecida"


def executar_query_firebird(connection, query: str, sql_logger=None) -> List[Sequence]:
    cursor = connection.cursor()
    if sql_logger:
        sql_logger(query)
    cursor.execute(query)
    try:
        resultados = cursor.fetchall()
    except fdb.ProgrammingError:
        resultados = []
    connection.commit()
    return resultados


def listar_constraints_firebird(connection) -> Set[str]:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT TRIM(rdb$constraint_name)
        FROM rdb$relation_constraints
        WHERE rdb$system_flag = 0 OR rdb$system_flag IS NULL
        """
    )
    return {linha[0].strip() for linha in cursor.fetchall() if linha[0]}


def listar_indices_firebird(connection) -> Set[str]:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT TRIM(rdb$index_name)
        FROM rdb$indices
        WHERE (rdb$system_flag = 0 OR rdb$system_flag IS NULL)
          AND rdb$index_name IS NOT NULL
        """
    )
    return {linha[0].strip() for linha in cursor.fetchall() if linha[0]}


def listar_procedures_firebird(connection) -> Set[str]:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT TRIM(rdb$procedure_name)
        FROM rdb$procedures
        WHERE rdb$system_flag = 0 OR rdb$system_flag IS NULL
        """
    )
    return {linha[0].strip() for linha in cursor.fetchall() if linha[0]}


def listar_triggers_firebird(connection) -> Set[str]:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT TRIM(rdb$trigger_name)
        FROM rdb$triggers
        WHERE rdb$system_flag = 0 OR rdb$system_flag IS NULL
        """
    )
    return {linha[0].strip() for linha in cursor.fetchall() if linha[0]}
