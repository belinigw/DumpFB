from typing import Iterable, List, Sequence, Set, Tuple

import pymssql


def conectar_mssql(config: dict):
    return pymssql.connect(
        server=config["server"],
        user=config["user"],
        password=config["password"],
        database=config["database"],
    )


def inserir_lote_mssql(
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
    placeholders = ", ".join(["%s"] * len(colunas))
    colunas_str = ", ".join(colunas)
    comando = f"INSERT INTO {tabela} ({colunas_str}) VALUES ({placeholders})"
    if sql_logger:
        sql_logger(comando)

    try:
        cursor.executemany(comando, dados)
        connection.commit()
    except Exception as erro:
        connection.rollback()
        raise RuntimeError(f"Erro ao inserir lote: {erro}") from erro


def listar_tabelas_mssql(connection) -> List[str]:
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM sys.tables")
    return [linha[0] for linha in cursor.fetchall()]


def _executar_acao_constraint(
    connection, tabela: str, acao: str, sql_logger=None
) -> None:
    comando = f"ALTER TABLE [{tabela}] {acao} CONSTRAINT ALL"
    if sql_logger:
        sql_logger(comando)
    cursor = connection.cursor()
    cursor.execute(comando)


def desativar_constraints_tabelas(
    connection, tabelas: Iterable[str], sql_logger=None
) -> None:
    for tabela in tabelas:
        _executar_acao_constraint(connection, tabela, "NOCHECK", sql_logger)
    connection.commit()


def ativar_constraints_tabelas(
    connection, tabelas: Iterable[str], sql_logger=None
) -> None:
    for tabela in tabelas:
        _executar_acao_constraint(connection, tabela, "CHECK", sql_logger)
    connection.commit()


def ativar_constraint(
    connection, tabela: str, constraint: str, sql_logger=None
) -> None:
    comando = f"ALTER TABLE [{tabela}] WITH CHECK CHECK CONSTRAINT [{constraint}]"
    if sql_logger:
        sql_logger(comando)
    cursor = connection.cursor()
    cursor.execute(comando)
    connection.commit()


def limpar_tabela_destino(connection, tabela: str, sql_logger=None) -> None:
    comando = f"DELETE FROM [{tabela}]"
    if sql_logger:
        sql_logger(comando)
    cursor = connection.cursor()
    cursor.execute(comando)
    connection.commit()


def listar_constraints_desativadas(connection) -> Sequence[Tuple[str, str]]:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT OBJECT_NAME(parent_object_id) AS tabela, name AS constraint_name
        FROM sys.check_constraints
        WHERE is_disabled = 1
        UNION
        SELECT OBJECT_NAME(parent_object_id) AS tabela, name AS constraint_name
        FROM sys.foreign_keys
        WHERE is_disabled = 1
        """
    )
    return cursor.fetchall()


def possui_coluna_identidade(connection, tabela: str) -> bool:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT 1
        FROM sys.columns
        WHERE object_id = OBJECT_ID(%s)
          AND COLUMNPROPERTY(object_id(%s), name, 'IsIdentity') = 1
        """,
        (tabela, tabela),
    )
    return cursor.fetchone() is not None


def definir_identity_insert(
    connection, tabela: str, habilitar: bool, sql_logger=None
) -> None:
    valor = "ON" if habilitar else "OFF"
    comando = f"SET IDENTITY_INSERT [{tabela}] {valor}"
    if sql_logger:
        sql_logger(comando)
    cursor = connection.cursor()
    cursor.execute(comando)
    connection.commit()


def obter_versao_mssql(connection) -> str:
    cursor = connection.cursor()
    cursor.execute("SELECT @@VERSION")
    resultado = cursor.fetchone()
    return resultado[0] if resultado else "Desconhecida"


def executar_query_mssql(connection, query: str, sql_logger=None) -> List[Sequence]:
    cursor = connection.cursor()
    if sql_logger:
        sql_logger(query)
    cursor.execute(query)
    try:
        resultados = cursor.fetchall()
    except pymssql.ProgrammingError:
        resultados = []
    connection.commit()
    return resultados


def listar_constraints_mssql(connection) -> Set[str]:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT name
        FROM sys.objects
        WHERE type IN ('C', 'F', 'PK', 'UQ', 'D')
          AND is_ms_shipped = 0
        """
    )
    return {linha[0] for linha in cursor.fetchall() if linha[0]}


def listar_indices_mssql(connection) -> Set[str]:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT name
        FROM sys.indexes
        WHERE name IS NOT NULL
          AND is_hypothetical = 0
        """
    )
    return {linha[0] for linha in cursor.fetchall() if linha[0]}


def listar_procedures_mssql(connection) -> Set[str]:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT name
        FROM sys.procedures
        WHERE is_ms_shipped = 0
        """
    )
    return {linha[0] for linha in cursor.fetchall() if linha[0]}


def listar_triggers_mssql(connection) -> Set[str]:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT name
        FROM sys.triggers
        WHERE is_ms_shipped = 0
        """
    )
    return {linha[0] for linha in cursor.fetchall() if linha[0]}
