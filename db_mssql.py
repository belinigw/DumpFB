import pymssql
from typing import Iterable, List, Sequence, Tuple


def conectar_mssql(config):
    cfg = config["mssql"]
    return pymssql.connect(
        server=cfg["server"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
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


def listar_tabelas_mssql(con) -> List[str]:
    cursor = con.cursor()
    cursor.execute("SELECT name FROM sys.tables")
    return [linha[0] for linha in cursor.fetchall()]


def _executar_acao_constraint(con, tabela: str, acao: str) -> None:
    cursor = con.cursor()
    cursor.execute(f"ALTER TABLE [{tabela}] {acao} CONSTRAINT ALL")


def desativar_constraints_tabelas(con, tabelas: Iterable[str]) -> None:
    for tabela in tabelas:
        _executar_acao_constraint(con, tabela, "NOCHECK")
    con.commit()


def ativar_constraints_tabelas(con, tabelas: Iterable[str]) -> None:
    for tabela in tabelas:
        _executar_acao_constraint(con, tabela, "CHECK")
    con.commit()


def limpar_tabela_destino(con, tabela: str) -> None:
    cursor = con.cursor()
    cursor.execute(f"DELETE FROM [{tabela}]")
    con.commit()


def listar_constraints_desativadas(con) -> Sequence[Tuple[str, str]]:
    cursor = con.cursor()
    cursor.execute(
        """
        SELECT OBJECT_NAME(parent_object_id) AS tabela, name AS constraint_name
        FROM sys.check_constraints
        WHERE is_disabled = 1
        """
    )
    return cursor.fetchall()
