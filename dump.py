import logging
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Optional, Sequence, Set, Tuple

from db_firebird import (
    buscar_lotes_firebird,
    conectar_firebird,
    inserir_lote_firebird,
    limpar_tabela_firebird,
    listar_constraints_firebird,
    listar_indices_firebird,
    listar_procedures_firebird,
    listar_tabelas_firebird,
    listar_triggers_firebird,
)
from db_mssql import (
    ativar_constraint,
    ativar_constraints_tabelas,
    conectar_mssql,
    definir_identity_insert,
    desativar_constraints_tabelas,
    inserir_lote_mssql,
    limpar_tabela_destino,
    listar_constraints_desativadas,
    listar_constraints_mssql,
    listar_indices_mssql,
    listar_procedures_mssql,
    listar_tabelas_mssql,
    listar_triggers_mssql,
    possui_coluna_identidade,
)

SQLLogger = Optional[Callable[[str], None]]
LogFunction = Callable[[str], None]
ConstraintResolver = Optional[Callable[[str, str], Optional[str]]]


@dataclass
class MigrationSummary:
    total_inseridos: int
    tempo_total: float
    constraints_pendentes: Sequence[Tuple[str, str]]
    comparacao_modelo: Dict[str, Dict[str, Sequence[str]]]


class BaseDestinationHandler:
    supports_constraints = False
    supports_identity_insert = False

    def __init__(self, connection, sql_logger: SQLLogger = None):
        self.connection = connection
        self.sql_logger = sql_logger

    def list_tables(self) -> Sequence[str]:
        raise NotImplementedError

    def disable_constraints(self) -> None:
        return None

    def enable_constraints(self) -> None:
        return None

    def list_disabled_constraints(self) -> Sequence[Tuple[str, str]]:
        return []

    def enable_specific_constraint(self, tabela: str, constraint: str) -> None:
        return None

    def clear_table(self, tabela: str) -> None:
        raise NotImplementedError

    def before_inserts(self, tabela: str) -> None:
        return None

    def after_inserts(self, tabela: str) -> None:
        return None

    def insert_batch(
        self, tabela: str, colunas: Sequence[str], dados: Iterable[Sequence]
    ) -> None:
        raise NotImplementedError

    def execute_sql(self, comando: str) -> None:
        cursor = self.connection.cursor()
        if self.sql_logger:
            self.sql_logger(comando)
        cursor.execute(comando)
        try:
            cursor.fetchall()
        except Exception:
            pass
        self.connection.commit()

    def metadata(self) -> Dict[str, Set[str]]:
        raise NotImplementedError


class MssqlDestinationHandler(BaseDestinationHandler):
    supports_constraints = True
    supports_identity_insert = True

    def __init__(self, connection, sql_logger: SQLLogger = None):
        super().__init__(connection, sql_logger)
        self._identity_ativado: Dict[str, bool] = {}

    def list_tables(self) -> Sequence[str]:
        return listar_tabelas_mssql(self.connection)

    def disable_constraints(self) -> None:
        tabelas = self.list_tables()
        desativar_constraints_tabelas(self.connection, tabelas, self.sql_logger)

    def enable_constraints(self) -> None:
        tabelas = self.list_tables()
        ativar_constraints_tabelas(self.connection, tabelas, self.sql_logger)

    def list_disabled_constraints(self) -> Sequence[Tuple[str, str]]:
        return listar_constraints_desativadas(self.connection)

    def enable_specific_constraint(self, tabela: str, constraint: str) -> None:
        ativar_constraint(self.connection, tabela, constraint, self.sql_logger)

    def clear_table(self, tabela: str) -> None:
        limpar_tabela_destino(self.connection, tabela, self.sql_logger)

    def before_inserts(self, tabela: str) -> None:
        if possui_coluna_identidade(self.connection, tabela):
            definir_identity_insert(self.connection, tabela, True, self.sql_logger)
            self._identity_ativado[tabela] = True

    def after_inserts(self, tabela: str) -> None:
        if self._identity_ativado.pop(tabela, False):
            definir_identity_insert(self.connection, tabela, False, self.sql_logger)

    def insert_batch(
        self, tabela: str, colunas: Sequence[str], dados: Iterable[Sequence]
    ) -> None:
        inserir_lote_mssql(self.connection, tabela, colunas, dados, self.sql_logger)

    def metadata(self) -> Dict[str, Set[str]]:
        return {
            "constraints": listar_constraints_mssql(self.connection),
            "indexes": listar_indices_mssql(self.connection),
            "procedures": listar_procedures_mssql(self.connection),
            "triggers": listar_triggers_mssql(self.connection),
        }


class FirebirdDestinationHandler(BaseDestinationHandler):
    def list_tables(self) -> Sequence[str]:
        return listar_tabelas_firebird(self.connection)

    def clear_table(self, tabela: str) -> None:
        limpar_tabela_firebird(self.connection, tabela, self.sql_logger)

    def insert_batch(
        self, tabela: str, colunas: Sequence[str], dados: Iterable[Sequence]
    ) -> None:
        inserir_lote_firebird(self.connection, tabela, colunas, dados, self.sql_logger)

    def metadata(self) -> Dict[str, Set[str]]:
        return {
            "constraints": listar_constraints_firebird(self.connection),
            "indexes": listar_indices_firebird(self.connection),
            "procedures": listar_procedures_firebird(self.connection),
            "triggers": listar_triggers_firebird(self.connection),
        }


def configurar_logger(log_path: str) -> None:
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _criar_handler_destino(
    tipo: str, connection, sql_logger: SQLLogger
) -> BaseDestinationHandler:
    if tipo == "mssql":
        return MssqlDestinationHandler(connection, sql_logger)
    if tipo == "firebird":
        return FirebirdDestinationHandler(connection, sql_logger)
    raise ValueError(f"Tipo de destino não suportado: {tipo}")


def _conectar_por_tipo(tipo: str, parametros: Dict[str, str]):
    if tipo == "firebird":
        return conectar_firebird(parametros)
    if tipo == "mssql":
        return conectar_mssql(parametros)
    raise ValueError(f"Tipo de banco não suportado: {tipo}")


def _obter_metadata_por_tipo(tipo: str, connection) -> Dict[str, Set[str]]:
    handler = _criar_handler_destino(tipo, connection, None)
    return handler.metadata()


def _comparar_modelo(
    config: Dict,
    destino_handler: BaseDestinationHandler,
    sql_logger: SQLLogger,
) -> Dict[str, Dict[str, Sequence[str]]]:
    if "model" not in config:
        raise ValueError("Configuração do banco modelo não encontrada em config.json.")

    modelo_cfg = config["model"]
    con_modelo = _conectar_por_tipo(modelo_cfg["type"], modelo_cfg["database"])
    try:
        metadata_modelo = _obter_metadata_por_tipo(modelo_cfg["type"], con_modelo)
    finally:
        try:
            con_modelo.close()
        except Exception:
            pass

    metadata_destino = destino_handler.metadata()

    comparacao: Dict[str, Dict[str, Sequence[str]]] = {}
    chaves = set(metadata_modelo.keys()) | set(metadata_destino.keys())
    for chave in sorted(chaves):
        itens_modelo = metadata_modelo.get(chave, set())
        itens_destino = metadata_destino.get(chave, set())
        comparacao[chave] = {
            "faltantes_no_destino": sorted(itens_modelo - itens_destino),
            "excedentes_no_destino": sorted(itens_destino - itens_modelo),
        }
    return comparacao


def executar_dump(
    tabela: str,
    config: Dict,
    connections: Optional[Dict[str, object]] = None,
    log_fn: LogFunction = print,
    sql_logger: SQLLogger = None,
    constraint_resolver: ConstraintResolver = None,
) -> MigrationSummary:
    chunk_size = config["settings"]["chunk_size"]
    log_path = config["settings"]["log_path"]
    configurar_logger(log_path)

    origem_cfg = config["source"]
    destino_cfg = config["destination"]

    if origem_cfg["type"].lower() != "firebird":
        raise ValueError("Atualmente apenas origem Firebird é suportada para migração.")

    con_origem = (
        connections["source"] if connections and "source" in connections else None
    )
    con_destino = (
        connections["destination"]
        if connections and "destination" in connections
        else None
    )
    fechar_origem = con_origem is None
    fechar_destino = con_destino is None

    if con_origem is None:
        log_fn(f"🔌 Conectando ao banco de origem ({origem_cfg['type']})...")
        con_origem = _conectar_por_tipo(origem_cfg["type"], origem_cfg["database"])

    if con_destino is None:
        log_fn(f"🔌 Conectando ao banco de destino ({destino_cfg['type']})...")
        con_destino = _conectar_por_tipo(destino_cfg["type"], destino_cfg["database"])

    destino_handler = _criar_handler_destino(
        destino_cfg["type"], con_destino, sql_logger
    )

    cursor_origem = con_origem.cursor()
    cursor_origem.execute(f"SELECT FIRST 1 * FROM {tabela}")
    colunas = [descricao[0] for descricao in cursor_origem.description]

    cursor_origem.execute(f"SELECT COUNT(*) FROM {tabela}")
    total_registros = cursor_origem.fetchone()[0]
    total_lotes = (total_registros // chunk_size) + (
        1 if total_registros % chunk_size > 0 else 0
    )

    log_fn(f"📊 Total de registros a migrar: {total_registros}")
    log_fn(f"📦 Iniciando exportação em {total_lotes} lotes...")

    start_time = time.time()
    offset = 0
    total_inseridos = 0

    resumo: Optional[MigrationSummary] = None
    try:
        try:
            if destino_handler.supports_constraints:
                log_fn("⛔ Desativando constraints de todas as tabelas do destino...")
                destino_handler.disable_constraints()

            log_fn(f"🧹 Limpando dados existentes na tabela de destino '{tabela}'...")
            destino_handler.clear_table(tabela)

            destino_handler.before_inserts(tabela)

            for indice, lote in enumerate(
                buscar_lotes_firebird(con_origem, tabela, chunk_size, offset), start=1
            ):
                try:
                    destino_handler.insert_batch(tabela, colunas, lote)
                    offset += chunk_size
                    total_inseridos += len(lote)
                    log_fn(
                        f"✅ Lote {indice}/{total_lotes} exportado ({len(lote)} registros)"
                    )
                    logging.info(
                        f"Tabela: {tabela} | Lote {indice} | {len(lote)} registros transferidos"
                    )
                except Exception as erro_lote:
                    mensagem = f"[ERRO] no lote {indice}: {erro_lote}"
                    log_fn(mensagem)
                    logging.error(mensagem)
                    break
        finally:
            destino_handler.after_inserts(tabela)

            constraints_pendentes: Sequence[Tuple[str, str]] = []
            if destino_handler.supports_constraints:
                try:
                    log_fn(
                        "🔁 Reativando constraints de todas as tabelas do destino..."
                    )
                    destino_handler.enable_constraints()
                    constraints_pendentes = destino_handler.list_disabled_constraints()

                    if constraints_pendentes and constraint_resolver:
                        for tabela_nome, constraint_nome in constraints_pendentes:
                            resolvido = False
                            while True:
                                comando_manual = constraint_resolver(
                                    tabela_nome, constraint_nome
                                )
                                if not comando_manual:
                                    break
                                try:
                                    destino_handler.execute_sql(comando_manual)
                                except Exception as erro_execucao:
                                    log_fn(
                                        f"[ERRO] ao executar comando manual: {erro_execucao}"
                                    )
                                    continue
                                try:
                                    destino_handler.enable_specific_constraint(
                                        tabela_nome, constraint_nome
                                    )
                                except Exception as erro_constraint:
                                    log_fn(
                                        f"[ERRO] ao reativar constraint {constraint_nome}: {erro_constraint}"
                                    )
                                    continue
                                pendentes_atual = (
                                    destino_handler.list_disabled_constraints()
                                )
                                if (
                                    tabela_nome,
                                    constraint_nome,
                                ) not in pendentes_atual:
                                    log_fn(
                                        f"🔒 Constraint {constraint_nome} reativada após ajuste manual."
                                    )
                                    resolvido = True
                                    break
                            if not resolvido:
                                log_fn(
                                    f"[AVISO] Constraint {constraint_nome} permaneceu desativada após tentativas manuais."
                                )
                    constraints_pendentes = destino_handler.list_disabled_constraints()
                except Exception as erro_constraints:
                    mensagem_erro = (
                        f"[ERRO] Falha ao reativar constraints: {erro_constraints}"
                    )
                    log_fn(mensagem_erro)
                    logging.error(mensagem_erro)

            tempo_total = time.time() - start_time
            log_fn(
                f"✅ Dump concluído. Total de registros inseridos: {total_inseridos}"
            )
            log_fn(f"⏱️ Tempo total: {tempo_total:.2f} segundos")
            logging.info(f"Dump finalizado com sucesso em {tempo_total:.2f} segundos")

            comparacao_modelo = _comparar_modelo(config, destino_handler, sql_logger)

            resumo = MigrationSummary(
                total_inseridos=total_inseridos,
                tempo_total=tempo_total,
                constraints_pendentes=constraints_pendentes,
                comparacao_modelo=comparacao_modelo,
            )
    finally:
        if fechar_destino and con_destino:
            try:
                con_destino.close()
            except Exception:
                pass
        if fechar_origem and con_origem:
            try:
                con_origem.close()
            except Exception:
                pass

    if resumo is None:
        raise RuntimeError(
            "Não foi possível concluir a migração da tabela especificada."
        )

    return resumo
