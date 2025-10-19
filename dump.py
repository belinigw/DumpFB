import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Optional, Sequence, Set, Tuple, List

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
    ativar_indice,
    ativar_trigger,
    conectar_mssql,
    definir_identity_insert,
    desativar_constraints_tabelas,
    desativar_indice,
    desativar_trigger,
    inserir_lote_mssql,
    limpar_tabela_destino,
    listar_constraints_desativadas,
    listar_constraints_mssql,
    listar_indices_ativos,
    listar_indices_mssql,
    listar_procedures_mssql,
    listar_tabelas_mssql,
    listar_triggers_ativas,
    listar_triggers_mssql,
    possui_coluna_identidade,
)

SQLLogger = Optional[Callable[[str], None]]
LogFunction = Callable[[str], None]
ConstraintResolver = Optional[Callable[[str, str], Optional[str]]]


class OperationCancelled(Exception):
    """Raised when the current migration was cancelled by the user."""

    pass


@dataclass
class MigrationSummary:
    total_inseridos: int
    tempo_total: float
    constraints_pendentes: Sequence[Tuple[str, str]]
    comparacao_modelo: Dict[str, Dict[str, Sequence[str]]]


class BaseDestinationHandler:
    supports_constraints = False
    supports_identity_insert = False
    supports_global_disable = False

    def __init__(self, connection, sql_logger: SQLLogger = None):
        self.connection = connection
        self.sql_logger = sql_logger

    def list_tables(self) -> Sequence[str]:
        raise NotImplementedError

    def disable_constraints(self) -> None:
        return None

    def enable_constraints(self) -> None:
        return None

    def disable_all_objects(self) -> None:
        return None

    def enable_all_objects(self) -> None:
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
    supports_global_disable = True

    def __init__(self, connection, sql_logger: SQLLogger = None):
        super().__init__(connection, sql_logger)
        self._identity_ativado: Dict[str, bool] = {}
        self._disabled_triggers: Dict[str, List[str]] = {}
        self._disabled_indexes: Dict[str, List[str]] = {}
        self._global_objects_disabled = False

    def list_tables(self) -> Sequence[str]:
        return listar_tabelas_mssql(self.connection)

    def disable_constraints(self) -> None:
        tabelas = self.list_tables()
        desativar_constraints_tabelas(self.connection, tabelas, self.sql_logger)

    def enable_constraints(self) -> None:
        tabelas = self.list_tables()
        ativar_constraints_tabelas(self.connection, tabelas, self.sql_logger)

    def disable_all_objects(self) -> None:
        if self._global_objects_disabled:
            return

        tabelas = self.list_tables()
        desativar_constraints_tabelas(self.connection, tabelas, self.sql_logger)

        self._disabled_triggers = {}
        for tabela, trigger in listar_triggers_ativas(self.connection):
            desativar_trigger(self.connection, tabela, trigger, self.sql_logger)
            self._disabled_triggers.setdefault(tabela, []).append(trigger)

        self._disabled_indexes = {}
        for tabela, indice in listar_indices_ativos(self.connection):
            desativar_indice(self.connection, tabela, indice, self.sql_logger)
            self._disabled_indexes.setdefault(tabela, []).append(indice)

        self.connection.commit()
        self._global_objects_disabled = True

    def enable_all_objects(self) -> None:
        if not self._global_objects_disabled:
            return

        tabelas = self.list_tables()
        ativar_constraints_tabelas(self.connection, tabelas, self.sql_logger)

        for tabela, indices in self._disabled_indexes.items():
            for indice in indices:
                ativar_indice(self.connection, tabela, indice, self.sql_logger)

        for tabela, triggers in self._disabled_triggers.items():
            for trigger in triggers:
                ativar_trigger(self.connection, tabela, trigger, self.sql_logger)

        self.connection.commit()
        self._disabled_indexes.clear()
        self._disabled_triggers.clear()
        self._global_objects_disabled = False

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


_TEXT_CODECS = ("utf-8", "latin-1", "cp1252")


def _parece_texto(valor: str) -> bool:
    if not valor:
        return True

    total = len(valor)
    legiveis = sum(
        1 for caractere in valor if caractere.isprintable() or caractere in "\r\n\t"
    )
    return legiveis / total >= 0.9


def _decodificar_bytes_sem_perda(valor: bytes) -> Tuple[Optional[str], Optional[str]]:
    for codec in _TEXT_CODECS:
        try:
            texto = valor.decode(codec)
        except UnicodeDecodeError:
            continue
        except Exception:
            logging.debug(
                "Codec %s não pôde ser usado para decodificar valor em bytes",
                codec,
                exc_info=True,
            )
            continue
        try:
            if texto.encode(codec) == valor and _parece_texto(texto):
                return texto, codec
        except Exception:
            logging.debug(
                "Falha ao revalidar valor após decodificação com codec %s",
                codec,
                exc_info=True,
            )
    return None, None


def _tentar_decodificar_bytes(
    valor: bytes, codec: str, log_fn: LogFunction
) -> Optional[str]:
    try:
        texto = valor.decode(codec)
    except Exception as erro:
        log_fn(
            f"[WARN] Falha ao decodificar bytes com codec '{codec}': {erro}. Tente outra opção."
        )
        logging.warning(
            "Falha ao decodificar valor em bytes",
            exc_info=True,
        )
        return None

    try:
        if texto.encode(codec) != valor:
            raise UnicodeError("Round-trip inconsistente")
    except Exception as erro:
        log_fn(
            f"[WARN] Decodificação com codec '{codec}' alteraria o conteúdo original: {erro}."
        )
        logging.warning(
            "Decodificação com perda detectada para codec %s",
            codec,
            exc_info=True,
        )
        return None

    return texto


def _registrar_resumo_sanitizacao(
    estatisticas: Dict[str, Dict[str, int]], log_fn: LogFunction
) -> None:
    mensagens: List[str] = []
    for coluna in sorted(estatisticas.keys()):
        eventos = estatisticas[coluna]
        for chave_evento, quantidade in sorted(eventos.items()):
            if chave_evento.startswith("codec:"):
                codec = chave_evento.split(":", 1)[1]
                mensagens.append(
                    f"Coluna '{coluna}': {quantidade} valor(es) decodificado(s) com codec {codec}."
                )
            elif chave_evento == "indecifrado":
                mensagens.append(
                    f"Coluna '{coluna}': {quantidade} valor(es) não pôde/puderam ser decodificados e permaneceram em bytes."
                )

    if not mensagens:
        return

    log_fn("[WARN] Resumo de ajustes aplicados ao lote:")
    logging.warning("Resumo de ajustes aplicados ao lote:")
    for mensagem in mensagens:
        log_fn(f" - {mensagem}")
        logging.warning(mensagem)


def sanitizar_lote(
    lote: Sequence[Sequence[object]],
    colunas: Sequence[str],
    log_fn: LogFunction = print,
) -> Sequence[Tuple[object, ...]]:
    lote_tratado: List[Tuple[object, ...]] = []
    estatisticas: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for indice_linha, linha in enumerate(lote, start=1):
        nova_linha: List[object] = []
        for indice_coluna, valor in enumerate(linha):
            if isinstance(valor, bytes):
                texto, codec_utilizado = _decodificar_bytes_sem_perda(valor)
                if texto is not None and codec_utilizado is not None:
                    nova_linha.append(texto)
                    if codec_utilizado != "utf-8":
                        coluna = colunas[indice_coluna]
                        estatisticas[coluna][f"codec:{codec_utilizado}"] += 1
                else:
                    nova_linha.append(valor)
                    coluna = colunas[indice_coluna]
                    estatisticas[coluna]["indecifrado"] += 1
            else:
                nova_linha.append(valor)
        lote_tratado.append(tuple(nova_linha))

    _registrar_resumo_sanitizacao(estatisticas, log_fn)
    return lote_tratado


def _ajustar_coluna_manual(coluna: str, valor: object, log_fn: LogFunction) -> object:
    if isinstance(valor, bytes):
        log_fn(
            f"Coluna '{coluna}' contém dados binários ({len(valor)} bytes). "
            "Escolha como deseja tratar o valor."
        )
        trecho = valor[:60]
        log_fn(f"Pré-visualização (primeiros 60 bytes): {trecho!r}")
        while True:
            log_fn(
                "Opções disponíveis: 1) UTF-8  2) Latin-1  3) Informar manualmente  "
                "4) Usar NULL  5) Manter bytes"
            )
            escolha = (
                input(f"Selecione uma opção para '{coluna}' [1-5]: ").strip() or "1"
            )
            if escolha == "1":
                decodificado = _tentar_decodificar_bytes(valor, "utf-8", log_fn)
                if decodificado is not None:
                    return decodificado
                continue
            if escolha == "2":
                decodificado = _tentar_decodificar_bytes(valor, "latin-1", log_fn)
                if decodificado is not None:
                    return decodificado
                continue
            if escolha == "3":
                return input(f"Digite o novo valor textual para '{coluna}': ")
            if escolha == "4":
                return None
            if escolha == "5":
                return valor
            log_fn("Opção inválida. Tente novamente.")
    prompt = (
        f"Coluna '{coluna}' possui valor {valor!r}. Pressione Enter para manter, "
        "digite um novo valor ou 'NULL' para gravar nulo: "
    )
    while True:
        entrada = input(prompt)
        if entrada == "":
            return valor
        if entrada.upper() == "NULL":
            return None
        return entrada


def _corrigir_registro_manual(
    colunas: Sequence[str], registro: Sequence[object], log_fn: LogFunction
) -> Tuple[object, ...]:
    log_fn("📝 Ajuste manual necessário. Informe novos valores para o registro.")
    valores = list(registro)
    for indice, coluna in enumerate(colunas):
        if indice < len(valores):
            valor_atual = valores[indice]
            valores[indice] = _ajustar_coluna_manual(coluna, valor_atual, log_fn)
        else:
            valores.append(_ajustar_coluna_manual(coluna, None, log_fn))
    return tuple(valores)


def _inserir_registros_com_intervencao(
    destino_handler: BaseDestinationHandler,
    tabela: str,
    colunas: Sequence[str],
    registros: Sequence[Sequence[object]],
    log_fn: LogFunction,
) -> int:
    inseridos = 0
    for linha_indice, registro in enumerate(registros, start=1):
        valores = tuple(registro)
        while True:
            try:
                destino_handler.insert_batch(tabela, colunas, [valores])
                inseridos += 1
                log_fn(
                    f"✅ Registro {linha_indice} inserido com sucesso após intervenção manual."
                )
                logging.info(
                    "Registro inserido após intervenção manual",
                    extra={"tabela": tabela, "linha": linha_indice},
                )
                break
            except Exception as erro:
                mensagem = (
                    f"[ERRO] Falha ao inserir registro {linha_indice}: {erro}. "
                    "Informe novos valores."
                )
                log_fn(mensagem)
                logging.error(mensagem)
                valores = _corrigir_registro_manual(colunas, valores, log_fn)
    return inseridos


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


def criar_handler_destino(
    tipo: str, connection, sql_logger: SQLLogger = None
) -> BaseDestinationHandler:
    return _criar_handler_destino(tipo, connection, sql_logger)


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
    gerenciar_constraints: bool = True,
    cancel_event: Optional[threading.Event] = None,
    limpar_destino: bool = True,
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
    constraints_pendentes: Sequence[Tuple[str, str]] = []
    cancelado = False
    try:
        try:
            if gerenciar_constraints and destino_handler.supports_constraints:
                log_fn("⛔ Desativando constraints de todas as tabelas do destino...")
                destino_handler.disable_constraints()

            if cancel_event and cancel_event.is_set():
                raise OperationCancelled(
                    "Processo cancelado antes da limpeza do destino."
                )

            if limpar_destino:
                log_fn(
                    f"🧹 Limpando dados existentes na tabela de destino '{tabela}'..."
                )
                destino_handler.clear_table(tabela)

            destino_handler.before_inserts(tabela)

            for indice, lote in enumerate(
                buscar_lotes_firebird(con_origem, tabela, chunk_size, offset), start=1
            ):
                if cancel_event and cancel_event.is_set():
                    raise OperationCancelled("Processo cancelado pelo usuário.")
                registros_brutos = [tuple(linha) for linha in lote]
                registros_lote = sanitizar_lote(registros_brutos, colunas, log_fn)
                try:
                    destino_handler.insert_batch(tabela, colunas, registros_lote)
                    offset += chunk_size
                    total_inseridos += len(registros_lote)
                    log_fn(
                        f"✅ Lote {indice}/{total_lotes} exportado ({len(registros_lote)} registros)"
                    )
                    logging.info(
                        f"Tabela: {tabela} | Lote {indice} | {len(registros_lote)} registros transferidos"
                    )
                except Exception as erro_lote:
                    mensagem = (
                        f"[ERRO] Falha ao inserir lote {indice}: {erro_lote}. "
                        "Tentando inserir registros individualmente."
                    )
                    log_fn(mensagem)
                    logging.error(mensagem)
                    inseridos = _inserir_registros_com_intervencao(
                        destino_handler, tabela, colunas, registros_lote, log_fn
                    )
                    total_inseridos += inseridos
                    offset += chunk_size
                    log_fn(
                        f"✅ Lote {indice}/{total_lotes} concluído com intervenção manual ({inseridos} registros)."
                    )
                    logging.info(
                        f"Tabela: {tabela} | Lote {indice} concluído após intervenção manual"
                    )
        finally:
            destino_handler.after_inserts(tabela)

            constraints_pendentes: Sequence[Tuple[str, str]] = []
            if gerenciar_constraints and destino_handler.supports_constraints:
                try:
                    log_fn(
                        "🔁 Reativando constraints de todas as tabelas do destino..."
                    )
                    destino_handler.enable_constraints()
                    constraints_pendentes = destino_handler.list_disabled_constraints()
                except Exception as erro_constraints:
                    mensagem_erro = (
                        f"[ERRO] Falha ao reativar constraints: {erro_constraints}"
                    )
                    log_fn(mensagem_erro)
                    logging.error(mensagem_erro)

            tempo_total = time.time() - start_time
            if cancelado:
                logging.info(
                    f"Migração da tabela '{tabela}' cancelada após {tempo_total:.2f} segundos"
                )
            else:
                log_fn(
                    f"✅ Dump concluído. Total de registros inseridos: {total_inseridos}"
                )
                log_fn(f"⏱️ Tempo total: {tempo_total:.2f} segundos")
                logging.info(
                    f"Dump finalizado com sucesso em {tempo_total:.2f} segundos"
                )

                comparacao_modelo = _comparar_modelo(
                    config, destino_handler, sql_logger
                )

                resumo = MigrationSummary(
                    total_inseridos=total_inseridos,
                    tempo_total=tempo_total,
                    constraints_pendentes=constraints_pendentes,
                    comparacao_modelo=comparacao_modelo,
                )
    except OperationCancelled:
        cancelado = True
        log_fn(f"⏹️ Migração da tabela '{tabela}' cancelada pelo usuário.")
        logging.info(f"Migração da tabela '{tabela}' cancelada pelo usuário.")
        raise
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
