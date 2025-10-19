import copy
import json
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from db_firebird import (
    conectar_firebird,
    executar_query_firebird,
    listar_tabelas_firebird,
    obter_versao_firebird,
)
from db_mssql import (
    conectar_mssql,
    executar_query_mssql,
    listar_tabelas_mssql,
    obter_versao_mssql,
)
from dump import MigrationSummary, criar_handler_destino, executar_dump

ConfigDict = Dict[str, object]
SQLListener = Callable[[str], None]
LogFunction = Callable[[str], None]
ConstraintPrompt = Callable[[str, str], Optional[str]]


class ApplicationController:
    def __init__(self, config_path: str = "config.json") -> None:
        self.config_path = Path(config_path)
        self.config: ConfigDict = self._load_config()
        self.source_connection = None
        self.destination_connection = None
        self._sql_history: List[str] = []
        self._sql_listeners: List[SQLListener] = []

    def _load_config(self) -> ConfigDict:
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Arquivo de configuração não encontrado: {self.config_path}"
            )
        with self.config_path.open("r", encoding="utf-8") as arquivo:
            return json.load(arquivo)

    def reload_config(self) -> None:
        self.config = self._load_config()

    def get_config(self) -> ConfigDict:
        return copy.deepcopy(self.config)

    def save_config(self, novo_config: ConfigDict) -> None:
        self.config = copy.deepcopy(novo_config)
        with self.config_path.open("w", encoding="utf-8") as arquivo:
            json.dump(self.config, arquivo, indent=2, ensure_ascii=False)
        self.disconnect()

    def register_sql_listener(self, listener: SQLListener) -> None:
        self._sql_listeners.append(listener)

    def clear_sql_history(self) -> None:
        self._sql_history.clear()

    def get_sql_history(self) -> List[str]:
        return list(self._sql_history)

    def _notify_sql(self, comando: str) -> None:
        self._sql_history.append(comando)
        for listener in self._sql_listeners:
            listener(comando)

    def _connect_database(self, configuracao: Dict[str, object]):
        tipo = str(configuracao["type"]).lower()
        parametros = configuracao["database"]
        if tipo == "firebird":
            return conectar_firebird(parametros)
        if tipo == "mssql":
            return conectar_mssql(parametros)
        raise ValueError(f"Tipo de banco desconhecido: {tipo}")

    def _list_tables(self, connection, tipo: str) -> Sequence[str]:
        tipo = tipo.lower()
        if tipo == "firebird":
            return listar_tabelas_firebird(connection)
        if tipo == "mssql":
            return listar_tabelas_mssql(connection)
        raise ValueError(f"Tipo de banco desconhecido: {tipo}")

    def connect(self, log_fn: LogFunction) -> Sequence[str]:
        self.disconnect()
        source_cfg = self.config["source"]
        destination_cfg = self.config["destination"]

        log_fn("🔌 Conectando aos bancos configurados...")
        self.source_connection = self._connect_database(source_cfg)
        self.destination_connection = self._connect_database(destination_cfg)

        tabelas = self._list_tables(self.source_connection, source_cfg["type"])
        log_fn(f"📋 {len(tabelas)} tabelas disponíveis carregadas da origem.")
        return tabelas

    def refresh_tables(self) -> Sequence[str]:
        self._ensure_connections()
        return self._list_tables(self.source_connection, self.config["source"]["type"])

    def disconnect(self) -> None:
        for conexao in (self.source_connection, self.destination_connection):
            if conexao is None:
                continue
            try:
                conexao.close()
            except Exception:
                pass
        self.source_connection = None
        self.destination_connection = None

    def is_connected(self) -> bool:
        return (
            self.source_connection is not None
            and self.destination_connection is not None
        )

    def _ensure_connections(self) -> None:
        if not self.is_connected():
            raise RuntimeError(
                "É necessário conectar aos bancos antes de executar esta ação."
            )

    def run_migration(
        self,
        tabelas: Sequence[str],
        log_fn: LogFunction,
        constraint_prompt: ConstraintPrompt,
    ) -> None:
        if not tabelas:
            raise ValueError("Nenhuma tabela selecionada para migração.")

        self._ensure_connections()

        destino_cfg = self.config["destination"]
        destino_handler = criar_handler_destino(
            destino_cfg["type"], self.destination_connection, self._notify_sql
        )

        objetos_desativados = False
        pendencias_finais: Sequence[Tuple[str, str]] = []
        try:
            if destino_handler.supports_global_disable:
                log_fn(
                    "⛔ Desativando constraints, índices e gatilhos de todas as tabelas do destino..."
                )
                try:
                    destino_handler.disable_all_objects()
                    objetos_desativados = True
                except Exception as erro:
                    log_fn(
                        f"[ERRO] Falha ao desativar objetos do destino de forma global: {erro}. Continuando com o fluxo padrão."
                    )

            for tabela in tabelas:
                log_fn(f"🔄 Iniciando migração da tabela '{tabela}'...")
                resumo: MigrationSummary = executar_dump(
                    tabela,
                    self.config,
                    connections={
                        "source": self.source_connection,
                        "destination": self.destination_connection,
                    },
                    log_fn=log_fn,
                    sql_logger=self._notify_sql,
                    constraint_resolver=constraint_prompt,
                    gerenciar_constraints=not objetos_desativados,
                )
                log_fn(
                    f"✅ Migração da tabela '{tabela}' concluída: {resumo.total_inseridos} registros em {resumo.tempo_total:.2f} segundos."
                )
                self._registrar_comparacao(tabela, resumo, log_fn)

            log_fn("🚀 Processo finalizado para todas as tabelas selecionadas.")
        finally:
            if objetos_desativados:
                try:
                    log_fn(
                        "🔁 Reativando constraints, índices e gatilhos de todas as tabelas do destino..."
                    )
                    destino_handler.enable_all_objects()
                except Exception as erro:
                    log_fn(f"[ERRO] Falha ao reativar objetos do destino: {erro}")
                pendencias_finais = self._resolver_constraints_pendentes(
                    destino_handler, log_fn, constraint_prompt
                )
        if pendencias_finais and constraint_prompt is None:
            for tabela_nome, constraint_nome in pendencias_finais:
                log_fn(
                    f"[AVISO] Constraint {constraint_nome} permaneceu desativada após tentativas manuais na tabela {tabela_nome}."
                )

    def _resolver_constraints_pendentes(
        self,
        destino_handler,
        log_fn: LogFunction,
        constraint_prompt: ConstraintPrompt,
    ) -> Sequence[Tuple[str, str]]:
        pendentes = list(destino_handler.list_disabled_constraints())
        if not pendentes:
            return []

        if constraint_prompt is None:
            return pendentes

        for tabela_nome, constraint_nome in pendentes:
            resolvido = False
            while True:
                comando_manual = constraint_prompt(tabela_nome, constraint_nome)
                if not comando_manual:
                    break
                try:
                    destino_handler.execute_sql(comando_manual)
                except Exception as erro_execucao:
                    log_fn(f"[ERRO] ao executar comando manual: {erro_execucao}")
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
                pendentes_atual = destino_handler.list_disabled_constraints()
                if (tabela_nome, constraint_nome) not in pendentes_atual:
                    log_fn(
                        f"🔒 Constraint {constraint_nome} reativada após ajuste manual."
                    )
                    resolvido = True
                    break
            if not resolvido:
                log_fn(
                    f"[AVISO] Constraint {constraint_nome} permaneceu desativada após tentativas manuais na tabela {tabela_nome}."
                )

        return list(destino_handler.list_disabled_constraints())

    def _registrar_comparacao(
        self, tabela: str, resumo: MigrationSummary, log_fn: LogFunction
    ) -> None:
        log_fn(f"📊 Comparação com banco modelo após migrar '{tabela}':")
        for categoria, diferencas in resumo.comparacao_modelo.items():
            faltantes = diferencas["faltantes_no_destino"]
            excedentes = diferencas["excedentes_no_destino"]
            if not faltantes and not excedentes:
                log_fn(f"  • {categoria}: ✅ sem diferenças")
                continue
            if faltantes:
                log_fn(
                    f"  • {categoria}: itens presentes no modelo e ausentes no destino: {', '.join(faltantes)}"
                )
            if excedentes:
                log_fn(
                    f"  • {categoria}: itens presentes apenas no destino: {', '.join(excedentes)}"
                )

    def count_records(self, tabelas: Sequence[str], log_fn: LogFunction) -> None:
        if not tabelas:
            raise ValueError("Nenhuma tabela selecionada para contagem.")

        self._ensure_connections()

        for tabela in tabelas:
            origem_total = self._contar_registros(
                self.source_connection, self.config["source"]["type"], tabela
            )
            destino_total = self._contar_registros(
                self.destination_connection, self.config["destination"]["type"], tabela
            )
            log_fn(
                f"📌 {tabela} - Total na origem: {origem_total} | Total no destino: {destino_total}"
            )

    def _contar_registros(self, conexao, tipo: str, tabela: str) -> int:
        consulta = f"SELECT COUNT(*) FROM {tabela}"
        resultados = self._executar_query(conexao, tipo, consulta)
        return int(resultados[0][0]) if resultados else 0

    def _executar_query(self, conexao, tipo: str, consulta: str):
        tipo = tipo.lower()
        if tipo == "firebird":
            return executar_query_firebird(conexao, consulta, self._notify_sql)
        if tipo == "mssql":
            return executar_query_mssql(conexao, consulta, self._notify_sql)
        raise ValueError(f"Tipo de banco desconhecido: {tipo}")

    def test_connection(self, destino: str, log_fn: LogFunction) -> None:
        destino = destino.lower()
        if destino not in {"source", "destination", "model"}:
            raise ValueError("Destino de teste inválido.")

        configuracao = self.config[destino]
        conexao = self._connect_database(configuracao)
        try:
            tipo = str(configuracao["type"]).lower()
            if tipo == "firebird":
                versao = obter_versao_firebird(conexao)
            elif tipo == "mssql":
                versao = obter_versao_mssql(conexao)
            else:
                raise ValueError(f"Tipo de banco desconhecido: {tipo}")

            log_fn(f"✅ Conexão com {destino} bem-sucedida. Versão: {versao}")

            info_query = self.config["settings"].get("info_query")
            if info_query:
                resultados = self._executar_query(conexao, tipo, info_query)
                log_fn(
                    f"ℹ️ Resultado da consulta de informações ({len(resultados)} linhas retornadas)."
                )
        finally:
            try:
                conexao.close()
            except Exception:
                pass

    def get_info_query(self) -> str:
        return str(self.config["settings"].get("info_query", ""))
