import logging
import time
from typing import Iterable, Sequence, Tuple

from db_firebird import conectar_firebird, buscar_lotes_firebird
from db_mssql import (
    ativar_constraints_tabelas,
    conectar_mssql,
    desativar_constraints_tabelas,
    inserir_lote_mssql,
    limpar_tabela_destino,
    listar_constraints_desativadas,
    listar_tabelas_mssql,
)


def configurar_logger(log_path):
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _registrar_status_constraints(
    log_fn, constraints_pendentes: Sequence[Tuple[str, str]]
) -> str:
    if constraints_pendentes:
        mensagem = "[AVISO] As seguintes constraints não foram reativadas:"
        log_fn(mensagem)
        logging.warning(mensagem)
        for tabela_nome, constraint_nome in constraints_pendentes:
            detalhe = f"- Tabela: {tabela_nome}, Constraint: {constraint_nome}"
            log_fn(detalhe)
            logging.warning(detalhe)
        return mensagem

    mensagem = "🔒 Todas as constraints foram reativadas com sucesso."
    log_fn(mensagem)
    logging.info(mensagem)
    return mensagem


def executar_dump(tabela, config, log_fn=print):
    chunk_size = config["settings"]["chunk_size"]
    log_path = config["settings"]["log_path"]
    configurar_logger(log_path)

    log_fn(f"🔌 Conectando ao Firebird...")
    con_fb = conectar_firebird(config)

    log_fn(f"🔌 Conectando ao MSSQL...")
    con_sql = conectar_mssql(config)

    tabelas_destino: Iterable[str] = ()
    constraints_pendentes: Sequence[Tuple[str, str]] = ()
    constraints_status = ""

    cursor_fb = con_fb.cursor()
    cursor_fb.execute(f"SELECT FIRST 1 * FROM {tabela}")
    colunas = [desc[0] for desc in cursor_fb.description]

    cursor_fb.execute(f"SELECT COUNT(*) FROM {tabela}")
    total_registros = cursor_fb.fetchone()[0]
    total_lotes = (total_registros // chunk_size) + (
        1 if total_registros % chunk_size > 0 else 0
    )

    log_fn(f"📊 Total de registros a migrar: {total_registros}")
    log_fn(f"📦 Iniciando exportação em {total_lotes} lotes...")

    start_time = time.time()
    offset = 0
    total_inseridos = 0

    try:
        tabelas_destino = listar_tabelas_mssql(con_sql)
        log_fn("⛔ Desativando constraints de todas as tabelas do destino...")
        desativar_constraints_tabelas(con_sql, tabelas_destino)
        logging.info("Constraints desativadas para todas as tabelas no destino")

        log_fn(f"🧹 Limpando dados existentes na tabela de destino '{tabela}'...")
        limpar_tabela_destino(con_sql, tabela)
        logging.info(f"Dados removidos da tabela de destino {tabela}")

        for i, lote in enumerate(
            buscar_lotes_firebird(con_fb, tabela, chunk_size, offset), start=1
        ):
            try:
                inserir_lote_mssql(con_sql, tabela, colunas, lote)
                offset += chunk_size
                total_inseridos += len(lote)
                log_fn(f"✅ Lote {i}/{total_lotes} exportado ({len(lote)} registros)")
                logging.info(
                    f"Tabela: {tabela} | Lote {i} | {len(lote)} registros transferidos"
                )
            except Exception as e:
                log_fn(f"[ERRO] no lote {i}: {e}")
                logging.error(f"Erro no lote {i}: {e}")
                break
    finally:
        if con_sql:
            try:
                if tabelas_destino:
                    log_fn(
                        "🔁 Reativando constraints de todas as tabelas do destino..."
                    )
                    ativar_constraints_tabelas(con_sql, tabelas_destino)
                    logging.info(
                        "Processo de reativação das constraints iniciado para todas as tabelas"
                    )

                constraints_pendentes = listar_constraints_desativadas(con_sql)
                constraints_status = _registrar_status_constraints(
                    log_fn, constraints_pendentes
                )
            except Exception as erro_constraints:
                mensagem_erro = (
                    f"[ERRO] Falha ao reativar constraints: {erro_constraints}"
                )
                log_fn(mensagem_erro)
                logging.error(mensagem_erro)
            finally:
                try:
                    con_sql.close()
                except Exception:
                    pass
        if con_fb:
            try:
                con_fb.close()
            except Exception:
                pass

    tempo_total = time.time() - start_time
    log_fn(f"✅ Dump concluído. Total de registros inseridos: {total_inseridos}")
    log_fn(f"⏱️ Tempo total: {tempo_total:.2f} segundos")
    logging.info(f"Dump finalizado com sucesso em {tempo_total:.2f} segundos")

    if not constraints_status:
        constraints_status = "[AVISO] Não foi possível determinar o status das constraints ao final do processo."
        log_fn(constraints_status)
        logging.warning(constraints_status)

    return total_inseridos, tempo_total, constraints_status
