import time
import logging
from db_firebird import conectar_firebird, buscar_lotes_firebird
from db_mssql import conectar_mssql, inserir_lote_mssql

def configurar_logger(log_path):
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def executar_dump(tabela, config, log_fn=print):
    chunk_size = config['settings']['chunk_size']
    log_path = config['settings']['log_path']
    configurar_logger(log_path)

    log_fn(f"🔌 Conectando ao Firebird...")
    con_fb = conectar_firebird(config)

    log_fn(f"🔌 Conectando ao MSSQL...")
    con_sql = conectar_mssql(config)

    cursor_fb = con_fb.cursor()
    cursor_fb.execute(f"SELECT FIRST 1 * FROM {tabela}")
    colunas = [desc[0] for desc in cursor_fb.description]

    cursor_fb.execute(f"SELECT COUNT(*) FROM {tabela}")
    total_registros = cursor_fb.fetchone()[0]
    total_lotes = (total_registros // chunk_size) + (1 if total_registros % chunk_size > 0 else 0)

    log_fn(f"📊 Total de registros a migrar: {total_registros}")
    log_fn(f"📦 Iniciando exportação em {total_lotes} lotes...")

    start_time = time.time()
    offset = 0
    total_inseridos = 0

    for i, lote in enumerate(buscar_lotes_firebird(con_fb, tabela, chunk_size, offset), start=1):
        try:
            inserir_lote_mssql(con_sql, tabela, colunas, lote)
            offset += chunk_size
            total_inseridos += len(lote)
            log_fn(f"✅ Lote {i}/{total_lotes} exportado ({len(lote)} registros)")
            logging.info(f"Tabela: {tabela} | Lote {i} | {len(lote)} registros transferidos")
        except Exception as e:
            log_fn(f"[ERRO] no lote {i}: {e}")
            logging.error(f"Erro no lote {i}: {e}")
            break

    tempo_total = time.time() - start_time
    log_fn(f"✅ Dump concluído. Total de registros inseridos: {total_inseridos}")
    log_fn(f"⏱️ Tempo total: {tempo_total:.2f} segundos")
    logging.info(f"Dump finalizado com sucesso em {tempo_total:.2f} segundos")

    return total_inseridos, tempo_total
