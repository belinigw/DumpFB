from pathlib import Path

from html_logger import HtmlLogWriter


def test_html_logger_generates_report(tmp_path):
    log_path = tmp_path / "logs" / "dump.log"
    writer = HtmlLogWriter("BancoTeste", log_path)

    mensagens = []
    wrapped = writer.wrap(mensagens.append)

    writer.set_source_size(1024)
    writer.set_destination_size(2048)

    wrapped("✅ Migração iniciada")
    writer.log_message("[ERRO] Falha ao processar lote")
    writer.merge_comparison(
        {
            "tabelas": {
                "faltantes_no_destino": ["TB_CLIENTE"],
                "excedentes_no_destino": [],
            }
        }
    )

    wrapped(f"📄 Relatório salvo em: {writer.file_path}")
    arquivo = writer.finalize()

    conteudo = Path(arquivo).read_text(encoding="utf-8")
    assert "BancoTeste" in conteudo
    assert "1.00 KB" in conteudo
    assert "2.00 KB" in conteudo
    assert "TB_CLIENTE" in conteudo
    assert "log-entry error" in conteudo
    assert "log-entry info" in conteudo
