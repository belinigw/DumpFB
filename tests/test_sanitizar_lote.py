import logging
import sys
import types


class _FakeProgrammingError(Exception):
    """Exceção utilizada pelo stub da biblioteca fdb nos testes."""


def _fake_connect_firebird(*args, **kwargs):  # pragma: no cover - não deve ser chamado
    raise RuntimeError("fdb.connect não deve ser utilizado nos testes.")


sys.modules.setdefault(
    "fdb",
    types.SimpleNamespace(
        connect=_fake_connect_firebird,
        ProgrammingError=_FakeProgrammingError,
    ),
)


def _fake_connect_mssql(*args, **kwargs):  # pragma: no cover - não deve ser chamado
    raise RuntimeError("pymssql.connect não deve ser utilizado nos testes.")


sys.modules.setdefault(
    "pymssql",
    types.SimpleNamespace(connect=_fake_connect_mssql),
)


from dump import sanitizar_lote


def test_sanitizar_lote_decodifica_bytes_para_string():
    mensagens = []
    colunas = ["descricao", "quantidade", "status"]
    lote = [(b"cafe", 10, "ok")]

    resultado = sanitizar_lote(lote, colunas, log_fn=mensagens.append)

    assert resultado == [("cafe", 10, "ok")]
    assert mensagens == []


def test_sanitizar_lote_registra_erro_quando_nao_decodifica(caplog):
    mensagens = []
    colunas = ["descricao"]
    lote = [(b"\xff\xfe",)]

    with caplog.at_level(logging.WARNING):
        resultado = sanitizar_lote(lote, colunas, log_fn=mensagens.append)

    assert resultado == [(None,)]
    assert len(mensagens) == 1
    assert "Falha ao decodificar bytes" in mensagens[0]
    assert any(
        "Falha ao decodificar bytes" in registro.message for registro in caplog.records
    )
