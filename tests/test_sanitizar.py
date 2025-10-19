import sys
import types

import pytest


def _criar_stub_db_modules() -> None:
    def _dummy(*args, **kwargs):  # pragma: no cover - apenas stub
        raise NotImplementedError

    mod_firebird = types.ModuleType("db_firebird")
    atributos_firebird = [
        "buscar_lotes_firebird",
        "conectar_firebird",
        "inserir_lote_firebird",
        "limpar_tabela_firebird",
        "listar_constraints_firebird",
        "listar_indices_firebird",
        "listar_procedures_firebird",
        "listar_tabelas_firebird",
        "listar_triggers_firebird",
    ]
    for atributo in atributos_firebird:
        setattr(mod_firebird, atributo, _dummy)
    sys.modules.setdefault("db_firebird", mod_firebird)

    mod_mssql = types.ModuleType("db_mssql")
    atributos_mssql = [
        "ativar_constraint",
        "ativar_constraints_tabelas",
        "ativar_indice",
        "ativar_trigger",
        "conectar_mssql",
        "definir_identity_insert",
        "desativar_constraints_tabelas",
        "desativar_indice",
        "desativar_trigger",
        "inserir_lote_mssql",
        "limpar_tabela_destino",
        "listar_constraints_desativadas",
        "listar_constraints_mssql",
        "listar_indices_ativos",
        "listar_indices_mssql",
        "listar_procedures_mssql",
        "listar_tabelas_mssql",
        "listar_triggers_ativas",
        "listar_triggers_mssql",
        "possui_coluna_identidade",
    ]
    for atributo in atributos_mssql:
        setattr(mod_mssql, atributo, _dummy)
    sys.modules.setdefault("db_mssql", mod_mssql)


_criar_stub_db_modules()

from dump import sanitizar_lote


def _coletor_logs():
    mensagens = []

    def registrar(mensagem: str) -> None:
        mensagens.append(mensagem)

    return registrar, mensagens


def test_sanitizar_lote_decodifica_utf8_sem_logs():
    log_fn, mensagens = _coletor_logs()
    lote = [(b"cafe", 1)]
    colunas = ["descricao", "id"]

    resultado = sanitizar_lote(lote, colunas, log_fn=log_fn)

    assert resultado == [("cafe", 1)]
    assert mensagens == []


def test_sanitizar_lote_faz_fallback_para_latin1():
    log_fn, mensagens = _coletor_logs()
    lote = [("café".encode("latin-1"),)]
    colunas = ["descricao"]

    resultado = sanitizar_lote(lote, colunas, log_fn=log_fn)

    assert resultado == [("café",)]
    assert mensagens[0] == "[WARN] Resumo de ajustes aplicados ao lote:"
    assert any("codec latin-1" in mensagem for mensagem in mensagens[1:])


def test_sanitizar_lote_mantem_bytes_quando_decodificacao_falha():
    log_fn, mensagens = _coletor_logs()
    lote = [(b"\xff\x00\xfe",)]
    colunas = ["conteudo"]

    resultado = sanitizar_lote(lote, colunas, log_fn=log_fn)

    assert resultado == [(b"\xff\x00\xfe",)]
    assert mensagens[0] == "[WARN] Resumo de ajustes aplicados ao lote:"
    assert any("não pôde" in mensagem for mensagem in mensagens[1:])
