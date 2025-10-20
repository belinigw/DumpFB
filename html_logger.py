"""Utilities for generating HTML migration logs."""

from __future__ import annotations

import html
import re
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Optional, Sequence, Set, List


LogFunction = Callable[[str], None]


def _sanitize_name(nome: str) -> str:
    nome = nome.strip()
    if not nome:
        return "Banco"
    normalizado = re.sub(r"[^0-9A-Za-z_-]+", "_", nome)
    return normalizado or "Banco"


def _formatar_tamanho(tamanho_bytes: Optional[int]) -> str:
    if not tamanho_bytes:
        return "Indisponível"

    unidades = ["bytes", "KB", "MB", "GB", "TB"]
    tamanho = float(tamanho_bytes)
    indice = 0
    while tamanho >= 1024 and indice < len(unidades) - 1:
        tamanho /= 1024
        indice += 1
    return f"{tamanho:.2f} {unidades[indice]}"


def _formatar_duracao(segundos: Optional[float]) -> str:
    if segundos is None:
        return "Indisponível"

    try:
        if segundos < 0:
            return "Indisponível"
    except TypeError:
        return "Indisponível"

    horas = int(segundos // 3600)
    minutos = int((segundos % 3600) // 60)
    segundos_restantes = segundos - (horas * 3600) - (minutos * 60)

    if horas:
        return f"{horas}h {minutos}m {segundos_restantes:.2f}s"
    if minutos:
        return f"{minutos}m {segundos_restantes:.2f}s"
    return f"{segundos_restantes:.2f}s"


def _inferir_nivel(mensagem: str) -> str:
    minusculo = mensagem.lower()
    if "[erro]" in minusculo or "⛔" in mensagem or "❌" in mensagem:
        return "error"
    if "⚠️" in mensagem or "[aviso]" in minusculo or "[warn" in minusculo:
        return "warning"
    return "info"


def _escape_message(mensagem: str) -> str:
    texto = html.escape(mensagem)
    return texto.replace("\n", "<br/>")


def _resolver_diretorio(base_path: Path) -> Path:
    if base_path.suffix:
        return base_path.parent
    return base_path


def obter_tamanho_banco_firebird(caminho: Optional[str]) -> Optional[int]:
    if not caminho:
        return None
    try:
        return Path(caminho).expanduser().stat().st_size
    except OSError:
        return None


def obter_tamanho_banco_destino(tipo: str, connection) -> Optional[int]:
    if connection is None:
        return None
    tipo_normalizado = tipo.lower()
    if tipo_normalizado == "mssql":
        cursor = None
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT SUM(size) * 8 * 1024 FROM sys.database_files")
            resultado = cursor.fetchone()
        except Exception:
            return None
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
        if not resultado:
            return None
        valor = resultado[0]
        if valor is None:
            return None
        try:
            return int(valor)
        except (TypeError, ValueError):
            return None
    return None


@dataclass
class _LogEntry:
    instante: datetime
    nivel: str
    mensagem: str


class HtmlLogWriter:
    """Collects messages during migration and renders a friendly HTML report."""

    def __init__(self, nome_banco: str, caminho_log: Path):
        self._generated_at = datetime.now()
        self._nome_banco = nome_banco or "Banco"
        base_path = Path(caminho_log)
        self._diretorio = _resolver_diretorio(base_path)
        self._diretorio.mkdir(parents=True, exist_ok=True)
        timestamp = self._generated_at.strftime("%d%m%Y-%H%M")
        self._arquivo = (
            self._diretorio / f"{_sanitize_name(self._nome_banco)}_{timestamp}.html"
        )

        self._lock = threading.Lock()
        self._entries: List[_LogEntry] = []
        self._source_size: Optional[int] = None
        self._destination_size: Optional[int] = None
        self._total_migration_seconds: Optional[float] = None
        self._comparison: Dict[str, Dict[str, Set[str]]] = {}

    @classmethod
    def from_config(cls, config: Dict[str, object]) -> "HtmlLogWriter":
        settings = config.get("settings", {})
        log_path = settings.get("log_path", "logs/dump.log")
        destino_cfg = config.get("destination", {})
        destino_db = destino_cfg.get("database", {})
        nome_banco = destino_db.get("database") or destino_db.get("name") or "Banco"
        return cls(nome_banco, Path(log_path))

    @property
    def file_path(self) -> Path:
        return self._arquivo

    def wrap(self, log_function: LogFunction) -> LogFunction:
        def wrapper(message: str) -> None:
            self.log_message(message)
            log_function(message)

        return wrapper

    def log_message(self, mensagem: str, nivel: Optional[str] = None) -> None:
        nivel_real = nivel or _inferir_nivel(mensagem)
        with self._lock:
            self._entries.append(
                _LogEntry(instante=datetime.now(), nivel=nivel_real, mensagem=mensagem)
            )

    def set_source_size(self, tamanho_bytes: Optional[int]) -> None:
        self._source_size = tamanho_bytes

    def set_destination_size(self, tamanho_bytes: Optional[int]) -> None:
        self._destination_size = tamanho_bytes

    def set_total_migration_time(self, duracao_segundos: Optional[float]) -> None:
        self._total_migration_seconds = duracao_segundos

    def merge_comparison(
        self, comparacao: Optional[Dict[str, Dict[str, Sequence[str]]]]
    ) -> None:
        if not comparacao:
            return
        with self._lock:
            for categoria, dados in comparacao.items():
                destino = self._comparison.setdefault(
                    categoria,
                    {"faltantes": set(), "excedentes": set()},
                )
                faltantes = dados.get("faltantes_no_destino", [])
                excedentes = dados.get("excedentes_no_destino", [])
                destino["faltantes"].update(faltantes)
                destino["excedentes"].update(excedentes)

    def _render_entries(self) -> str:
        itens: List[str] = []
        for entry in self._entries:
            mensagem_html = _escape_message(entry.mensagem)
            linha = (
                f'<li class="log-entry {entry.nivel}">'  # noqa: E501
                f'<span class="timestamp">{entry.instante.strftime("%H:%M:%S")}</span>'
                f'<span class="message">{mensagem_html}</span>'
                "</li>"
            )
            itens.append(linha)
        return "\n".join(itens)

    def _render_comparison(self) -> str:
        if not self._comparison:
            return '<p class="empty">Nenhuma diferença encontrada entre o destino e o modelo.</p>'
        linhas: List[str] = []
        for categoria in sorted(self._comparison.keys()):
            dados = self._comparison[categoria]
            faltantes = ", ".join(sorted(dados["faltantes"])) or "Nenhum"
            excedentes = ", ".join(sorted(dados["excedentes"])) or "Nenhum"
            linhas.append(
                "<tr>"
                f"<td>{html.escape(categoria.title())}</td>"
                f"<td>{html.escape(faltantes)}</td>"
                f"<td>{html.escape(excedentes)}</td>"
                "</tr>"
            )
        return (
            '<table class="comparison">'
            "<thead><tr><th>Categoria</th><th>Presentes no modelo e ausentes no destino</th>"
            "<th>Presentes no destino e ausentes no modelo</th></tr></thead>"
            "<tbody>" + "".join(linhas) + "</tbody></table>"
        )

    def finalize(self) -> Path:
        conteudo = f"""<!DOCTYPE html>
<html lang=\"pt-BR\">
<head>
<meta charset=\"utf-8\"/>
<title>Relatório de Migração - {html.escape(self._nome_banco)}</title>
<style>
body {{ font-family: Arial, sans-serif; background: #f7f9fc; color: #263238; margin: 0; padding: 20px; }}
header {{ background: #1E88E5; color: white; padding: 20px; border-radius: 8px; }}
.summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin: 20px 0; }}
.card {{ background: white; border-radius: 8px; padding: 16px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }}
.card h3 {{ margin-top: 0; font-size: 1.05rem; color: #546E7A; }}
.card p {{ margin: 0; font-size: 1.2rem; font-weight: bold; }}
section {{ margin-bottom: 32px; }}
section h2 {{ color: #1E88E5; }}
.log-entries {{ list-style: none; padding: 0; margin: 0; background: white; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }}
.log-entry {{ display: flex; gap: 16px; padding: 12px 16px; border-bottom: 1px solid #ECEFF1; align-items: baseline; }}
.log-entry:last-child {{ border-bottom: none; }}
.log-entry.error {{ color: #C62828; font-weight: bold; }}
.log-entry.warning {{ color: #EF6C00; }}
.timestamp {{ min-width: 80px; font-weight: bold; color: #455A64; }}
.comparison {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }}
.comparison th, .comparison td {{ padding: 12px 16px; border-bottom: 1px solid #ECEFF1; text-align: left; }}
.comparison th {{ background: #E3F2FD; color: #1E88E5; font-size: 0.95rem; text-transform: uppercase; }}
.comparison tr:last-child td {{ border-bottom: none; }}
.empty {{ font-style: italic; color: #78909C; }}
</style>
</head>
<body>
<header>
  <h1>Relatório de Migração - {html.escape(self._nome_banco)}</h1>
  <p>Gerado em {self._generated_at.strftime('%d/%m/%Y %H:%M')}</p>
</header>
<section>
  <h2>Resumo</h2>
    <div class=\"summary\">
      <div class=\"card\">
        <h3>Tamanho do banco FDB original</h3>
        <p>{_formatar_tamanho(self._source_size)}</p>
      </div>
      <div class=\"card\">
        <h3>Tamanho do banco SQL final</h3>
        <p>{_formatar_tamanho(self._destination_size)}</p>
      </div>
      <div class=\"card\">
        <h3>Tempo total da migração</h3>
        <p>{_formatar_duracao(self._total_migration_seconds)}</p>
      </div>
    </div>
</section>
<section>
  <h2>Logs da Migração</h2>
  <ul class=\"log-entries\">
    {self._render_entries()}
  </ul>
</section>
<section>
  <h2>Relatório comparativo pós-migração</h2>
  {self._render_comparison()}
</section>
</body>
</html>
"""
        self._arquivo.write_text(conteudo, encoding="utf-8")
        return self._arquivo
