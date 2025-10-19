import json
import threading
import tkinter as tk
from tkinter import messagebox
from typing import List

from dump import executar_dump
from db_firebird import conectar_firebird, listar_tabelas_firebird
from db_mssql import conectar_mssql

CONFIG_PATH = "config.json"

BUTTON_COLORS = {
    "primary": {
        "background": "#1976D2",
        "foreground": "#FFFFFF",
        "activebackground": "#115293",
        "activeforeground": "#FFFFFF",
    },
    "secondary": {
        "background": "#455A64",
        "foreground": "#FFFFFF",
        "activebackground": "#1C313A",
        "activeforeground": "#FFFFFF",
    },
    "success": {
        "background": "#2E7D32",
        "foreground": "#FFFFFF",
        "activebackground": "#1B5E20",
        "activeforeground": "#FFFFFF",
    },
    "warning": {
        "background": "#F9A825",
        "foreground": "#1A1A1A",
        "activebackground": "#F57F17",
        "activeforeground": "#1A1A1A",
    },
    "info": {
        "background": "#0288D1",
        "foreground": "#FFFFFF",
        "activebackground": "#01579B",
        "activeforeground": "#FFFFFF",
    },
}


def criar_botao_colorido(
    parent, texto, comando, *, estilo="primary", fonte=("Arial", 10)
):
    cores = BUTTON_COLORS.get(estilo, BUTTON_COLORS["primary"])
    return tk.Button(
        parent,
        text=texto,
        font=fonte,
        command=comando,
        bg=cores["background"],
        fg=cores["foreground"],
        activebackground=cores["activebackground"],
        activeforeground=cores["activeforeground"],
        relief=tk.RAISED,
        bd=1,
        cursor="hand2",
        padx=10,
        pady=5,
    )


def carregar_config():
    with open(CONFIG_PATH, "r") as arquivo:
        return json.load(arquivo)


def salvar_config(configuracoes):
    with open(CONFIG_PATH, "w") as arquivo:
        json.dump(configuracoes, arquivo, indent=2)


def escrever_saida(caixa_saida, texto):
    caixa_saida.insert(tk.END, texto + "\n")
    caixa_saida.see(tk.END)


class TableSelector(tk.Frame):
    def __init__(self, master, columns: int = 3):
        super().__init__(master)
        self.columns = max(1, columns)
        self.all_tables: List[str] = []
        self.selected_tables = set()
        self.checkbutton_variables = {}

        self.search_value = tk.StringVar()
        self.search_value.trace_add("write", self._on_search_change)

        search_frame = tk.Frame(self)
        search_frame.pack(fill="x", padx=10, pady=(0, 5))

        tk.Label(search_frame, text="Pesquisar tabelas:", font=("Arial", 11)).pack(
            side=tk.LEFT
        )

        self.search_entry = tk.Entry(
            search_frame, textvariable=self.search_value, font=("Arial", 11)
        )
        self.search_entry.pack(side=tk.LEFT, fill="x", expand=True, padx=(5, 5))

        criar_botao_colorido(
            search_frame,
            "Limpar",
            self._clear_search,
            estilo="secondary",
            fonte=("Arial", 10),
        ).pack(side=tk.LEFT, padx=(5, 0))

        list_container = tk.Frame(self)
        list_container.pack(fill="both", expand=True)

        self.canvas = tk.Canvas(list_container, borderwidth=0)
        self.canvas.pack(side=tk.LEFT, fill="both", expand=True)

        self.scrollbar_vertical = tk.Scrollbar(
            list_container, orient=tk.VERTICAL, command=self.canvas.yview
        )
        self.scrollbar_vertical.pack(side=tk.RIGHT, fill=tk.Y)

        self.canvas.configure(yscrollcommand=self.scrollbar_vertical.set)

        self.inner_frame = tk.Frame(self.canvas)
        self.canvas_window = self.canvas.create_window(
            (0, 0), window=self.inner_frame, anchor="nw"
        )

        self.inner_frame.bind("<Configure>", self._update_scroll_region)

        self.scrollbar_horizontal = tk.Scrollbar(
            list_container, orient=tk.HORIZONTAL, command=self.canvas.xview
        )
        self.scrollbar_horizontal.pack(fill=tk.X, side=tk.BOTTOM)

        self.canvas.configure(xscrollcommand=self.scrollbar_horizontal.set)

    def focus_search(self):
        self.search_entry.focus_set()

    def set_tables(self, tables: List[str]):
        self.all_tables = sorted(tables)
        self.selected_tables.intersection_update(self.all_tables)
        self._rebuild_checkbuttons()

    def get_selected_tables(self) -> List[str]:
        return [t for t in self.all_tables if t in self.selected_tables]

    def _filtered_tables(self) -> List[str]:
        termo = self.search_value.get().strip().lower()
        if not termo:
            return list(self.all_tables)
        return [tabela for tabela in self.all_tables if termo in tabela.lower()]

    def _rebuild_checkbuttons(self):
        for widget in self.inner_frame.winfo_children():
            widget.destroy()
        self.checkbutton_variables.clear()

        tabelas_filtradas = self._filtered_tables()

        if not tabelas_filtradas:
            tk.Label(
                self.inner_frame, text="Nenhuma tabela encontrada.", font=("Arial", 11)
            ).grid(row=0, column=0, padx=10, pady=10, sticky="w")
            self._update_scroll_region()
            return

        for column in range(self.columns):
            self.inner_frame.grid_columnconfigure(column, weight=1, pad=5)

        for indice, tabela in enumerate(tabelas_filtradas):
            variavel = tk.BooleanVar(value=tabela in self.selected_tables)
            self.checkbutton_variables[tabela] = variavel
            caixa_selecao = tk.Checkbutton(
                self.inner_frame,
                text=tabela,
                variable=variavel,
                onvalue=True,
                offvalue=False,
                anchor="w",
                padx=10,
                pady=2,
                command=lambda nome=tabela, var=variavel: self._toggle_selection(
                    nome, var.get()
                ),
            )
            linha = indice // self.columns
            coluna = indice % self.columns
            caixa_selecao.grid(row=linha, column=coluna, sticky="w")

        self._update_scroll_region()

    def _toggle_selection(self, tabela, selecionada):
        if selecionada:
            self.selected_tables.add(tabela)
        else:
            self.selected_tables.discard(tabela)

    def _clear_search(self):
        self.search_value.set("")

    def _on_search_change(self, *_):
        self._rebuild_checkbuttons()

    def _update_scroll_region(self, _event=None):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))


def obter_tabelas_selecionadas(table_selector: TableSelector):
    return table_selector.get_selected_tables()


def executar_migracao(table_selector, caixa_saida):
    tabelas = obter_tabelas_selecionadas(table_selector)
    if not tabelas:
        escrever_saida(caixa_saida, "[ERRO] Selecione ao menos uma tabela para migrar.")
        return

    def run():
        try:
            configuracoes = carregar_config()
            for tabela in tabelas:
                escrever_saida(
                    caixa_saida, f"🔄 Iniciando migração da tabela '{tabela}'..."
                )
                total, tempo, _ = executar_dump(
                    tabela,
                    configuracoes,
                    log_fn=lambda msg, tabela=tabela: escrever_saida(
                        caixa_saida, f"[{tabela}] {msg}"
                    ),
                )
                escrever_saida(
                    caixa_saida,
                    f"✅ Migração da tabela '{tabela}' concluída: {total} registros em {tempo:.2f} segundos.",
                )
            escrever_saida(
                caixa_saida,
                "🚀 Processo finalizado para todas as tabelas selecionadas.",
            )
        except Exception as erro:
            escrever_saida(caixa_saida, f"[ERRO] {erro}")

    threading.Thread(target=run, daemon=True).start()


def testar_conexao_firebird(caixa_saida):
    try:
        configuracoes = carregar_config()
        conexao_firebird = conectar_firebird(configuracoes)
        conexao_firebird.close()
        escrever_saida(caixa_saida, "✅ Conexão com Firebird bem-sucedida.")
    except Exception as erro:
        escrever_saida(caixa_saida, f"[ERRO] Firebird: {erro}")


def testar_conexao_mssql(caixa_saida):
    try:
        configuracoes = carregar_config()
        conexao_mssql = conectar_mssql(configuracoes)
        conexao_mssql.close()
        escrever_saida(caixa_saida, "✅ Conexão com MSSQL bem-sucedida.")
    except Exception as erro:
        escrever_saida(caixa_saida, f"[ERRO] MSSQL: {erro}")


def contar_registros(table_selector, caixa_saida):
    tabelas = obter_tabelas_selecionadas(table_selector)
    if not tabelas:
        escrever_saida(
            caixa_saida, "[ERRO] Selecione ao menos uma tabela para contagem."
        )
        return

    def run():
        conexao_firebird = None
        conexao_mssql = None
        try:
            configuracoes = carregar_config()
            conexao_firebird = conectar_firebird(configuracoes)
            cursor_firebird = conexao_firebird.cursor()
            conexao_mssql = conectar_mssql(configuracoes)
            cursor_mssql = conexao_mssql.cursor()

            for tabela in tabelas:
                cursor_firebird.execute(f"SELECT COUNT(*) FROM {tabela}")
                total_firebird = cursor_firebird.fetchone()[0]

                cursor_mssql.execute(f"SELECT COUNT(*) FROM {tabela}")
                total_mssql = cursor_mssql.fetchone()[0]

                escrever_saida(
                    caixa_saida,
                    f"📌 {tabela} - Total na origem (Firebird): {total_firebird} registros",
                )
                escrever_saida(
                    caixa_saida,
                    f"📌 {tabela} - Total no destino (MSSQL): {total_mssql} registros",
                )

        except Exception as erro:
            escrever_saida(caixa_saida, f"[ERRO] ao contar registros: {erro}")
        finally:
            if conexao_firebird:
                try:
                    conexao_firebird.close()
                except Exception:
                    pass
            if conexao_mssql:
                try:
                    conexao_mssql.close()
                except Exception:
                    pass

    threading.Thread(target=run, daemon=True).start()


def carregar_tabelas(table_selector, caixa_saida):
    escrever_saida(caixa_saida, "🔄 Carregando tabelas disponíveis do Firebird...")

    def run():
        conexao_firebird = None
        try:
            configuracoes = carregar_config()
            conexao_firebird = conectar_firebird(configuracoes)
            tabelas = listar_tabelas_firebird(conexao_firebird)

            def atualizar_lista():
                table_selector.set_tables(tabelas)
                escrever_saida(
                    caixa_saida, f"📋 {len(tabelas)} tabelas disponíveis carregadas."
                )

            table_selector.after(0, atualizar_lista)
        except Exception as erro:
            table_selector.after(
                0,
                lambda: escrever_saida(
                    caixa_saida, f"[ERRO] ao carregar tabelas: {erro}"
                ),
            )
        finally:
            if conexao_firebird:
                try:
                    conexao_firebird.close()
                except Exception:
                    pass

    threading.Thread(target=run, daemon=True).start()


def abrir_edicao_config():
    configuracoes = carregar_config()

    def salvar_e_fechar():
        try:
            configuracoes["firebird"]["database"] = entrada_firebird_banco.get()
            configuracoes["firebird"]["host"] = entrada_firebird_host.get()
            configuracoes["firebird"]["port"] = int(entrada_firebird_porta.get())
            configuracoes["firebird"]["user"] = entrada_firebird_usuario.get()
            configuracoes["firebird"]["password"] = entrada_firebird_senha.get()

            configuracoes["mssql"]["server"] = entrada_mssql_servidor.get()
            configuracoes["mssql"]["database"] = entrada_mssql_banco.get()
            configuracoes["mssql"]["user"] = entrada_mssql_usuario.get()
            configuracoes["mssql"]["password"] = entrada_mssql_senha.get()

            configuracoes["settings"]["chunk_size"] = int(entrada_tamanho_bloco.get())
            salvar_config(configuracoes)
            janela_configuracao.destroy()
        except Exception as erro:
            messagebox.showerror("Erro ao salvar", str(erro))

    janela_configuracao = tk.Toplevel()
    janela_configuracao.title("Editar Configuração")
    janela_configuracao.geometry("520x550")

    tk.Label(janela_configuracao, text="Firebird - Caminho do Banco:").pack()
    entrada_firebird_banco = tk.Entry(janela_configuracao, width=60)
    entrada_firebird_banco.insert(0, configuracoes["firebird"]["database"])
    entrada_firebird_banco.pack()

    tk.Label(janela_configuracao, text="Firebird - Host:").pack()
    entrada_firebird_host = tk.Entry(janela_configuracao, width=60)
    entrada_firebird_host.insert(0, configuracoes["firebird"]["host"])
    entrada_firebird_host.pack()

    tk.Label(janela_configuracao, text="Firebird - Porta:").pack()
    entrada_firebird_porta = tk.Entry(janela_configuracao, width=60)
    entrada_firebird_porta.insert(0, str(configuracoes["firebird"]["port"]))
    entrada_firebird_porta.pack()

    tk.Label(janela_configuracao, text="Firebird - Usuário:").pack()
    entrada_firebird_usuario = tk.Entry(janela_configuracao, width=60)
    entrada_firebird_usuario.insert(0, configuracoes["firebird"]["user"])
    entrada_firebird_usuario.pack()

    tk.Label(janela_configuracao, text="Firebird - Senha:").pack()
    entrada_firebird_senha = tk.Entry(janela_configuracao, width=60)
    entrada_firebird_senha.insert(0, configuracoes["firebird"]["password"])
    entrada_firebird_senha.pack()

    tk.Label(janela_configuracao, text="MSSQL - Servidor:").pack()
    entrada_mssql_servidor = tk.Entry(janela_configuracao, width=60)
    entrada_mssql_servidor.insert(0, configuracoes["mssql"]["server"])
    entrada_mssql_servidor.pack()

    tk.Label(janela_configuracao, text="MSSQL - Nome do Banco:").pack()
    entrada_mssql_banco = tk.Entry(janela_configuracao, width=60)
    entrada_mssql_banco.insert(0, configuracoes["mssql"]["database"])
    entrada_mssql_banco.pack()

    tk.Label(janela_configuracao, text="MSSQL - Usuário:").pack()
    entrada_mssql_usuario = tk.Entry(janela_configuracao, width=60)
    entrada_mssql_usuario.insert(0, configuracoes["mssql"]["user"])
    entrada_mssql_usuario.pack()

    tk.Label(janela_configuracao, text="MSSQL - Senha:").pack()
    entrada_mssql_senha = tk.Entry(janela_configuracao, width=60)
    entrada_mssql_senha.insert(0, configuracoes["mssql"]["password"])
    entrada_mssql_senha.pack()

    tk.Label(janela_configuracao, text="Tamanho do bloco (chunk_size):").pack()
    entrada_tamanho_bloco = tk.Entry(janela_configuracao, width=20)
    entrada_tamanho_bloco.insert(0, str(configuracoes["settings"]["chunk_size"]))
    entrada_tamanho_bloco.pack()

    criar_botao_colorido(
        janela_configuracao, "Salvar Configuração", salvar_e_fechar, estilo="success"
    ).pack(pady=10)


def criar_interface():
    root = tk.Tk()
    root.title("Migração Firebird → MSSQL (pymssql)")
    root.geometry("780x620")

    tabela_selector = TableSelector(root, columns=3)
    tabela_selector.pack(fill="both", expand=False, padx=10, pady=(10, 5))
    tabela_selector.focus_search()

    frame_botoes = tk.Frame(root)
    frame_botoes.pack(pady=10)

    criar_botao_colorido(
        frame_botoes,
        "Iniciar Migração",
        lambda: executar_migracao(tabela_selector, caixa_saida),
        estilo="success",
    ).grid(row=0, column=0, padx=5, pady=5)

    criar_botao_colorido(
        frame_botoes,
        "Editar Configuração",
        abrir_edicao_config,
        estilo="secondary",
    ).grid(row=0, column=1, padx=5, pady=5)

    criar_botao_colorido(
        frame_botoes,
        "Testar Firebird",
        lambda: testar_conexao_firebird(caixa_saida),
        estilo="info",
    ).grid(row=0, column=2, padx=5, pady=5)

    criar_botao_colorido(
        frame_botoes,
        "Testar MSSQL",
        lambda: testar_conexao_mssql(caixa_saida),
        estilo="info",
    ).grid(row=0, column=3, padx=5, pady=5)

    criar_botao_colorido(
        frame_botoes,
        "Contar Registros",
        lambda: contar_registros(tabela_selector, caixa_saida),
        estilo="warning",
    ).grid(row=0, column=4, padx=5, pady=5)

    criar_botao_colorido(
        frame_botoes,
        "Carregar Tabelas",
        lambda: carregar_tabelas(tabela_selector, caixa_saida),
        estilo="primary",
    ).grid(row=0, column=5, padx=5, pady=5)

    tk.Label(root, text="Saída de Log:").pack()

    caixa_saida = tk.Text(root, wrap="word", height=20, font=("Courier", 10))
    caixa_saida.pack(fill="both", expand=True, padx=10, pady=5)

    scrollbar = tk.Scrollbar(root, command=caixa_saida.yview)
    caixa_saida.config(yscrollcommand=scrollbar.set)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    root.after(200, lambda: carregar_tabelas(tabela_selector, caixa_saida))

    root.mainloop()


if __name__ == "__main__":
    criar_interface()
