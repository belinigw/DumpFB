import tkinter as tk
from tkinter import messagebox
import json
import threading
from typing import List

from dump import executar_dump
from db_firebird import conectar_firebird, listar_tabelas_firebird
from db_mssql import conectar_mssql

CONFIG_PATH = "config.json"


def carregar_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def salvar_config(config):
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)


def escrever_saida(caixa_saida, texto):
    caixa_saida.insert(tk.END, texto + "\n")
    caixa_saida.see(tk.END)


class TableSelector(tk.Frame):
    def __init__(self, master, columns=3):
        super().__init__(master)
        self.columns = max(1, columns)
        self.all_tables: List[str] = []
        self.selected_tables = set()
        self.check_vars = {}

        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", self._on_search_change)

        search_frame = tk.Frame(self)
        search_frame.pack(fill="x", padx=10, pady=(0, 5))

        tk.Label(search_frame, text="Pesquisar tabelas:", font=("Arial", 11)).pack(
            side=tk.LEFT
        )

        self.search_entry = tk.Entry(
            search_frame, textvariable=self.search_var, font=("Arial", 11)
        )
        self.search_entry.pack(side=tk.LEFT, fill="x", expand=True, padx=(5, 5))

        tk.Button(search_frame, text="Limpar", command=self._clear_search).pack(
            side=tk.LEFT
        )

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
            self, orient=tk.HORIZONTAL, command=self.canvas.xview
        )
        self.scrollbar_horizontal.pack(fill=tk.X, padx=10, pady=(0, 10))

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
        termo = self.search_var.get().strip().lower()
        if not termo:
            return list(self.all_tables)
        return [tabela for tabela in self.all_tables if termo in tabela.lower()]

    def _rebuild_checkbuttons(self):
        for widget in self.inner_frame.winfo_children():
            widget.destroy()
        self.check_vars.clear()

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
            self.check_vars[tabela] = variavel
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
        self.search_var.set("")

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
            config = carregar_config()
            for tabela in tabelas:
                escrever_saida(
                    caixa_saida, f"🔄 Iniciando migração da tabela '{tabela}'..."
                )
                total, tempo = executar_dump(
                    tabela,
                    config,
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
        except Exception as e:
            escrever_saida(caixa_saida, f"[ERRO] {e}")

    threading.Thread(target=run, daemon=True).start()


def testar_conexao_firebird(caixa_saida):
    try:
        config = carregar_config()
        con = conectar_firebird(config)
        con.close()
        escrever_saida(caixa_saida, "✅ Conexão com Firebird bem-sucedida.")
    except Exception as e:
        escrever_saida(caixa_saida, f"[ERRO] Firebird: {e}")


def testar_conexao_mssql(caixa_saida):
    try:
        config = carregar_config()
        con = conectar_mssql(config)
        con.close()
        escrever_saida(caixa_saida, "✅ Conexão com MSSQL bem-sucedida.")
    except Exception as e:
        escrever_saida(caixa_saida, f"[ERRO] MSSQL: {e}")


def contar_registros(table_selector, caixa_saida):
    tabelas = obter_tabelas_selecionadas(table_selector)
    if not tabelas:
        escrever_saida(
            caixa_saida, "[ERRO] Selecione ao menos uma tabela para contagem."
        )
        return

    def run():
        con_fb = None
        con_sql = None
        try:
            config = carregar_config()
            con_fb = conectar_firebird(config)
            cur_fb = con_fb.cursor()
            con_sql = conectar_mssql(config)
            cur_sql = con_sql.cursor()

            for tabela in tabelas:
                cur_fb.execute(f"SELECT COUNT(*) FROM {tabela}")
                total_fb = cur_fb.fetchone()[0]

                cur_sql.execute(f"SELECT COUNT(*) FROM {tabela}")
                total_sql = cur_sql.fetchone()[0]

                escrever_saida(
                    caixa_saida,
                    f"📌 {tabela} - Total na origem (Firebird): {total_fb} registros",
                )
                escrever_saida(
                    caixa_saida,
                    f"📌 {tabela} - Total no destino (MSSQL): {total_sql} registros",
                )

            con_fb.close()
            con_sql.close()

        except Exception as e:
            escrever_saida(caixa_saida, f"[ERRO] ao contar registros: {e}")
        finally:
            if con_fb:
                try:
                    con_fb.close()
                except Exception:
                    pass
            if con_sql:
                try:
                    con_sql.close()
                except Exception:
                    pass

    threading.Thread(target=run, daemon=True).start()


def carregar_tabelas(table_selector, caixa_saida):
    escrever_saida(caixa_saida, "🔄 Carregando tabelas disponíveis do Firebird...")

    def run():
        con = None
        try:
            config = carregar_config()
            con = conectar_firebird(config)
            tabelas = listar_tabelas_firebird(con)

            def atualizar_lista():
                table_selector.set_tables(tabelas)
                escrever_saida(
                    caixa_saida, f"📋 {len(tabelas)} tabelas disponíveis carregadas."
                )

            table_selector.after(0, atualizar_lista)
        except Exception as e:
            table_selector.after(
                0,
                lambda: escrever_saida(caixa_saida, f"[ERRO] ao carregar tabelas: {e}"),
            )
        finally:
            if con:
                con.close()

    threading.Thread(target=run, daemon=True).start()


def abrir_edicao_config():
    config = carregar_config()

    def salvar_e_fechar():
        try:
            config["firebird"]["database"] = firebird_db.get()
            config["firebird"]["host"] = firebird_host.get()
            config["firebird"]["port"] = int(firebird_port.get())
            config["firebird"]["user"] = firebird_user.get()
            config["firebird"]["password"] = firebird_pwd.get()

            config["mssql"]["server"] = mssql_server.get()
            config["mssql"]["database"] = mssql_db.get()
            config["mssql"]["user"] = mssql_user.get()
            config["mssql"]["password"] = mssql_pwd.get()

            config["settings"]["chunk_size"] = int(chunk_size.get())
            salvar_config(config)
            config_window.destroy()
        except Exception as e:
            messagebox.showerror("Erro ao salvar", str(e))

    config_window = tk.Toplevel()
    config_window.title("Editar Configuração")
    config_window.geometry("520x550")

    # Firebird
    tk.Label(config_window, text="Firebird - Caminho do Banco:").pack()
    firebird_db = tk.Entry(config_window, width=60)
    firebird_db.insert(0, config["firebird"]["database"])
    firebird_db.pack()

    tk.Label(config_window, text="Firebird - Host:").pack()
    firebird_host = tk.Entry(config_window, width=60)
    firebird_host.insert(0, config["firebird"]["host"])
    firebird_host.pack()

    tk.Label(config_window, text="Firebird - Porta:").pack()
    firebird_port = tk.Entry(config_window, width=60)
    firebird_port.insert(0, str(config["firebird"]["port"]))
    firebird_port.pack()

    tk.Label(config_window, text="Firebird - Usuário:").pack()
    firebird_user = tk.Entry(config_window, width=60)
    firebird_user.insert(0, config["firebird"]["user"])
    firebird_user.pack()

    tk.Label(config_window, text="Firebird - Senha:").pack()
    firebird_pwd = tk.Entry(config_window, width=60)
    firebird_pwd.insert(0, config["firebird"]["password"])
    firebird_pwd.pack()

    # MSSQL
    tk.Label(config_window, text="MSSQL - Servidor:").pack()
    mssql_server = tk.Entry(config_window, width=60)
    mssql_server.insert(0, config["mssql"]["server"])
    mssql_server.pack()

    tk.Label(config_window, text="MSSQL - Nome do Banco:").pack()
    mssql_db = tk.Entry(config_window, width=60)
    mssql_db.insert(0, config["mssql"]["database"])
    mssql_db.pack()

    tk.Label(config_window, text="MSSQL - Usuário:").pack()
    mssql_user = tk.Entry(config_window, width=60)
    mssql_user.insert(0, config["mssql"]["user"])
    mssql_user.pack()

    tk.Label(config_window, text="MSSQL - Senha:").pack()
    mssql_pwd = tk.Entry(config_window, width=60)
    mssql_pwd.insert(0, config["mssql"]["password"])
    mssql_pwd.pack()

    # Outros
    tk.Label(config_window, text="Tamanho do bloco (chunk_size):").pack()
    chunk_size = tk.Entry(config_window, width=20)
    chunk_size.insert(0, str(config["settings"]["chunk_size"]))
    chunk_size.pack()

    tk.Button(config_window, text="Salvar Configuração", command=salvar_e_fechar).pack(
        pady=10
    )


def criar_interface():
    root = tk.Tk()
    root.title("Migração Firebird → MSSQL (pymssql)")
    root.geometry("780x620")

    tabela_selector = TableSelector(root, columns=3)
    tabela_selector.pack(fill="both", expand=False, padx=10, pady=(10, 5))
    tabela_selector.focus_search()

    frame_botoes = tk.Frame(root)
    frame_botoes.pack(pady=10)

    tk.Button(
        frame_botoes,
        text="Iniciar Migração",
        font=("Arial", 10),
        command=lambda: executar_migracao(tabela_selector, caixa_saida),
    ).grid(row=0, column=0, padx=5)

    tk.Button(
        frame_botoes,
        text="Editar Configuração",
        font=("Arial", 10),
        command=abrir_edicao_config,
    ).grid(row=0, column=1, padx=5)

    tk.Button(
        frame_botoes,
        text="Testar Firebird",
        font=("Arial", 10),
        command=lambda: testar_conexao_firebird(caixa_saida),
    ).grid(row=0, column=2, padx=5)

    tk.Button(
        frame_botoes,
        text="Testar MSSQL",
        font=("Arial", 10),
        command=lambda: testar_conexao_mssql(caixa_saida),
    ).grid(row=0, column=3, padx=5)

    tk.Button(
        frame_botoes,
        text="Contar Registros",
        font=("Arial", 10),
        command=lambda: contar_registros(tabela_selector, caixa_saida),
    ).grid(row=0, column=4, padx=5)

    tk.Button(
        frame_botoes,
        text="Carregar Tabelas",
        font=("Arial", 10),
        command=lambda: carregar_tabelas(tabela_selector, caixa_saida),
    ).grid(row=0, column=5, padx=5)

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
