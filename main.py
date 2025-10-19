import threading
import tkinter as tk
from tkinter import messagebox, ttk
from typing import Dict, List, Optional, Sequence

from controller import ApplicationController

BUTTON_COLORS = {
    "primary": {
        "background": "#1E88E5",
        "foreground": "#FFFFFF",
        "activebackground": "#1565C0",
        "activeforeground": "#FFFFFF",
    },
    "secondary": {
        "background": "#78909C",
        "foreground": "#FFFFFF",
        "activebackground": "#546E7A",
        "activeforeground": "#FFFFFF",
    },
    "success": {
        "background": "#43A047",
        "foreground": "#FFFFFF",
        "activebackground": "#2E7D32",
        "activeforeground": "#FFFFFF",
    },
    "warning": {
        "background": "#FB8C00",
        "foreground": "#1A1A1A",
        "activebackground": "#EF6C00",
        "activeforeground": "#1A1A1A",
    },
    "info": {
        "background": "#00838F",
        "foreground": "#FFFFFF",
        "activebackground": "#006064",
        "activeforeground": "#FFFFFF",
    },
    "danger": {
        "background": "#E53935",
        "foreground": "#FFFFFF",
        "activebackground": "#C62828",
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
        disabledforeground="#CFD8DC",
        highlightthickness=0,
    )


class TableSelector(tk.Frame):
    def __init__(self, master, min_column_width: int = 200):
        super().__init__(master)
        self.min_column_width = min_column_width
        self.columns = 1
        self.all_tables: List[str] = []
        self.selected_tables = set()
        self.checkbutton_variables: Dict[str, tk.BooleanVar] = {}

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

        criar_botao_colorido(
            search_frame,
            "Selecionar Todas",
            self.select_all_tables,
            estilo="info",
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
        self.bind("<Configure>", self._on_resize)

        self.scrollbar_horizontal = tk.Scrollbar(
            list_container, orient=tk.HORIZONTAL, command=self.canvas.xview
        )
        self.scrollbar_horizontal.pack(fill=tk.X, side=tk.BOTTOM)

        self.canvas.configure(xscrollcommand=self.scrollbar_horizontal.set)

    def focus_search(self):
        self.search_entry.focus_set()

    def set_tables(self, tables: Sequence[str]):
        self.all_tables = sorted(tables)
        self.selected_tables.intersection_update(self.all_tables)
        self._rebuild_checkbuttons()

    def get_selected_tables(self) -> List[str]:
        return [t for t in self.all_tables if t in self.selected_tables]

    def select_all_tables(self):
        self.selected_tables = set(self.all_tables)
        for tabela, variavel in self.checkbutton_variables.items():
            variavel.set(True)

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

    def _on_resize(self, event):
        largura = max(1, event.width)
        novas_colunas = max(1, largura // self.min_column_width)
        if novas_colunas != self.columns:
            self.columns = novas_colunas
            self._rebuild_checkbuttons()

    def _update_scroll_region(self, _event=None):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))


class ConstraintDialog(tk.Toplevel):
    def __init__(self, master, tabela: str, constraint: str):
        super().__init__(master)
        self.title("Ajustar Constraint")
        self.resizable(False, False)
        self.transient(master)
        self.grab_set()

        mensagem = (
            f"A constraint '{constraint}' na tabela '{tabela}' não pôde ser reativada.\n"
            "Informe um comando SQL para ajustar os dados ou clique em Ignorar."
        )
        tk.Label(self, text=mensagem, wraplength=400, justify=tk.LEFT).pack(
            padx=15, pady=(15, 10)
        )

        self.texto_sql = tk.Text(self, width=60, height=6)
        self.texto_sql.pack(padx=15, pady=(0, 10))

        botoes = tk.Frame(self)
        botoes.pack(pady=(0, 15))

        criar_botao_colorido(
            botoes, "Executar", self._confirmar, estilo="success", fonte=("Arial", 10)
        ).pack(side=tk.LEFT, padx=5)
        criar_botao_colorido(
            botoes, "Ignorar", self._cancelar, estilo="secondary", fonte=("Arial", 10)
        ).pack(side=tk.LEFT, padx=5)

        self.resultado: Optional[str] = None
        self.texto_sql.focus_set()
        self.protocol("WM_DELETE_WINDOW", self._cancelar)

    def _confirmar(self):
        comando = self.texto_sql.get("1.0", tk.END).strip()
        self.resultado = comando if comando else None
        self.destroy()

    def _cancelar(self):
        self.resultado = None
        self.destroy()


class ConnectionEditor(tk.LabelFrame):
    FIREBIRD_FIELDS = (
        ("database", "Caminho do Banco"),
        ("host", "Host"),
        ("port", "Porta"),
        ("user", "Usuário"),
        ("password", "Senha"),
    )
    MSSQL_FIELDS = (
        ("server", "Servidor"),
        ("database", "Banco"),
        ("user", "Usuário"),
        ("password", "Senha"),
    )

    def __init__(self, master, titulo: str, dados_iniciais: Dict):
        super().__init__(master, text=titulo)
        self.tipo_var = tk.StringVar(value=dados_iniciais.get("type", "firebird"))
        self.frames: Dict[str, tk.Frame] = {}
        self.entries: Dict[str, Dict[str, tk.Entry]] = {}

        linha_tipo = tk.Frame(self)
        linha_tipo.pack(fill="x", padx=10, pady=5)
        tk.Label(linha_tipo, text="Tipo:").pack(side=tk.LEFT)
        tipo_combo = ttk.Combobox(
            linha_tipo,
            textvariable=self.tipo_var,
            values=["firebird", "mssql"],
            state="readonly",
            width=12,
        )
        tipo_combo.pack(side=tk.LEFT, padx=(5, 0))
        tipo_combo.bind("<<ComboboxSelected>>", lambda *_: self._atualizar_tipo())

        conteudo = tk.Frame(self)
        conteudo.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self._criar_frame_tipo(
            conteudo,
            "firebird",
            self.FIREBIRD_FIELDS,
            dados_iniciais.get("database", {}),
        )
        self._criar_frame_tipo(
            conteudo, "mssql", self.MSSQL_FIELDS, dados_iniciais.get("database", {})
        )
        self._atualizar_tipo()

    def _criar_frame_tipo(
        self,
        container: tk.Frame,
        tipo: str,
        campos: Sequence[tuple],
        valores_iniciais: Dict,
    ) -> None:
        frame = tk.Frame(container)
        self.frames[tipo] = frame
        self.entries[tipo] = {}
        for chave, rotulo in campos:
            linha = tk.Frame(frame)
            linha.pack(fill="x", pady=2)
            tk.Label(linha, text=f"{rotulo}:").pack(side=tk.LEFT)
            entrada = tk.Entry(linha, width=40)
            entrada.pack(side=tk.LEFT, padx=(5, 0), fill="x", expand=True)
            valor = valores_iniciais.get(chave)
            if valor is not None:
                entrada.insert(0, str(valor))
            self.entries[tipo][chave] = entrada

    def _atualizar_tipo(self):
        tipo = self.tipo_var.get()
        for chave, frame in self.frames.items():
            frame.pack_forget()
            if chave == tipo:
                frame.pack(fill="both", expand=True)

    def obter_dados(self) -> Dict:
        tipo = self.tipo_var.get()
        dados = {}
        for campo, entrada in self.entries[tipo].items():
            valor = entrada.get().strip()
            if campo == "port":
                dados[campo] = int(valor)
            else:
                dados[campo] = valor
        return {"type": tipo, "database": dados}


def criar_interface():
    controller = ApplicationController()

    root = tk.Tk()
    root.title("Migração Firebird ↔ MSSQL")
    root.geometry("900x700")

    table_selector = TableSelector(root)
    table_selector.pack(fill="both", expand=False, padx=10, pady=(10, 5))
    table_selector.focus_search()

    botoes_superiores = tk.Frame(root)
    botoes_superiores.pack(pady=5)

    botoes_operacoes: List[tk.Button] = []
    estado_operacao = {"em_andamento": False}

    def definir_botoes_habilitados(habilitado: bool):
        for botao in botoes_operacoes:
            botao.config(state=tk.NORMAL if habilitado else tk.DISABLED)

    def escrever_saida(widget: tk.Text, texto: str):
        widget.config(state=tk.NORMAL)
        widget.insert(tk.END, texto + "\n")
        widget.see(tk.END)
        widget.config(state=tk.DISABLED)

    def log_message(mensagem: str):
        root.after(0, lambda: escrever_saida(log_texto, mensagem))

    def registrar_sql(comando: str):
        root.after(0, lambda: escrever_saida(caixa_sql, comando))

    controller.register_sql_listener(registrar_sql)

    def iniciar_operacao():
        estado_operacao["em_andamento"] = True
        definir_botoes_habilitados(False)

    def finalizar_operacao():
        estado_operacao["em_andamento"] = False
        definir_botoes_habilitados(True)

    def executar_em_thread(acao):
        if estado_operacao["em_andamento"]:
            return
        controller.reset_cancel_event()
        iniciar_operacao()

        def wrapper():
            try:
                acao()
            except Exception as erro:
                log_message(f"[ERRO] {erro}")
            finally:
                root.after(0, finalizar_operacao)

        threading.Thread(target=wrapper, daemon=True).start()

    def limpar_sql_ui():
        caixa_sql.config(state=tk.NORMAL)
        caixa_sql.delete("1.0", tk.END)
        caixa_sql.config(state=tk.DISABLED)

    def atualizar_tabelas_ui(tabelas: Sequence[str]):
        table_selector.set_tables(tabelas)

    def conectar():
        def acao():
            controller.clear_sql_history()
            root.after(0, limpar_sql_ui)
            tabelas = controller.connect(log_message)
            root.after(0, lambda: atualizar_tabelas_ui(tabelas))

        executar_em_thread(acao)

    def atualizar_tabelas():
        def acao():
            tabelas = controller.refresh_tables()
            root.after(0, lambda: atualizar_tabelas_ui(tabelas))
            log_message(f"📋 {len(tabelas)} tabelas atualizadas da origem.")

        executar_em_thread(acao)

    def obter_tabelas_selecionadas() -> List[str]:
        return table_selector.get_selected_tables()

    def solicitar_ajuste_constraint(tabela: str, constraint: str) -> Optional[str]:
        evento = threading.Event()
        resposta: Dict[str, Optional[str]] = {"sql": None}

        def abrir_dialogo():
            dialogo = ConstraintDialog(root, tabela, constraint)
            root.wait_window(dialogo)
            resposta["sql"] = dialogo.resultado
            evento.set()

        root.after(0, abrir_dialogo)
        evento.wait()
        return resposta["sql"]

    def iniciar_migracao():
        tabelas = obter_tabelas_selecionadas()
        if not tabelas:
            messagebox.showwarning(
                "Seleção obrigatória", "Selecione ao menos uma tabela para migrar."
            )
            return

        def acao():
            controller.run_migration(tabelas, log_message, solicitar_ajuste_constraint)

        executar_em_thread(acao)

    def limpar_banco_destino():
        def acao():
            controller.clear_destination_database(log_message)

        executar_em_thread(acao)

    def contar_registros():
        tabelas = obter_tabelas_selecionadas()
        if not tabelas:
            messagebox.showwarning(
                "Seleção obrigatória",
                "Selecione ao menos uma tabela para contagem de registros.",
            )
            return

        def acao():
            controller.count_records(tabelas, log_message)

        executar_em_thread(acao)

    def testar_conexao(destino: str):
        def acao():
            controller.test_connection(destino, log_message)

        executar_em_thread(acao)

    def copiar_sql():
        conteudo = controller.get_sql_history()
        root.clipboard_clear()
        root.clipboard_append("\n".join(conteudo))
        messagebox.showinfo(
            "SQL copiado", "Comandos SQL copiados para a área de transferência."
        )

    def cancelar_operacao():
        if not estado_operacao["em_andamento"]:
            log_message("ℹ️ Nenhuma operação em andamento para cancelar.")
            return
        controller.cancel_current_operation()
        log_message("⏹️ Cancelamento solicitado. Aguardando finalização segura...")

    def abrir_configuracoes():
        config_atual = controller.get_config()

        janela = tk.Toplevel(root)
        janela.title("Editar Configuração")
        janela.geometry("680x700")
        janela.transient(root)

        scroll_container = tk.Frame(janela)
        scroll_container.pack(fill="both", expand=True)

        canvas = tk.Canvas(scroll_container, highlightthickness=0)
        canvas.pack(side=tk.LEFT, fill="both", expand=True)

        scrollbar = tk.Scrollbar(
            scroll_container, orient=tk.VERTICAL, command=canvas.yview
        )
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        canvas.configure(yscrollcommand=scrollbar.set)

        conteudo = tk.Frame(canvas)
        canvas_window = canvas.create_window((0, 0), window=conteudo, anchor="nw")

        def _atualizar_scroll(_event=None):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _ajustar_largura(event):
            canvas.itemconfigure(canvas_window, width=event.width)

        conteudo.bind("<Configure>", _atualizar_scroll)
        canvas.bind("<Configure>", _ajustar_largura)

        editores = {}
        for chave, titulo in (
            ("source", "Origem"),
            ("destination", "Destino"),
            ("model", "Banco Modelo"),
        ):
            editor = ConnectionEditor(conteudo, titulo, config_atual.get(chave, {}))
            editor.pack(fill="x", padx=10, pady=10)
            editores[chave] = editor

        settings_frame = tk.LabelFrame(conteudo, text="Configurações Gerais")
        settings_frame.pack(fill="x", padx=10, pady=10)

        linha_chunk = tk.Frame(settings_frame)
        linha_chunk.pack(fill="x", pady=5)
        tk.Label(linha_chunk, text="Chunk Size:").pack(side=tk.LEFT)
        chunk_entry = tk.Entry(linha_chunk, width=10)
        chunk_entry.pack(side=tk.LEFT, padx=(5, 0))
        chunk_entry.insert(0, str(config_atual["settings"].get("chunk_size", 5000)))

        linha_workers = tk.Frame(settings_frame)
        linha_workers.pack(fill="x", pady=5)
        tk.Label(linha_workers, text="Trabalhadores paralelos:").pack(side=tk.LEFT)
        workers_entry = tk.Entry(linha_workers, width=10)
        workers_entry.pack(side=tk.LEFT, padx=(5, 0))
        workers_entry.insert(0, str(config_atual["settings"].get("worker_count", 1)))

        linha_log = tk.Frame(settings_frame)
        linha_log.pack(fill="x", pady=5)
        tk.Label(linha_log, text="Caminho do Log:").pack(side=tk.LEFT)
        log_entry = tk.Entry(linha_log, width=40)
        log_entry.pack(side=tk.LEFT, padx=(5, 0), fill="x", expand=True)
        log_entry.insert(
            0, str(config_atual["settings"].get("log_path", "logs/dump.log"))
        )

        tk.Label(settings_frame, text="Consulta de Informações:").pack()
        info_text = tk.Text(settings_frame, width=60, height=4)
        info_text.pack(fill="x", pady=(0, 5))
        info_text.insert(tk.END, controller.get_info_query())

        def salvar():
            try:
                novo_config = config_atual.copy()
                for chave, editor in editores.items():
                    novo_config[chave] = editor.obter_dados()
                novo_config["settings"] = {
                    "chunk_size": int(chunk_entry.get().strip()),
                    "worker_count": int(workers_entry.get().strip()),
                    "log_path": log_entry.get().strip(),
                    "info_query": info_text.get("1.0", tk.END).strip(),
                }
                controller.save_config(novo_config)
                messagebox.showinfo(
                    "Configuração", "Configuração salva com sucesso. Refaça a conexão."
                )
                janela.destroy()
            except Exception as erro:
                messagebox.showerror("Erro ao salvar", str(erro))

        botoes_inferiores = tk.Frame(janela)
        botoes_inferiores.pack(fill="x", pady=10)

        criar_botao_colorido(
            botoes_inferiores, "Salvar", salvar, estilo="success"
        ).pack()

    botao_conectar = criar_botao_colorido(
        botoes_superiores, "Conectar", conectar, estilo="primary"
    )
    botao_conectar.grid(row=0, column=0, padx=5, pady=5)
    botoes_operacoes.append(botao_conectar)

    botao_migrar = criar_botao_colorido(
        botoes_superiores, "Iniciar Migração", iniciar_migracao, estilo="success"
    )
    botao_migrar.grid(row=0, column=1, padx=5, pady=5)
    botoes_operacoes.append(botao_migrar)

    botao_contar = criar_botao_colorido(
        botoes_superiores, "Contar Registros", contar_registros, estilo="warning"
    )
    botao_contar.grid(row=0, column=2, padx=5, pady=5)
    botoes_operacoes.append(botao_contar)

    botao_atualizar = criar_botao_colorido(
        botoes_superiores, "Atualizar Tabelas", atualizar_tabelas, estilo="info"
    )
    botao_atualizar.grid(row=0, column=3, padx=5, pady=5)
    botoes_operacoes.append(botao_atualizar)

    botao_limpar = criar_botao_colorido(
        botoes_superiores, "Limpar Banco", limpar_banco_destino, estilo="warning"
    )
    botao_limpar.grid(row=0, column=4, padx=5, pady=5)
    botoes_operacoes.append(botao_limpar)

    botao_cancelar = criar_botao_colorido(
        botoes_superiores, "Cancelar", cancelar_operacao, estilo="danger"
    )
    botao_cancelar.grid(row=0, column=5, padx=5, pady=5)

    botoes_inferiores = tk.Frame(root)
    botoes_inferiores.pack(pady=5)

    criar_botao_colorido(
        botoes_inferiores,
        "Testar Origem",
        lambda: testar_conexao("source"),
        estilo="info",
    ).grid(row=0, column=0, padx=5, pady=5)

    criar_botao_colorido(
        botoes_inferiores,
        "Testar Destino",
        lambda: testar_conexao("destination"),
        estilo="info",
    ).grid(row=0, column=1, padx=5, pady=5)

    criar_botao_colorido(
        botoes_inferiores,
        "Testar Modelo",
        lambda: testar_conexao("model"),
        estilo="info",
    ).grid(row=0, column=2, padx=5, pady=5)

    criar_botao_colorido(
        botoes_inferiores,
        "Editar Configuração",
        abrir_configuracoes,
        estilo="secondary",
    ).grid(row=0, column=3, padx=5, pady=5)

    notebook = ttk.Notebook(root)
    notebook.pack(fill="both", expand=True, padx=10, pady=10)

    aba_log = tk.Frame(notebook)
    aba_sql = tk.Frame(notebook)
    notebook.add(aba_log, text="Logs")
    notebook.add(aba_sql, text="SQL")

    log_texto = tk.Text(aba_log, wrap="word", height=18, font=("Courier", 10))
    log_texto.pack(side=tk.LEFT, fill="both", expand=True)
    log_scroll = tk.Scrollbar(aba_log, command=log_texto.yview)
    log_scroll.pack(side=tk.RIGHT, fill=tk.Y)
    log_texto.config(yscrollcommand=log_scroll.set, state=tk.DISABLED)

    caixa_sql = tk.Text(aba_sql, wrap="word", height=18, font=("Courier", 10))
    caixa_sql.pack(side=tk.LEFT, fill="both", expand=True)
    sql_scroll = tk.Scrollbar(aba_sql, command=caixa_sql.yview)
    sql_scroll.pack(side=tk.RIGHT, fill=tk.Y)
    caixa_sql.config(yscrollcommand=sql_scroll.set, state=tk.DISABLED)

    barra_sql = tk.Frame(root)
    barra_sql.pack(pady=(0, 10))

    criar_botao_colorido(
        barra_sql, "Copiar SQL", copiar_sql, estilo="primary", fonte=("Arial", 10)
    ).pack(side=tk.LEFT, padx=5)
    criar_botao_colorido(
        barra_sql,
        "Limpar SQL",
        lambda: (controller.clear_sql_history(), limpar_sql_ui()),
        estilo="secondary",
        fonte=("Arial", 10),
    ).pack(side=tk.LEFT, padx=5)

    definir_botoes_habilitados(True)

    root.mainloop()


if __name__ == "__main__":
    criar_interface()
