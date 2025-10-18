import tkinter as tk
from tkinter import messagebox
import json
import threading
from dump import executar_dump
from db_firebird import conectar_firebird
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

def executar_migracao(entry_tabela, caixa_saida):
    tabela = entry_tabela.get().strip()
    if not tabela:
        escrever_saida(caixa_saida, "[ERRO] Informe o nome da tabela.")
        return

    def run():
        try:
            config = carregar_config()
            escrever_saida(caixa_saida, f"🔄 Iniciando migração da tabela '{tabela}'...")
            total, tempo = executar_dump(tabela, config, log_fn=lambda msg: escrever_saida(caixa_saida, msg))
            escrever_saida(caixa_saida, f"✅ Migração concluída: {total} registros em {tempo:.2f} segundos.")
        except Exception as e:
            escrever_saida(caixa_saida, f"[ERRO] {e}")

    threading.Thread(target=run).start()

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

def contar_registros(entry_tabela, caixa_saida):
    tabela = entry_tabela.get().strip()
    if not tabela:
        escrever_saida(caixa_saida, "[ERRO] Informe a tabela para contar os registros.")
        return

    def run():
        try:
            config = carregar_config()
            con_fb = conectar_firebird(config)
            cur_fb = con_fb.cursor()
            cur_fb.execute(f"SELECT COUNT(*) FROM {tabela}")
            total_fb = cur_fb.fetchone()[0]
            con_fb.close()

            con_sql = conectar_mssql(config)
            cur_sql = con_sql.cursor()
            cur_sql.execute(f"SELECT COUNT(*) FROM {tabela}")
            total_sql = cur_sql.fetchone()[0]
            con_sql.close()

            escrever_saida(caixa_saida, f"📌 Total na origem (Firebird): {total_fb} registros")
            escrever_saida(caixa_saida, f"📌 Total no destino (MSSQL): {total_sql} registros")

        except Exception as e:
            escrever_saida(caixa_saida, f"[ERRO] ao contar registros: {e}")

    threading.Thread(target=run).start()

def abrir_edicao_config():
    config = carregar_config()

    def salvar_e_fechar():
        try:
            config['firebird']['database'] = firebird_db.get()
            config['firebird']['host'] = firebird_host.get()
            config['firebird']['port'] = int(firebird_port.get())
            config['firebird']['user'] = firebird_user.get()
            config['firebird']['password'] = firebird_pwd.get()

            config['mssql']['server'] = mssql_server.get()
            config['mssql']['database'] = mssql_db.get()
            config['mssql']['user'] = mssql_user.get()
            config['mssql']['password'] = mssql_pwd.get()

            config['settings']['chunk_size'] = int(chunk_size.get())
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
    firebird_db.insert(0, config['firebird']['database'])
    firebird_db.pack()

    tk.Label(config_window, text="Firebird - Host:").pack()
    firebird_host = tk.Entry(config_window, width=60)
    firebird_host.insert(0, config['firebird']['host'])
    firebird_host.pack()

    tk.Label(config_window, text="Firebird - Porta:").pack()
    firebird_port = tk.Entry(config_window, width=60)
    firebird_port.insert(0, str(config['firebird']['port']))
    firebird_port.pack()

    tk.Label(config_window, text="Firebird - Usuário:").pack()
    firebird_user = tk.Entry(config_window, width=60)
    firebird_user.insert(0, config['firebird']['user'])
    firebird_user.pack()

    tk.Label(config_window, text="Firebird - Senha:").pack()
    firebird_pwd = tk.Entry(config_window, width=60)
    firebird_pwd.insert(0, config['firebird']['password'])
    firebird_pwd.pack()

    # MSSQL
    tk.Label(config_window, text="MSSQL - Servidor:").pack()
    mssql_server = tk.Entry(config_window, width=60)
    mssql_server.insert(0, config['mssql']['server'])
    mssql_server.pack()

    tk.Label(config_window, text="MSSQL - Nome do Banco:").pack()
    mssql_db = tk.Entry(config_window, width=60)
    mssql_db.insert(0, config['mssql']['database'])
    mssql_db.pack()

    tk.Label(config_window, text="MSSQL - Usuário:").pack()
    mssql_user = tk.Entry(config_window, width=60)
    mssql_user.insert(0, config['mssql']['user'])
    mssql_user.pack()

    tk.Label(config_window, text="MSSQL - Senha:").pack()
    mssql_pwd = tk.Entry(config_window, width=60)
    mssql_pwd.insert(0, config['mssql']['password'])
    mssql_pwd.pack()

    # Outros
    tk.Label(config_window, text="Tamanho do bloco (chunk_size):").pack()
    chunk_size = tk.Entry(config_window, width=20)
    chunk_size.insert(0, str(config['settings']['chunk_size']))
    chunk_size.pack()

    tk.Button(config_window, text="Salvar Configuração", command=salvar_e_fechar).pack(pady=10)

def criar_interface():
    root = tk.Tk()
    root.title("Migração Firebird → MSSQL (pymssql)")
    root.geometry("680x580")

    tk.Label(root, text="Tabela a migrar:", font=("Arial", 12)).pack(pady=5)
    entry_tabela = tk.Entry(root, font=("Arial", 12), width=50)
    entry_tabela.pack(pady=5)

    frame_botoes = tk.Frame(root)
    frame_botoes.pack(pady=10)

    tk.Button(frame_botoes, text="Iniciar Migração", font=("Arial", 10),
              command=lambda: executar_migracao(entry_tabela, caixa_saida)).grid(row=0, column=0, padx=5)

    tk.Button(frame_botoes, text="Editar Configuração", font=("Arial", 10),
              command=abrir_edicao_config).grid(row=0, column=1, padx=5)

    tk.Button(frame_botoes, text="Testar Firebird", font=("Arial", 10),
              command=lambda: testar_conexao_firebird(caixa_saida)).grid(row=0, column=2, padx=5)

    tk.Button(frame_botoes, text="Testar MSSQL", font=("Arial", 10),
              command=lambda: testar_conexao_mssql(caixa_saida)).grid(row=0, column=3, padx=5)

    tk.Button(frame_botoes, text="Contar Registros", font=("Arial", 10),
              command=lambda: contar_registros(entry_tabela, caixa_saida)).grid(row=0, column=4, padx=5)

    tk.Label(root, text="Saída de Log:").pack()

    caixa_saida = tk.Text(root, wrap="word", height=20, font=("Courier", 10))
    caixa_saida.pack(fill="both", expand=True, padx=10, pady=5)

    scrollbar = tk.Scrollbar(root, command=caixa_saida.yview)
    caixa_saida.config(yscrollcommand=scrollbar.set)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    root.mainloop()

if __name__ == "__main__":
    criar_interface()
