import sqlite3

conn = sqlite3.connect("data_messages.db")
cursor = conn.cursor()

# Obter o número de linhas (registros)
cursor.execute("SELECT COUNT(*) FROM data_messages")
num_linhas = cursor.fetchone()[0]

# Obter o número de colunas (campos)
cursor.execute("PRAGMA table_info(data_messages)")
num_colunas = len(cursor.fetchall())

print(f"Número de linhas: {num_linhas}")
print(f"Número de colunas: {num_colunas}")

conn.close()