import sqlite3

def create_database():
    
    # Connect to database
    conn = sqlite3.connect('ecommerce.db')
    cursor = conn.cursor()

    # Category table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS categorias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL UNIQUE,
        descricao TEXT
    )
    ''')
    
    # Product table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS produtos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        descricao TEXT,
        preco REAL NOT NULL CHECK(preco > 0),
        estoque INTEGER DEFAULT 0 CHECK(estoque >= 0),
        categoria_id INTEGER NOT NULL,
        data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        ativo BOOLEAN DEFAULT TRUE,
        FOREIGN KEY (categoria_id) REFERENCES categorias(id)
    )
    ''')
    
    # Clients table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS clientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL,
        cpf TEXT UNIQUE,
        telefone TEXT,
        data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        endereco_json TEXT,
        ativo BOOLEAN DEFAULT TRUE
    )
    ''')
    
    # Logistics centers table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS centros_logisticos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        endereco_json TEXT NOT NULL,
        capacidade INTEGER NOT NULL CHECK(capacidade > 0),
        capacidade_utilizada INTEGER DEFAULT 0 CHECK(capacidade_utilizada >= 0),
        ativo BOOLEAN DEFAULT TRUE,
        CONSTRAINT capacidade_valida CHECK (capacidade_utilizada <= capacidade)
    )
    ''')
    
    # Order table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pedidos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id INTEGER NOT NULL,
        data_pedido TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status TEXT NOT NULL DEFAULT 'novo' CHECK(status IN ('novo', 'processando', 'enviado', 'entregue', 'cancelado')),
        centro_logistico_id INTEGER,
        valor_total REAL CHECK(valor_total >= 0),
        endereco_entrega_json TEXT NOT NULL,
        FOREIGN KEY (cliente_id) REFERENCES clientes(id),
        FOREIGN KEY (centro_logistico_id) REFERENCES centros_logisticos(id)
    )
    ''')
    
    # Items table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS itens_pedido (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pedido_id INTEGER NOT NULL,
        produto_id INTEGER NOT NULL,
        quantidade INTEGER NOT NULL CHECK(quantidade > 0),
        preco_unitario REAL NOT NULL CHECK(preco_unitario >= 0),
        FOREIGN KEY (pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE,
        FOREIGN KEY (produto_id) REFERENCES produtos(id),
        UNIQUE (pedido_id, produto_id)
    )
    ''')
    
    # Delivery table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS entregas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pedido_id INTEGER NOT NULL UNIQUE,
        data_envio TIMESTAMP,
        data_prevista TIMESTAMP NOT NULL,
        data_entrega TIMESTAMP,
        status TEXT NOT NULL DEFAULT 'pendente' CHECK(status IN ('pendente', 'processando', 'transito', 'entregue', 'atrasada', 'cancelada')),
        transportadora TEXT,
        codigo_rastreio TEXT,
        FOREIGN KEY (pedido_id) REFERENCES pedidos(id)
    )
    ''')
    
    # Metrics table for dashboard
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS metricas_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome_metrica TEXT NOT NULL UNIQUE,
        valor_json TEXT NOT NULL,
        data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_pedidos_status ON pedidos(status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_itens_pedido_produto ON itens_pedido(produto_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_entregas_status ON entregas(status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_produtos_categoria ON produtos(categoria_id)')
    
    conn.commit()
    conn.close()

    print("Estrutura do banco de dados criada com sucesso!")

if __name__ == "__main__":
    create_database()