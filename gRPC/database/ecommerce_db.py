import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, Time
from sqlalchemy.orm import sessionmaker, declarative_base
import datetime

# --- Defining database connection and engine ---
# This will create a file 'my_data.db' to be my database in this same folder
DATABASE_URL = "sqlite:///./ecommerce.db"
engine = create_engine(DATABASE_URL, echo=False)

# --- Defining ORM model using SQLAlchemy ---
Base = declarative_base()

class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    cliente_id = Column(Integer)
    produto_id = Column(Integer)
    categoria_id = Column(Integer)
    produto = Column(String)
    quantidade = Column(Integer)
    preco_unitario = Column(Float)
    valor_total = Column(Float)
    data_pedido = Column(Date)
    hora_pedido = Column(Time)
    mes = Column(Integer)
    ano = Column(Integer)
    canal_venda = Column(String)
    centro_logistico_mais_proximo = Column(String)
    cidade_cliente = Column(String)
    estado_cliente = Column(String)
    dias_para_entrega = Column(Integer)

    def __repr__(self):
        return (
            f"<Order(id={self.id}, cliente_id={self.cliente_id}, produto_id={self.produto_id}, "
            f"categoria_id={self.categoria_id}, produto='{self.produto}', quantidade={self.quantidade}, "
            f"preco_unitario={self.preco_unitario}, valor_total={self.valor_total}, data_pedido={self.data_pedido}, "
            f"hora_pedido={self.hora_pedido}, mes={self.mes}, ano={self.ano}, canal_venda='{self.canal_venda}', "
            f"centro_logistico_mais_proximo='{self.centro_logistico_mais_proximo}', cidade_cliente='{self.cidade_cliente}', "
            f"estado_cliente='{self.estado_cliente}', dias_para_entrega={self.dias_para_entrega})>"

        )
    

class NewOrder(Base):
    __tablename__ = "new_orders"

    id = Column(Integer, primary_key=True, index=True)
    cliente_id = Column(Integer)
    produto_id = Column(Integer)
    categoria_id = Column(Integer)
    produto = Column(String)
    quantidade = Column(Integer)
    preco_unitario = Column(Float)
    valor_total = Column(Float)
    data_pedido = Column(Date)
    hora_pedido = Column(Time)
    mes = Column(Integer)
    ano = Column(Integer)
    canal_venda = Column(String)
    centro_logistico_mais_proximo = Column(String)
    cidade_cliente = Column(String)
    estado_cliente = Column(String)
    dias_para_entrega = Column(Integer)

    def __repr__(self):
        return (
            f"<Order(id={self.id}, cliente_id={self.cliente_id}, produto_id={self.produto_id}, "
            f"categoria_id={self.categoria_id}, produto='{self.produto}', quantidade={self.quantidade}, "
            f"preco_unitario={self.preco_unitario}, valor_total={self.valor_total}, data_pedido={self.data_pedido}, "
            f"hora_pedido={self.hora_pedido}, mes={self.mes}, ano={self.ano}, canal_venda='{self.canal_venda}', "
            f"centro_logistico_mais_proximo='{self.centro_logistico_mais_proximo}', cidade_cliente='{self.cidade_cliente}', "
            f"estado_cliente='{self.estado_cliente}', dias_para_entrega={self.dias_para_entrega})>"

        )
    
# --- Creating the tables ---
# This checks if the table exists and creates it if it doesn't.
Base.metadata.create_all(bind=engine)
print("Tables 'orders' and 'new_orders' created or already existing.")

# --- Creating a Session to Interact with the Database ---
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
db_session = SessionLocal() # This is our ORM session

format_date = "%Y-%m-%d"
format_time = "%H:%M:%S"

def newOrder(
    cliente_id: int,
    produto_id: int,
    categoria_id: int,
    produto: str,
    quantidade: int,
    preco_unitario: float,
    valor_total: float,
    data_pedido: str,
    hora_pedido: str,
    mes: int,
    ano: int,
    canal_venda: str,
    centro_logistico_mais_proximo: str,
    cidade_cliente: str,
    estado_cliente: str,
    dias_para_entrega: int
):
    return Order(
        cliente_id = cliente_id,
        produto_id = produto_id,
        categoria_id = categoria_id,
        produto = produto,
        quantidade = quantidade,
        preco_unitario = preco_unitario,
        valor_total = valor_total,
        data_pedido = datetime.datetime.strptime(data_pedido, format_date).date(),
        hora_pedido = datetime.datetime.strptime(hora_pedido, format_time).time(),
        mes = mes,
        ano = ano,
        canal_venda = canal_venda,
        centro_logistico_mais_proximo = centro_logistico_mais_proximo,
        cidade_cliente = cidade_cliente,
        estado_cliente = estado_cliente,
        dias_para_entrega = dias_para_entrega,
    )

if __name__ == "__main__":
    print("Adding mock data to 'orders' table...")
    new_orders = [
        newOrder(
            cliente_id = 341268,
            produto_id = 16,
            categoria_id = 12,
            produto = "Placa‑mãe",
            quantidade = 1,
            preco_unitario = 2100.39,
            valor_total = 2100.39,
            data_pedido = "2023-05-09",
            hora_pedido = "23:45:05",
            mes = 4,
            ano = 2024,
            canal_venda = "site",
            centro_logistico_mais_proximo = "Rio de Janeiro",
            cidade_cliente = "Souza de Novais",
            estado_cliente = "AL",
            dias_para_entrega = 8
        ),
        newOrder(
            cliente_id = 246921,
            produto_id = 11,
            categoria_id = 12,
            produto = "HD Externo",
            quantidade = 5,
            preco_unitario = 1454.16,
            valor_total = 7270.8,
            data_pedido = "2023-08-02",
            hora_pedido = "14:40:43",
            mes = 4,
            ano = 2024,
            canal_venda = "site",
            centro_logistico_mais_proximo = "São Paulo",
            cidade_cliente = "Rezende do Sul",
            estado_cliente = "AC",
            dias_para_entrega = 9,
        ),
    ]
    db_session.add_all(new_orders)
    db_session.commit() # Save changes to the database
    print(f"{len(new_orders)} order added.")

    print("\n--- Reading 'products' table into Pandas DataFrame using ORM Query ---")
    orm_query = db_session.query(Order) # SQLAlchemy ORM query object
    # Convert the ORM query results directly to a DataFrame
    df_from_orm = pd.read_sql(orm_query.statement, con=db_session.bind)
    print(df_from_orm.head())