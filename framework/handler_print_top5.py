import matplotlib.pyplot as plt
from dataframe import DataFrame
from pedidos_handler import CSVHandler
from handler_top_5_produtos import top_5_mais_vendidos

def plot_top_5(df: DataFrame, coluna_produto: str = "produto", coluna_quantidade: str = "sum(quantidade)"):
    """
    Plota um gráfico de ranking (barras horizontais) dos 5 produtos mais vendidos.

    Entradas:
        df (DataFrame): DataFrame com os top 5 produtos e suas quantidades vendidas.
        coluna_produto (str): Nome da coluna dos produtos.
        coluna_quantidade (str): Nome da coluna com os totais vendidos (após agregação).
    """
    dados = df.to_dicts()
    
    produtos = [d[coluna_produto] for d in dados]
    quantidades = [d[coluna_quantidade] for d in dados]

    produtos = produtos[::-1]
    quantidades = quantidades[::-1]

    plt.figure(figsize=(8, 5))
    bars = plt.barh(produtos, quantidades, color="#4CAF50", edgecolor="black")

    # Anotações de valor
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 0.5, bar.get_y() + bar.get_height() / 2, str(int(width)), va='center')

    plt.xlabel("Quantidade Vendida")
    plt.title("Top 5 Produtos Mais Vendidos")
    plt.grid(axis='x', linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.show()
    
    
if __name__ == "__main__":
    csv_handler = CSVHandler()
    df_orders_history = csv_handler.extract_data("mock_data_db.csv")
    print("Size of orders history before update: ", df_orders_history.shape())

    top_vendidos = top_5_mais_vendidos(df_orders_history)
    
    plot_top_5(top_vendidos)