import csv
import random

N = 10000

def gerar_pedidos_csv(qtd_pedidos, nome_arquivo="mock_data_db.csv"):
    produtos_disponiveis = [
        "Notebook", "Mouse", "Teclado", "Smartphone", "Fone de Ouvido",
        "Monitor", "Cadeira Gamer", "Mesa para Computador", "Impressora",
        "Webcam", "HD Externo", "SSD", "Placa de Vídeo", "Memória RAM",
        "Fonte ATX", "Placa-mãe", "Roteador Wi-Fi", "Leitor de Cartão SD",
        "Grampeador", "Luminária de Mesa", "Estabilizador", "Suporte para Notebook",
        "Mousepad Gamer", "Caixa de Som Bluetooth", "Power Bank", "Scanner",
        "Projetor", "Filtro de Linha", "Cabo USB-C"
    ]

    centros_logisticos = [
        "São Paulo", "Rio de Janeiro", "Belo Horizonte",
        "Curitiba", "Porto Alegre", "Salvador", "Manaus",
        "Brasília", "Fortaleza", "Cuiabá"
    ]

    with open(nome_arquivo, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["produto", "quantidade", "centro_logistico_mais_proximo"])

        for _ in range(qtd_pedidos):
            produto = random.choice(produtos_disponiveis)
            quantidade = random.choices(
                population=range(1, 11),
                weights=[10, 9, 8, 6, 5, 3, 2, 1, 1, 1],
                k=1
            )[0]
            centro = random.choice(centros_logisticos)

            writer.writerow([produto, quantidade, centro])

    print(f"Arquivo '{nome_arquivo}' gerado com sucesso com {qtd_pedidos} pedidos.")

# Exemplo de uso
if __name__ == "__main__":
    gerar_pedidos_csv(N)  # Gere quantos quiser aqui