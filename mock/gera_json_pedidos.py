import json
import random

N = 1000
 
def gerar_pedidos(qtd_pedidos):
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

    pedidos = []

    for _ in range(qtd_pedidos):
        produto = random.choice(produtos_disponiveis)
        quantidade = random.choices(
                population=range(1, 11),
                weights=[15, 10, 5, 3, 3, 2, 2, 2, 1, 1],
                k=1
            )[0]
        centro = random.choice(centros_logisticos)

        pedido = {
            "produto": produto,
            "quantidade": quantidade,
            "centro_logistico_mais_proximo": centro
        }

        pedidos.append(pedido)
    
    dados = {"pedidos": pedidos}
    
    with open("mock_data_pedidos_novos.json", "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=2)
        
    print("Arquivo 'mock_data_pedidos_novos.json' criado com sucesso.") 

if __name__ == "__main__":
    gerar_pedidos(N)