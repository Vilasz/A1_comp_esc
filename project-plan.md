# Projeto de Exemplo – Plataforma de E-commerce e Gestão de Cadeia de Suprimentos

Bem-vindo(a) ao nosso repositório que contém o **micro-framework** e a **aplicação de exemplo** para o contexto de **e-commerce** e **gestão de cadeia de suprimentos**, pelo menos em nossa ideia inicial. Este documento (README) descreve, em detalhes, como estamos estruturando e desenvolvendo este trabalho.

---

## 1. Introdução e Motivação

Este projeto tem como objetivo a criação de um _micro-framework_ que possibilite construir pipelines de processamento de dados (ETL) de forma **concorrente** e **paralela**, alinhado às boas práticas de **eficiência**, **balanceamento de carga** e **baixo acoplamento**.  

Escolhemos o domínio de **Plataforma de E-commerce e Gestão de Cadeia de Suprimentos** para exemplificar o uso do micro-framework, pois esse cenário envolve diversos fluxos de dados, como:
- Pedidos de clientes.
- Informações de estoque.
- Dados de fornecedores e logística (transporte).
- Dados de entregas, devoluções, relatórios de vendas etc.

Dessa forma, podemos demonstrar a capacidade do framework de integrar diferentes fontes de dados, executar transformações e análises em paralelo e fornecer resultados em um **dashboard**.


## 2. Objetivos do Projeto

1. **Micro-Framework**  
   - Fornecer componentes genéricos para manipular Dataframes, Repositórios de dados (arquivos, banco de dados etc.), Tratadores (transformadores de dados) e Triggers (mecanismos de execução).
   - Oferecer suporte a **concorrência** e **paralelismo**, garantindo eficiência e boa escalabilidade.

2. **Projeto de Exemplo (E-commerce + Cadeia de Suprimentos)**  
   - Criar um pipeline ETL que utilize **três fontes de dados** (sendo **duas** delas de naturezas diferentes, por exemplo, um banco de dados SQlite, arquivos CSV e/ou dados em memória).
   - Ter **cinco tratadores** (transformações), onde pelo menos um tratador deve ter grau 2 de entrada e 2 de saída (ou seja, agrupa 2 fluxos e gera 2 fluxos).
   - Incluir **mocks** que simulem os dados de diferentes serviços (por exemplo, simulação de novos pedidos, simulação de atualização de estoque etc.).
   - Fornecer um **dashboard** que apresente **pelo menos 5 análises** resultantes do pipeline, exibindo métricas de tempo de processamento.

3. **Evolução Futura**  
   - Estruturar o projeto de forma modular, para que possamos adicionar novos requisitos, fontes de dados, tratadores e técnicas de forma incremental em trabalhos futuros.

---

## 3. Arquitetura do Micro-Framework

A **arquitetura** do micro-framework se baseia em quatro componentes principais:

1. **Dataframe**  
   - Representação tabular dos dados, com colunas (propriedades) e linhas (instâncias).  
   - Deve permitir operações de acesso, filtragem e iteração, sem recorrer a bibliotecas de alto nível (como `pandas`).  
   - `DataframeBase` (classe base genérica) poderá ser estendida para tipos específicos do domínio (por exemplo, `PedidosDataframe`, `EstoqueDataframe`).

2. **Repositórios de Dados**  
   - Responsáveis por **extrair** e **carregar** dataframes de/para algum meio de armazenamento.  
   - Utilizam um **padrão Strategy**: cada tipo de repositório (CSV, SQlite, In-Memory, etc.) implementa a interface `IDataRepository`.  
   - O **framework** fornecerá alguns repositórios **built-in**:
     - `InMemoryRepository` (mock ou repositório em memória).
     - `FileRepository` (para arquivos CSV/TXT).
     - `SqliteRepository` (para interação com banco de dados local).
   - Cada repositório terá métodos como:
     - `load() -> Dataframe`
     - `save(Dataframe) -> void`

3. **Tratadores (Handlers ou Processors)**  
   - Executam transformações sobre o dataframe, podendo receber 1 ou mais entradas e gerar 1 ou mais saídas.  
   - Um dos tratadores obrigatórios terá **grau 2 de entrada e 2 de saída** (por ex.: combina dados de Pedidos com dados de Estoque para gerar dois outputs: “Status de pedido” e “Alerta de estoque”).  
   - A classe base pode se chamar `BaseHandler`, e cada handler concreto implementa um método `process(inputDataframes) -> outputDataframes`.

4. **Triggers**  
   - Mecanismo de disparo da execução do pipeline.  
   - O framework terá:
     - `TimerTrigger` (executa a cada X milissegundos).
     - `RequestTrigger` (executa a cada chamada de função, simulando requisições de rede).
   - Em nosso projeto, podemos usar **TimerTrigger** para simular, por exemplo, um disparo de processamento a cada 5 segundos, coletando novos pedidos e atualizando relatórios.

### 3.1 Estrutura de Diretórios (Sugestão)

```bash
├── micro_framework
│   ├── dataframe
│   │   ├── dataframe_base.py        # Classe base de Dataframe
│   │   └── pedidos_dataframe.py     # Exemplo de dataframe específico
│   ├── repositories
│   │   ├── i_data_repository.py     # Interface do repositório
│   │   ├── sqlite_repository.py     # Implementação p/ SQLite
│   │   ├── file_repository.py       # Implementação p/ CSV/TXT
│   │   └── in_memory_repository.py  # Implementação in-memory
│   ├── handlers
│   │   ├── base_handler.py          # Classe base
│   │   └── <handlers_concretos>.py  # Vários tratadores
│   ├── triggers
│   │   ├── timer_trigger.py
│   │   └── request_trigger.py
│   └── utils
│       └── concurrency_tools.py     # Ferramentas para threads, filas, etc.
├── ecommerce_example
│   ├── mocks
│   │   ├── mock_estoque.py
│   │   ├── mock_pedidos.py
│   │   └── mock_fornecedores.py
│   ├── pipeline.py                  # Monta todo o pipeline do exemplo
│   ├── dashboard.py                 # Dashboard com visualização
│   └── main.py                      # Ponto de entrada do projeto de exemplo
├── README.md
└── ...
```

# Pipeline de Exemplo no Contexto de E-commerce (Planejamento – Continuação)

# 4. Pipeline de Exemplo no Contexto de E-commerce (Planejamento Detalhado)

Nesta seção, detalharemos o fluxo do pipeline ETL para a plataforma de E-commerce e Gestão de Cadeia de Suprimentos, integrando os conceitos de concorrência e paralelismo estudados nas aulas. O planejamento a seguir foi elaborado considerando a necessidade de balancear a carga computacional, proteger regiões críticas e gerenciar o acesso concorrente aos recursos.

---

## 4.1 Fluxo Geral do Pipeline

- **Coleta de Dados:**
  - **SQLite:** Registros históricos de pedidos.
  - **Arquivos CSV:** Informações de estoque e dados de fornecedores.
  - **Dados em Memória (Mock):** Pedidos novos e atualizações em tempo real.

- **Transformações (Handlers):**
  - **Handler 1 – Filtrar Pedidos Pendentes:** Identifica e extrai pedidos que ainda não foram processados.
  - **Handler 2 – Consolidar Dados de Estoque:** Verifica disponibilidade e atualiza quantidades.
  - **Handler 3 – Calcular Previsão de Entrega:** Usa dados logísticos para estimar prazos.
  - **Handler 4 (Grau 2): Combinar Pedidos e Estoque:** Recebe dois fluxos de entrada (Pedidos e Estoque) e gera:
    - **Relatório de Disponibilidade.**
    - **Alerta de Reposição.**
  - **Handler 5 – Gerar Métricas de Vendas:** Consolida dados para análises estatísticas.

- **Saída (Carregamento):**
  - Armazenamento dos resultados em repositórios (CSV para relatórios, SQLite para histórico) e encaminhamento dos dados para o dashboard.

- **Dashboard:**
  - Exibição de **pelo menos 5 análises**, tais como:
    - Top 5 produtos mais vendidos.
    - Tempo médio de entrega.
    - Total de vendas por período.
    - Pedidos pendentes.
    - Tendência de vendas por categoria.
  - Visualização das métricas de latência – diferença entre o momento de emissão dos dados e a finalização da análise.

---

## 4.2 Diagrama do Pipeline

A seguir, um diagrama ilustrativo (em ASCII) que mostra a organização do fluxo de dados:

        +-----------------------+
        |     Fontes de Dados   |
        +-----------------------+
        |  SQLite    | CSV  | In-Memory (Mock)  |
        +-----+-----+------+-------------------+
              |            | 
              v            v
      +--------------------------+
      |   Coleta de Dados (ETL)  |
      +--------------------------+
              |
              v
 +-----------------------------+
 |     Handlers / Transformações     |
 |                             |
 |  1. Filtrar Pedidos         |
 |  2. Consolidar Estoque      |
 |  3. Previsão de Entrega     |
 |  4. Combinação (Grau 2)       |
 |      - Relatório Disponibilidade |
 |      - Alerta de Reposição       |
 |  5. Métricas de Vendas      |
 +-----------------------------+
              |
              v
     +----------------------+
     |     Carregamento     |
     | (CSV, SQLite, etc.)  |
     +----------------------+
              |
              v
     +----------------------+
     |      Dashboard       |
     |   (Visualização de   |
     |  análises e latência)|
     +----------------------+

> **Placeholder para Diagrama Gráfico:**  
> ![Diagrama do Pipeline APACHE AIRFLOW](https://www.google.com/url?sa=i&url=https%3A%2F%2Fstackacademy.com.br%2Fcurso-data-pipelines-com-apache-airflow%2F&psig=AOvVaw1bhUdlMoVknjFnuqTl3bG-&ust=1743057130521000&source=images&cd=vfe&opi=89978449&ved=0CBQQjRxqFwoTCNDg1_yPp4wDFQAAAAAdAAAAABA1)

---

## 4.3 Considerações de Concorrência e Paralelismo

### Identificação de Problemas
- **Regiões Críticas:**  
  - Acesso simultâneo a Dataframes e repositórios (por exemplo, múltiplos handlers tentando ler/escrever em arquivos ou banco de dados).
  - Concorrência entre threads ao combinar fluxos no Handler de grau 2.
- **Condições de Corrida:**  
  - Execução paralela de handlers que podem gerar resultados inconsistentes se o acesso a recursos compartilhados não for protegido.
- **Balanceamento de Carga:**  
  - Distribuição desigual do processamento entre threads pode causar gargalos.

### Soluções Propostas (Inspiradas nos Mecanismos de Controle Estudados)
- **Locks e Mutexes:**  
  - Proteger operações críticas (ex.: leitura e escrita em repositórios) utilizando mutexes para evitar race conditions.
- **Semáforos:**  
  - Gerenciar a entrada em regiões críticas quando mais de um thread pode processar dados, mas de forma controlada.
- **Queues (Filas):**  
  - Utilizar estruturas de fila para a passagem de dados entre os handlers, garantindo a ordem e evitando conflitos.
- **Thread Pools:**  
  - Configurar pools de threads para limitar a quantidade de threads simultâneas de acordo com a capacidade do hardware.
- **Monitores:**  
  - Encapsular a lógica de acesso concorrente aos recursos compartilhados, tornando os objetos “thread-safe”.

<!-- **Dica Visual:**  -->
<!-- > Inclua um diagrama de fluxo de controle (por exemplo, um diagrama de estados) para ilustrar a transição entre estados de execução, bloqueio e liberação de recursos em regiões críticas.  
> **Placeholder para Diagrama de Controle:**  
> ![Diagrama de Controle de Concorrência](path/to/diagrama_concorrencia.png)
-->

## 4.4 Estratégia de Balanceamento e Sincronização

1. **Pré-implementação:**
   - Mapear todos os pontos do pipeline onde múltiplas threads podem acessar os mesmos recursos.
   - Definir os "hot-spots" (pontos de maior contenção) que exigirão mecanismos de sincronização.

2. **Durante a Implementação:**
   - **Testes Unitários e de Integração:**  
     - Implementar testes para validar que os locks, semáforos e queues estão funcionando conforme esperado, sem deadlocks ou condições de corrida.
   - **Análise de Performance:**  
     - Medir o tempo de execução das diferentes fases do pipeline e ajustar a configuração do thread pool para otimizar o balanceamento.
   - **Iteração e Ajuste Fino:**  
     - Refinar a granularidade dos locks (por exemplo, bloquear apenas a seção crítica e não o handler inteiro) para minimizar a espera ocupada.

3. **Pós-Implementação:**
   - Coletar métricas detalhadas (latência, throughput, tempo de espera) e ajustar a distribuição de carga.
   - Revisar e documentar os problemas de concorrência encontrados e as soluções adotadas, com base nos conceitos de exclusão mútua, progresso e espera limitada.


## 4.5 Resumo do Planejamento do Pipeline

- **Fluxo de Dados:** Desde a coleta até a apresentação das análises, com transformação em múltiplas etapas.
- **Proteção de Regiões Críticas:** Aplicação de mecanismos de controle (mutex, semáforo, queues) para evitar condições de corrida e deadlocks.
- **Balanceamento de Carga:** Utilização de thread pools e estratégias de sincronização para maximizar o desempenho e utilizar todas as unidades de processamento disponíveis.
- **Visualização:** Um dashboard que não só apresenta os resultados, mas também métricas de desempenho (ex.: latência de processamento) para avaliação contínua da eficiência do pipeline.

