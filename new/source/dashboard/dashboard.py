# dashboard.py 
"""
Dashboard Streamlit para a base ecommerce.db.

Métricas exibidas
-----------------
• Cards de KPI (clientes, pedidos, itens, receita)
• Faturamento mensal (linha)
• Top‑10 produtos por receita (barra horizontal)
• Pedidos por status (pizza)
• Treemap receita × centro logístico
• Tabela detalhada de pedidos com filtros
"""

from __future__ import annotations

import sqlite3
import textwrap
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


# CONFIGURAÇÕES BÁSICAS                                             


st.set_page_config(
    page_title="E‑commerce Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="📊",
)

CSS = """
<style>
.big-number {font-size: 2.8rem; font-weight: 700; margin: -0.5rem 0 0.5rem;}
.metric-label {text-transform: uppercase; letter-spacing: 0.08rem;
               font-size: 0.75rem; color: #888;}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


# SELETOR DO BANCO (SIDEBAR)                                        


DEFAULT_DB = Path(__file__).with_name("ecommerce.db")

st.sidebar.header("Configuração")
db_input = st.sidebar.text_input(
    "Caminho do SQLite",
    value=str(DEFAULT_DB),
    help="Informe o caminho completo ou relativo do arquivo .db",
)
DB_PATH = Path(db_input).expanduser().resolve()


# CONEXÃO / CONSULTAS (caching agressivo)                            



@st.cache_resource(
    show_spinner=False,
    hash_funcs={sqlite3.Connection: id},  # evita hashing caro/instável
)
def _conn(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        st.sidebar.error(f"Arquivo não encontrado: {db_path}")
        st.stop()
    con = sqlite3.connect(str(db_path), check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


CON = _conn(DB_PATH)


@st.cache_data(ttl=300, show_spinner=False, hash_funcs={sqlite3.Connection: id})
def load_tables() -> dict[str, pd.DataFrame]:
    """Lê todas as tabelas relevantes em um dicionário de DataFrames."""
    dfs: dict[str, pd.DataFrame] = {}
    for tbl in (
        "clientes",
        "produtos",
        "pedidos",
        "itens_pedido",
        "entregas",
        "centros_logisticos",
    ):
        dfs[tbl] = pd.read_sql_query(f"SELECT * FROM {tbl}", CON)
    return dfs


dfs = load_tables()
clientes = dfs["clientes"]
produtos = dfs["produtos"]
pedidos = dfs["pedidos"]
itens = dfs["itens_pedido"]
entregas = dfs["entregas"]
centros = dfs["centros_logisticos"]


# MÉTRICAS DE TOPO (KPI CARDS)                                       



# ── ajuste do helper 
def _metric_block(label: str, value, col):
    """Renderiza um card de KPI. Aceita str ou número."""
    if isinstance(value, (int, float)):
        value_str = f"{value:,.0f}"
    else:  # já vem como string formatada
        value_str = str(value)

    with col:
        st.markdown(
            f'<div class="metric-label">{label}</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="big-number">{value_str}</div>',
            unsafe_allow_html=True,
        )

col1, col2, col3, col4 = st.columns(4)

# chamada para o KPI Receita  
_metric_block("clientes", len(clientes), col1)
_metric_block("pedidos", len(pedidos), col2)
_metric_block("itens vendidos", itens["quantidade"].sum(), col3)
_metric_block("receita (R$)", f'{float(pedidos["valor_total"].sum()):,.0f}', col4)

st.markdown("---")


# TRANSFORMAÇÕES AUXILIARES                                          


# receita por mês (usa data_envio se existir; senão rótulo "Sem data")
ent_map = entregas.set_index("pedido_id")["data_envio"]
ent_map_dict = ent_map.to_dict()

pedidos = pedidos.copy()  # evita SettingWithCopyWarning

pedidos["data_envio"] = pd.to_datetime(
    pedidos["id"].map(ent_map_dict), errors="coerce"  # strings → datetime / NaT
)

pedidos["mes"] = (
    pedidos["data_envio"]
    .dt.to_period("M")
    .astype(str)
    .fillna("Sem data")
)

rev_mes = (
    pedidos.groupby("mes", dropna=False)["valor_total"]
    .sum()
    .reset_index()
    .sort_values("mes")
)

# detalhamento item × receita
itens_join = (
    itens.merge(produtos[["id", "nome"]], left_on="produto_id", right_on="id", suffixes=("", "_prod"))
    .merge(pedidos[["id", "centro_logistico_id"]], left_on="pedido_id", right_on="id", suffixes=("", "_ped"))
)
itens_join["receita"] = itens_join["quantidade"] * itens_join["preco_unitario"]

top_prod = (
    itens_join.groupby("nome", as_index=False)["receita"]
    .sum()
    .nlargest(10, "receita")
)

rev_centro = (
    itens_join.merge(
        centros[["id", "nome"]].rename(columns={"nome": "centro_nome"}),
        left_on="centro_logistico_id",
        right_on="id",
    )
    .groupby("centro_nome", as_index=False)["receita"]
    .sum()
    .rename(columns={"centro_nome": "Centro", "receita": "Receita"})
)



# GRÁFICOS                                                           


col_a, col_b = st.columns([2, 1])

with col_a:
    fig_rev = px.line(
        rev_mes,
        x="mes",
        y="valor_total",
        markers=True,
        labels={"mes": "Mês", "valor_total": "Receita (R$)"},
        title="Faturamento mensal",
    )
    st.plotly_chart(fig_rev, use_container_width=True)

with col_b:
    fig_status = px.pie(
        pedidos,
        names="status",
        title="Distribuição de pedidos por status",
        hole=0.35,
    )
    st.plotly_chart(fig_status, use_container_width=True)

col_c, col_d = st.columns([1, 1])

with col_c:
    fig_top = px.bar(
        top_prod.sort_values("receita"),
        x="receita",
        y="nome",
        orientation="h",
        text="receita",
        labels={"nome": "", "receita": "Receita (R$)"},
        title="Top‑10 produtos por receita",
    )
    fig_top.update_layout(yaxis_categoryorder="total ascending")
    st.plotly_chart(fig_top, use_container_width=True)

with col_d:
    fig_heat = px.treemap(
        rev_centro,
        path=["Centro"],
        values="Receita",
        title="Receita por centro logístico",
    )

    st.plotly_chart(fig_heat, use_container_width=True)

st.markdown("---")


# TABELA DETALHADA (com filtros)                                     


st.subheader("Detalhe de pedidos")

# filtros
col_f1, col_f2, col_f3 = st.columns(3)
status_sel = col_f1.multiselect(
    "Status", sorted(pedidos["status"].unique()), default=[]
)
centro_sel = col_f2.multiselect(
    "Centro logístico",
    sorted(centros["nome"]),
    default=[],
)
busca_cliente = col_f3.text_input("Busca por cliente (contém)")

# aplica filtros
ped_f = pedidos.copy()
if status_sel:
    ped_f = ped_f[ped_f["status"].isin(status_sel)]
if centro_sel:
    ids_centros = centros[centros["nome"].isin(centro_sel)]["id"]
    ped_f = ped_f[ped_f["centro_logistico_id"].isin(ids_centros)]
if busca_cliente:
    ids_cli = clientes[clientes["nome"].str.contains(busca_cliente, case=False, na=False)]["id"]
    ped_f = ped_f[ped_f["cliente_id"].isin(ids_cli)]

# agrega itens por pedido
it_sum = itens.groupby("pedido_id")["quantidade"].sum()
ped_f = ped_f.assign(itens=ped_f["id"].map(it_sum).fillna(0).astype(int))

st.dataframe(
    ped_f[
        [
            "id",
            "cliente_id",
            "status",
            "valor_total",
            "itens",
            "centro_logistico_id",
        ]
    ]
    .rename(
        columns={
            "id": "Pedido",
            "cliente_id": "Cliente",
            "valor_total": "Valor (R$)",
            "itens": "Qtd. itens",
            "centro_logistico_id": "Centro ID",
        }
    )
    .sort_values("Pedido", ascending=False),
    use_container_width=True,
)


# RODAPÉ                                                             


with st.expander("📄 SQL das consultas", expanded=False):
    st.code(
        textwrap.dedent(
            """
            -- receita mensal
            SELECT strftime('%Y-%m', e.data_envio) AS mes,
                   SUM(p.valor_total)              AS receita
            FROM pedidos p
            JOIN entregas e ON e.pedido_id = p.id
            GROUP BY 1
            ORDER BY 1;

            -- top‑10 produtos
            SELECT pr.nome,
                   SUM(ip.quantidade * ip.preco_unitario) AS receita
            FROM itens_pedido ip
            JOIN produtos pr ON pr.id = ip.produto_id
            GROUP BY pr.nome
            ORDER BY receita DESC
            LIMIT 10;
            """
        ),
        language="sql",
    )

st.caption(f"Dados carregados de: **{DB_PATH}**")
# 
