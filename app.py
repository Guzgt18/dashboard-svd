import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import plotly.express as px
import json
import os

# ── 1. Configuração da página ────────────────────────────────
st.set_page_config(
    page_title="Dashboard de Vendas SVD",
    page_icon="📊",
    layout="wide"
)

# ── 2. Autenticação ──────────────────────────────────────────
@st.cache_data(ttl=3600)
def carregar_dados():
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds_env = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_env:
        creds = Credentials.from_service_account_info(json.loads(creds_env), scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(
            "dashpython-494416-dddeafb9a87e.json", scopes=SCOPES
        )

    gc = gspread.authorize(creds)
    worksheet = gc.open_by_key(
        "1mGQWM8CCBeNy8TdupA7OMR8kNerWQiRQ1X8TEm97mG0"
    ).worksheet("VOL. VENDAS GERAL")

    dados = worksheet.get_all_values()
    headers = dados[0]
    colunas = ["DATA", "ANO_MÊS", "CLIENTE", "DESCRIÇÃO", "ATIVO", "QTD", "LARGURA"]
    indices = {col: headers.index(col) for col in colunas if col in headers}
    rows = [{col: row[idx] for col, idx in indices.items()} for row in dados[1:]]

    df = pd.DataFrame(rows)
    
    # Corrige vírgula decimal e converte QTD
    df["QTD"] = df["QTD"].str.replace(",", ".").str.strip()
    df["QTD"] = pd.to_numeric(df["QTD"], errors="coerce").fillna(0)
    
    df = df[df["ATIVO"] == "S"].copy()
    df["ANO"] = df["ANO_MÊS"].str[:4]
    df["MES"] = df["ANO_MÊS"].str[5:]
    return df

# ── 3. Carrega dados ─────────────────────────────────────────
with st.spinner("Carregando dados da planilha..."):
    df_raw = carregar_dados()

# ── 4. Filtros na barra lateral ──────────────────────────────
st.sidebar.title("🔍 Filtros")

anos = sorted(df_raw["ANO"].unique())
meses = {"01":"Janeiro","02":"Fevereiro","03":"Março","04":"Abril",
         "05":"Maio","06":"Junho","07":"Julho","08":"Agosto",
         "09":"Setembro","10":"Outubro","11":"Novembro","12":"Dezembro"}

col1, col2 = st.sidebar.columns(2)
ano_ini = col1.selectbox("De (Ano)", anos, index=0)
mes_ini = col2.selectbox("De (Mês)", list(meses.keys()), format_func=lambda x: meses[x], index=0)

col3, col4 = st.sidebar.columns(2)
ano_fim = col3.selectbox("Até (Ano)", anos, index=len(anos)-1)
mes_fim = col4.selectbox("Até (Mês)", list(meses.keys()), format_func=lambda x: meses[x], index=11)

cliente_sel = st.sidebar.multiselect("🏢 Cliente", sorted(df_raw["CLIENTE"].unique()), placeholder="Todos os clientes")

# ── 5. Filtragem ─────────────────────────────────────────────
period_ini = f"{ano_ini}-{mes_ini}"
period_fim = f"{ano_fim}-{mes_fim}"

df = df_raw[
    (df_raw["ANO_MÊS"] >= period_ini) &
    (df_raw["ANO_MÊS"] <= period_fim)
]
if cliente_sel:
    df = df[df["CLIENTE"].isin(cliente_sel)]

# ── 6. KPIs ──────────────────────────────────────────────────
st.title("📊 Dashboard de Vendas SVD")

vendas_mes = df.groupby("ANO_MÊS", as_index=False)["QTD"].sum()
melhor_mes = vendas_mes.loc[vendas_mes["QTD"].idxmax(), "ANO_MÊS"] if not vendas_mes.empty else "-"

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Vendido (m)", f"{df['QTD'].sum():,.0f} m")
k2.metric("Clientes Únicos", df["CLIENTE"].nunique())
k3.metric("Produtos Distintos", df["DESCRIÇÃO"].nunique())
k4.metric("Melhor Mês", melhor_mes)

st.divider()

# ── 7. Gráficos ──────────────────────────────────────────────
fig_linha = px.line(vendas_mes, x="ANO_MÊS", y="QTD",
                    title="Evolução de Vendas por Mês (metros)",
                    markers=True, template="plotly_dark")
st.plotly_chart(fig_linha, use_container_width=True)

col_a, col_b = st.columns(2)

top_clientes = df.groupby("CLIENTE", as_index=False)["QTD"].sum().nlargest(10, "QTD")
fig_cli = px.bar(top_clientes.sort_values("QTD"), x="QTD", y="CLIENTE",
                 orientation="h", title="Top 10 Clientes",
                 template="plotly_dark", color="QTD", color_continuous_scale="Blues")
col_a.plotly_chart(fig_cli, use_container_width=True)

top_produtos = df.groupby("DESCRIÇÃO", as_index=False)["QTD"].sum().nlargest(10, "QTD")
fig_prod = px.bar(top_produtos.sort_values("QTD"), x="QTD", y="DESCRIÇÃO",
                  orientation="h", title="Top 10 Produtos",
                  template="plotly_dark", color="QTD", color_continuous_scale="Greens")
col_b.plotly_chart(fig_prod, use_container_width=True)

# ── 8. Tabela ────────────────────────────────────────────────
st.subheader("📋 Dados Filtrados")
st.dataframe(
    df[["DATA","ANO_MÊS","CLIENTE","DESCRIÇÃO","QTD","LARGURA"]].head(100),
    use_container_width=True
)
