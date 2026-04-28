import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import plotly.express as px
import json
import os
import re
from datetime import datetime, date

# ── 1. Configuração da página ────────────────────────────────
st.set_page_config(
    page_title="Dashboard de Vendas SVD",
    page_icon="📊",
    layout="wide"
)

# ── 2. Funções utilitárias ───────────────────────────────────
def fmt_br(valor):
    return f"{valor:,.0f}".replace(",", ".")

def limpar_numero(valor):
    if not valor or str(valor).strip() == "":
        return 0.0
    s = str(valor).strip()
    s = re.sub(r'\s+', '', s)
    if re.match(r'^\d{1,3}(\.\d{3})*(,\d+)?$', s):
        s = s.replace(".", "").replace(",", ".")
    elif re.match(r'^\d{1,3}(,\d{3})*(\.\d+)?$', s):
        s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

# ── 3. Carregamento de dados ─────────────────────────────────
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
    df["QTD"] = df["QTD"].apply(limpar_numero)
    df = df[df["ATIVO"] == "S"].copy()
    df["ANO"] = df["ANO_MÊS"].str[:4]
    df["MES"] = df["ANO_MÊS"].str[5:]
    df["DATA_DT"] = pd.to_datetime(df["DATA"], dayfirst=True, errors="coerce")
    return df

# ── 4. Carrega dados ─────────────────────────────────────────
with st.spinner("Carregando dados..."):
    df_raw = carregar_dados()

# ── 5. Navegação por páginas ─────────────────────────────────
st.sidebar.title("📊 SVD Dashboard")
pagina = st.sidebar.radio("Navegar", ["🏠 Visão Geral", "👥 Clientes", "📦 Produtos"])

# ── 6. Filtros globais ───────────────────────────────────────
st.sidebar.divider()
st.sidebar.markdown("**🔍 Filtros**")

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

cliente_sel = st.sidebar.multiselect("🏢 Cliente", sorted(df_raw["CLIENTE"].unique()), placeholder="Todos")

# Filtragem base
period_ini = f"{ano_ini}-{mes_ini}"
period_fim = f"{ano_fim}-{mes_fim}"

df = df_raw[
    (df_raw["ANO_MÊS"] >= period_ini) &
    (df_raw["ANO_MÊS"] <= period_fim)
].copy()

if cliente_sel:
    df = df[df["CLIENTE"].isin(cliente_sel)]

# Session state para filtros por clique
for key in ["mes_selecionado", "cliente_selecionado", "produto_selecionado"]:
    if key not in st.session_state:
        st.session_state[key] = None

if any([st.session_state.mes_selecionado, st.session_state.cliente_selecionado, st.session_state.produto_selecionado]):
    st.sidebar.divider()
    st.sidebar.markdown("**🖱️ Seleção nos gráficos:**")
    if st.session_state.mes_selecionado:
        st.sidebar.info(f"📅 {st.session_state.mes_selecionado}")
    if st.session_state.cliente_selecionado:
        st.sidebar.info(f"🏢 {st.session_state.cliente_selecionado}")
    if st.session_state.produto_selecionado:
        st.sidebar.info(f"📦 {st.session_state.produto_selecionado}")
    if st.sidebar.button("🗑️ Limpar seleções"):
        for key in ["mes_selecionado", "cliente_selecionado", "produto_selecionado"]:
            st.session_state[key] = None
        st.rerun()

if st.session_state.mes_selecionado:
    df = df[df["ANO_MÊS"] == st.session_state.mes_selecionado]
if st.session_state.cliente_selecionado:
    df = df[df["CLIENTE"] == st.session_state.cliente_selecionado]
if st.session_state.produto_selecionado:
    df = df[df["DESCRIÇÃO"] == st.session_state.produto_selecionado]

# ════════════════════════════════════════════════════════════
# PÁGINA 1 — VISÃO GERAL
# ════════════════════════════════════════════════════════════
if pagina == "🏠 Visão Geral":

    st.title("📊 Dashboard de Vendas SVD")

    vendas_mes = df.groupby("ANO_MÊS", as_index=False)["QTD"].sum()
    melhor_mes = vendas_mes.loc[vendas_mes["QTD"].idxmax(), "ANO_MÊS"] if not vendas_mes.empty else "-"

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Vendido (m)", f"{fmt_br(df['QTD'].sum())} m")
    k2.metric("Clientes Únicos", fmt_br(df["CLIENTE"].nunique()))
    k3.metric("Produtos Distintos", fmt_br(df["DESCRIÇÃO"].nunique()))
    k4.metric("Melhor Mês", melhor_mes)

    st.divider()
    st.caption("💡 Clique em qualquer ponto ou barra para filtrar")

    fig_linha = px.line(vendas_mes, x="ANO_MÊS", y="QTD",
                        title="Evolução de Vendas por Mês (metros)",
                        markers=True, template="plotly_dark")
    fig_linha.update_yaxes(tickformat=",.0f")
    fig_linha.update_traces(marker=dict(size=10), line=dict(width=2))

    sel_linha = st.plotly_chart(fig_linha, use_container_width=True,
                                on_select="rerun", selection_mode=["points"])

    if sel_linha and sel_linha.get("selection") and sel_linha["selection"].get("points"):
        mes_clicado = str(sel_linha["selection"]["points"][0].get("x", ""))[:7]
        if mes_clicado and mes_clicado != st.session_state.mes_selecionado:
            st.session_state.mes_selecionado = mes_clicado
            st.session_state.cliente_selecionado = None
            st.session_state.produto_selecionado = None
            st.rerun()

    col_a, col_b = st.columns(2)

    top_clientes = df.groupby("CLIENTE", as_index=False)["QTD"].sum().nlargest(10, "QTD")
    fig_cli = px.bar(top_clientes.sort_values("QTD"), x="QTD", y="CLIENTE",
                     orientation="h", title="Top 10 Clientes",
                     template="plotly_dark", color="QTD", color_continuous_scale="Blues")
    fig_cli.update_xaxes(tickformat=",.0f")

    sel_cli = col_a.plotly_chart(fig_cli, use_container_width=True,
                                  on_select="rerun", selection_mode=["points"])

    if sel_cli and sel_cli.get("selection") and sel_cli["selection"].get("points"):
        cli = sel_cli["selection"]["points"][0].get("y")
        if cli and cli != st.session_state.cliente_selecionado:
            st.session_state.cliente_selecionado = cli
            st.session_state.mes_selecionado = None
            st.rerun()

    top_produtos = df.groupby("DESCRIÇÃO", as_index=False)["QTD"].sum().nlargest(10, "QTD")
    fig_prod = px.bar(top_produtos.sort_values("QTD"), x="QTD", y="DESCRIÇÃO",
                      orientation="h", title="Top 10 Produtos",
                      template="plotly_dark", color="QTD", color_continuous_scale="Greens")
    fig_prod.update_xaxes(tickformat=",.0f")

    sel_prod = col_b.plotly_chart(fig_prod, use_container_width=True,
                                   on_select="rerun", selection_mode=["points"])

    if sel_prod and sel_prod.get("selection") and sel_prod["selection"].get("points"):
        prod = sel_prod["selection"]["points"][0].get("y")
        if prod and prod != st.session_state.produto_selecionado:
            st.session_state.produto_selecionado = prod
            st.session_state.mes_selecionado = None
            st.rerun()

    st.subheader("📋 Dados Filtrados")
    st.dataframe(
        df[["DATA","ANO_MÊS","CLIENTE","DESCRIÇÃO","QTD","LARGURA"]].head(100),
        use_container_width=True
    )

# ════════════════════════════════════════════════════════════
# PÁGINA 2 — CLIENTES
# ════════════════════════════════════════════════════════════
elif pagina == "👥 Clientes":

    st.title("👥 Análise de Clientes")

    hoje = pd.Timestamp.today().normalize()
    mes_atual = hoje.strftime("%Y-%m")

    # ── Métricas RFM por cliente ─────────────────────────────
    df_valid = df_raw[df_raw["DATA_DT"].notna()].copy()

    rfm = df_valid.groupby("CLIENTE").agg(
        ultima_compra=("DATA_DT", "max"),
        total_pedidos=("DATA_DT", "count"),
        total_volume=("QTD", "sum"),
        primeira_compra=("DATA_DT", "min")
    ).reset_index()

    rfm["recencia_dias"] = (hoje - rfm["ultima_compra"]).dt.days

    # Tempo médio entre pedidos
    def tempo_medio(cliente):
        datas = df_valid[df_valid["CLIENTE"] == cliente]["DATA_DT"].sort_values().unique()
        if len(datas) < 2:
            return None
        diffs = [(datas[i+1] - datas[i]).days for i in range(len(datas)-1)]
        return round(sum(diffs) / len(diffs), 0)

    rfm["tempo_medio_dias"] = rfm["CLIENTE"].apply(tempo_medio)

    # Clientes que compraram no mês atual
    clientes_mes_atual = set(df_raw[df_raw["ANO_MÊS"] == mes_atual]["CLIENTE"].unique())
    rfm["comprou_mes_atual"] = rfm["CLIENTE"].isin(clientes_mes_atual)

    # Classificação de risco
    def classificar_risco(row):
        if row["comprou_mes_atual"]:
            return "✅ Ativo"
        elif row["recencia_dias"] > 60:
            return "🔴 Alto Risco"
        elif row["recencia_dias"] > 30:
            return "🟡 Médio Risco"
        else:
            return "🟢 Baixo Risco"

    rfm["risco"] = rfm.apply(classificar_risco, axis=1)

    # ── KPIs ─────────────────────────────────────────────────
    total_cli = len(rfm)
    ativos = len(rfm[rfm["comprou_mes_atual"]])
    inativos = total_cli - ativos
    alto_risco = len(rfm[rfm["risco"] == "🔴 Alto Risco"])

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total de Clientes", fmt_br(total_cli))
    k2.metric("✅ Ativos este mês", fmt_br(ativos))
    k3.metric("😴 Inativos este mês", fmt_br(inativos))
    k4.metric("🔴 Alto Risco", fmt_br(alto_risco))

    st.divider()

    # ── Gráfico de dispersão RFM ─────────────────────────────
    st.subheader("🎯 Dispersão — Recência vs. Tempo Médio entre Pedidos")
    st.caption("Clientes no canto inferior esquerdo são os mais engajados (compram frequentemente e compraram recentemente)")

    rfm_plot = rfm[rfm["tempo_medio_dias"].notna()].copy()

    fig_scatter = px.scatter(
        rfm_plot,
        x="recencia_dias",
        y="tempo_medio_dias",
        color="risco",
        size="total_volume",
        hover_name="CLIENTE",
        hover_data={
            "recencia_dias": True,
            "tempo_medio_dias": True,
            "total_pedidos": True,
            "total_volume": ":,.0f",
            "risco": False
        },
        labels={
            "recencia_dias": "Recência (dias desde última compra)",
            "tempo_medio_dias": "Tempo médio entre pedidos (dias)",
            "total_volume": "Volume total (m)",
            "total_pedidos": "Nº de pedidos"
        },
        color_discrete_map={
            "✅ Ativo": "#2ecc71",
            "🟢 Baixo Risco": "#27ae60",
            "🟡 Médio Risco": "#f39c12",
            "🔴 Alto Risco": "#e74c3c"
        },
        title="Recência x Tempo Médio entre Pedidos",
        template="plotly_dark"
    )
    fig_scatter.update_layout(legend_title="Classificação")
    st.plotly_chart(fig_scatter, use_container_width=True)

    st.divider()

    # ── Tabela de classificação ──────────────────────────────
    st.subheader("📋 Classificação de Clientes por Risco")

    col_filtro1, col_filtro2 = st.columns(2)
    risco_filtro = col_filtro1.multiselect(
        "Filtrar por risco",
        ["✅ Ativo", "🟢 Baixo Risco", "🟡 Médio Risco", "🔴 Alto Risco"],
        default=["🔴 Alto Risco", "🟡 Médio Risco"]
    )
    busca_cliente = col_filtro2.text_input("🔍 Buscar cliente")

    tabela = rfm.copy()
    if risco_filtro:
        tabela = tabela[tabela["risco"].isin(risco_filtro)]
    if busca_cliente:
        tabela = tabela[tabela["CLIENTE"].str.contains(busca_cliente, case=False, na=False)]

    tabela = tabela.sort_values("recencia_dias", ascending=False)
    tabela["ultima_compra"] = tabela["ultima_compra"].dt.strftime("%d/%m/%Y")
    tabela["total_volume"] = tabela["total_volume"].apply(fmt_br)
    tabela["tempo_medio_dias"] = tabela["tempo_medio_dias"].fillna("-")

    st.dataframe(
        tabela[["CLIENTE", "risco", "recencia_dias", "tempo_medio_dias",
                "total_pedidos", "total_volume", "ultima_compra"]].rename(columns={
            "CLIENTE": "Cliente",
            "risco": "Classificação",
            "recencia_dias": "Recência (dias)",
            "tempo_medio_dias": "Tempo médio (dias)",
            "total_pedidos": "Nº Pedidos",
            "total_volume": "Volume Total (m)",
            "ultima_compra": "Última Compra"
        }),
        use_container_width=True,
        height=500
    )

# ════════════════════════════════════════════════════════════
# PÁGINA 3 — PRODUTOS
# ════════════════════════════════════════════════════════════
elif pagina == "📦 Produtos":

    st.title("📦 Análise de Produtos")

    prod_resumo = df.groupby("DESCRIÇÃO", as_index=False).agg(
        volume=("QTD", "sum"),
        pedidos=("QTD", "count"),
        clientes=("CLIENTE", "nunique")
    ).sort_values("volume", ascending=False)

    total_vol = prod_resumo["volume"].sum()
    prod_resumo["participacao"] = (prod_resumo["volume"] / total_vol * 100).round(2)
    prod_resumo["participacao_acum"] = prod_resumo["participacao"].cumsum().round(2)

    # Curva ABC
    def abc(acum):
        if acum <= 80:
            return "🅰️ Classe A"
        elif acum <= 95:
            return "🅱️ Classe B"
        else:
            return "🇨 Classe C"

    prod_resumo["curva_abc"] = prod_resumo["participacao_acum"].apply(abc)

    k1, k2, k3 = st.columns(3)
    k1.metric("Total de Produtos", fmt_br(len(prod_resumo)))
    k2.metric("Classe A (80% do volume)", fmt_br(len(prod_resumo[prod_resumo["curva_abc"] == "🅰️ Classe A"])))
    k3.metric("Volume Total (m)", f"{fmt_br(total_vol)} m")

    st.divider()

    col_a, col_b = st.columns(2)

    top20 = prod_resumo.head(20)
    fig_prod = px.bar(top20.sort_values("volume"), x="volume", y="DESCRIÇÃO",
                      orientation="h", title="Top 20 Produtos por Volume",
                      template="plotly_dark", color="curva_abc",
                      color_discrete_map={
                          "🅰️ Classe A": "#2ecc71",
                          "🅱️ Classe B": "#f39c12",
                          "🇨 Classe C": "#e74c3c"
                      })
    fig_prod.update_xaxes(tickformat=",.0f")

    sel_prod = col_a.plotly_chart(fig_prod, use_container_width=True,
                                   on_select="rerun", selection_mode=["points"])

    if sel_prod and sel_prod.get("selection") and sel_prod["selection"].get("points"):
        prod = sel_prod["selection"]["points"][0].get("y")
        if prod and prod != st.session_state.produto_selecionado:
            st.session_state.produto_selecionado = prod
            st.rerun()

    fig_abc = px.pie(
        prod_resumo.groupby("curva_abc", as_index=False)["volume"].sum(),
        values="volume", names="curva_abc",
        title="Distribuição por Curva ABC",
        template="plotly_dark",
        color="curva_abc",
        color_discrete_map={
            "🅰️ Classe A": "#2ecc71",
            "🅱️ Classe B": "#f39c12",
            "🇨 Classe C": "#e74c3c"
        }
    )
    col_b.plotly_chart(fig_abc, use_container_width=True)

    st.subheader("📋 Tabela de Produtos — Curva ABC")

    abc_filtro = st.multiselect("Filtrar por classe",
                                 ["🅰️ Classe A", "🅱️ Classe B", "🇨 Classe C"],
                                 default=["🅰️ Classe A"])

    tabela_prod = prod_resumo.copy()
    if abc_filtro:
        tabela_prod = tabela_prod[tabela_prod["curva_abc"].isin(abc_filtro)]

    tabela_prod["volume"] = tabela_prod["volume"].apply(fmt_br)
    tabela_prod["participacao"] = tabela_prod["participacao"].apply(lambda x: f"{x:.2f}%")
    tabela_prod["participacao_acum"] = tabela_prod["participacao_acum"].apply(lambda x: f"{x:.2f}%")

    st.dataframe(
        tabela_prod[["DESCRIÇÃO", "curva_abc", "volume", "participacao",
                     "participacao_acum", "pedidos", "clientes"]].rename(columns={
            "DESCRIÇÃO": "Produto",
            "curva_abc": "Classe ABC",
            "volume": "Volume (m)",
            "participacao": "Participação",
            "participacao_acum": "Participação Acum.",
            "pedidos": "Nº Pedidos",
            "clientes": "Nº Clientes"
        }),
        use_container_width=True,
        height=500
    )
    