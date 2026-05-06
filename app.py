# ============================================================
# DASHBOARD DE VENDAS SVD
# ============================================================

import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import plotly.express as px
import json
import os
import re
import urllib.request
from streamlit_autorefresh import st_autorefresh
from mrp import render_mrp

# ── 1. CONFIGURAÇÃO DA PÁGINA (Deve ser o primeiro comando Streamlit) ──
st.set_page_config(
    page_title="Dashboard de Vendas SVD",
    page_icon="📊",
    layout="wide"
)

# ── 2. USUÁRIOS E CONTROLE DE ACESSO ─────────────────────────
USERS = {
    "Gustavo.Oliveira":    {"senha": "1232026*", "nome": "Gustavo"},
    "Alessandro.Rodrigues":{"senha": "1232026@", "nome": "Alessandro"},
}

def login_form():
    with st.form("login_form"):
        st.subheader("🔐 Acesso ao Sistema PCP")
        user_input = st.text_input("Usuário")
        pass_input = st.text_input("Senha", type="password")
        submit = st.form_submit_button("Entrar")
        if submit:
            if user_input in USERS and USERS[user_input]["senha"] == pass_input:
                st.session_state["logged_in"] = True
                st.session_state["user_name"] = USERS[user_input]["nome"]
                st.rerun()
            else:
                st.error("Usuário ou senha incorretos")

# Se não estiver logado: mostra login e PARA aqui — nada mais executa
if not st.session_state.get("logged_in", False):
    login_form()
    st.stop()

# ── 3. FUNÇÕES DE PROCESSAMENTO OTIMIZADAS ──────────────────

@st.cache_data(ttl=600)
def processar_rfm(_df_filtrado, update_trigger):
    
    hoje = pd.Timestamp.today().normalize()

    # Agrupamento principal
    rfm = _df_filtrado.groupby("CLIENTE").agg(
        ultima_compra  =("DATA_DT", "max"),
        total_pedidos  =("DATA_DT", "count"),
        total_valor    =("Valor",    "sum"),
        estado         =("Estado",  "first"),
        cidade         =("Cidade",  "first")
    ).reset_index()

    rfm["recencia_dias"] = (hoje - rfm["ultima_compra"]).dt.days

    # Cálculo do tempo médio entre pedidos (Vetorizado)
    df_datas = _df_filtrado[['CLIENTE', 'DATA_DT']].drop_duplicates().sort_values(['CLIENTE', 'DATA_DT'])
    df_datas['diff'] = df_datas.groupby('CLIENTE')['DATA_DT'].diff().dt.days
    tempo_medio = df_datas.groupby('CLIENTE')['diff'].mean().reset_index()
    tempo_medio.columns = ['CLIENTE', 'tempo_medio_dias']
    
    rfm = pd.merge(rfm, tempo_medio, on='CLIENTE', how='left')

    # Lógica de Risco/Status
    def definir_risco(r):
        if r <= 30: return "Ativo"
        elif r <= 90: return "Em Alerta"
        else: return "Risco"
    
    rfm['risco'] = rfm['recencia_dias'].apply(definir_risco)
    return rfm

# ── CSS dos cards ─────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="stMetric"] {
    background-color: rgba(255, 255, 255, 0.05) !important;
    border: 1px solid rgba(255, 255, 255, 0.15) !important;
    border-radius: 10px !important;
    padding: 12px 16px !important;
}
[data-testid="stMetricLabel"] { font-size: 0.8rem !important; opacity: 0.7 !important; }
[data-testid="stMetricValue"] { font-size: 1.1rem !important; font-weight: 700 !important; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar: saudação APENAS aqui + logout ────────────────────
nome_usuario = st.session_state.get("user_name", "Usuário")

st.sidebar.markdown(f"### 👤 Bem-vindo, Sr. {nome_usuario}")

if st.sidebar.button("Sair do Sistema"):
    st.session_state["logged_in"] = False
    st.session_state["user_name"] = ""
    st.rerun()

# ── 3. Funções utilitárias ───────────────────────────────────

def fmt_valor(valor):
    """
  
    """
    if valor >= 1_000_000:
        return f"R$ {valor/1_000_000:.1f} M".replace(".", ",")
    elif valor >= 1_000:
        return f"R$ {valor/1_000:.1f} K".replace(".", ",")
    else:
        return f"R$ {valor:,.0f}".replace(",", ".")

def fmt_rs(valor):
    """

    """
    return "R$ " + f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_br(valor):
    """
   
    """
    return f"{valor:,.0f}".replace(",", ".")

def limpar_numero(valor):
    """

    """
    if not valor or str(valor).strip() == "":
        return 0.0
    s = str(valor).strip()
    s = s.replace("R$", "").replace("R $", "").strip()
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

# ── 4. Carregamento de dados ─────────────────────────────────
# @st.cache_data guarda o resultado em memória por ttl segundos.
# Sem cache os dados seriam baixados do Google Sheets a cada
# interação, tornando o dashboard muito lento com 280k+ linhas.
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
    r"C:\Users\gusgt\credentials\dashpython-494416-4c76af5b36c4.json", scopes=SCOPES
)

    gc = gspread.authorize(creds)
    worksheet = gc.open_by_key(
        "1mGQWM8CCBeNy8TdupA7OMR8kNerWQiRQ1X8TEm97mG0"
    ).worksheet("VOL. VENDAS GERAL")

    dados = worksheet.get_all_values()
    headers = dados[0]

    colunas = ["DATA", "ANO_MÊS", "CLIENTE", "CÓDIGO", "DESCRIÇÃO",
               "ATIVO", "LARGURA", "QTD", "Valor", "Estado", "Cidade", "ID"]
    indices = {col: headers.index(col) for col in colunas if col in headers}
    rows = [{col: row[idx] for col, idx in indices.items()} for row in dados[1:]]

    df = pd.DataFrame(rows)
    df["QTD"]   = df["QTD"].apply(limpar_numero)
    df["Valor"] = df["Valor"].apply(limpar_numero)
    df = df[df["ATIVO"] == "S"].copy()
    df["ANO"]     = df["ANO_MÊS"].str[:4]
    df["MES"]     = df["ANO_MÊS"].str[5:]
    df["DATA_DT"] = pd.to_datetime(df["DATA"], dayfirst=True, errors="coerce")
    df["Estado"]  = df["Estado"].str.strip()
    df["Cidade"]  = df["Cidade"].str.strip()
    return df

# ── 5. Carrega os dados ──────────────────────────────────────
@st.cache_data(ttl=600)
def processar_rfm(_df_filtrado, update_trigger):
    hoje = pd.Timestamp.today().normalize()

    # 1. Agrupamento principal
    rfm = _df_filtrado.groupby("CLIENTE").agg(
        ultima_compra  =("DATA_DT", "max"),
        total_pedidos  =("DATA_DT", "count"),
        total_valor    =("Valor",    "sum"),
        estado         =("Estado",  "first"),
        cidade         =("Cidade",  "first")
    ).reset_index()

    rfm["recencia_dias"] = (hoje - rfm["ultima_compra"]).dt.days

    # 2. Cálculo do tempo médio 
    df_datas = _df_filtrado[['CLIENTE', 'DATA_DT']].drop_duplicates().sort_values(['CLIENTE', 'DATA_DT'])
    df_datas['diff'] = df_datas.groupby('CLIENTE')['DATA_DT'].diff().dt.days
    tempo_medio = df_datas.groupby('CLIENTE')['diff'].mean().reset_index()
    tempo_medio.columns = ['CLIENTE', 'tempo_medio_dias']
    
    rfm = pd.merge(rfm, tempo_medio, on='CLIENTE', how='left')

    def calcular_risco(row):
        if row['recencia_dias'] <= 30:
            return "Ativo"
        elif row['recencia_dias'] <= 90:
            return "Em Alerta"
        else:
            return "Risco"

    rfm['risco'] = rfm.apply(calcular_risco, axis=1)

    return rfm
with st.spinner("Carregando dados..."):
    df_raw = carregar_dados()

# ── 6. Sidebar — navegação ───────────────────────────────────
st.sidebar.title("📊 GESTÃO ALLTAK")
pagina = st.sidebar.radio("Navegar", ["🏠 Visão Geral", "👥 Clientes", "📦 Produtos", "🏭 MRP / Planejamento"])

# ── 7. Filtros globais ───────────────────────────────────────
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

# ── 8. Atualização de dados ──────────────────────────────────
st.sidebar.divider()
with st.sidebar.expander("🔄 Atualização dos dados"):
    if st.button("🔄 Atualizar agora", key="btn_atualizar"):
        st.cache_data.clear()
        st.rerun()
    intervalo = st.selectbox(
        "⏱️ Atualizar automaticamente",
        options=[0, 5, 15, 30, 60],
        format_func=lambda x: "Desativado" if x == 0 else f"A cada {x} minutos",
        index=0,
        key="sel_intervalo"
    )
    if intervalo > 0:
        st_autorefresh(interval=intervalo * 60 * 1000, key="autorefresh")
        st.caption(f"🟢 Atualizando a cada {intervalo} min")

# ── 9. Filtragem base ────────────────────────────────────────
period_ini = f"{ano_ini}-{mes_ini}"
period_fim = f"{ano_fim}-{mes_fim}"

df = df_raw[
    (df_raw["ANO_MÊS"] >= period_ini) &
    (df_raw["ANO_MÊS"] <= period_fim)
].copy()

if cliente_sel:
    df = df[df["CLIENTE"].isin(cliente_sel)]

# ── 10. Session state ────────────────────────────────────────

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
    if st.sidebar.button("🗑️ Limpar seleções", key="btn_limpar"):
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

    vendas_mes = df.groupby("ANO_MÊS", as_index=False).agg(
        QTD=("QTD", "sum"), Valor=("Valor", "sum")
    )
    melhor_mes_val = vendas_mes.loc[vendas_mes["Valor"].idxmax(), "ANO_MÊS"] if not vendas_mes.empty else "-"
    melhor_mes_qtd = vendas_mes.loc[vendas_mes["QTD"].idxmax(),   "ANO_MÊS"] if not vendas_mes.empty else "-"

    st.markdown("#### 💰 Faturamento")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total Faturado",       fmt_valor(df["Valor"].sum()))
    k2.metric("Ticket Médio/Cliente", fmt_valor(df.groupby("CLIENTE")["Valor"].sum().mean() if df["CLIENTE"].nunique() > 0 else 0))
    k3.metric("Melhor Mês (R$)",      melhor_mes_val)
    k4.metric("Média Mensal",         fmt_valor(vendas_mes["Valor"].mean() if not vendas_mes.empty else 0))

    st.markdown("#### 📏 Volume (metros)")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Vendido",      f"{fmt_br(df['QTD'].sum())} m")
    m2.metric("Clientes Únicos",    fmt_br(df["CLIENTE"].nunique()))
    m3.metric("Produtos Distintos", fmt_br(df["DESCRIÇÃO"].nunique()))
    m4.metric("Melhor Mês (m)",     melhor_mes_qtd)

    st.divider()
    st.caption("💡 Clique em qualquer ponto ou barra para filtrar")

    tab1, tab2 = st.tabs(["💰 Faturamento (R$)", "📏 Volume (metros)"])

    with tab1:
        fig_val = px.line(vendas_mes, x="ANO_MÊS", y="Valor",
                          title="Evolução do Faturamento por Mês",
                          markers=True, template="plotly_dark")
        fig_val.update_yaxes(tickprefix="R$ ", tickformat=",.0f")
        fig_val.update_traces(marker=dict(size=10), line=dict(width=2, color="#2ecc71"))
        sel_val = st.plotly_chart(fig_val, use_container_width=True,
                                  on_select="rerun", selection_mode=["points"])
        if sel_val and sel_val.get("selection") and sel_val["selection"].get("points"):
            mes_clicado = str(sel_val["selection"]["points"][0].get("x", ""))[:7]
            if mes_clicado and mes_clicado != st.session_state.mes_selecionado:
                st.session_state.mes_selecionado = mes_clicado
                st.rerun()

    with tab2:
        fig_qtd = px.line(vendas_mes, x="ANO_MÊS", y="QTD",
                          title="Evolução de Vendas por Mês (metros)",
                          markers=True, template="plotly_dark")
        fig_qtd.update_yaxes(tickformat=",.0f")
        fig_qtd.update_traces(marker=dict(size=10), line=dict(width=2))
        sel_qtd = st.plotly_chart(fig_qtd, use_container_width=True,
                                  on_select="rerun", selection_mode=["points"])
        if sel_qtd and sel_qtd.get("selection") and sel_qtd["selection"].get("points"):
            mes_clicado = str(sel_qtd["selection"]["points"][0].get("x", ""))[:7]
            if mes_clicado and mes_clicado != st.session_state.mes_selecionado:
                st.session_state.mes_selecionado = mes_clicado
                st.rerun()

    col_a, col_b = st.columns(2)

    top_clientes = df.groupby("CLIENTE", as_index=False).agg(
        Valor=("Valor","sum"), QTD=("QTD","sum")
    ).nlargest(10, "Valor")
    fig_cli = px.bar(top_clientes.sort_values("Valor"), x="Valor", y="CLIENTE",
                     orientation="h", title="Top 10 Clientes (R$)",
                     template="plotly_dark", color="Valor", color_continuous_scale="Blues")
    fig_cli.update_xaxes(tickprefix="R$ ", tickformat=",.0f")
    sel_cli = col_a.plotly_chart(fig_cli, use_container_width=True,
                                  on_select="rerun", selection_mode=["points"])
    if sel_cli and sel_cli.get("selection") and sel_cli["selection"].get("points"):
        cli = sel_cli["selection"]["points"][0].get("y")
        if cli and cli != st.session_state.cliente_selecionado:
            st.session_state.cliente_selecionado = cli
            st.session_state.mes_selecionado = None
            st.rerun()

    top_produtos = df.groupby("DESCRIÇÃO", as_index=False).agg(
        Valor=("Valor","sum"), QTD=("QTD","sum")
    ).nlargest(10, "Valor")
    fig_prod = px.bar(top_produtos.sort_values("Valor"), x="Valor", y="DESCRIÇÃO",
                      orientation="h", title="Top 10 Produtos (R$)",
                      template="plotly_dark", color="Valor", color_continuous_scale="Greens")
    fig_prod.update_xaxes(tickprefix="R$ ", tickformat=",.0f")
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
        df[["DATA","ANO_MÊS","CLIENTE","DESCRIÇÃO","QTD","Valor","Estado","Cidade"]].head(100),
        use_container_width=True
    )

# ════════════════════════════════════════════════════════════
# PÁGINA 2 — CLIENTES
# ════════════════════════════════════════════════════════════
elif pagina == "👥 Clientes":

    st.title("👥 Análise de Clientes")

    hoje      = pd.Timestamp.today().normalize()
    mes_atual = hoje.strftime("%Y-%m")  


    df_valid = df[df["DATA_DT"].notna()].copy()

    if df_valid.empty:
        st.warning("Nenhum dado encontrado para os filtros selecionados.")
    else:

        trigger = f"{len(df_valid)}_{df_valid['Valor'].sum()}"
        

        rfm = processar_rfm(df_valid, trigger)


        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total de Clientes", fmt_br(len(rfm)))
        c2.metric("Faturamento Médio/Cli", fmt_valor(rfm['total_valor'].mean()))
        c3.metric("Recência Média", f"{int(rfm['recencia_dias'].mean())} dias")
        

        tempo_geral = rfm['tempo_medio_dias'].mean()
        c4.metric("Ciclo Médio Pedido", f"{int(tempo_geral) if pd.notna(tempo_geral) else 0} dias")

        st.divider()

    # ── Mapa por estado ──────────────────────────────────────
    st.subheader("🗺️ Distribuição Regional por Estado")

    mapa_estados = df.groupby("Estado", as_index=False).agg(
        Valor   =("Valor",   "sum"),
        QTD     =("QTD",     "sum"),
        Clientes=("CLIENTE", "nunique")
    )
    mapa_estados = mapa_estados[mapa_estados["Estado"].str.strip() != ""]

    siglas = {
        "Acre":"AC","Alagoas":"AL","Amazonas":"AM","Amapá":"AP",
        "Bahia":"BA","Ceará":"CE","Distrito Federal":"DF","Espírito Santo":"ES",
        "Goiás":"GO","Maranhão":"MA","Minas Gerais":"MG","Mato Grosso do Sul":"MS",
        "Mato Grosso":"MT","Pará":"PA","Paraíba":"PB","Pernambuco":"PE",
        "Piauí":"PI","Paraná":"PR","Rio de Janeiro":"RJ","Rio Grande do Norte":"RN",
        "Rondônia":"RO","Roraima":"RR","Rio Grande do Sul":"RS","Santa Catarina":"SC",
        "Sergipe":"SE","São Paulo":"SP","Tocantins":"TO"
    }

    coords = {
        "AC":(-9.02,-70.81),"AL":(-9.57,-36.78),"AM":(-3.47,-65.10),
        "AP":(1.41,-51.77), "BA":(-12.96,-41.70),"CE":(-5.20,-39.53),
        "DF":(-15.78,-47.93),"ES":(-19.19,-40.34),"GO":(-15.98,-49.86),
        "MA":(-5.42,-45.44),"MG":(-18.10,-44.38),"MS":(-20.51,-54.54),
        "MT":(-12.64,-55.42),"PA":(-3.79,-52.48),"PB":(-7.28,-36.72),
        "PE":(-8.38,-37.86),"PI":(-7.72,-42.73), "PR":(-24.89,-51.55),
        "RJ":(-22.25,-42.66),"RN":(-5.81,-36.59),"RO":(-10.83,-63.34),
        "RR":(1.99,-61.33), "RS":(-30.03,-53.22),"SC":(-27.33,-50.22),
        "SE":(-10.57,-37.45),"SP":(-22.21,-48.79),"TO":(-9.46,-48.26)
    }

    mapa_estados["sigla"] = mapa_estados["Estado"].map(siglas)
    mapa_estados["lat"]   = mapa_estados["sigla"].map(lambda x: coords.get(x,(None,None))[0] if x else None)
    mapa_estados["lon"]   = mapa_estados["sigla"].map(lambda x: coords.get(x,(None,None))[1] if x else None)
    mapa_estados          = mapa_estados[mapa_estados["lat"].notna()]

    metrica_mapa = st.radio(
        "Métrica do mapa",
        ["Faturamento (R$)", "Volume (m)", "Clientes"],
        horizontal=True
    )
    col_mapa = "Valor" if metrica_mapa == "Faturamento (R$)" else ("QTD" if metrica_mapa == "Volume (m)" else "Clientes")

    try:

        geojson_url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
        with urllib.request.urlopen(geojson_url) as response:
            geojson_brasil = json.loads(response.read().decode())

        fig_mapa = px.choropleth(
            mapa_estados,
            geojson=geojson_brasil,
            locations="Estado",
            featureidkey="properties.name",
            color=col_mapa,
            color_continuous_scale=[[0,"#a1e9ff"],[0.5,"#2374ca"],[1,"#113047"]],
            scope="south america",
            title=f"Mapa — {metrica_mapa} por Estado",
        )

        fig_mapa.add_scattergeo(
            lat=mapa_estados["lat"],
            lon=mapa_estados["lon"],
            text=mapa_estados["sigla"],
            mode="text",
            textfont=dict(size=9, color="white", family="Arial Black"),
            showlegend=False,
            hoverinfo="skip"
        )

        fig_mapa.update_geos(
            showcountries=False,
            showland=False,
            showocean=False,
            showlakes=False,
            showrivers=False,
            showframe=False,
            showcoastlines=False,
            fitbounds="locations",
            bgcolor="rgba(0,0,0,0)",
        )
        fig_mapa.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            geo_bgcolor="rgba(0,0,0,0)",
            coloraxis_colorbar=dict(
                title=dict(text=metrica_mapa, font=dict(color="white")),
                tickfont=dict(color="white"),
            ),
            margin=dict(l=0, r=0, t=40, b=0),
            height=800
        )

    except Exception as e:
        st.warning(f"Usando mapa simplificado. ({e})")
        fig_mapa = px.scatter_geo(
            mapa_estados,
            lat="lat", lon="lon",
            size=col_mapa, color=col_mapa,
            hover_name="Estado",
            color_continuous_scale=[[0,"#052344"],[1,"#a0ccff"]],
            scope="south america",
            title=f"Mapa — {metrica_mapa} por Estado",
            size_max=60
        )
        fig_mapa.add_scattergeo(
            lat=mapa_estados["lat"],
            lon=mapa_estados["lon"],
            text=mapa_estados["sigla"],
            mode="text",
            textfont=dict(size=9, color="white", family="Arial Black"),
            showlegend=False, hoverinfo="skip"
        )
        fig_mapa.update_geos(
            showcountries=False,
            showland=False,
            showocean=False,
            showframe=False,
            fitbounds="locations",
            bgcolor="rgba(0,0,0,0)"
        )
        fig_mapa.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=40, b=0)
        )

    st.plotly_chart(fig_mapa, use_container_width=True)

    # Ranking por estado
    st.subheader("📊 Ranking por Estado")
    col_e1, col_e2 = st.columns(2)

    fig_est_val = px.bar(
        mapa_estados.sort_values("Valor").tail(15),
        x="Valor", y="Estado", orientation="h",
        title="Top Estados — Faturamento (R$)",
        template="plotly_dark", color="Valor", color_continuous_scale="Blues"
    )
    fig_est_val.update_xaxes(tickprefix="R$ ", tickformat=",.0f")
    col_e1.plotly_chart(fig_est_val, use_container_width=True)

    fig_est_cli = px.bar(
        mapa_estados.sort_values("Clientes").tail(15),
        x="Clientes", y="Estado", orientation="h",
        title="Top Estados — Nº de Clientes",
        template="plotly_dark", color="Clientes", color_continuous_scale="Greens"
    )
    col_e2.plotly_chart(fig_est_cli, use_container_width=True)

    st.divider()

    # ── Dispersão RFM ────────────────────────────────────────
    # Eixo X = recência (menor = melhor)
    # Eixo Y = tempo médio entre pedidos (menor = compra mais frequente)
    st.subheader("🎯 Dispersão — Recência vs. Tempo Médio entre Pedidos")
    rfm_plot = rfm[rfm["tempo_medio_dias"].notna()].copy()

    fig_scatter = px.scatter(
        rfm_plot,
        x="recencia_dias", y="tempo_medio_dias",
        color="risco", size="total_valor",
        hover_name="CLIENTE",
        hover_data={
            "recencia_dias":    True,
            "tempo_medio_dias": True,
            "total_pedidos":    True,
            "total_valor":      ":,.2f",
            "estado":           True,
            "cidade":           True,
            "risco":            False
        },
        labels={
            "recencia_dias":    "Recência (dias desde última compra)",
            "tempo_medio_dias": "Tempo médio entre pedidos (dias)",
            "total_valor":      "Faturamento total (R$)",
            "total_pedidos":    "Nº de pedidos"
        },
        color_discrete_map={
            "✅ Ativo":       "#2ecc71",
            "🟢 Baixo Risco": "#27ae60",
            "🟡 Médio Risco": "#f39c12",
            "🔴 Alto Risco":  "#e74c3c"
        },
        title="Recência x Tempo Médio entre Pedidos",
        template="plotly_dark"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    st.divider()

# ════════════════════════════════════════════════════════════
# PÁGINA 3 — PRODUTOS 
# ════════════════════════════════════════════════════════════
elif pagina == "📦 Produtos":

    st.title("📦 Análise de Produtos")

    # 1. Agrupamento focado em VOLUME (Metros)
    prod_resumo = df.groupby("DESCRIÇÃO", as_index=False).agg(
        valor   =("Valor", "sum"),
        volume  =("QTD",   "sum"),
        pedidos =("QTD",   "count"),
        clientes=("CLIENTE","nunique")
    ).sort_values("volume", ascending=False) # Ordenação numérica real

    total_vol = prod_resumo["volume"].sum()
    
    if total_vol > 0:
        prod_resumo["participacao"]      = (prod_resumo["volume"] / total_vol * 100).round(2)
        prod_resumo["participacao_acum"] = prod_resumo["participacao"].cumsum().round(2)
    else:
        prod_resumo["participacao"] = 0
        prod_resumo["participacao_acum"] = 0

    # Função ABC 
    def abc(acum):
        if acum <= 80:   return "A"
        elif acum <= 95: return "B"
        else:            return "C"

    prod_resumo["curva_abc"] = prod_resumo["participacao_acum"].apply(abc)

    mapa_cores = {
        "A": "#032757",
        "B": "#e2ff63",
        "C": "#ec4949"
    }

    # 2. Métricas
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Faturamento Total",   fmt_valor(prod_resumo['valor'].sum()))
    k2.metric("Volume Total",        f"{fmt_br(total_vol)} m")
    k3.metric("Total de Produtos",   fmt_br(len(prod_resumo)))
    k4.metric("Classe A (80% vol.)", fmt_br(len(prod_resumo[prod_resumo["curva_abc"] == "A"])))
    
    st.divider()

    # 3. Gráficos 
    col_a, col_b = st.columns(2)
    with col_a:
        fig_prod = px.bar(prod_resumo.head(20).sort_values("volume"), x="volume", y="DESCRIÇÃO",
                          orientation="h", title="Top 20 Produtos por Volume",
                          template="plotly_dark", color="curva_abc", color_discrete_map=mapa_cores)
        st.plotly_chart(fig_prod, use_container_width=True)
    
    with col_b:
        fig_abc = px.pie(prod_resumo.groupby("curva_abc", as_index=False)["volume"].sum(),
                         values="volume", names="curva_abc", title="Distribuição ABC (Volume)",
                         template="plotly_dark", color="curva_abc", color_discrete_map=mapa_cores)
        st.plotly_chart(fig_abc, use_container_width=True)

    # 4. TABELA 
    st.subheader("📋 Tabela de Produtos — Curva ABC (Metros)")
    
    abc_filtro = st.multiselect(
        "Filtrar por classe",
        ["A", "B", "C"],
        default=["A", "B", "C"]
    )

    tabela_display = prod_resumo.copy()
    if abc_filtro:
        tabela_display = tabela_display[tabela_display["curva_abc"].isin(abc_filtro)]
    
    st.dataframe(
        tabela_display[["DESCRIÇÃO", "curva_abc", "volume", "valor", "participacao", "participacao_acum"]],
        column_config={
            "DESCRIÇÃO": "Produto",
            "curva_abc": "Classe ABC",
            "volume": st.column_config.NumberColumn("Volume (m)", format="%.0f m"),
            "valor": st.column_config.NumberColumn("Faturamento", format="R$ %.2f"),
            "participacao": st.column_config.NumberColumn("% Individual", format="%.2f%%"),
            "participacao_acum": st.column_config.NumberColumn("% Acumulado", format="%.2f%%"),
        },
        use_container_width=True,
        hide_index=True
    )
    
elif pagina == "🏭 MRP / Planejamento":
    render_mrp()