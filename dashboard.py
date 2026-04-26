import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, dash_table, Input, Output
import dash_bootstrap_components as dbc
import os
import json
import gc

# ── 1. Autenticação ──────────────────────────────────────────
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds_env = os.environ.get("GOOGLE_CREDENTIALS")
if creds_env:
    print("Usando credenciais do ambiente (Render)")
    creds = Credentials.from_service_account_info(json.loads(creds_env), scopes=SCOPES)
else:
    print("Usando arquivo local de credenciais")
    creds = Credentials.from_service_account_file(
        "dashpython-494416-f473e55aca52.json", scopes=SCOPES
    )

gc_client = gspread.authorize(creds)

# ── 2. Leitura otimizada da planilha ─────────────────────────
def carregar_dados():
    try:
        print("Iniciando conexão com Google Sheets...")
        worksheet = gc_client.open_by_key(
            "1mGQWM8CCBeNy8TdupA7OMR8kNerWQiRQ1X8TEm97mG0"
        ).worksheet("VOL. VENDAS GERAL")

        print("Planilha aberta com sucesso!")
        dados = worksheet.get_all_values()
        print(f"Total de linhas: {len(dados)}")

        headers = dados[0]
        print(f"Colunas encontradas: {headers}")

        colunas_necessarias = ["DATA", "ANO_MÊS", "CLIENTE", "DESCRIÇÃO", "ATIVO", "QTD", "LARGURA"]
        indices = {col: headers.index(col) for col in colunas_necessarias if col in headers}
        print(f"Índices mapeados: {indices}")

        rows = []
        for row in dados[1:]:
            rows.append({col: row[idx] for col, idx in indices.items()})

        df = pd.DataFrame(rows)
        df["QTD"] = pd.to_numeric(df["QTD"], errors="coerce").fillna(0)
        df = df[df["ATIVO"] == "S"].copy()
        df["ANO"] = df["ANO_MÊS"].str[:4]
        df["MES"] = df["ANO_MÊS"].str[5:]

        print(f"Dados carregados: {len(df)} linhas")
        gc.collect()
        return df

    except Exception as e:
        print(f"ERRO ao carregar dados: {e}")
        return pd.DataFrame(columns=["DATA","ANO_MÊS","CLIENTE","DESCRIÇÃO","ATIVO","QTD","LARGURA","ANO","MES"])

df_raw = carregar_dados()

anos_disp     = sorted(df_raw["ANO"].unique()) if not df_raw.empty else ["2024"]
meses_disp    = ["01","02","03","04","05","06","07","08","09","10","11","12"]
nomes_mes     = {"01":"Janeiro","02":"Fevereiro","03":"Março","04":"Abril",
                 "05":"Maio","06":"Junho","07":"Julho","08":"Agosto",
                 "09":"Setembro","10":"Outubro","11":"Novembro","12":"Dezembro"}
clientes_disp = sorted(df_raw["CLIENTE"].unique()) if not df_raw.empty else []

# ── 3. App ───────────────────────────────────────────────────
app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
server = app.server

def dropdown_ano(id_, valor):
    return dcc.Dropdown(
        id=id_,
        options=[{"label": a, "value": a} for a in anos_disp],
        value=valor, clearable=False, style={"color": "#000"},
    )

def dropdown_mes(id_, valor):
    return dcc.Dropdown(
        id=id_,
        options=[{"label": nomes_mes[m], "value": m} for m in meses_disp],
        value=valor, clearable=False, style={"color": "#000"},
    )

app.layout = dbc.Container([

    html.H1("📊 Dashboard de Vendas SVD", className="text-center my-4"),

    dbc.Card([
        dbc.Row([
            dbc.Col([
                html.Label("📅 De", className="text-white fw-bold mb-1"),
                dbc.Row([
                    dbc.Col(dropdown_ano("ano-ini", anos_disp[0]),  width=6),
                    dbc.Col(dropdown_mes("mes-ini", meses_disp[0]), width=6),
                ]),
            ], md=4),
            dbc.Col([
                html.Label("📅 Até", className="text-white fw-bold mb-1"),
                dbc.Row([
                    dbc.Col(dropdown_ano("ano-fim", anos_disp[-1]),  width=6),
                    dbc.Col(dropdown_mes("mes-fim", meses_disp[-1]), width=6),
                ]),
            ], md=4),
            dbc.Col([
                html.Label("🏢 Cliente", className="text-white fw-bold mb-1"),
                dcc.Dropdown(
                    id="filtro-cliente",
                    options=[{"label": c, "value": c} for c in clientes_disp],
                    placeholder="Todos os clientes",
                    multi=True, style={"color": "#000"},
                ),
            ], md=4),
        ], className="p-3"),
    ], className="mb-4"),

    dbc.Row([
        dbc.Col(dbc.Card([html.H5("Total Vendido (m)", className="text-muted"), html.H3(id="kpi-total",    className="text-info")],    body=True, className="text-center"), md=3),
        dbc.Col(dbc.Card([html.H5("Clientes Únicos",   className="text-muted"), html.H3(id="kpi-clientes", className="text-success")], body=True, className="text-center"), md=3),
        dbc.Col(dbc.Card([html.H5("Produtos Distintos",className="text-muted"), html.H3(id="kpi-produtos", className="text-warning")], body=True, className="text-center"), md=3),
        dbc.Col(dbc.Card([html.H5("Melhor Mês",        className="text-muted"), html.H3(id="kpi-mes",      className="text-danger")],  body=True, className="text-center"), md=3),
    ], className="mb-4"),

    dbc.Row([dbc.Col(dcc.Graph(id="graf-linha"), md=12)], className="mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="graf-clientes"), md=6),
        dbc.Col(dcc.Graph(id="graf-produtos"), md=6),
    ], className="mb-4"),

    html.H5("📋 Dados Filtrados", className="text-white"),
    dash_table.DataTable(
        id="tabela",
        columns=[{"name": c, "id": c} for c in ["DATA","ANO_MÊS","CLIENTE","DESCRIÇÃO","QTD","LARGURA"]],
        page_size=10,
        filter_action="native",
        sort_action="native",
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "#2c2c2c", "color": "white", "fontWeight": "bold"},
        style_cell={"backgroundColor": "#1e1e1e", "color": "white", "textAlign": "left"},
    ),

], fluid=True)


# ── 4. Callback ──────────────────────────────────────────────
@app.callback(
    Output("kpi-total",     "children"),
    Output("kpi-clientes",  "children"),
    Output("kpi-produtos",  "children"),
    Output("kpi-mes",       "children"),
    Output("graf-linha",    "figure"),
    Output("graf-clientes", "figure"),
    Output("graf-produtos", "figure"),
    Output("tabela",        "data"),
    Input("ano-ini",        "value"),
    Input("mes-ini",        "value"),
    Input("ano-fim",        "value"),
    Input("mes-fim",        "value"),
    Input("filtro-cliente", "value"),
)
def atualizar(ano_ini, mes_ini, ano_fim, mes_fim, clientes_sel):
    period_ini = f"{ano_ini}-{mes_ini}"
    period_fim = f"{ano_fim}-{mes_fim}"

    df = df_raw[
        (df_raw["ANO_MÊS"] >= period_ini) &
        (df_raw["ANO_MÊS"] <= period_fim)
    ]

    if clientes_sel:
        df = df[df["CLIENTE"].isin(clientes_sel)]

    vendas_mes   = df.groupby("ANO_MÊS", as_index=False)["QTD"].sum()
    top_clientes = df.groupby("CLIENTE",  as_index=False)["QTD"].sum().nlargest(10, "QTD")
    top_produtos = df.groupby("DESCRIÇÃO",as_index=False)["QTD"].sum().nlargest(10, "QTD")

    total      = df["QTD"].sum()
    n_clientes = df["CLIENTE"].nunique()
    n_produtos = df["DESCRIÇÃO"].nunique()
    melhor_mes = vendas_mes.loc[vendas_mes["QTD"].idxmax(), "ANO_MÊS"] if not vendas_mes.empty else "-"

    fig_linha = px.line(vendas_mes, x="ANO_MÊS", y="QTD",
                        title="Evolução de Vendas por Mês (metros)",
                        markers=True, template="plotly_dark")

    fig_clientes = px.bar(top_clientes.sort_values("QTD"), x="QTD", y="CLIENTE",
                          orientation="h", title="Top 10 Clientes por Volume",
                          template="plotly_dark", color="QTD", color_continuous_scale="Blues")

    fig_produtos = px.bar(top_produtos.sort_values("QTD"), x="QTD", y="DESCRIÇÃO",
                          orientation="h", title="Top 10 Produtos por Volume",
                          template="plotly_dark", color="QTD", color_continuous_scale="Greens")

    return (
        f"{total:,.0f} m",
        str(n_clientes),
        str(n_produtos),
        str(melhor_mes),
        fig_linha,
        fig_clientes,
        fig_produtos,
        df.head(50).to_dict("records"),
    )


if __name__ == "__main__":
    app.run(debug=False)
    