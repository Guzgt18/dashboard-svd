# ============================================================
# MRP — Sistema PCP (nova versão)
# ============================================================

import json
import math
import os
import pickle
import re
import threading
import unicodedata
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd
import streamlit as st

CACHE_DIR = Path("cache_mrp")
CACHE_DIR.mkdir(exist_ok=True)
CACHE_TTL_MIN = 30

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Planilhas de dados
SVD_KEY = "1mGQWM8CCBeNy8TdupA7OMR8kNerWQiRQ1X8TEm97mG0"
LAMINADORAS_KEY = "1nUq2iXMXfHhvtdfw0WxqxkRLic_E8qr-2lKkv5l03OU"
DEFAULT_SHEET_NAME = "LAMINADORAS"


def normalize_column_name(value: str) -> str:
    text = str(value).strip()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ASCII", "ignore").decode("ASCII")
    return re.sub(r"[^A-Z0-9 ]+", "", text).strip().upper()


def is_na_value(value) -> bool:
    if value is None:
        return True
    text = str(value).strip().upper()
    return text in {"N/A", "NA", "NAO UTILIZA", "NÃO UTILIZA", "-", ""}


def limpar_numero(valor):
    if valor is None:
        return 0.0
    if isinstance(valor, (int, float)):
        return float(valor)
    text = str(valor).strip().replace("\xa0", " ")
    if is_na_value(text):
        return 0.0
    text = text.replace("R$", "").replace("$", "").replace(" ", "")
    if text == "":
        return 0.0
    if text.count(",") > 0 and text.count(".") == 0:
        text = text.replace(".", "").replace(",", ".")
    elif text.count(",") > 0 and text.count(".") > 0:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    text = re.sub(r"[^0-9.\-]", "", text)
    try:
        return float(text)
    except ValueError:
        return 0.0


def extract_cilindro_value(valor):
    if is_na_value(valor):
        return 0.0
    text = str(valor).upper().replace(" ", "")
    match = re.search(r"(\d+(?:[\.,]\d+)?)G", text)
    if match:
        return limpar_numero(match.group(1))
    return 0.0


def parse_number(value):
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("\xa0", " ")
    text = text.replace("R$", "").replace("$", "").replace(" ", "")
    if text == "":
        return 0.0
    text = text.replace(".", "").replace(",", ".") if text.count(",") > 0 and text.count(".") == 0 else text
    try:
        return float(text)
    except ValueError:
        return 0.0


def get_credentials():
    env = os.environ.get("GOOGLE_CREDENTIALS")
    if env:
        from google.oauth2.service_account import Credentials
        return Credentials.from_service_account_info(json.loads(env), scopes=SCOPES)

    default_path = r"C:\Users\gusgt\credentials\dashpython-494416-4c76af5b36c4.json"
    if os.path.exists(default_path):
        from google.oauth2.service_account import Credentials
        return Credentials.from_service_account_file(default_path, scopes=SCOPES)

    raise FileNotFoundError(
        "Credenciais do Google não encontradas. Defina GOOGLE_CREDENTIALS ou configure o arquivo de credenciais local."
    )


def get_gspread_client():
    try:
        import gspread
    except ImportError as exc:
        st.error("Pacote 'gspread' não encontrado. Instale com: pip install gspread google-auth")
        raise exc

    creds = get_credentials()
    return gspread.authorize(creds)


def _sheet_df(key: str, worksheet_name: str) -> pd.DataFrame:
    """Carrega dados de uma planilha Google Sheets."""
    try:
        gc = get_gspread_client()
        try:
            worksheet = gc.open_by_key(key).worksheet(worksheet_name)
        except Exception:
            workbook = gc.open_by_key(key)
            worksheets = workbook.worksheets()
            if worksheets:
                worksheet = worksheets[0]
            else:
                return pd.DataFrame()

        data = worksheet.get_all_values()
        if not data or len(data) < 2:
            return pd.DataFrame()

        headers = [str(h).strip() for h in data[0]]
        if not headers:
            return pd.DataFrame()
        
        rows = [dict(zip(headers, row)) for row in data[1:] if any(row)]
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows)
        return df if not df.empty else pd.DataFrame()
    except Exception as e:
        st.warning(f"Erro ao carregar planilha {worksheet_name}: {e}")
        return pd.DataFrame()


def _cache_path(name: str) -> Path:
    return CACHE_DIR / f"{name}.pkl"


def _save_cache(name: str, df: pd.DataFrame):
    with open(_cache_path(name), "wb") as f:
        pd.to_pickle({"ts": datetime.now(), "df": df}, f)


def _load_cache(name: str):
    path = _cache_path(name)
    if not path.exists():
        return None, None
    try:
        data = pd.read_pickle(path)
        return data.get("df"), data.get("ts")
    except (EOFError, pickle.UnpicklingError, KeyError) as e:
        # Arquivo corrompido - deletar e recarregar
        path.unlink(missing_ok=True)
        return None, None


def _cache_stale(ts):
    return ts is None or (datetime.now() - ts).total_seconds() > CACHE_TTL_MIN * 60


def _validate_cache(name: str, df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    if name == "vendas":
        cod_col = find_column(df, ["CODIGO", "COD", "PRODUTO", "C�DIGO", "CÓDIGO"])
        qtd_col = find_column(df, ["QTD", "VENDA", "VENDAS", "TOTAL"])
        return bool(cod_col and qtd_col)
    if name == "vr":
        dif_col = find_column(df, ["D", "DIFERENCA", "SALDO"])
        cod_col = find_column(df, ["C P", "CODIGO", "COD", "PRODUTO", "C�DIGO", "CÓDIGO"])
        return bool(dif_col and cod_col)
    if name == "laminadoras":
        return find_column(df, ["LAMINADORA"]) is not None and find_column(df, ["CODIGO"]) is not None
    return True


def cached(name: str, fn):
    df, ts = _load_cache(name)
    if df is None or not _validate_cache(name, df):
        df = fn()
        _save_cache(name, df)
        ts = datetime.now()
    elif _cache_stale(ts):
        threading.Thread(target=lambda: _save_cache(name, fn()), daemon=True).start()
    return df, ts


def limpar_cache():
    import shutil

    shutil.rmtree(str(CACHE_DIR), ignore_errors=True)
    CACHE_DIR.mkdir(exist_ok=True)


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza nomes de colunas e tipos de dados."""
    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    
    if df.empty:
        return df.copy()
    
    df = df.copy()
    try:
        df.columns = [normalize_column_name(c) for c in df.columns]
    except Exception:
        return df
    
    try:
        for c in df.columns:
            if c in df.columns and df[c].dtype == object:
                df[c] = df[c].astype(str).str.strip()
    except Exception:
        pass
    
    return df


def find_column(df: pd.DataFrame, patterns):
    col_map = {normalize_column_name(c): c for c in df.columns}
    for pattern in patterns:
        pattern_norm = normalize_column_name(pattern)
        for normalized, original in col_map.items():
            if pattern_norm in normalized:
                return original
    return None


def parse_date(value):
    if value is None:
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        return value
    text = str(value).strip()
    if text == "":
        return pd.NaT
    for fmt in ["%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%m/%d/%Y"]:
        try:
            return pd.to_datetime(text, format=fmt, dayfirst=True)
        except Exception:
            continue
    try:
        return pd.to_datetime(text, dayfirst=True, errors="coerce")
    except Exception:
        return pd.NaT


def load_laminadoras_data() -> pd.DataFrame:
    def _load():
        df = _sheet_df(LAMINADORAS_KEY, DEFAULT_SHEET_NAME)
        if df.empty:
            return df
        df = normalize_dataframe(df)
        df.columns = [str(c).strip() for c in df.columns]
        # Parse numeric columns in laminadoras
        for col in ["LARGURA", "METROS_POR_MINUTO", "MINUTOS_POR_METRO", "TROCA_DE_BOBINA_MAE", "SETUP", "ESTOQUE PAPEL SILINICONADO", "ESTOUE ADESIVO", "ESTOQUE VMP"]:
            if col in df.columns:
                df[col] = df[col].apply(limpar_numero)
        df["LAMINADORA_NUM"] = df[find_column(df, ["LAMINADORA"])].astype(str).str.strip().str.upper().str.replace("L", "")
        df["CILINDRO_GM2"] = df[find_column(df, ["CILINDRO:", "CILINDRO"] )].apply(extract_cilindro_value)
        return df

    df, _ = cached("laminadoras", _load)
    return df


def load_mrp_data(sheet_name: str = DEFAULT_SHEET_NAME) -> pd.DataFrame:
    return load_laminadoras_data()


def load_vendas_data():
    """Carrega dados de vendas da aba VOL. VENDAS GERAL"""
    def _load():
        df = _sheet_df(SVD_KEY, "VOL. VENDAS GERAL")
        if df.empty:
            return df
        df = normalize_dataframe(df)
        df.columns = [str(c).strip() for c in df.columns]

        data_col = find_column(df, ["DATA"])
        if data_col:
            df["DATA_DT"] = df[data_col].apply(parse_date)
            df["ANO"] = df["DATA_DT"].dt.year
        cod_col = find_column(df, ["CODIGO", "C�DIGO", "CÓDIGO"])
        qtd_col = find_column(df, ["QTD", "QUANTIDADE", "QUANTIDADE VENDIDA", "QTD"])
        if cod_col:
            df["_cod"] = df[cod_col].astype(str).str.strip()
        if qtd_col:
            df["_qtd"] = df[qtd_col].apply(limpar_numero)
        return df

    df, _ = cached("vendas", _load)
    return df


def load_vr_data():
    """Carrega dados de VENDA/REVENDA"""
    def _load():
        df = _sheet_df(SVD_KEY, "VENDA/REVENDA")
        if df.empty:
            return df
        df = normalize_dataframe(df)
        df.columns = [str(c).strip() for c in df.columns]

        dif_col = find_column(df, ["D", "DIFERENCA", "SALDO"])
        cod_col = find_column(df, ["C P", "CODIGO", "COD", "PRODUTO", "C�DIGO", "CÓDIGO"])
        if dif_col:
            df["DIFERENCA_NUM"] = df[dif_col].apply(limpar_numero)
        if cod_col:
            df["CODIGO"] = df[cod_col].astype(str).str.strip()
        return df

    df, _ = cached("vr", _load)
    return df


def calcular_abc(df_vendas: pd.DataFrame) -> pd.DataFrame:
    """Calcula curva ABC baseada no volume total vendido."""
    if df_vendas.empty:
        return pd.DataFrame()

    if "_cod" not in df_vendas.columns or "_qtd" not in df_vendas.columns:
        return pd.DataFrame()

    df_abc = df_vendas.groupby("_cod") ["_qtd"].sum().reset_index()
    df_abc = df_abc.sort_values("_qtd", ascending=False)

    total_vendas = df_abc["_qtd"].sum()
    if total_vendas <= 0:
        return pd.DataFrame()

    df_abc["cumulativo"] = df_abc["_qtd"].cumsum() / total_vendas * 100

    def classificar_abc(cum):
        if cum <= 80:
            return "A", 5
        elif cum <= 95:
            return "B", 3
        else:
            return "C", 1

    df_abc[["curva_abc", "pontos_abc"]] = df_abc["cumulativo"].apply(classificar_abc).apply(pd.Series)
    return df_abc[["_cod", "curva_abc", "pontos_abc"]]


def calcular_giro_ano(df_vendas: pd.DataFrame) -> pd.Series:
    """Calcula giro do ano atual normalizado de 0 a 5."""
    if df_vendas.empty or "_cod" not in df_vendas.columns:
        return pd.Series(dtype=float)

    ano_atual = pd.Timestamp.today().year
    if "ANO" in df_vendas.columns:
        df_ano = df_vendas[df_vendas["ANO"] == ano_atual]
    else:
        data_col = find_column(df_vendas, ["DATA", "DATA_DT"])
        df_ano = df_vendas[df_vendas[data_col].apply(parse_date).dt.year == ano_atual] if data_col else df_vendas

    if df_ano.empty:
        return pd.Series(dtype=float)

    df_giro = df_ano.groupby("_cod")["_qtd"].sum().reset_index()
    if df_giro["_qtd"].empty:
        return pd.Series(dtype=float)

    min_val = df_giro["_qtd"].min()
    max_val = df_giro["_qtd"].max()
    if max_val > min_val:
        df_giro["pontos_giro"] = ((df_giro["_qtd"] - min_val) / (max_val - min_val) * 5).round(2)
    else:
        df_giro["pontos_giro"] = 5.0

    return pd.Series(df_giro["pontos_giro"].values, index=df_giro["_cod"].astype(str).str.strip())


def calcular_minimo_historico(df_vendas: pd.DataFrame) -> pd.Series:
    """Calcula mínimo recomendado de produção com base em histórico de vendas."""
    if df_vendas.empty or "_cod" not in df_vendas.columns or "_qtd" not in df_vendas.columns:
        return pd.Series(dtype=float)

    if "DATA_DT" not in df_vendas.columns:
        return pd.Series(dtype=float)

    hoje = pd.Timestamp.today().normalize()
    seis_meses_atras = hoje - pd.DateOffset(months=6)
    quatro_semanas_atras = hoje - pd.DateOffset(weeks=4)
    mesmo_mes_ano_passado = (hoje - pd.DateOffset(years=1)).replace(day=1)
    proximo_mes = mesmo_mes_ano_passado + pd.DateOffset(months=1)

    df_6m = df_vendas[df_vendas["DATA_DT"] >= seis_meses_atras].copy()
    medias_6m = (
        df_6m.groupby(["_cod", df_6m["DATA_DT"].dt.to_period("M")])["_qtd"]
        .sum()
        .groupby(level=0)
        .mean()
    )

    df_saz = df_vendas[(df_vendas["DATA_DT"] >= mesmo_mes_ano_passado) & (df_vendas["DATA_DT"] < proximo_mes)].copy()
    saz = df_saz.groupby("_cod")["_qtd"].sum()

    df_4s = df_vendas[df_vendas["DATA_DT"] >= quatro_semanas_atras].copy()
    tend = (df_4s.groupby("_cod")["_qtd"].sum() / 4 * 4.33)

    base = pd.concat([medias_6m, saz], axis=1).max(axis=1).fillna(0)
    fator = (tend / base.replace(0, 1)).clip(0.8, 1.5).fillna(0)
    minimo = (base * fator).round(0).fillna(0)
    return minimo


def calcular_criticidade(df_prod: pd.DataFrame, df_vr: pd.DataFrame, df_vendas: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula criticidade baseada em:
    1. Débito total (diferença < 0): 0-5 pontos
    2. Curva ABC: 0-5 pontos
    3. Giro de estoque no ano atual: 0-5 pontos
    4. Mínimo histórico com ajuste de demanda
    5. Verificação de insumos pelo centro de trabalho
    """
    if df_vr.empty or df_vendas.empty:
        st.warning("Dados de vendas ou VENDA/REVENDA não disponíveis para calcular criticidade.")
        return pd.DataFrame()

    diferenca_col = "D" if "D" in df_vr.columns else find_column(df_vr, ["DIFERENCA", "SALDO", "Diferenca"])
    codigo_col_vr = "C P" if "C P" in df_vr.columns else find_column(df_vr, ["CODIGO", "COD", "PRODUTO", "Cod Produto"])
    desc_col_vr = "D P" if "D P" in df_vr.columns else find_column(df_vr, ["DESCRICAO", "DESCRIÇÃO", "Desc Produto"])

    if not diferenca_col or not codigo_col_vr:
        st.error("Colunas DIFERENCA ou CODIGO não encontradas em VENDA/REVENDA.")
        return pd.DataFrame()

    df_crit = df_vr[df_vr[diferenca_col].apply(parse_number) < 0].copy()
    df_crit["DIFERENCA_NUM"] = df_crit[diferenca_col].apply(parse_number)
    df_crit["CODIGO"] = df_crit[codigo_col_vr].astype(str).str.strip()
    if desc_col_vr:
        df_crit["DESCRICAO"] = df_crit[desc_col_vr].astype(str).str.strip()

    abc_df = calcular_abc(df_vendas)
    df_crit = df_crit.merge(abc_df, left_on="CODIGO", right_on="_cod", how="left")
    df_crit["pontos_abc"] = df_crit["pontos_abc"].fillna(1)
    df_crit["curva_abc"] = df_crit["curva_abc"].fillna("C")

    giro_series = calcular_giro_ano(df_vendas)
    df_crit["pontos_giro"] = df_crit["CODIGO"].map(giro_series).fillna(0)

    debito_abs = df_crit["DIFERENCA_NUM"].abs()
    if debito_abs.max() > debito_abs.min():
        df_crit["pontos_debito"] = ((debito_abs - debito_abs.min()) / (debito_abs.max() - debito_abs.min()) * 5).round(2)
    else:
        df_crit["pontos_debito"] = 5.0

    minimo_historico = calcular_minimo_historico(df_vendas)
    df_crit["minimo_historico"] = df_crit["CODIGO"].map(minimo_historico).fillna(0)
    df_crit["metros_a_produzir"] = df_crit.apply(
        lambda row: max(abs(row["DIFERENCA_NUM"]), row["minimo_historico"]), axis=1
    )

    df_crit["criticidade_total"] = (
        df_crit["pontos_debito"] + df_crit["pontos_abc"] + df_crit["pontos_giro"]
    ).round(2)

    df_crit["status_insumo"] = df_crit.apply(
        lambda row: verificar_insumos(row["CODIGO"], row["metros_a_produzir"], df_prod), axis=1
    )

    return df_crit.sort_values("criticidade_total", ascending=False).reset_index(drop=True)


def calcular_tempo_producao(metros: float, minutos_por_metro: float, troca_mae: float) -> tuple[float, float, float]:
    """Calcula tempo de produção, setup e tempo total."""
    metros = abs(float(metros or 0.0))
    tempo_producao = metros * float(minutos_por_metro or 0.0)
    n_trocas_bobina = max(1, math.floor(metros / 3000))
    tempo_troca = n_trocas_bobina * float(troca_mae or 0.0)
    return tempo_producao, tempo_troca, tempo_producao + tempo_troca


def calcular_consumo_adesivo(cilindro_gm2: float, largura_m: float, metros_prod: float) -> float:
    """Calcula consumo de adesivo em kg."""
    if cilindro_gm2 <= 0 or largura_m <= 0 or metros_prod <= 0:
        return 0.0
    return float(cilindro_gm2) * 2 / 1000 * float(largura_m) * abs(float(metros_prod))


def verificar_insumos(codigo: str, metros_necessarios: float, df_laminadoras: pd.DataFrame) -> str:
    """Verifica se há insumos suficientes para produzir o produto."""
    if df_laminadoras.empty:
        return "🔴 Sem dados de laminadoras"

    codigo = str(codigo).strip()
    codigo_col = find_column(df_laminadoras, ["CODIGO", "CÓDIGO", "COD"])
    if not codigo_col:
        return "🔴 Código de produto não encontrado em laminadoras"

    linhas = df_laminadoras[df_laminadoras[codigo_col].astype(str).str.strip() == codigo]
    if linhas.empty:
        return "🔴 Produto não mapeado no centro de trabalho"

    # Selecionar a linha com melhor velocidade (maior eficiência)
    velocidade_col = find_column(linhas, ["METROS_POR_MINUTO", "VELOCIDADE"])
    if velocidade_col:
        linhas = linhas[linhas[velocidade_col].apply(parse_number) > 0]
        if not linhas.empty:
            linhas = linhas.sort_values(velocidade_col, ascending=False).head(1)

    if linhas.empty:
        return "🔴 Produto não pode ser fabricado (sem velocidade cadastrada)"

    linha = linhas.iloc[0]
    faltas = []

    papel_cod_col = find_column(linhas, ["CÓDIGO PAPEL SILICONADO", "CODIGO PAPEL SILICONADO"])
    estoque_papel_col = find_column(linhas, ["ESTOQUE PAPEL SILINICONADO", "ESTOQUE PAPEL SILICONADO"])
    if papel_cod_col and estoque_papel_col:
        estoque_papel = parse_number(linha.get(estoque_papel_col, 0))
        if estoque_papel < abs(metros_necessarios):
            faltas.append("Papel siliconado")

    adesivo_cod_col = find_column(linhas, ["CÓDIGO ADESIVO", "CODIGO ADESIVO"])
    adesivo_estoque_col = find_column(linhas, ["ESTOUE ADESIVO", "ESTOQUE ADESIVO"])
    if adesivo_cod_col and adesivo_estoque_col:
        consumo = calcular_consumo_adesivo(
            parse_number(linha.get(find_column(linhas, ["CILINDRO_GM2", "CILINDRO", "CILINDRO:"]))),
            parse_number(linha.get(find_column(linhas, ["LARGURA"]))),
            metros_necessarios,
        )
        estoque_adesivo = parse_number(linha.get(adesivo_estoque_col, 0))
        if estoque_adesivo < consumo:
            faltas.append("Adesivo")

    vmp_col = find_column(linhas, ["CÓDIGO VMP", "CODIGO VMP"])
    estoque_vmp_col = find_column(linhas, ["ESTOQUE VMP"])
    if vmp_col and estoque_vmp_col:
        estoque_vmp = parse_number(linha.get(estoque_vmp_col, 0))
        if estoque_vmp <= 0:
            faltas.append("VMP")

    return "✅ OK" if not faltas else f"🔴 Falta: {', '.join(faltas)}"


def distribuir_por_maquinas(df_producao: pd.DataFrame, df_prod: pd.DataFrame) -> pd.DataFrame:
    """
    Distribui produtos por laminadoras considerando:
    - Equilibrio de carga entre máquinas (prioridade)
    - Agrupamento por linha para minimizar setup (secundário)
    - Produtos exclusivos respeitam sua restrição
    - Preenche o dia completo (05:30-23:00)
    """
    # Agrupar produtos por código para ver opções de máquina
    prod_opcoes = {}
    for _, row in df_prod.iterrows():
        cod = str(row.get("CODIGO", "")).strip()
        lam = str(row.get("LAMINADORA", "")).strip()
        if cod and lam:
            velocidade = parse_number(row.get("METROS_POR_MINUTO", row.get("VELOCIDADE", 0)))
            min_por_metro = 1 / velocidade if velocidade > 0 else 0
            if cod not in prod_opcoes:
                prod_opcoes[cod] = []
            prod_opcoes[cod].append({
                "lam": lam,
                "linha": str(row.get("LINHA DE PRODUTO", "")).strip(),
                "largura": parse_number(row.get("LARGURA", 1.02)),
                "processo": str(row.get("PROCESSO", "")).strip(),
                "cilindro": parse_number(row.get("CILINDRO:", 20)),
                "min_por_metro": min_por_metro,
                "setup": parse_number(row.get("SETUP", 50)),
                "troca": parse_number(row.get("TROCA_DE_BOBINA_MAE", 13)),
            })

    # Separar exclusivos e flexíveis
    exclusivos = []
    flexiveis = []
    for _, prod in df_producao.iterrows():
        cod = prod["CODIGO"]
        opcoes = prod_opcoes.get(cod, [])
        lams = list(set(o["lam"] for o in opcoes))
        linha_prod = opcoes[0]["linha"] if opcoes else ""
        if len(lams) == 1:
            exclusivos.append({**prod.to_dict(), "lams": lams, "opcoes": opcoes, "linha_produto": linha_prod})
        elif len(lams) > 1:
            flexiveis.append({**prod.to_dict(), "lams": lams, "opcoes": opcoes, "linha_produto": linha_prod})

    # Ordenar ambos por criticidade (alta prioridade primeiro)
    exclusivos.sort(key=lambda x: -x["criticidade_total"])
    flexiveis.sort(key=lambda x: -x["criticidade_total"])

    # Capacidade das máquinas (05:30-23:00 = 17,5h produtivas por dia)
    CAPACIDADE_DIARIA_MIN = 17.5 * 60  # 1050 minutos
    carga_atual = {f"L{i}": 0 for i in range(1, 5)}  # L1-L4
    linha_atual = {f"L{i}": None for i in range(1, 5)}
    produtos_por_maquina = {f"L{i}": 0 for i in range(1, 5)}  # Contar produtos

    distribuicao = []

    # ===== FASE 1: Alocar exclusivos =====
    for prod in exclusivos:
        lam = prod["lams"][0]
        if carga_atual[lam] < CAPACIDADE_DIARIA_MIN:
            opcao = prod["opcoes"][0]
            distribuicao.append({**prod, "laminadora_alocada": lam})
            metros = abs(prod["DIFERENCA_NUM"])
            tempo_est = metros * opcao["min_por_metro"] + opcao["setup"]
            carga_atual[lam] += min(tempo_est, CAPACIDADE_DIARIA_MIN - carga_atual[lam])
            linha_atual[lam] = opcao["linha"]
            produtos_por_maquina[lam] += 1

    # ===== FASE 2: Alocar flexíveis com balanceamento =====
    # Ordenar flexíveis não alocados para tomar decisões
    nao_alocados = flexiveis.copy()
    
    while nao_alocados:
        melhor_aloc = None
        melhor_idx = -1
        melhor_score = float('inf')
        
        for idx, prod in enumerate(nao_alocados):
            metros = abs(prod["DIFERENCA_NUM"])
            
            # Para cada máquina que pode fabricar este produto
            for opcao in prod["opcoes"]:
                lam = opcao["lam"]
                
                # Verificar se cabe em capacidade
                if carga_atual[lam] >= CAPACIDADE_DIARIA_MIN:
                    continue
                
                setup = opcao["setup"] if linha_atual[lam] != opcao["linha"] else 0
                tempo_est = metros * opcao["min_por_metro"] + setup
                
                if carga_atual[lam] + tempo_est > CAPACIDADE_DIARIA_MIN:
                    continue
                
                # Score: prioriza máquinas com MENOR carga, depois mesma linha
                carga_percentual = carga_atual[lam] / CAPACIDADE_DIARIA_MIN
                mesma_linha = 0 if linha_atual[lam] == opcao["linha"] else 10
                score = (carga_percentual, mesma_linha, -prod["criticidade_total"])
                
                if score < melhor_score:
                    melhor_score = score
                    melhor_aloc = (idx, prod, lam, opcao, tempo_est)
                    melhor_idx = idx
        
        if melhor_aloc is None:
            break  # Nenhum produto mais pode ser alocado
        
        idx, prod, lam, opcao, tempo_est = melhor_aloc
        distribuicao.append({**prod, "laminadora_alocada": lam})
        carga_atual[lam] += tempo_est
        linha_atual[lam] = opcao["linha"]
        produtos_por_maquina[lam] += 1
        nao_alocados.pop(melhor_idx)

    return pd.DataFrame(distribuicao)


def gerar_ordens_producao(df_distribuido: pd.DataFrame) -> pd.DataFrame:
    """
    Gera ordens de produção com sequência e horários considerando setup.
    """
    TURNOS = {
        "🌅 1º Turno": {"inicio": 5.5, "fim": 14.5},
        "🌆 2º Turno": {"inicio": 14.5, "fim": 23.0}
    }

    ordens = []
    cursor_por_maquina = {f"L{i}": 5.5 for i in range(1, 5)}  # Início em 05:30
    linha_atual = {f"L{i}": None for i in range(1, 5)}

    def ordenar_por_linha_e_criticidade(df_lam):
        filas = df_lam.to_dict("records")
        ordered = []
        current_line = None

        while filas:
            same_line = [r for r in filas if r["opcoes"][0].get("linha", "") == current_line] if current_line else []
            if same_line:
                escolha = max(same_line, key=lambda r: r["criticidade_total"])
            else:
                escolha = max(filas, key=lambda r: r["criticidade_total"])

            ordered.append(escolha)
            filas.remove(escolha)
            current_line = escolha["opcoes"][0].get("linha", "")

        return ordered

    for lam in sorted(df_distribuido["laminadora_alocada"].unique()):
        df_lam = df_distribuido[df_distribuido["laminadora_alocada"] == lam].copy()
        ordered_prods = ordenar_por_linha_e_criticidade(df_lam)

        for prod in ordered_prods:
            opcao = prod["opcoes"][0]
            linha = opcao["linha"]
            setup = opcao["setup"] if linha != linha_atual[lam] else 0
            linha_atual[lam] = linha

            metros = abs(prod["DIFERENCA_NUM"])
            tempo_prod = metros * opcao["min_por_metro"]
            tempo_total = tempo_prod + setup

            inicio = cursor_por_maquina[lam]
            fim = inicio + tempo_total / 60

            if inicio >= 23.0 or fim > 23.0:
                continue

            if inicio < 14.5:
                turno = "🌅 1º Turno" if fim <= 14.5 else "🌅→🌆 1º e 2º Turno"
            else:
                turno = "🌆 2º Turno"

            ordens.append({
                "Laminadora": lam,
                "Código": prod["CODIGO"],
                "Descrição": prod.get("DESCRICAO", ""),
                "Metros": metros,
                "Criticidade": prod["criticidade_total"],
                "Turno": turno,
                "Início": f"{int(inicio):02d}:{int((inicio % 1) * 60):02d}",
                "Fim": f"{int(fim):02d}:{int((fim % 1) * 60):02d}",
                "Tempo Produção (h)": round(tempo_prod / 60, 2),
                "Setup (min)": setup,
                "Tempo Total (h)": round(tempo_total / 60, 2),
                "Linha": linha,
                "Processo": opcao["processo"],
                "Justificativa Alteração": "",
            })

            cursor_por_maquina[lam] = fim

    return pd.DataFrame(ordens)


def build_metrics(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}

    codigo_col = find_column(df, ["CODIGO", "COD", "PRODUTO"])
    desc_col = find_column(df, ["DESCRICAO", "DESCRIÇÃO", "PRODUTO"])
    diferenca_col = find_column(df, ["DIFERENCA", "SALDO", "ESTOQUE"])
    vendas_col = find_column(df, ["VENDIDO", "QTDE", "QTD", "VOLUME"])

    metrics = {
        "linhas": len(df),
        "produtos_distintos": df[codigo_col].nunique() if codigo_col else df.shape[0],
        "clientes_distintos": df[find_column(df, ["CLIENTE"])].nunique() if find_column(df, ["CLIENTE"]) else 0,
        "total_deficit": df[diferenca_col].apply(parse_number).sum() if diferenca_col else 0,
        "total_vendas": df[vendas_col].apply(parse_number).sum() if vendas_col else 0,
    }
    return metrics


def render_mrp():
    st.set_page_config(
        page_title="MRP — Sistema PCP",
        page_icon="🏭",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("🏭 Sistema PCP — Planejamento de Produção")

    with st.expander("Configurações de Fonte de Dados"):
        st.text_input("Chave da planilha MRP", SVD_KEY, key="mrp_sheet_key")
        st.text_input("Nome da aba MRP", DEFAULT_SHEET_NAME, key="mrp_sheet_name")
        if st.button("Limpar cache de dados"):
            limpar_cache()

    # Carregar dados
    df_prod = load_mrp_data(st.session_state.get("mrp_sheet_name", DEFAULT_SHEET_NAME))
    df_vr = load_vr_data()
    df_vendas = load_vendas_data()

    if df_prod.empty:
        st.error("Não foi possível carregar dados da planilha MRP. Verifique as credenciais e o nome da aba.")
        return

    # Inicializar session state
    for key in ["lista_critica", "lista_producao", "lista_espera", "distribuicao", "ordens", "alteracoes_log", "fase"]:
        if key not in st.session_state:
            if key in ["lista_critica", "lista_producao"]:
                st.session_state[key] = pd.DataFrame()
            elif key == "lista_espera":
                st.session_state[key] = []
            elif key == "alteracoes_log":
                st.session_state[key] = []
            elif key in ["distribuicao", "ordens"]:
                st.session_state[key] = pd.DataFrame()
            else:
                st.session_state[key] = "classificar"

    # Abas do sistema
    tabs = st.tabs([
        "1️⃣ Classificar Criticidade",
        "2️⃣ Lista de Produção",
        "3️⃣ Distribuição por Máquina",
        "4️⃣ Ordens de Produção",
        "⚙️ Análise de Gargalos"
    ])

    # TAB 1: Classificar Criticidade
    with tabs[0]:
        st.subheader("1️⃣ Classificação de Criticidade")
        st.markdown("""
        **Critérios de pontuação (0-5 cada):**
        - **Débito total**: Quanto maior o débito negativo, maior a pontuação
        - **Curva ABC**: A=5, B=3, C=1
        - **Giro de estoque no ano atual**: Normalizado 0-5
        """)

        col1, col2 = st.columns(2)
        limite_deficit = col1.slider("Incluir produtos com saldo abaixo de (m):", -50000, 500, 0, 100, key="limite_deficit")
        min_crit = col2.slider("Criticidade mínima para incluir:", 0.0, 15.0, 0.0, 0.5, key="min_crit")

        if st.button("🚀 Calcular Criticidade", type="primary"):
            with st.spinner("Classificando produtos..."):
                df_crit = calcular_criticidade(df_prod, df_vr, df_vendas)
                df_crit = df_crit[df_crit["DIFERENCA_NUM"] <= limite_deficit]
                df_crit = df_crit[df_crit["criticidade_total"] >= min_crit]
                st.session_state["lista_critica"] = df_crit
                st.session_state["fase"] = "producao"
            st.success(f"✅ {len(df_crit)} produtos classificados!")

        if not st.session_state["lista_critica"].empty:
            df_crit = st.session_state["lista_critica"]

            col1, col2, col3 = st.columns(3)
            col1.metric("Total classificados", len(df_crit))
            col2.metric("Crítico médio", f"{df_crit['criticidade_total'].mean():.1f}")
            col3.metric("Com falta de insumo", len(df_crit[df_crit["status_insumo"].str.contains("🔴")]))

            # Top 20 produtos mais críticos
            st.subheader("Top 20 — Produtos Mais Críticos")
            top_crit = df_crit.head(20)
            st.dataframe(
                top_crit[["CODIGO", "DESCRICAO", "DIFERENCA_NUM", "curva_abc", "criticidade_total", "status_insumo"]],
                column_config={
                    "DIFERENCA_NUM": st.column_config.NumberColumn("Saldo (m)", format="%.0f"),
                    "criticidade_total": st.column_config.NumberColumn("🎯 Criticidade", format="%.2f"),
                },
                hide_index=True,
                use_container_width=True
            )

    # TAB 2: Lista de Produção
    with tabs[1]:
        st.subheader("2️⃣ Lista de Produção")

        if st.session_state["lista_critica"].empty:
            st.info("Calcule a criticidade na aba anterior primeiro.")
        else:
            df_crit = st.session_state["lista_critica"]
            lista_espera = st.session_state.get("lista_espera", [])
            if not isinstance(lista_espera, list):
                lista_espera = list(lista_espera)
                st.session_state["lista_espera"] = lista_espera

            st.markdown("""
            **Instruções:** Revise cada produto e decida se deve ser incluído na produção imediata.
            - Produtos com falta de insumo estão marcados em 🔴
            - Clique em "⏳ Para espera" se não quiser produzir agora
            - Produtos não enviados para espera entram automaticamente na lista de produção
            """)

            for idx, row in df_crit.iterrows():
                cod = row["CODIGO"]
                desc = row.get("DESCRICAO", cod)
                crit = row["criticidade_total"]
                saldo = row["DIFERENCA_NUM"]
                insumo = row["status_insumo"]
                abc = row.get("curva_abc", "C")

                cor_borda = "🔴" if "🔴" in insumo else "🟡" if crit < 5 else "🟢"
                in_espera = cod in lista_espera

                with st.container(border=True):
                    col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 2])
                    col1.markdown(f"**{desc}** `{cod}`")
                    col2.metric("Saldo", f"{saldo:,.0f}m")
                    col3.metric("🎯 Crítico", f"{crit:.1f}")
                    col4.metric("ABC", abc)
                    col5.markdown(f"**Insumo:** {insumo}")

                    if in_espera:
                        if st.button(f"↩️ Voltar à produção", key=f"voltar_{idx}"):
                            lista_espera.remove(cod)
                            st.session_state["lista_espera"] = lista_espera
                        st.markdown("_⏳ Em espera_")
                    else:
                        if st.button(f"⏳ Para espera", key=f"espera_{idx}"):
                            lista_espera.append(cod)
                            st.session_state["lista_espera"] = lista_espera

            st.divider()
            col1, col2 = st.columns(2)
            col1.metric("✅ Na produção", len(df_crit) - len(lista_espera))
            col2.metric("⏳ Em espera", len(lista_espera))

            if st.button("✅ Confirmar Lista e Avançar", type="primary"):
                lista_producao = df_crit[~df_crit["CODIGO"].isin(lista_espera)].copy()
                st.session_state["lista_producao"] = lista_producao
                st.session_state["fase"] = "distribuir"
                st.success(f"Lista confirmada com {len(lista_producao)} produtos!")

    # TAB 3: Distribuição por Máquina
    with tabs[2]:
        st.subheader("3️⃣ Distribuição por Máquina")

        if st.session_state["lista_producao"].empty:
            st.info("Confirme a lista de produção na aba anterior.")
        else:
            lista_prod = st.session_state["lista_producao"]

            st.info("A distribuição automática prioriza produtos exclusivos e equilibra máquinas para produtos flexíveis.")

            if st.button("⚙️ Gerar Distribuição", type="primary"):
                with st.spinner("Distribuindo produtos nas laminadoras..."):
                    df_dist = distribuir_por_maquinas(lista_prod, df_prod)
                    st.session_state["distribuicao"] = df_dist
                st.success("Distribuição gerada!")

            if not st.session_state["distribuicao"].empty:
                df_dist = st.session_state["distribuicao"]

                # Resumo por máquina
                resumo = df_dist.groupby("laminadora_alocada").agg(
                    Produtos=("CODIGO", "count"),
                    Criticidade_Média=("criticidade_total", "mean"),
                    Metros_Total=("DIFERENCA_NUM", lambda x: x.abs().sum())
                ).round(2)

                st.subheader("Resumo por Laminadora")
                st.dataframe(resumo, use_container_width=True)

                # Detalhes por máquina
                for lam in sorted(df_dist["laminadora_alocada"].unique()):
                    with st.expander(f"📋 {lam} — {len(df_dist[df_dist['laminadora_alocada'] == lam])} produtos"):
                        df_lam = df_dist[df_dist["laminadora_alocada"] == lam]
                        st.dataframe(
                            df_lam[["CODIGO", "DESCRICAO", "criticidade_total", "DIFERENCA_NUM", "status_insumo"]],
                            column_config={
                                "DIFERENCA_NUM": st.column_config.NumberColumn("Metros", format="%.0f"),
                                "criticidade_total": st.column_config.NumberColumn("Criticidade", format="%.2f"),
                            },
                            hide_index=True,
                            use_container_width=True
                        )

                if st.button("✅ Confirmar Distribuição e Gerar Ordens", type="primary"):
                    with st.spinner("Gerando ordens de produção..."):
                        df_ordens = gerar_ordens_producao(df_dist)
                        st.session_state["ordens"] = df_ordens
                    st.success("Ordens geradas!")

    # TAB 4: Ordens de Produção
    with tabs[3]:
        st.subheader("4️⃣ Ordens de Produção")

        if st.session_state["ordens"].empty:
            st.info("Gere as ordens na aba anterior.")
        else:
            df_ordens = st.session_state["ordens"]

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total de Ordens", df_ordens["Código"].nunique())
            col2.metric("Laminadoras", df_ordens["Laminadora"].nunique())
            col3.metric("Horas Programadas", f"{df_ordens['Tempo Total (h)'].sum():.1f}h")
            col4.metric("Sequência Vinculante", "✅ Gerada")

            # Filtros
            col1, col2 = st.columns(2)
            lam_filtro = col1.selectbox("Laminadora", ["Todas"] + sorted(df_ordens["Laminadora"].unique()))
            turno_filtro = col2.selectbox("Turno", ["Todos"] + sorted(df_ordens["Turno"].unique()))

            df_filt = df_ordens.copy()
            if lam_filtro != "Todas":
                df_filt = df_filt[df_filt["Laminadora"] == lam_filtro]
            if turno_filtro != "Todos":
                df_filt = df_filt[df_filt["Turno"] == turno_filtro]

            st.dataframe(
                df_filt,
                column_config={
                    "Metros": st.column_config.NumberColumn(format="%.0f m"),
                    "Criticidade": st.column_config.NumberColumn(format="%.2f"),
                    "Tempo Produção (h)": st.column_config.NumberColumn(format="%.2f h"),
                    "Tempo Total (h)": st.column_config.NumberColumn(format="%.2f h"),
                    "Setup (min)": st.column_config.NumberColumn(format="%.0f min"),
                },
                hide_index=True,
                width="stretch"
            )

            # Exportar
            csv_data = df_filt.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Exportar Ordens (CSV)",
                data=csv_data,
                file_name="ordens_producao.csv",
                mime="text/csv"
            )

            # Alterações manuais
            if not df_filt.empty:
                st.subheader("✏️ Alterações Manuais (Requer Justificativa)")
                st.warning("⚠️ Alterações na sequência ou máquina exigem justificativa obrigatória.")

                alteracoes_log = st.session_state.get("alteracoes_log", [])

                for idx, row in df_filt.iterrows():
                    with st.expander(f"Alterar {row['Código']} — {row['Laminadora']}"):
                        col1, col2, col3 = st.columns(3)
                        nova_lam = col1.selectbox(
                            "Nova Laminadora",
                            ["(manter)"] + [f"L{i}" for i in range(1, 5)],
                            key=f"lam_{idx}"
                        )
                        nova_seq = col2.number_input(
                            "Nova Posição na Sequência",
                            min_value=1,
                            max_value=len(df_filt),
                            value=row.name + 1,
                            key=f"seq_{idx}"
                        )
                        justificativa = col3.text_input(
                            "Justificativa",
                            key=f"just_{idx}"
                        )

                        if st.button("Confirmar Alteração", key=f"alt_{idx}"):
                            if not justificativa.strip():
                                st.error("⛔ Justificativa obrigatória!")
                            else:
                                # Registrar alteração
                                alteracao = {
                                    "Código": row["Código"],
                                    "Laminadora Original": row["Laminadora"],
                                    "Nova Laminadora": nova_lam if nova_lam != "(manter)" else row["Laminadora"],
                                    "Sequência Original": row.name + 1,
                                    "Nova Sequência": nova_seq,
                                    "Justificativa": justificativa,
                                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                alteracoes_log.append(alteracao)
                                st.session_state["alteracoes_log"] = alteracoes_log

                                # Aplicar alteração
                                if nova_lam != "(manter)":
                                    st.session_state["ordens"].at[idx, "Laminadora"] = nova_lam
                                # Reordenar se necessário (simplificado)
                                st.success("✅ Alteração registrada!")

                # Log de alterações
                if alteracoes_log:
                    st.subheader("📋 Log de Alterações")
                    st.dataframe(pd.DataFrame(alteracoes_log), hide_index=True, use_container_width=True)

    # TAB 5: Análise de Gargalos
    with tabs[4]:
        st.subheader("⚙️ Análise de Gargalos e Capacidade")

        if st.session_state["ordens"].empty:
            st.info("Gere as ordens primeiro.")
        else:
            df_ordens = st.session_state["ordens"]

            # Capacidade por laminadora
            cap_por_lam = df_ordens.groupby("Laminadora").agg(
                Ordens=("Código", "nunique"),
                Horas=("Tempo Total (h)", "sum"),
                Metros=("Metros", "sum")
            ).round(2)

            cap_por_lam["Ocupação (%)"] = (cap_por_lam["Horas"] / 17 * 100).round(1)  # 17h diárias
            cap_por_lam["Status"] = cap_por_lam["Ocupação (%)"].apply(
                lambda x: "🔴 Crítico" if x > 90 else "🟡 Atenção" if x > 70 else "🟢 OK"
            )

            st.subheader("Ocupação por Laminadora")
            st.dataframe(
                cap_por_lam[["Ordens", "Horas", "Metros", "Ocupação (%)", "Status"]],
                column_config={
                    "Horas": st.column_config.NumberColumn(format="%.1f h"),
                    "Metros": st.column_config.NumberColumn(format="%.0f m"),
                    "Ocupação (%)": st.column_config.NumberColumn(format="%.1f %%"),
                },
                hide_index=True,
                use_container_width=True
            )

            # Gráfico de ocupação
            import plotly.express as px
            fig = px.bar(
                cap_por_lam.reset_index(),
                x="Laminadora",
                y="Ocupação (%)",
                color="Status",
                color_discrete_map={"🔴 Crítico": "#e74c3c", "🟡 Atenção": "#f39c12", "🟢 OK": "#2ecc71"},
                title="Ocupação por Laminadora (%)",
                template="plotly_dark"
            )
            fig.add_hline(y=90, line_dash="dash", line_color="#e74c3c")
            fig.add_hline(y=70, line_dash="dash", line_color="#f39c12")
            st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    render_mrp()
