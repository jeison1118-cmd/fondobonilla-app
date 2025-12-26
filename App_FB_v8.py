
# -*- coding: utf-8 -*-
# Fondo Bonilla (v8) — Excel + UI con Inversionista (Teresa Pérez)
# Requisitos: pip install streamlit pandas openpyxl python-dateutil

import streamlit as st
import pandas as pd
import numpy as np
import uuid
import os
import shutil
import random
import string
from io import BytesIO
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

APP_NAME = "Fondo Bonilla (v8)"
EXCEL_PATH = "fondo.xlsx"
BACKUP_DIR = "backups"

# Parámetros de mora
GRACIA_DIAS = 5
MORA_TASA_MENSUAL = 0.02  # 2% mensual (no capitaliza)

PRIMARY_COLOR = "#0E7A57"
ACCENT_COLOR = "#FFD166"
BG_GRADIENT = "linear-gradient(135deg, #0E7A57 0%, #1B998B 50%, #066F4C 100%)"



# --- Google Sheets auth + helpers (Service Account via st.secrets or local file) ---
import requests
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe

# ID de tu Google Sheet (proporcionado por el usuario)
SHEET_ID = "1RbVD9oboyVfSPiwHS5B4xD9h9i6cxyXLY9uXhdQx62s"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def _gs_credentials():
    import streamlit as st
    import os
    if "gcp_service_account" in st.secrets:
        sa_info = st.secrets["gcp_service_account"]
        return Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    # Fallback local: service_account.json junto al script
    json_path = Path(__file__).with_name("service_account.json")
    if not json_path.exists():
        st.error("Faltan credenciales. Sube el JSON a Secrets (Cloud) o coloca service_account.json (local).")
        st.stop()
    return Credentials.from_service_account_file(str(json_path), scopes=SCOPES)

@st.cache_resource
def _gs_client():
    creds = _gs_credentials()
    return gspread.authorize(creds)

@st.cache_resource
def _get_spreadsheet():
    gc = _gs_client()
    return gc.open_by_key(SHEET_ID)

def _get_ws(worksheet_name: str):
    sh = _get_spreadsheet()
    # Crea la pestaña si no existe
    try:
        return sh.worksheet(worksheet_name)
    except Exception:
        return sh.add_worksheet(title=worksheet_name, rows=1000, cols=20)

def read_df(sheet_name: str):
    ws = _get_ws(sheet_name)
    df = get_as_dataframe(
        ws,
        evaluate_formulas=True,
        header=0,
        na_value="",
        drop_empty_rows=True,
        drop_empty_columns=True,
    )
    import pandas as pd
    return df if df is not None else pd.DataFrame()

def write_df(sheet_name: str, df):
    ws = _get_ws(sheet_name)
    ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

def export_sheet_to_xlsx(local_path: str = "/tmp/fondo.xlsx"):
    # Exporta el Google Sheet completo a .xlsx para backup/descarga
    creds = _gs_credentials()
    creds = creds.with_scopes(["https://www.googleapis.com/auth/drive.readonly"])  # narrow scope
    creds.refresh(Request())
    headers = {"Authorization": f"Bearer {creds.token}"}
    url = f"https://www.googleapis.com/drive/v3/files/{SHEET_ID}/export"
    params = {"mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    Path(local_path).write_bytes(r.content)
    return local_path

def inject_css():
    st.markdown(
        f"""
        <style>
        /* Puedes añadir estilos aquí si lo necesitas */
        </style>
        """,
        unsafe_allow_html=True,
    )


# ----------------- Helpers -----------------
def format_cop(n: float) -> str:
    try:
        s = f"{int(round(n)):,}"
    except Exception:
        s = "0"
    return "$" + s.replace(",", ".")


def parse_percent(text: str) -> float:
    s = str(text or "").strip().replace(" ", "").replace(",", ".")
    if s.endswith("%"):
        s = s[:-1]
    if s == "":
        return 0.0
    try:
        v = float(s)
        return v / 100.0
    except:
        return 0.0


def percent_str(decimal_rate: float) -> str:
    try:
        return f"{round(decimal_rate * 100)}%"
    except:
        return "0%"


def safe_str(v):
    if v is None:
        return ""
    s = str(v)
    return "" if s.lower() == "nan" else s.strip()


def safe_int(v, default=0):
    """Convierte a int con fallback; acepta None, '', str numérica y float."""
    try:
        if v is None:
            return int(default)
        if isinstance(v, str):
            s = v.strip()
            if s == "":
                return int(default)
            return int(round(float(s)))
        return int(round(float(v)))
    except Exception:
        return int(default)


def to_date(x):
    try:
        return pd.to_datetime(x).date()
    except:
        return None


def normalize_datetime_cols(df: pd.DataFrame, cols: list, to_string: bool = True) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(safe_str) if to_string else pd.to_datetime(df[c], errors="coerce")
    return df


def generate_display_id(existing: set, length: int = 5) -> str:
    alphabet = string.ascii_uppercase + string.digits
    while True:
        code = "".join(random.choices(alphabet, k=length))
        if code not in existing:
            return code


def nombre_cliente_por_id(clientes_df: pd.DataFrame, cliente_id: str) -> str:
    try:
        row = clientes_df[clientes_df["cliente_id"] == cliente_id].iloc[0]
        return safe_str(row.get("nombre")) or "Cliente"
    except Exception:
        return "Cliente"


# ----------------- Excel -----------------
def init_empty_frames():
    clientes = pd.DataFrame(
        columns=["cliente_id", "nombre", "identificacion", "telefono", "email", "creado_en", "actualizado_en"]
    )
    prestamos = pd.DataFrame(
        columns=[
            "prestamo_id",
            "display_id",
            "cliente_id",
            "monto",
            "tasa_mensual",
            "fecha_inicio",
            "plan_inicio",
            "plazo_meses",
            "frecuencia",
            "saldo_capital",
            "estado",
            "ultimo_calculo",
            "cuota_fija",
            "creado_en",
            "actualizado_en",
        ]
    )
    pagos = pd.DataFrame(
        columns=[
            "pago_id",
            "prestamo_id",
            "fecha_pago",
            "monto_pago",
            "interes_aplicado",
            "capital_aplicado",
            "dias",
            "observaciones",
            "creado_en",
            "metodo",
            "saldo_antes",
            "saldo_despues",
            "dias_mora",
            "mora_aplicada",
            "tipo_pago",
            "adelanto_extra",
        ]
    )
    parametros = pd.DataFrame(columns=["clave", "valor"])
    integrantes = pd.DataFrame(
        columns=["integrante_id", "nombre", "identificacion", "cupos", "creado_en", "actualizado_en"]
    )
    aportes_tarifas = pd.DataFrame(columns=["anio", "valor_por_cupo"])
    aportes_pagos = pd.DataFrame(
        columns=["aporte_id", "integrante_id", "periodo", "fecha_pago", "cupos_pagados", "monto_pagado", "observaciones", "creado_en"]
    )
    return clientes, prestamos, pagos, parametros, integrantes, aportes_tarifas, aportes_pagos



def ensure_base_sheets():
    # Crea pestañas si no existen y inicializa con estructuras vacías cuando estén vacías
    (clientes, prestamos, pagos, parametros, integrantes, aportes_tarifas, aportes_pagos) = init_empty_frames()
    try:
        # Inicializar parámetros mínimos si la pestaña está vacía
        df_param = read_df("parametros")
        if df_param.empty:
            df_param = pd.DataFrame([{"clave": "capital_inicial", "valor": 0}])
            write_df("parametros", df_param)
        # Asegurar pestañas de núcleo
        for name, df in {
            "clientes": clientes,
            "prestamos": prestamos,
            "pagos": pagos,
            "integrantes": integrantes,
            "aportes_tarifas": aportes_tarifas,
            "aportes_pagos": aportes_pagos,
        }.items():
            cur = read_df(name)
            if cur.empty and df is not None:
                write_df(name, df)
        # Inversionista base
        inv_df = read_df("inversionista")
        if inv_df.empty:
            inv_df = pd.DataFrame([
                {
                    "nombre": "Teresa Pérez",
                    "capital_inicial": 1000000,
                    "ganancia_anual": 70000,
                    "ganancia_pendiente": 70000,
                    "estado": "activo",
                    "creado_en": datetime.now(),
                    "actualizado_en": datetime.now(),
                }
            ])
            write_df("inversionista", inv_df)
        inv_movs_df = read_df("inversionista_movs")
        if inv_movs_df.empty:
            inv_movs_df = pd.DataFrame(columns=["mov_id", "fecha", "tipo", "monto", "observaciones", "creado_en"])
            write_df("inversionista_movs", inv_movs_df)
    except Exception as e:
        import streamlit as st
        st.warning(f"No se pudo asegurar pestañas base: {e}")
def safe_read(sheet):
    try:
        return read_df(sheet)
    except:
        return pd.DataFrame()


def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df


def load_data():
    ensure_base_sheets()
    clientes = safe_read("clientes")
    prestamos = safe_read("prestamos")
    pagos = safe_read("pagos")
    parametros = safe_read("parametros")
    integrantes = safe_read("integrantes")
    aportes_tarifas = safe_read("aportes_tarifas")
    aportes_pagos = safe_read("aportes_pagos")

    clientes = ensure_cols(clientes, ["cliente_id", "nombre", "identificacion", "telefono", "email", "creado_en", "actualizado_en"])
    prestamos = ensure_cols(
        prestamos,
        [
            "prestamo_id",
            "display_id",
            "cliente_id",
            "monto",
            "tasa_mensual",
            "fecha_inicio",
            "plan_inicio",
            "plazo_meses",
            "frecuencia",
            "saldo_capital",
            "estado",
            "ultimo_calculo",
            "cuota_fija",
            "creado_en",
            "actualizado_en",
        ],
    )
    pagos = ensure_cols(
        pagos,
        [
            "pago_id",
            "prestamo_id",
            "fecha_pago",
            "monto_pago",
            "interes_aplicado",
            "capital_aplicado",
            "dias",
            "observaciones",
            "creado_en",
            "metodo",
            "saldo_antes",
            "saldo_despues",
            "dias_mora",
            "mora_aplicada",
            "tipo_pago",
            "adelanto_extra",
        ],
    )
    parametros = ensure_cols(parametros, ["clave", "valor"])
    integrantes = ensure_cols(integrantes, ["integrante_id", "nombre", "identificacion", "cupos", "creado_en", "actualizado_en"])
    aportes_tarifas = ensure_cols(aportes_tarifas, ["anio", "valor_por_cupo"])
    aportes_pagos = ensure_cols(aportes_pagos, ["aporte_id", "integrante_id", "periodo", "fecha_pago", "cupos_pagados", "monto_pagado", "observaciones", "creado_en"])

    # Migración ligera: si falta plan_inicio, usar fecha_inicio
    if "plan_inicio" not in prestamos.columns:
        prestamos["plan_inicio"] = prestamos.get("fecha_inicio", pd.Series(dtype=object))
        prestamos["plan_inicio"] = prestamos.apply(
            lambda r: r["plan_inicio"] if not pd.isna(r["plan_inicio"]) else r.get("fecha_inicio"), axis=1
        )

    # Migración: display_id y cuota_fija
    existing_codes = set(str(x) for x in prestamos["display_id"].dropna().astype(str))
    changed = False

    for i, row in prestamos.iterrows():
        if pd.isna(row.get("display_id")) or not str(row.get("display_id")).strip():
            code = generate_display_id(existing_codes, 5)
            prestamos.at[i, "display_id"] = code
            existing_codes.add(code)
            changed = True

    for i, row in prestamos.iterrows():
        try:
            if (
                pd.isna(row.get("cuota_fija"))
                and not pd.isna(row.get("monto"))
                and not pd.isna(row.get("tasa_mensual"))
                and not pd.isna(row.get("plazo_meses"))
            ):
                cuota = calcular_cuota_fija(float(row["monto"]), float(row["tasa_mensual"]), int(row["plazo_meses"]))
                prestamos.at[i, "cuota_fija"] = cuota
                changed = True
        except Exception:
            pass

    if changed:
        save_data(clientes, prestamos, pagos, parametros)
        save_aportes_data(integrantes, aportes_tarifas, aportes_pagos)

    return clientes, prestamos, pagos, parametros, integrantes, aportes_tarifas, aportes_pagos


# --- Inversionista (helpers) ---
def _safe_read_sheet(sheet):
    try:
        return read_df(sheet)
    except Exception:
        return pd.DataFrame()


def ensure_inv_cols(inv_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["nombre", "capital_inicial", "ganancia_anual", "ganancia_pendiente", "estado", "creado_en", "actualizado_en"]
    for c in cols:
        if c not in inv_df.columns:
            inv_df[c] = pd.Series(dtype=object)
    return inv_df


def ensure_inv_mov_cols(mov_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["mov_id", "fecha", "tipo", "monto", "observaciones", "creado_en"]
    for c in cols:
        if c not in mov_df.columns:
            mov_df[c] = pd.Series(dtype=object)
    return mov_df


def load_inversionista():
    inv = _safe_read_sheet("inversionista")
    inv = ensure_inv_cols(inv)
    movs = _safe_read_sheet("inversionista_movs")
    movs = ensure_inv_mov_cols(movs)
    # Si no hay fila, inicializar Teresa por defecto
    if inv.empty:
        inv = pd.DataFrame(
            [
                {
                    "nombre": "Teresa Pérez",
                    "capital_inicial": 1000000,
                    "ganancia_anual": 70000,
                    "ganancia_pendiente": 70000,
                    "estado": "activo",
                    "creado_en": datetime.now(),
                    "actualizado_en": datetime.now(),
                }
            ]
        )
    return inv, movs


def save_inversionista_data(inv_df, movs_df):
    write_df("inversionista", inv_df)
    write_df("inversionista_movs", movs_df)


def save_data(clientes, prestamos, pagos, parametros, path=EXCEL_PATH):
    # Guardar sin borrar otras hojas: usar modo 'a' si el archivo existe
    mode = "a" if os.path.exists(path) else "w"
    with pd.ExcelWriter(path, engine="openpyxl", mode=mode, if_sheet_exists="replace") as writer:
        clientes.to_excel(writer, sheet_name="clientes", index=False)
        prestamos.to_excel(writer, sheet_name="prestamos", index=False)
        pagos.to_excel(writer, sheet_name="pagos", index=False)
        parametros.to_excel(writer, sheet_name="parametros", index=False)


def save_aportes_data(integrantes, aportes_tarifas, aportes_pagos):
    write_df("integrantes", integrantes)
    write_df("aportes_tarifas", aportes_tarifas)
    write_df("aportes_pagos", aportes_pagos)


def create_backup():
    import os
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(BACKUP_DIR, f"fondo_{ts}.xlsx")
    try:
        path = export_sheet_to_xlsx(dst)
        return path
    except Exception as e:
        return None


# ----------------- Lógica -----------------
def calcular_cuota_fija(P, i_mensual, n):
    P = safe_int(P, default=0)
    n = max(safe_int(n, default=1), 1)
    i_mensual = float(i_mensual or 0.0)
    if i_mensual <= 0:
        return int(round(P / n)) if n > 0 else P
    cuota = P * (i_mensual / (1 - (1 + i_mensual) ** (-n)))
    return int(round(cuota))


def resolver_plazo_por_cuota(P: int, i_mensual: float, C_deseada: int) -> int:
    P = safe_int(P, default=0)
    C = safe_int(C_deseada, default=1)
    i = float(i_mensual or 0.0)
    if C <= 0:
        raise ValueError("La cuota deseada debe ser mayor que 0.")
    if P <= 0:
        return 1
    if i <= 0:
        from math import ceil
        return max(1, int(ceil(P / C)))
    if C <= int(P * i):
        raise ValueError("La cuota deseada es insuficiente (no cubre el interés del mes).")
    from math import log
    try:
        n_real = -log(1.0 - (P * i) / C) / log(1.0 + i)
        n_int = int(round(n_real))
        return max(1, n_int)
    except Exception:
        n = 1
        while n < 600:
            c = calcular_cuota_fija(P, i, n)
            if c <= C:
                return n
            n += 1
        return 600


def simulador_cuotas_fijas(P: float, i_mensual: float, n: int, fecha_inicio: date):
    """Genera tabla de amortización francesa con cuota fija redondeada a entero COP."""
    P = safe_int(P, default=0)
    n = max(safe_int(n, default=1), 1)
    fecha_inicio = to_date(fecha_inicio) or date.today()
    cuota = calcular_cuota_fija(P, i_mensual, n)
    saldo = P
    filas = []
    for k in range(1, n + 1):
        fecha_k = fecha_inicio + relativedelta(months=+k)
        interes_k = int(round(saldo * float(i_mensual or 0.0)))
        capital_k = cuota - interes_k
        if capital_k > saldo:
            capital_k = saldo
        cuota_ajustada = interes_k + capital_k
        saldo_nuevo = saldo - capital_k
        if k == n:
            capital_k += saldo_nuevo
            cuota_ajustada = interes_k + capital_k
            saldo_nuevo = 0
        filas.append(
            {
                "# CUOTA": k,
                "MES": fecha_k,
                "SALDO INICIAL": saldo,
                "CUOTA": cuota_ajustada,
                "INTERES": interes_k,
                "CAPITAL": capital_k,
                "SALDO DESPUES DEL PAGO": saldo_nuevo,
            }
        )
        saldo = saldo_nuevo
    tabla = pd.DataFrame(filas)
    total_interes = int(round(float(tabla["INTERES"].sum())))
    total_pagado = int(round(float(tabla["CUOTA"].sum())))
    return cuota, total_interes, total_pagado, tabla


# ---- Pagos contados por plan (epoch) ----
def contar_cuotas_normales_desde_epoch(pagos_df, prestamo_row):
    if pagos_df.empty:
        return 0
    pid = prestamo_row["prestamo_id"]
    t0 = to_date(prestamo_row.get("plan_inicio")) or to_date(prestamo_row.get("fecha_inicio")) or date.today()
    df = pagos_df[
        (pagos_df["prestamo_id"] == pid)
        & (pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x) > t0))
    ]
    if df.empty:
        return 0
    df = df[df["tipo_pago"].isin(["cuota_normal", "cuota_normal_con_adelanto"])]
    return df.shape[0]


def fecha_esperada(prestamo_row, pagos_df):
    k = contar_cuotas_normales_desde_epoch(pagos_df, prestamo_row) + 1
    f0 = to_date(prestamo_row.get("plan_inicio")) or to_date(prestamo_row.get("fecha_inicio")) or date.today()
    return f0 + relativedelta(months=+k)


def calcular_mora(saldo, fecha_pago, fecha_esp):
    fe_gracia = fecha_esp + timedelta(days=GRACIA_DIAS)
    if fecha_pago <= fe_gracia:
        return 0, 0
    dias_mora = (fecha_pago - fe_gracia).days
    mora = int(round(saldo * (MORA_TASA_MENSUAL / 30.0) * dias_mora))
    return dias_mora, mora


def cuotas_restantes(prestamo_row, pagos_df):
    saldo = safe_int(prestamo_row.get("saldo_capital"), default=0)
    if saldo <= 0:
        return 0
    plazo = safe_int(prestamo_row.get("plazo_meses"), default=0)
    pagadas_desde_epoch = contar_cuotas_normales_desde_epoch(pagos_df, prestamo_row)
    return max(plazo - pagadas_desde_epoch, 0)


# ---- Crear y pagar ----
def crear_cliente_si_no_existe(clientes_df, nombre, identificacion, telefono="", email=""):
    if identificacion and not clientes_df.empty:
        match = clientes_df[clientes_df["identificacion"] == identificacion]
        if not match.empty:
            return clientes_df, match.iloc[0]["cliente_id"]
    cid = str(uuid.uuid4())
    now = datetime.now()
    nuevo = {
        "cliente_id": cid,
        "nombre": nombre,
        "identificacion": identificacion,
        "telefono": telefono,
        "email": email,
        "creado_en": now,
        "actualizado_en": now,
    }
    clientes_df = pd.concat([clientes_df, pd.DataFrame([nuevo])], ignore_index=True)
    return clientes_df, cid


def crear_prestamo(prestamos_df, cliente_id, monto, tasa_mensual_decimal, fecha_inicio, plazo_meses):
    pid = str(uuid.uuid4())
    now = datetime.now()
    fecha_inicio = to_date(fecha_inicio) or date.today()
    cuota_fija = calcular_cuota_fija(float(monto), float(tasa_mensual_decimal), int(plazo_meses))
    existing_codes = set(str(x) for x in prestamos_df.get("display_id", pd.Series(dtype=str)).dropna().astype(str))
    display_id = generate_display_id(existing_codes, 5)
    nuevo = {
        "prestamo_id": pid,
        "display_id": display_id,
        "cliente_id": cliente_id,
        "monto": int(round(float(monto))),
        "tasa_mensual": float(tasa_mensual_decimal),
        "fecha_inicio": fecha_inicio,
        "plan_inicio": fecha_inicio,
        "plazo_meses": int(plazo_meses),
        "frecuencia": "mensual",
        "saldo_capital": int(round(float(monto))),
        "estado": "activo",
        "ultimo_calculo": fecha_inicio,
        "cuota_fija": cuota_fija,
        "creado_en": now,
        "actualizado_en": now,
    }
    prestamos_df = pd.concat([prestamos_df, pd.DataFrame([nuevo])], ignore_index=True)
    return prestamos_df, pid


def registrar_pago_cuota(prestamos_df, pagos_df, prestamo_id, fecha_pago, adelanto_extra=0, observaciones=""):
    pr_row = prestamos_df[prestamos_df["prestamo_id"] == prestamo_id]
    if pr_row.empty:
        raise ValueError("Préstamo no encontrado.")
    pr = pr_row.iloc[0]
    fecha_pago = to_date(fecha_pago) or date.today()
    saldo_antes = safe_int(pr.get("saldo_capital"), default=0)
    tasa_mensual = float(pr["tasa_mensual"]) if not pd.isna(pr["tasa_mensual"]) else 0.0
    cuota_fija = safe_int(pr.get("cuota_fija"), default=0)
    f_esp = fecha_esperada(pr, pagos_df)
    dias_mora, mora_aplicada = calcular_mora(saldo_antes, fecha_pago, f_esp)
    interes_mes = int(round(saldo_antes * tasa_mensual))
    capital_por_cuota = max(cuota_fija - interes_mes, 0)
    if capital_por_cuota > saldo_antes:
        capital_por_cuota = saldo_antes
    adelanto_extra = safe_int(adelanto_extra, default=0)
    capital_total = min(capital_por_cuota + adelanto_extra, saldo_antes)
    monto_total = cuota_fija + mora_aplicada + adelanto_extra
    saldo_despues = saldo_antes - capital_total

    prestamos_df.loc[prestamos_df["prestamo_id"] == prestamo_id, ["saldo_capital", "ultimo_calculo", "actualizado_en"]] = [
        saldo_despues,
        fecha_pago,
        datetime.now(),
    ]
    if saldo_despues <= 0:
        prestamos_df.loc[prestamos_df["prestamo_id"] == prestamo_id, "estado"] = "cerrado"

    pago_row = {
        "pago_id": str(uuid.uuid4()),
        "prestamo_id": prestamo_id,
        "fecha_pago": fecha_pago,  # clave correcta
        "monto_pago": monto_total,
        "interes_aplicado": interes_mes,
        "capital_aplicado": capital_total,
        "dias": 0,
        "observaciones": observaciones,
        "creado_en": datetime.now(),
        "metodo": "cuota_mensual",
        "saldo_antes": saldo_antes,
        "saldo_despues": saldo_despues,
        "dias_mora": dias_mora,
        "mora_aplicada": mora_aplicada,
        "tipo_pago": "cuota_normal_con_adelanto" if adelanto_extra > 0 else "cuota_normal",
        "adelanto_extra": adelanto_extra,
    }
    pagos_df = pd.concat([pagos_df, pd.DataFrame([pago_row])], ignore_index=True)
    return prestamos_df, pagos_df, pago_row


# ----------------- UI -----------------
st.set_page_config(page_title=f"{APP_NAME} — UI", page_icon="💰", layout="wide")
inject_css()

st.markdown(
    f"""
### 💰 {APP_NAME}

Contabilidad en Excel · Cuota fija · Mora por días · Adelanto extra · Re‑amortización · Aportes · Inversionista
""",
    unsafe_allow_html=True,
)

ensure_base_sheets()
(clientes, prestamos, pagos, parametros, integrantes, aportes_tarifas, aportes_pagos) = load_data()

# Sidebar
st.sidebar.markdown(f"### {APP_NAME}")
st.sidebar.caption("Persistencia en Google Sheets")
st.sidebar.divider()


with st.sidebar.expander("🔍 Diagnóstico Google Sheets", expanded=True):
    try:
        # 1) Credenciales y cliente
        creds = _gs_credentials()
        st.write("✅ Credenciales OK")
        gc = _gs_client()
        st.write("✅ Cliente gspread OK")

        # 2) Abre el Sheet y lista pestañas
        sh = _get_spreadsheet()
        st.write("✅ Abre Sheet ID:", SHEET_ID)
        ws_list = [ws.title for ws in sh.worksheets()]
        st.write("📄 Pestañas:", ws_list)

        # 3) Tamaños de cada pestaña clave
        for name in ["clientes", "prestamos", "pagos", "parametros",
                     "integrantes", "aportes_tarifas", "aportes_pagos",
                     "inversionista", "inversionista_movs"]:
            try:
                df_test = read_df(name)
                st.write(f"🧪 {name}: {df_test.shape[0]} filas x {df_test.shape[1]} cols")
            except Exception as e:
                st.write(f"⚠️ {name}: error leyendo → {e}")

    except Exception as e:
        st.error(f"❌ Falla de conexión: {e}")

if st.sidebar.button("💾 Guardar todo en Google Sheets", key="side_save"):
    save_data(clientes, prestamos, pagos, parametros)
    save_aportes_data(integrantes, aportes_tarifas, aportes_pagos)
    inv_df, inv_movs_df = load_inversionista()
    save_inversionista_data(inv_df, inv_movs_df)
    st.sidebar.success("Datos guardados en \"fondo.xlsx\".")

if st.sidebar.button("🛟 Crear backup", key="side_backup"):
    dst = create_backup()
    st.sidebar.success(f"Backup: {dst}") if dst else st.sidebar.warning("No se pudo crear el backup.")

uploaded_calc = st.sidebar.file_uploader("📥 Importar calculadora (Hoja 1)", type=["xlsx"], key="side_upload_calc")
if uploaded_calc is not None and st.sidebar.button("➡️ Importar primera hoja", key="side_import_btn"):
    try:
        # Importador simple
        df = pd.read_excel(uploaded_calc, sheet_name=0, engine="openpyxl", header=None)
        nombre_cliente = None
        for r in range(min(10, df.shape[0])):
            for c in range(df.shape[1]):
                val = df.iloc[r, c]
                if isinstance(val, str) and val.strip().upper().startswith("PRESTAMO "):
                    nombre_cliente = val.strip()[9:].strip()
                    break
            if nombre_cliente:
                break
        claves = {"prestamo": None, "fecha": None, "interes mes": None, "tiempo meses": None}
        for r in range(min(40, df.shape[0])):
            row = [str(x).strip().lower() if isinstance(x, str) else x for x in df.iloc[r, :].tolist()]
            for c, v in enumerate(row):
                if v in ["prestamo", "fecha", "interes mes", "tiempo meses"]:
                    claves[v] = df.iloc[r, c + 1] if c + 1 < df.shape[1] else None

        P = safe_int(claves["prestamo"], default=0)
        F0 = to_date(claves["fecha"]) or date.today()
        i_m = claves["interes mes"]
        n = safe_int(claves["tiempo meses"], default=1)
        if isinstance(i_m, str):
            tasa_mensual = parse_percent(i_m)
        else:
            try:
                f = float(i_m or 0)
                tasa_mensual = f / 100.0 if f > 1 else f
            except:
                tasa_mensual = 0.0

        clientes, cid = crear_cliente_si_no_existe(
            clientes, nombre_cliente or "Cliente Fondo Bonilla", identificacion=f"{(nombre_cliente or 'Cliente')[:20]}-AUTO"
        )
        prestamos, pid = crear_prestamo(prestamos, cid, P, tasa_mensual, F0, n)
        save_data(clientes, prestamos, pagos, parametros)
        st.sidebar.success(
            f"Importado para '{nombre_cliente or 'Cliente Fondo Bonilla'}' (ID: {prestamos[prestamos['prestamo_id']==pid]['display_id'].iloc[0]})."
        )
        st.session_state["selected_prestamo_id"] = pid
        st.session_state["selected_prestamo_display"] = prestamos[prestamos["prestamo_id"] == pid]["display_id"].iloc[0]
    except Exception as e:
        st.sidebar.error(f"Error importando: {e}")

# Tabs
TABS = ["📊 Panel", "➕ Nuevo Préstamo", "💳 Registrar Pago", "📑 Reportes", "🧮 Simulador", "🗃️ Datos / Parámetros", "♻️ Re‑amortización", "👥 Aportes", "👤 Inversionista"]
tabs = st.tabs(TABS)

# --------- Panel ---------
with tabs[0]:
    st.subheader("Panel general")

    def resumen_fondo_ext(prestamos_df, pagos_df, parametros_df, integ_df=None, aportes_pagos_df=None, inv_df=None, inv_movs_df=None):
        # Base
        capital_inicial = 0.0
        row = parametros_df[parametros_df["clave"] == "capital_inicial"]
        if not row.empty:
            try:
                capital_inicial = float(row.iloc[0]["valor"])
            except Exception:
                capital_inicial = 0.0

        total_desembolsado = float(prestamos_df["monto"].sum()) if not prestamos_df.empty else 0.0
        saldo_capital_total = float(prestamos_df["saldo_capital"].sum()) if not prestamos_df.empty else 0.0
        intereses_cobrados = float(pagos_df["interes_aplicado"].sum()) if ("interes_aplicado" in pagos_df.columns and not pagos_df.empty) else 0.0
        capital_recuperado = float(pagos_df["capital_aplicado"].sum()) if ("capital_aplicado" in pagos_df.columns and not pagos_df.empty) else 0.0
        aportes_cobrados = float(aportes_pagos_df["monto_pagado"].sum()) if (aportes_pagos_df is not None and not aportes_pagos_df.empty) else 0.0

        caja_actual = capital_inicial - total_desembolsado + (intereses_cobrados + capital_recuperado + aportes_cobrados)
        total_prestado = saldo_capital_total
        total_fondo = caja_actual + total_prestado

        # Pasivo inversionista: capital + ganancia pendiente
        pasivo_inv = 0.0
        if inv_df is not None and not inv_df.empty:
            try:
                cap = float(inv_df.iloc[0].get("capital_inicial", 0) or 0)
            except Exception:
                cap = 0.0
            try:
                gan_p = float(inv_df.iloc[0].get("ganancia_pendiente", inv_df.iloc[0].get("ganancia_anual", 0)) or 0)
            except Exception:
                gan_p = 0.0
            pasivo_inv = cap + gan_p

        total_sin_inversion = total_fondo - pasivo_inv
        total_cupos = int(integ_df["cupos"].sum()) if (integ_df is not None and not integ_df.empty) else 0
        total_por_cupo = (total_sin_inversion / total_cupos) if total_cupos > 0 else 0.0

        hoy = date.today()
        if not pagos_df.empty and "fecha_pago" in pagos_df.columns:
            mask_year = pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x).year == hoy.year)
            intereses_anio = float(pagos_df.loc[mask_year, "interes_aplicado"].sum()) if "interes_aplicado" in pagos_df.columns else 0.0
        else:
            intereses_anio = 0.0

        return {
            "caja_actual": caja_actual,
            "total_prestado": total_prestado,
            "intereses_anio": intereses_anio,
            "total_fondo": total_fondo,
            "total_sin_inversion": total_sin_inversion,
            "pasivo_inversionista": pasivo_inv,
            "total_por_cupo": total_por_cupo,
            "total_cupos": total_cupos,
        }

    inv_df, inv_movs_df = load_inversionista()
    R = resumen_fondo_ext(prestamos, pagos, parametros, integrantes, aportes_pagos, inv_df, inv_movs_df)

    # --- Métricas del Panel (v8) ---
    c1, c2, c3 = st.columns(3, gap="large")
    c1.metric("Caja actual", format_cop(R["caja_actual"]))
    c2.metric("Total prestado", format_cop(R["total_prestado"]))
    c3.metric(f"Intereses {date.today().year}", format_cop(R["intereses_anio"]))

    c4, c5 = st.columns(2, gap="large")
    c4.metric("Total del fondo", format_cop(R["total_fondo"]))
    c5.metric("Total sin inversión", format_cop(R["total_sin_inversion"]))

    c6 = st.columns(1)[0]
    c6.metric("Total por cupo (sin inversión)", format_cop(R["total_por_cupo"]))

    st.markdown("### Préstamos activos")
    activos = prestamos[prestamos["estado"].fillna("activo") == "activo"].copy()
    if activos.empty:
        st.info("No hay préstamos activos.")
    else:
        activos["cliente"] = activos["cliente_id"].apply(lambda cid: nombre_cliente_por_id(clientes, cid))
        activos["tasa_%"] = activos["tasa_mensual"].apply(percent_str)
        activos["cuotas_restantes"] = activos.apply(lambda r: cuotas_restantes(r, pagos), axis=1)
        activos_show = activos[
            [
                "display_id",
                "cliente",
                "monto",
                "tasa_%",
                "plazo_meses",
                "cuotas_restantes",
                "saldo_capital",
                "estado",
                "cuota_fija",
                "plan_inicio",
                "fecha_inicio",
                "ultimo_calculo",
                "creado_en",
            ]
        ].copy()
        for c in ["monto", "saldo_capital", "cuota_fija"]:
            activos_show[c] = activos_show[c].apply(format_cop)
        activos_show = normalize_datetime_cols(activos_show, ["plan_inicio", "fecha_inicio", "ultimo_calculo", "creado_en"], to_string=True)
        st.dataframe(activos_show, use_container_width=True)

# --------- Nuevo Préstamo ---------
with tabs[1]:
    st.subheader("Registrar nuevo préstamo (mensual)")
    with st.expander("Crear/Seleccionar cliente", expanded=False):
        with st.form("form_cliente"):
            colA, colB, colC, colD = st.columns(4)
            with colA:
                nombre = st.text_input("Nombre", key="newloan_cli_nombre")
            with colB:
                identificacion = st.text_input("Identificación", key="newloan_cli_id")
            with colC:
                telefono = st.text_input("Teléfono", key="newloan_cli_tel")
            with colD:
                email = st.text_input("Email", key="newloan_cli_email")
            submit_cliente = st.form_submit_button("Guardar / Usar cliente")
        if submit_cliente:
            clientes, cid = crear_cliente_si_no_existe(clientes, nombre, identificacion, telefono, email)
            save_data(clientes, prestamos, pagos, parametros)
            st.success(f"Cliente listo (ID interno: {cid[:8]}).")

    if clientes.empty:
        st.warning("Primero crea al menos un cliente.")
    else:
        labels, id_map = [], {}
        for _, row in clientes.iterrows():
            lbl = f"{safe_str(row['nombre'])} ({safe_str(row['identificacion'])})"
            labels.append(lbl)
            id_map[lbl] = safe_str(row["cliente_id"])
        selected = st.selectbox("Cliente", options=labels, key="newloan_cli_select")
        cliente_id = id_map[selected]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            monto = st.number_input("Monto (COP)", min_value=0.0, value=1_000_000.0, step=50_000.0, key="newloan_monto")
        with col2:
            tasa_text = st.text_input("Tasa mensual (%)", value="3%", key="newloan_tasa")
        with col3:
            plazo_meses = st.number_input("Plazo (meses)", min_value=1, value=6, step=1, key="newloan_plazo")
        with col4:
            fecha_inicio = st.date_input("Fecha de inicio", value=date.today(), key="newloan_fecha")

        if st.button("➕ Crear préstamo", key="newloan_create"):
            tasa_mensual_decimal = parse_percent(tasa_text)
            prestamos, pid = crear_prestamo(prestamos, cliente_id, monto, tasa_mensual_decimal, fecha_inicio, plazo_meses)
            save_data(clientes, prestamos, pagos, parametros)
            pr = prestamos[prestamos["prestamo_id"] == pid].iloc[0]
            st.success(f"Préstamo creado — ID: {pr['display_id']} · Cuota fija: {format_cop(pr['cuota_fija'])}")

# --------- Registrar Pago ---------
with tabs[2]:
    st.subheader("Registrar pago de cuota (fija) + adelanto extra (opcional)")
    activos = prestamos[prestamos["estado"].fillna("activo") == "activo"]
    if activos.empty:
        st.info("No hay préstamos activos para registrar pagos.")
    else:
        opciones_labels, map_display_to_uuid = [], {}
        for _, row in activos.iterrows():
            nombre_cli = nombre_cliente_por_id(clientes, row["cliente_id"])
            lbl = f"{nombre_cli} · ID:{safe_str(row['display_id'])} · Saldo:{format_cop(row['saldo_capital'])}"
            opciones_labels.append(lbl)
            map_display_to_uuid[safe_str(row["display_id"])] = safe_str(row["prestamo_id"])

        pre_disp = st.session_state.get("selected_prestamo_display")
        try:
            default_index = next(i for i, l in enumerate(opciones_labels) if f"ID:{pre_disp}" in l) if pre_disp else 0
        except StopIteration:
            default_index = 0

        elegido_lbl = st.selectbox("Préstamo", options=opciones_labels, index=default_index, key="pay_select_prestamo")
        try:
            display_id_elegido = elegido_lbl.split("ID:")[1].split("·")[0].strip()
        except Exception:
            display_id_elegido = elegido_lbl

        pid = map_display_to_uuid.get(display_id_elegido)
        pr_df = prestamos[prestamos["prestamo_id"] == pid]
        if pr_df.empty:
            st.error("No se encontró el préstamo seleccionado.")
        else:
            pr = pr_df.iloc[0]
            restantes = cuotas_restantes(pr, pagos)
            info_line = (
                f"Cliente: {nombre_cliente_por_id(clientes, pr['cliente_id'])} · ID: {pr['display_id']} · "
                f"Cuota fija: {format_cop(pr['cuota_fija'])} · Tasa: {percent_str(pr['tasa_mensual'])} · "
                f"Saldo: {format_cop(pr['saldo_capital'])} · Cuotas restantes: {restantes}"
            )
            st.info(info_line)

            fecha_pago = st.date_input("Fecha de pago", value=date.today(), key="pay_fecha")
            adelanto_extra = st.number_input("Adelanto extra a capital (COP)", min_value=0, value=0, step=50_000, key="pay_extra")
            observaciones = st.text_input("Observaciones", key="pay_obs")
            modo_reauto = st.selectbox(
                "Re‑amortizar automáticamente el abono extra?",
                options=["No re‑amortizar", "Reducir plazo (aumentar cuota)", "Reducir cuota (aumentar plazo)"],
                key="pay_reauto",
            )

            saldo_antes = safe_int(pr.get("saldo_capital"), default=0)
            interes_mes = int(round(saldo_antes * float(pr["tasa_mensual"]) if not pd.isna(pr["tasa_mensual"]) else 0.0))
            f_esp = fecha_esperada(pr, pagos)
            dias_mora, mora_aplicada = calcular_mora(saldo_antes, to_date(fecha_pago) or date.today(), f_esp)
            cuota_fija = safe_int(pr.get("cuota_fija"), default=0)
            total_a_pagar = cuota_fija + mora_aplicada + safe_int(adelanto_extra, default=0)
            st.caption(f"Interés: {format_cop(interes_mes)} · Mora: {format_cop(mora_aplicada)} · Total: {format_cop(total_a_pagar)}")
            st.warning("La cuota normal no admite montos inferiores a la cuota fija.", icon="⚠️")

            if st.button("💾 Registrar pago", key="pay_save"):
                try:
                    prestamos, pagos, pago_row = registrar_pago_cuota(prestamos, pagos, pid, fecha_pago, adelanto_extra, observaciones)
                    saldo_despues = safe_int(pago_row["saldo_despues"], default=0)
                    tasa = float(pr["tasa_mensual"]) if not pd.isna(pr["tasa_mensual"]) else 0.0
                    cuota_actual = safe_int(pr.get("cuota_fija"), default=0)
                    plazo_actual = safe_int(pr.get("plazo_meses"), default=1)

                    if saldo_despues > 0 and safe_int(adelanto_extra, default=0) > 0 and modo_reauto != "No re‑amortizar":
                        if modo_reauto == "Reducir plazo (aumentar cuota)":
                            n_calc = resolver_plazo_por_cuota(saldo_despues, tasa, cuota_actual)
                            cuota_calc = calcular_cuota_fija(saldo_despues, tasa, n_calc)
                            prestamos.loc[prestamos["prestamo_id"] == pid, ["plazo_meses", "cuota_fija", "plan_inicio", "actualizado_en"]] = [
                                int(n_calc),
                                int(cuota_calc),
                                to_date(fecha_pago) or date.today(),
                                datetime.now(),
                            ]
                            st.info(f"Re‑amortización automática aplicada: nuevo plazo {int(n_calc)} meses · cuota {format_cop(cuota_calc)}")
                        elif modo_reauto == "Reducir cuota (aumentar plazo)":
                            pr_tmp = pr.copy()
                            pr_tmp["plan_inicio"] = to_date(fecha_pago) or date.today()
                            pagadas_prev = contar_cuotas_normales_desde_epoch(pagos, pr_tmp)
                            n_rest = max(plazo_actual - pagadas_prev, 1)
                            cuota_calc = calcular_cuota_fija(saldo_despues, tasa, n_rest)
                            prestamos.loc[prestamos["prestamo_id"] == pid, ["plazo_meses", "cuota_fija", "plan_inicio", "actualizado_en"]] = [
                                int(n_rest),
                                int(cuota_calc),
                                to_date(fecha_pago) or date.today(),
                                datetime.now(),
                            ]
                            st.info(f"Re‑amortización automática aplicada: nuevo plazo {int(n_rest)} meses · nueva cuota {format_cop(cuota_calc)}")

                    save_data(clientes, prestamos, pagos, parametros)
                    clientes, prestamos, pagos, parametros, integrantes, aportes_tarifas, aportes_pagos = load_data()
                    st.success(
                        f"Pago OK. Interés: {format_cop(pago_row['interes_aplicado'])} · "
                        f"Capital: {format_cop(pago_row['capital_aplicado'])} · "
                        f"Mora: {format_cop(pago_row['mora_aplicada'])} · "
                        f"Saldo antes: {format_cop(pago_row['saldo_antes'])} → después: {format_cop(pago_row['saldo_despues'])}"
                    )
                except Exception as e:
                    st.error(f"Error registrando pago: {e}")

# --------- Reportes ---------
with tabs[3]:
    st.subheader("Reportes y exportación")
    st.markdown("### Préstamos")
    prs = prestamos.copy()
    prs["cliente"] = prs["cliente_id"].apply(lambda cid: nombre_cliente_por_id(clientes, cid))
    prs["tasa_%"] = prs["tasa_mensual"].apply(percent_str)
    prs["cuotas_restantes"] = prs.apply(lambda r: cuotas_restantes(r, pagos), axis=1)
    nombres_opts = ["Todos"] + sorted(list(set(prs["cliente"].dropna().astype(str).tolist())))
    sel_cli = st.selectbox("Filtrar por cliente", options=nombres_opts, index=0, key="repo_cli_filter")
    if sel_cli != "Todos":
        prs = prs[prs["cliente"] == sel_cli]
    prs_show = prs[
        [
            "display_id",
            "cliente",
            "monto",
            "tasa_%",
            "plazo_meses",
            "cuotas_restantes",
            "saldo_capital",
            "estado",
            "cuota_fija",
            "plan_inicio",
            "fecha_inicio",
            "ultimo_calculo",
            "creado_en",
        ]
    ].copy()
    for c in ["monto", "saldo_capital", "cuota_fija"]:
        prs_show[c] = prs_show[c].apply(format_cop)
    prs_show = normalize_datetime_cols(prs_show, ["plan_inicio", "fecha_inicio", "ultimo_calculo", "creado_en"], to_string=True)
    st.dataframe(prs_show, use_container_width=True)

    st.markdown("### Pagos")
    pagos_show = pagos.copy()
    for c in ["monto_pago", "interes_aplicado", "capital_aplicado", "mora_aplicada"]:
        if c in pagos_show.columns:
            pagos_show[c] = pagos_show[c].apply(format_cop)
    pagos_show = normalize_datetime_cols(pagos_show, ["fecha_pago", "creado_en"], to_string=True)
    st.dataframe(pagos_show, use_container_width=True)

    # Métrica de rendimiento anual (robusta)
    def total_fondo_fin_mes(prestamos_df, pagos_df, parametros_df, aportes_pagos_df, fecha_corte):
        capital_inicial = 0.0
        row = parametros_df[parametros_df["clave"] == "capital_inicial"]
        if not row.empty:
            try:
                capital_inicial = float(row.iloc[0]["valor"])
            except:
                pass
        desembolsado_hasta = float(
            prestamos_df[prestamos_df["fecha_inicio"].apply(lambda x: to_date(x) and to_date(x) <= fecha_corte)]["monto"].sum()
        )
        intereses_hasta = float(
            pagos_df[pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x) <= fecha_corte)]["interes_aplicado"].sum()
        ) if ("interes_aplicado" in pagos_df.columns and not pagos_df.empty) else 0.0
        capital_rec_hasta = float(
            pagos_df[pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x) <= fecha_corte)]["capital_aplicado"].sum()
        ) if ("capital_aplicado" in pagos_df.columns and not pagos_df.empty) else 0.0
        aportes_hasta = (
            float(
                aportes_pagos_df[
                    aportes_pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x) <= fecha_corte)
                ]["monto_pagado"].sum()
            )
            if (aportes_pagos_df is not None and not aportes_pagos_df.empty)
            else 0.0
        )
        caja = capital_inicial - desembolsado_hasta + (intereses_hasta + capital_rec_hasta + aportes_hasta)
        saldo_capital = 0
        for _, pr in prestamos_df.iterrows():
            monto0 = safe_int(pr.get("monto"), default=0)
            pid = pr.get("prestamo_id")
            pagos_pr = pagos_df[
                (pagos_df["prestamo_id"] == pid) & (pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x) <= fecha_corte))
            ]
            cap_rec = safe_int(pagos_pr["capital_aplicado"].sum() if not pagos_pr.empty else 0, default=0)
            saldo_capital += max(monto0 - cap_rec, 0)
        return caja + saldo_capital

    def rendimiento_anual(prestamos_df, pagos_df, parametros_df, aportes_pagos_df, anio):
        intereses_anio = float(
            pagos_df[pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x).year == anio)]["interes_aplicado"].sum()
        ) if ("interes_aplicado" in pagos_df.columns and not pagos_df.empty) else 0.0
        meses = [date(anio, m, 1) + relativedelta(months=+1, days=-1) for m in range(1, 13) if date(anio, m, 1) <= date.today()]
        tf = [total_fondo_fin_mes(prestamos_df, pagos_df, parametros_df, aportes_pagos_df, fm) for fm in meses]
        prom_tf = (sum(tf) / len(tf)) if tf else 0.0
        porcentaje = (intereses_anio / prom_tf) if prom_tf > 0 else 0.0
        return intereses_anio, porcentaje

    anio = date.today().year
    int_cop, pct = rendimiento_anual(prestamos, pagos, parametros, aportes_pagos, anio)
    c1, c2 = st.columns(2)
    c1.metric(f"Intereses {anio}", format_cop(int_cop))
    c2.metric(f"Rentabilidad {anio}", f"{round(pct*100, 2)}%")

    if st.button("⬇️ Descargar todo (Excel)", key="report_download_btn"):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            clientes.to_excel(writer, sheet_name="clientes", index=False)
            prestamos.to_excel(writer, sheet_name="prestamos", index=False)
            pagos.to_excel(writer, sheet_name="pagos", index=False)
            parametros.to_excel(writer, sheet_name="parametros", index=False)
            integrantes.to_excel(writer, sheet_name="integrantes", index=False)
            aportes_tarifas.to_excel(writer, sheet_name="aportes_tarifas", index=False)
            aportes_pagos.to_excel(writer, sheet_name="aportes_pagos", index=False)
            inv_df, inv_movs_df = load_inversionista()
            inv_df.to_excel(writer, sheet_name="inversionista", index=False)
            inv_movs_df.to_excel(writer, sheet_name="inversionista_movs", index=False)
        output.seek(0)
        st.download_button("Descargar 'fondo_export.xlsx'", data=output, file_name="fondo_export.xlsx", key="report_download_link")

# --------- Simulador ---------
with tabs[4]:
    st.subheader("Simulador de crédito — método francés")
    colA, colB, colC, colD = st.columns(4)
    with colA:
        principal = st.number_input("Monto (P)", min_value=0.0, value=1_000_000.0, step=50_000.0, key="sim_monto")
    with colB:
        tasa_text = st.text_input("Tasa mensual (%)", value="3%", key="sim_tasa")
    with colC:
        n = st.number_input("Meses (n)", min_value=1, value=6, step=1, key="sim_n")
    with colD:
        f_inicio = st.date_input("Fecha 1ª cuota", value=date.today(), key="sim_fecha")
    if st.button("🧮 Calcular simulación", key="sim_calc"):
        try:
            i_m = parse_percent(tasa_text)
            cuota, t_int, t_pag, tabla = simulador_cuotas_fijas(principal, i_m, int(n), f_inicio)
            st.session_state["sim"] = {"P": principal, "i_m": i_m, "n": int(n), "f_inicio": f_inicio, "cuota": cuota, "t_int": t_int, "t_pag": t_pag, "tabla": tabla}
        except Exception as e:
            st.error(f"Error en simulación: {e}")

    sim = st.session_state.get("sim")
    if sim:
        c1, c2, c3 = st.columns(3)
        c1.metric("Cuota fija", format_cop(sim["cuota"]))
        c2.metric("Interés total", format_cop(sim["t_int"]))
        c3.metric("Total pagado", format_cop(sim["t_pag"]))
        tabla_show = sim["tabla"].copy()
        for c in ["SALDO INICIAL", "CUOTA", "INTERES", "CAPITAL", "SALDO DESPUES DEL PAGO"]:
            tabla_show[c] = tabla_show[c].apply(format_cop)
        st.markdown("### Tabla de amortización")
        st.dataframe(tabla_show, use_container_width=True)

# --------- Parámetros ---------
with tabs[5]:
    st.subheader("Parámetros del fondo")
    row = parametros[parametros["clave"] == "capital_inicial"]
    val = 0.0
    if not row.empty:
        try:
            val = float(row.iloc[0]["valor"])
        except:
            val = 0.0
    nuevo_capital = st.number_input("Capital inicial (COP)", min_value=0.0, value=val, step=100_000.0, key="params_capital_ini")
    if st.button("💾 Guardar capital inicial", key="params_save"):
        if row.empty:
            parametros = pd.concat([parametros, pd.DataFrame([{"clave": "capital_inicial", "valor": int(round(nuevo_capital))}])], ignore_index=True)
        else:
            parametros.loc[parametros["clave"] == "capital_inicial", "valor"] = int(round(nuevo_capital))
        save_data(clientes, prestamos, pagos, parametros)
        st.success("Capital inicial guardado.")

# --------- Re‑amortización ---------
with tabs[6]:
    st.subheader("Re‑amortización de préstamo (actualiza plan_inicio)")
    activos = prestamos[prestamos["estado"].fillna("activo") == "activo"]
    if activos.empty:
        st.info("No hay préstamos activos.")
    else:
        labels, id_map = [], {}
        for _, pr_item in activos.iterrows():
            nombre_cli = nombre_cliente_por_id(clientes, pr_item["cliente_id"])
            labels.append(f"{nombre_cli} · ID:{pr_item['display_id']} · Saldo:{format_cop(pr_item['saldo_capital'])}")
            id_map[labels[-1]] = pr_item["prestamo_id"]

        pre_label = None
        if st.session_state.get("rea_display_id"):
            for lbl in labels:
                if f"ID:{st.session_state['rea_display_id']}" in lbl:
                    pre_label = lbl
                    break
        idx = labels.index(pre_label) if pre_label in labels else 0
        sel_label = st.selectbox("Préstamo", options=labels, index=idx, key="rea_select")
        pid_rea = id_map[sel_label]
        pr = prestamos[prestamos["prestamo_id"] == pid_rea].iloc[0]
        restantes_rea = cuotas_restantes(pr, pagos)
        st.info(f"ID: {pr['display_id']} · Saldo: {format_cop(pr['saldo_capital'])} · Cuota: {format_cop(pr['cuota_fija'])} · Cuotas restantes: {restantes_rea}")

        fecha_rea = st.date_input("Fecha de re‑amortización", value=date.today(), key="rea_fecha")
        tasa_text = st.text_input("Nueva tasa mensual (%)", value=percent_str(pr["tasa_mensual"]) if not pd.isna(pr["tasa_mensual"]) else "0%", key="rea_tasa")
        i_m = parse_percent(tasa_text)
        saldo_actual = safe_int(pr.get("saldo_capital"), default=0)
        modo = st.radio("Elige modalidad", options=["Reducir cuota (aumentar plazo)", "Reducir plazo (aumentar cuota)"], index=0, key="rea_modo")

        if modo == "Reducir cuota (aumentar plazo)":
            nueva_cuota_deseada = st.text_input("Cuota deseada (COP)", value=str(safe_int(pr.get("cuota_fija"), default=1)), key="rea_cuota_deseada")
            try:
                cuota_in = safe_int(nueva_cuota_deseada, default=safe_int(pr.get("cuota_fija"), default=1))
                if cuota_in <= 0:
                    raise ValueError("Cuota deseada inválida (debe ser > 0).")
                preview_n = resolver_plazo_por_cuota(saldo_actual, i_m, cuota_in)
                cuota_ajustada = calcular_cuota_fija(saldo_actual, i_m, preview_n)
                st.caption(f"Plazo estimado: {preview_n} meses · Cuota ajustada: {format_cop(cuota_ajustada)} (tasa {percent_str(i_m)})")
                _, _, _, tabla_prev = simulador_cuotas_fijas(saldo_actual, i_m, preview_n, to_date(fecha_rea) or date.today())
                tabla_show = tabla_prev.copy()
                for c in ["SALDO INICIAL", "CUOTA", "INTERES", "CAPITAL", "SALDO DESPUES DEL PAGO"]:
                    tabla_show[c] = tabla_show[c].apply(format_cop)
                st.markdown("### Vista previa — Tabla de amortización")
                st.dataframe(tabla_show, use_container_width=True)
            except Exception as e:
                st.warning(f"No se puede estimar el plazo con la cuota indicada: {e}")
            if st.button("Aplicar re‑amortización", key="rea_apply_reduce_cuota"):
                try:
                    cuota_in = safe_int(nueva_cuota_deseada, default=safe_int(pr.get("cuota_fija"), default=1))
                    if cuota_in <= 0:
                        raise ValueError("Cuota deseada inválida (debe ser > 0).")
                    n_calc = resolver_plazo_por_cuota(saldo_actual, i_m, cuota_in)
                    cuota_calc = calcular_cuota_fija(saldo_actual, i_m, n_calc)
                    prestamos.loc[prestamos["prestamo_id"] == pid_rea, ["tasa_mensual", "plazo_meses", "cuota_fija", "plan_inicio", "actualizado_en"]] = [
                        i_m,
                        int(n_calc),
                        int(cuota_calc),
                        to_date(fecha_rea) or date.today(),
                        datetime.now(),
                    ]
                    save_data(clientes, prestamos, pagos, parametros)
                    st.success(f"Re‑amortizado: nueva cuota {format_cop(cuota_calc)} · nuevo plazo {int(n_calc)} meses · plan desde {to_date(fecha_rea) or date.today()}")
                except Exception as e:
                    st.error(f"Error al re‑amortizar: {e}")
        else:
            nuevo_plazo = st.text_input("Nuevo plazo (meses)", value=str(safe_int(pr.get("plazo_meses"), default=1)), key="rea_nuevo_plazo")
            try:
                plazo_in = max(safe_int(nuevo_plazo, default=safe_int(pr.get("plazo_meses"), default=1)), 1)
                nueva_cuota_calc = calcular_cuota_fija(saldo_actual, i_m, plazo_in)
                st.caption(f"Cuota estimada: {format_cop(nueva_cuota_calc)} (tasa {percent_str(i_m)})")
                _, _, _, tabla_prev = simulador_cuotas_fijas(saldo_actual, i_m, plazo_in, to_date(fecha_rea) or date.today())
                tabla_show = tabla_prev.copy()
                for c in ["SALDO INICIAL", "CUOTA", "INTERES", "CAPITAL", "SALDO DESPUES DEL PAGO"]:
                    tabla_show[c] = tabla_show[c].apply(format_cop)
                st.markdown("### Vista previa — Tabla de amortización")
                st.dataframe(tabla_show, use_container_width=True)
            except Exception as e:
                st.warning(f"No se puede estimar la cuota con el plazo indicado: {e}")
            if st.button("Aplicar re‑amortización", key="rea_apply_reduce_plazo"):
                try:
                    plazo_in = max(safe_int(nuevo_plazo, default=safe_int(pr.get("plazo_meses"), default=1)), 1)
                    nueva_cuota_calc = calcular_cuota_fija(saldo_actual, i_m, plazo_in)
                    prestamos.loc[prestamos["prestamo_id"] == pid_rea, ["tasa_mensual", "plazo_meses", "cuota_fija", "plan_inicio", "actualizado_en"]] = [
                        i_m,
                        int(plazo_in),
                        int(nueva_cuota_calc),
                        to_date(fecha_rea) or date.today(),
                        datetime.now(),
                    ]
                    save_data(clientes, prestamos, pagos, parametros)
                    st.success(f"Re‑amortizado: nuevo plazo {int(plazo_in)} meses · nueva cuota {format_cop(nueva_cuota_calc)} · plan desde {to_date(fecha_rea) or date.today()}")
                except Exception as e:
                    st.error(f"Error al re‑amortizar: {e}")

# --------- Aportes ---------
with tabs[7]:
    st.subheader("Aportes de integrantes (registro por período)")
    integ_edit = normalize_datetime_cols(integrantes.copy(), ["creado_en", "actualizado_en"], to_string=True)
    st.dataframe(integ_edit, use_container_width=True)

    with st.form("form_integrantes_edit"):
        nombre_edit = st.text_input("Nombre del integrante a actualizar", key="ap_integ_edit_nombre")
        cupos_nuevo = st.number_input("Nuevo número de cupos", min_value=0, value=0, step=1, key="ap_integ_edit_cupos")
        submitted_edit = st.form_submit_button("Actualizar cupos")
    if submitted_edit:
        mask = integrantes["nombre"].str.lower() == nombre_edit.strip().lower()
        if mask.any():
            integrantes.loc[mask, ["cupos", "actualizado_en"]] = [int(cupos_nuevo), datetime.now()]
            save_aportes_data(integrantes, aportes_tarifas, aportes_pagos)
            st.success("Cupos actualizados.")
        else:
            st.warning("No se encontró ese nombre.")

    anio_actual = date.today().year
    row_tar = aportes_tarifas[aportes_tarifas["anio"] == anio_actual]
    val_tarifa = 70000 if row_tar.empty else int(row_tar.iloc[0]["valor_por_cupo"])
    nueva_tarifa = st.number_input(f"Tarifa por cupo ({anio_actual})", min_value=0, value=val_tarifa, step=5000, key="ap_tarifa_anio")
    if st.button("Guardar tarifa", key="ap_tarifa_save"):
        if row_tar.empty:
            aportes_tarifas = pd.concat([aportes_tarifas, pd.DataFrame([{"anio": anio_actual, "valor_por_cupo": int(nueva_tarifa)}])], ignore_index=True)
        else:
            aportes_tarifas.loc[aportes_tarifas["anio"] == anio_actual, "valor_por_cupo"] = int(nueva_tarifa)
        save_aportes_data(integrantes, aportes_tarifas, aportes_pagos)
        st.success("Tarifa guardada.")

    st.markdown("### Registrar aportes del período")
    st.caption("Período (AAAA‑MM) = mes contable del aporte. Fecha de cobro = día real recibido.")
    periodo = st.text_input("Período (AAAA‑MM)", value=f"{anio_actual}-{date.today().month:02d}", key="ap_periodo")
    fecha_pago_ap = st.date_input("Fecha de cobro", value=date.today(), key="ap_fecha_cobro")

    nuevos_aportes = []
    for i, row in integrantes.iterrows():
        cupos_pag = st.number_input(
            f"{row['nombre']} — cupos vigentes: {safe_int(row['cupos'], default=0)}",
            min_value=0,
            max_value=safe_int(row["cupos"], default=0),
            value=0,
            step=1,
            key=f"ap_{row['integrante_id']}",
        )
        if cupos_pag > 0:
            monto = int(cupos_pag * nueva_tarifa)
            nuevos_aportes.append(
                {
                    "aporte_id": str(uuid.uuid4()),
                    "integrante_id": row["integrante_id"],
                    "periodo": periodo,
                    "fecha_pago": fecha_pago_ap,
                    "cupos_pagados": int(cupos_pag),
                    "monto_pagado": monto,
                    "observaciones": "",
                    "creado_en": datetime.now(),
                }
            )

    if st.button("💾 Registrar aportes del período", key="ap_guardar_periodo"):
        if nuevos_aportes:
            aportes_pagos = pd.concat([aportes_pagos, pd.DataFrame(nuevos_aportes)], ignore_index=True)
            save_aportes_data(integrantes, aportes_tarifas, aportes_pagos)
            st.success(f"Aportes registrados: {len(nuevos_aportes)}")
        else:
            st.info("No se seleccionaron aportes.")

    st.markdown("### Igualación de cupos / nuevo integrante")

    def resumen_fondo_ext_simple():
        inv_df, inv_movs_df = load_inversionista()
        capital_inicial = 0.0
        row = parametros[parametros["clave"] == "capital_inicial"]
        if not row.empty:
            try:
                capital_inicial = float(row.iloc[0]["valor"])
            except Exception:
                pass
        total_desembolsado = float(prestamos["monto"].sum()) if not prestamos.empty else 0.0
        saldo_capital_total = float(prestamos["saldo_capital"].sum()) if not prestamos.empty else 0.0
        intereses_cobrados = float(pagos["interes_aplicado"].sum()) if ("interes_aplicado" in pagos.columns and not pagos.empty) else 0.0
        capital_recuperado = float(pagos["capital_aplicado"].sum()) if ("capital_aplicado" in pagos.columns and not pagos.empty) else 0.0
        aportes_cobrados = float(aportes_pagos["monto_pagado"].sum()) if not aportes_pagos.empty else 0.0
        caja_actual = capital_inicial - total_desembolsado + (intereses_cobrados + capital_recuperado + aportes_cobrados)
        total_fondo = caja_actual + saldo_capital_total
        # Pasivo inversionista
        pasivo_inv = 0.0
        if not inv_df.empty:
            try:
                cap = float(inv_df.iloc[0].get("capital_inicial", 0) or 0)
            except Exception:
                cap = 0.0
            try:
                gan_p = float(inv_df.iloc[0].get("ganancia_pendiente", inv_df.iloc[0].get("ganancia_anual", 0)) or 0)
            except Exception:
                gan_p = 0.0
            pasivo_inv = cap + gan_p
        total_sin_inv = total_fondo - pasivo_inv
        total_cupos = int(integrantes["cupos"].sum()) if not integrantes.empty else 0
        return (total_sin_inv / total_cupos) if total_cupos > 0 else 0.0

    total_por_cupo_actual = resumen_fondo_ext_simple()
    st.caption(f"Total por cupo actual: {format_cop(total_por_cupo_actual)}")

    colI1, colI2 = st.columns(2)
    with colI1:
        st.markdown("**Agregar cupos a integrante existente**")
        nombres_opts = [row["nombre"] for _, row in integrantes.iterrows()]
        if nombres_opts:
            nombre_sel = st.selectbox("Integrante", options=nombres_opts, key="igual_nombre")
            cupos_add = st.number_input("Cupos a agregar", min_value=0, value=0, step=1, key="igual_cupos")
            fecha_igual = st.date_input("Fecha de igualación", value=date.today(), key="igual_fecha")
            if st.button("Aplicar igualación y agregar cupos", key="igual_btn_1"):
                if cupos_add > 0:
                    integ_mask = integrantes["nombre"].str.lower() == nombre_sel.strip().lower()
                    if integ_mask.any():
                        iid = integrantes.loc[integ_mask, "integrante_id"].iloc[0]
                        monto_eq = int(round(total_por_cupo_actual * cupos_add))
                        aporte_row = {
                            "aporte_id": str(uuid.uuid4()),
                            "integrante_id": iid,
                            "periodo": f"{anio_actual}-{date.today().month:02d}",
                            "fecha_pago": fecha_igual,
                            "cupos_pagados": int(cupos_add),
                            "monto_pagado": monto_eq,
                            "observaciones": "igualación por cupos nuevos",
                            "creado_en": datetime.now(),
                        }
                        aportes_pagos = pd.concat([aportes_pagos, pd.DataFrame([aporte_row])], ignore_index=True)
                        cupos_act = safe_int(integrantes.loc[integ_mask, "cupos"].iloc[0], default=0)
                        integrantes.loc[integ_mask, ["cupos", "actualizado_en"]] = [cupos_act + int(cupos_add), datetime.now()]
                        save_aportes_data(integrantes, aportes_tarifas, aportes_pagos)
                        st.success(f"Cupos agregados (+{int(cupos_add)}). Igualación: {format_cop(monto_eq)}")
                else:
                    st.info("Indica número de cupos a agregar.")
    with colI2:
        st.markdown("**Agregar nuevo integrante**")
        with st.form("form_nuevo_integrante"):
            nombre_new = st.text_input("Nombre", key="nuevo_nombre")
            ident_new = st.text_input("Identificación", key="nuevo_ident")
            cupos_new = st.number_input("Cupos iniciales", min_value=0, value=0, step=1, key="nuevo_cupos")
            fecha_igual_new = st.date_input("Fecha de igualación", value=date.today(), key="nuevo_fecha")
            submit_new = st.form_submit_button("Crear integrante + aplicar igualación")
        if submit_new:
            iid = str(uuid.uuid4())
            now = datetime.now()
            row_new = {
                "integrante_id": iid,
                "nombre": nombre_new,
                "identificacion": ident_new,
                "cupos": int(cupos_new),
                "creado_en": now,
                "actualizado_en": now,
            }
            integrantes = pd.concat([integrantes, pd.DataFrame([row_new])], ignore_index=True)
            monto_eq = int(round(total_por_cupo_actual * int(cupos_new)))
            if int(cupos_new) > 0:
                aporte_row = {
                    "aporte_id": str(uuid.uuid4()),
                    "integrante_id": iid,
                    "periodo": f"{anio_actual}-{date.today().month:02d}",
                    "fecha_pago": fecha_igual_new,
                    "cupos_pagados": int(cupos_new),
                    "monto_pagado": monto_eq,
                    "observaciones": "igualación por nuevo integrante",
                    "creado_en": datetime.now(),
                }
                aportes_pagos = pd.concat([aportes_pagos, pd.DataFrame([aporte_row])], ignore_index=True)
            save_aportes_data(integrantes, aportes_tarifas, aportes_pagos)
            st.success(f"Integrante creado. Igualación: {format_cop(monto_eq)} (por {int(cupos_new)} cupo/s).")

# --------- Inversionista ---------
with tabs[8]:
    st.subheader("Inversionista — Teresa Pérez")
    inv_df, inv_movs_df = load_inversionista()
    if inv_df.empty:
        st.warning("No hay datos del inversionista; se inicializarán al guardar.", icon="⚠️")
    else:
        inv = inv_df.iloc[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("Capital inicial", format_cop(inv.get("capital_inicial", 0)))
        c2.metric("Ganancia anual", format_cop(inv.get("ganancia_anual", 0)))
        c3.metric("Ganancia pendiente", format_cop(inv.get("ganancia_pendiente", inv.get("ganancia_anual", 0))))
        st.caption(
            f"Total de referencia: {format_cop((inv.get('capital_inicial',0) or 0) + (inv.get('ganancia_pendiente', inv.get('ganancia_anual',0)) or 0))}"
        )
        st.info(f"Estado: {safe_str(inv.get('estado'))}")
        st.divider()

        colA, colB = st.columns(2)
        with colA:
            st.markdown("**Dividendos entregados**")
            fecha_div = st.date_input("Fecha de entrega", value=date.today(), key="inv_fecha_div")
            if st.button("Registrar dividendos (70.000)", key="inv_btn_div"):
                try:
                    pend = int(float(inv.get("ganancia_pendiente", inv.get("ganancia_anual", 0)) or 0))
                    if pend <= 0:
                        st.info("No hay ganancia pendiente por entregar.")
                    else:
                        mov = {
                            "mov_id": str(uuid.uuid4()),
                            "fecha": to_date(fecha_div) or date.today(),
                            "tipo": "dividendo",
                            "monto": pend,
                            "observaciones": "entrega dividendo anual",
                            "creado_en": datetime.now(),
                        }
                        inv_movs_df = pd.concat([inv_movs_df, pd.DataFrame([mov])], ignore_index=True)
                        inv_df.at[0, "ganancia_pendiente"] = 0
                        inv_df.at[0, "actualizado_en"] = datetime.now()
                        save_inversionista_data(inv_df, inv_movs_df)
                        st.success(f"Dividendos entregados: {format_cop(pend)}")
                except Exception as e:
                    st.error(f"Error: {e}")

        with colB:
            st.markdown("**Cancelación de inversión**")
            fecha_cap = st.date_input("Fecha de cancelación", value=date.today(), key="inv_fecha_cap")
            if st.button("Cancelar inversión (devolver capital)", key="inv_btn_cap"):
                try:
                    cap = int(float(inv_df.iloc[0].get("capital_inicial", 0) or 0))
                    est = safe_str(inv_df.iloc[0].get("estado"))
                    if cap <= 0 or est == "cancelado":
                        st.info("No hay capital pendiente o la inversión ya está cancelada.")
                    else:
                        mov = {
                            "mov_id": str(uuid.uuid4()),
                            "fecha": to_date(fecha_cap) or date.today(),
                            "tipo": "cancelacion",
                            "monto": cap,
                            "observaciones": "devolución de capital",
                            "creado_en": datetime.now(),
                        }
                        inv_movs_df = pd.concat([inv_movs_df, pd.DataFrame([mov])], ignore_index=True)
                        inv_df.at[0, "capital_inicial"] = 0
                        inv_df.at[0, "estado"] = "cancelado"
                        inv_df.at[0, "actualizado_en"] = datetime.now()
                        save_inversionista_data(inv_df, inv_movs_df)
                        st.success(f"Inversión cancelada: {format_cop(cap)}")
                except Exception as e:
                    st.error(f"Error: {e}")

        st.divider()
        st.markdown("### Historial de movimientos")
        movs_show = inv_movs_df.copy()
        movs_show = normalize_datetime_cols(movs_show, ["fecha", "creado_en"], to_string=True)
        if not movs_show.empty:
            movs_show["monto"] = movs_show["monto"].apply(format_cop)
        st.dataframe(movs_show, use_container_width=True)
