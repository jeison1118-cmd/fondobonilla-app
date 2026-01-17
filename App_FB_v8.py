
# -*- coding: utf-8 -*-
# Fondo Bonilla (v8 + auth, SIN SIDEBAR) — Login en cuerpo y Logout en topbar
# Requisitos: pip install streamlit pandas openpyxl python-dateutil gspread gspread_dataFrame google-auth bcrypt

import streamlit as st
import pandas as pd
import numpy as np
import uuid
import os
import random
import string
from io import BytesIO
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
import requests
import gspread
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from gspread.exceptions import WorksheetNotFound
import time
import bcrypt
import streamlit as st

APP_NAME = "Fondo Bonilla (v8)"
EXCEL_PATH = "fondo.xlsx"
BACKUP_DIR = "backups"
GRACIA_DIAS = 5
MORA_TASA_MENSUAL = 0.02
SHEET_ID = "1RbVD9oboyVfSPiwHS5B4xD9h9i6cxyXLY9uXhdQx62s"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]


st.set_page_config(
    page_title="Fondo Bonilla",
    page_icon="logo.png"    # archivo en la raíz
)


# --- Fix Toolbar/header: asegurar que los clics no queden bloqueados ---
st.markdown("""
<style>
/* 1) Que el header/toolbar esté por encima de cualquier overlay */
[data-testid="stHeader"] { 
  position: relative; 
  z-index: 1000 !important; 
}

/* 2) Rehabilitar el Toolbar si alguna regla previa lo desactivó */
[data-testid="stToolbar"] { 
  visibility: visible !important; 
  opacity: 1 !important; 
  pointer-events: auto !important; 
}

/* 3) Evitar que cualquier overlay intercepte clics del header */
[data-testid="stAppViewContainer"]::before,
.fb-watermark::after {
  pointer-events: none !important;
}

/* 4) Por si algún CSS previo ocultó el botón del menú o su área clickable */
header [role="button"],
[data-testid="baseButton-headerNoPadding






import streamlit.components.v1 as components

# Inyección PWA: manifest + iconos (USANDO ETIQUETAS <link> / <meta> dentro de un string)
components.html(
    """
    <link rel="manifest"
          href="https://raw.githubusercontent.com/jeison1118-cmd/fondobonilla-app/main/manifest.json" />

    <!-- Favicon (compatibilidad general) -->
    <link rel="icon" type="image/png"
          href="https://raw.githubusercontent.com/jeison1118-cmd/fondobonilla-app/main/assets/logo-192.png" />

    <!-- iOS: icono especial 180x180 -->
    <link rel="apple-touch-icon"
          href="https://raw.githubusercontent.com/jeison1118-cmd/fondobonilla-app/main/assets/apple-touch-icon.png" />

    <!-- iOS: abrir como app a pantalla completa -->
    <meta name="apple-mobile-web-app-capable" content="yes" />
    """,
    height=0
)




# === [NUEVO] Marca de agua global + utilidades (logo en base64) ===
import base64
from pathlib import Path
import streamlit.components.v1 as components

LOGO_PATH = "assets/logo.png"

def _logo_b64():
    """Devuelve el logo en base64 (o None si no existe)."""
    try:
        with open(LOGO_PATH, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception:
        return None

def inject_global_watermark_css():
    """Marca de agua sutil para TODA la app (fondo del área principal)."""
    b64 = _logo_b64()
    if not b64:
        return
    st.markdown(
        f"""
        <style>
        /* Capa fija sobre el área principal */
        [data-testid="stAppViewContainer"]::before {{
          content: "";
          position: fixed;
          top: 4rem; bottom: 1rem; left: 1rem; right: 1rem;
          background: url("data:image/png;base64,{b64}") center 42% no-repeat;
          background-size: 420px;
          opacity: .035;           /* Muy sutil para no molestar */
          pointer-events: none;
          z-index: 0;
        }}
        [data-testid="stHeader"] {{ z-index: 1; }}
        .main {{ z-index: 1; position: relative; }}
        </style>
        """,
        unsafe_allow_html=True
    )

def inject_section_watermark_css():
    """Clase .fb-watermark para poner una marca de agua visible en un bloque."""
    b64 = _logo_b64()
    if not b64:
        return
    st.markdown(
        f"""
        <style>
        .fb-watermark {{
          position: relative;
        }}
        /* Overlay por ENCIMA del contenido para que no se pierda en fondos blancos */
        .fb-watermark::after {{
          content: "";
          position: absolute; inset: 0;
          background: url("data:image/png;base64,{b64}") center 38% no-repeat;
          background-size: 240px;
          opacity: .08;             /* Sutil y visible */
          pointer-events: none;
          z-index: 5;
        }}
        .fb-watermark > * {{
          position: relative; z-index: 10;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

# Activa las marcas de agua (si no existe el PNG, simplemente no se muestran)
inject_global_watermark_css()
inject_section_watermark_css()




# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# CSS para ocultar completamente la Sidebar y el botón de toggle del header
st.markdown("""
    <style>
    /* Oculta la sidebar completa */
    [data-testid="stSidebar"] {display: none !important;}
    /* Oculta el botón/caret del header (hamburger) */
    [data-testid="baseButton-headerNoPadding"] {display: none !important;}
    /* Asegura ancho completo del main cuando no hay sidebar */
    .main {margin-left: 0rem !important;}
    </style>
""", unsafe_allow_html=True)
# <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

READ_ONLY_MSG = "Acceso de solo lectura. Solicita rol admin para usar esta sección."

# -------------------- Sheets helpers --------------------
def _read_ws_with_backoff(ws, evaluate_formulas=False, **options):
    delay = 0.5
    for _attempt in range(6):
        try:
            return get_as_dataframe(ws, evaluate_formulas=evaluate_formulas, **options)
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if '429' in msg or 'Quota exceeded' in msg or 'RATE_LIMIT' in msg:
                time.sleep(delay)
                delay = min(delay*2, 8.0)
                continue
            raise

def _write_and_update_cache(sheet_name: str, df: pd.DataFrame):
    write_df(sheet_name, df)
    cache = st.session_state.get('data_cache', {})
    cache[sheet_name] = df.copy()
    st.session_state['data_cache'] = cache

def _gs_credentials():
    if "gcp_service_account" in st.secrets:
        return Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)
    json_path = Path(__file__).with_name("service_account.json")
    if not json_path.exists():
        st.error("Faltan credenciales.")
        st.stop()
    return Credentials.from_service_account_file(str(json_path), scopes=SCOPES)

@st.cache_resource
def _gs_client():
    return gspread.authorize(_gs_credentials())

@st.cache_resource
def _get_spreadsheet():
    return _gs_client().open_by_key(SHEET_ID)

def _get_ws(worksheet_name: str):
    sh = _get_spreadsheet()
    try:
        return sh.worksheet(worksheet_name)
    except WorksheetNotFound:
        return sh.add_worksheet(title=worksheet_name, rows=1000, cols=20)

# Read/write
def read_df(sheet_name: str):
    ws = _get_ws(sheet_name)
    df = _read_ws_with_backoff(ws, evaluate_formulas=True, header=0, na_value="", drop_empty_rows=True, drop_empty_columns=True)
    return df if df is not None else pd.DataFrame()

def write_df(sheet_name: str, df):
    ws = _get_ws(sheet_name)
    ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)

# Cache
@st.cache_data(ttl=180)
def read_all():
    return {
        "clientes": read_df("clientes"),
        "prestamos": read_df("prestamos"),
        "pagos": read_df("pagos"),
        "parametros": read_df("parametros"),
        "integrantes": read_df("integrantes"),
        "aportes_tarifas": read_df("aportes_tarifas"),
        "aportes_pagos": read_df("aportes_pagos"),
        "inversionista": read_df("inversionista"),
        "inversionista_movs": read_df("inversionista_movs"),
    }

def _invalidate_cache():
    try:
        read_all.clear()
    except Exception:
        pass

def get_data():
    if "data_cache" in st.session_state:
        return st.session_state["data_cache"]
    d = read_all()
    st.session_state["data_cache"] = d
    return d


# -------------------- Auth (login en cuerpo, logout en topbar) --------------------
def auth_login() -> bool:
    """Formulario de login centrado en el cuerpo (sin sidebar)."""
    st.subheader("Iniciar sesión")
    left, center, right = st.columns([1, 2, 1])
    with center:
        with st.form("login_form"):
            user = st.text_input("Usuario", key="login_user").strip()
            pwd = st.text_input("Contraseña", type="password", key="login_pwd")
            submitted = st.form_submit_button("Entrar")

        if not submitted:
            return False

        udb = st.secrets.get("users", {})
        target = None
        for _, u in dict(udb).items():
            if str(u.get("username", "")).strip() == user:
                target = u
                break

        if not target:
            st.error("Usuario o contraseña incorrectos")
            return False

        stored = str(target.get("password_hash", "")).strip()
        try:
            ok = stored and bcrypt.checkpw(pwd.encode("utf-8"), stored.encode("utf-8"))
        except Exception:
            ok = False

        if ok:
            st.session_state["auth_user"] = user
            st.session_state["auth_role"] = target.get("role", "reader")
            # 👇 fuerza la recarga para que desaparezca el login y entre al gate ya autenticado
            st.rerun()
            # Nota: tras st.rerun(), esta función re-ejecuta el script completo y ya no
            # volverá a esta rama. El return es decorativo.
            return True
        else:
            st.error("Usuario o contraseña incorrectos")
            return False

def get_role():
    return st.session_state.get("auth_role", None)

def can_edit() -> bool:
    return get_role() == "admin"


def logout():
    """Topbar de sesión en el cuerpo (derecha)."""
    c1, c2, c3 = st.columns([6, 3, 2])
    with c3:
        st.caption(f"Sesión: {st.session_state.get('auth_user','')} ({st.session_state.get('auth_role','')})")
        if st.button("Cerrar sesión", key="logout_btn"):
            # 🔄 Limpiar simulación y otros estados volátiles al cerrar sesión
            for k in ["auth_user", "auth_role", "sim"]:
                st.session_state.pop(k, None)
            st.rerun()



# Gate de autenticación (sin sidebar)
if "auth_user" not in st.session_state:
    logged_in = auth_login()
    if not logged_in:
        st.stop()
else:
    logout()

# -------------------- UI helpers --------------------
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

# -------------------- Data load --------------------
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
    clientes = safe_read("clientes")
    prestamos = safe_read("prestamos")
    pagos = safe_read("pagos")
    parametros = safe_read("parametros")
    integrantes = safe_read("integrantes")
    aportes_tarifas = safe_read("aportes_tarifas")
    aportes_pagos = safe_read("aportes_pagos")

    clientes = ensure_cols(clientes, ["cliente_id","nombre","identificacion","telefono","email","creado_en","actualizado_en"])
    prestamos = ensure_cols(prestamos, ["prestamo_id","display_id","cliente_id","monto","tasa_mensual","fecha_inicio","plan_inicio","plazo_meses","frecuencia","saldo_capital","estado","ultimo_calculo","cuota_fija","creado_en","actualizado_en"])
    pagos = ensure_cols(pagos, ["pago_id","prestamo_id","fecha_pago","monto_pago","interes_aplicado","capital_aplicado","dias","observaciones","creado_en","metodo","saldo_antes","saldo_despues","dias_mora","mora_aplicada","tipo_pago","adelanto_extra"])
    parametros = ensure_cols(parametros, ["clave","valor"])
    integrantes = ensure_cols(integrantes, ["integrante_id","nombre","identificacion","cupos","creado_en","actualizado_en"])
    aportes_tarifas = ensure_cols(aportes_tarifas, ["anio","valor_por_cupo"])
    aportes_pagos = ensure_cols(aportes_pagos, ["aporte_id","integrante_id","periodo","fecha_pago","cupos_pagados","monto_pagado","observaciones","creado_en"])

    if "plan_inicio" not in prestamos.columns:
        prestamos["plan_inicio"] = prestamos.get("fecha_inicio", pd.Series(dtype=object))
    prestamos["plan_inicio"] = prestamos.apply(lambda r: r["plan_inicio"] if not pd.isna(r["plan_inicio"]) else r.get("fecha_inicio"), axis=1)

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
            if (pd.isna(row.get("cuota_fija")) and not pd.isna(row.get("monto")) and not pd.isna(row.get("tasa_mensual")) and not pd.isna(row.get("plazo_meses"))):
                prestamos.at[i, "cuota_fija"] = calcular_cuota_fija(float(row["monto"]), float(row["tasa_mensual"]), int(row["plazo_meses"]))
                changed = True
        except Exception:
            pass

    if changed:
        save_data(clientes, prestamos, pagos, parametros)
        save_aportes_data(integrantes, aportes_tarifas, aportes_pagos)

    return clientes, prestamos, pagos, parametros, integrantes, aportes_tarifas, aportes_pagos

# -------------------- Inversionista --------------------
def _safe_read_sheet(sheet):
    try:
        return read_df(sheet)
    except Exception:
        return pd.DataFrame()

def ensure_inv_cols(inv_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["nombre","capital_inicial","ganancia_anual","ganancia_pendiente","estado","creado_en","actualizado_en"]
    for c in cols:
        if c not in inv_df.columns:
            inv_df[c] = pd.Series(dtype=object)
    return inv_df

def ensure_inv_mov_cols(mov_df: pd.DataFrame) -> pd.DataFrame:
    cols = ["mov_id","fecha","tipo","monto","observaciones","creado_en"]
    for c in cols:
        if c not in mov_df.columns:
            mov_df[c] = pd.Series(dtype=object)
    return mov_df

def load_inversionista():
    inv = _safe_read_sheet("inversionista")
    inv = ensure_inv_cols(inv)
    movs = _safe_read_sheet("inversionista_movs")
    movs = ensure_inv_mov_cols(movs)
    if inv.empty:
        inv = pd.DataFrame([{"nombre":"Teresa Pérez","capital_inicial":1000000,"ganancia_anual":70000,"ganancia_pendiente":70000,"estado":"activo","creado_en":datetime.now(),"actualizado_en":datetime.now()}])
    return inv, movs

def save_inversionista_data(inv_df, movs_df):
    _write_and_update_cache("inversionista", inv_df)
    _write_and_update_cache("inversionista_movs", movs_df)

# -------------------- Persistencia --------------------
def save_data(clientes, prestamos, pagos, parametros, path=EXCEL_PATH):
    _write_and_update_cache("clientes", clientes)
    _write_and_update_cache("prestamos", prestamos)
    _write_and_update_cache("pagos", pagos)
    _write_and_update_cache("parametros", parametros)

def save_aportes_data(integrantes, aportes_tarifas, aportes_pagos):
    _write_and_update_cache("integrantes", integrantes)
    _write_and_update_cache("aportes_tarifas", aportes_tarifas)
    _write_and_update_cache("aportes_pagos", aportes_pagos)

# -------------------- Lógica --------------------
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
        filas.append({"# CUOTA": k, "MES": fecha_k, "SALDO INICIAL": saldo, "CUOTA": cuota_ajustada, "INTERES": interes_k, "CAPITAL": capital_k, "SALDO DESPUES DEL PAGO": saldo_nuevo})
        saldo = saldo_nuevo
    tabla = pd.DataFrame(filas)
    total_interes = int(round(float(tabla["INTERES"].sum())))
    total_pagado = int(round(float(tabla["CUOTA"].sum())))
    return cuota, total_interes, total_pagado, tabla

def contar_cuotas_normales_desde_epoch(pagos_df, prestamo_row):
    if pagos_df.empty:
        return 0
    pid = prestamo_row["prestamo_id"]
    t0 = to_date(prestamo_row.get("plan_inicio")) or to_date(prestamo_row.get("fecha_inicio")) or date.today()
    df = pagos_df[(pagos_df["prestamo_id"] == pid) & (pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x) > t0))]
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
    pagadas = contar_cuotas_normales_desde_epoch(pagos_df, prestamo_row)
    return max(plazo - pagadas, 0)

# Crear/pagar
def crear_cliente_si_no_existe(clientes_df, nombre, identificacion, telefono="", email=""):
    if identificacion and not clientes_df.empty:
        match = clientes_df[clientes_df["identificacion"] == identificacion]
        if not match.empty:
            return clientes_df, match.iloc[0]["cliente_id"]
    cid = str(uuid.uuid4())
    now = datetime.now()
    nuevo = {"cliente_id": cid, "nombre": nombre, "identificacion": identificacion, "telefono": telefono, "email": email, "creado_en": now, "actualizado_en": now}
    clientes_df = pd.concat([clientes_df, pd.DataFrame([nuevo])], ignore_index=True)
    return clientes_df, cid

def crear_prestamo(prestamos_df, cliente_id, monto, tasa_mensual_decimal, fecha_inicio, plazo_meses):
    pid = str(uuid.uuid4())
    now = datetime.now()
    fecha_inicio = to_date(fecha_inicio) or date.today()
    cuota_fija = calcular_cuota_fija(float(monto), float(tasa_mensual_decimal), int(plazo_meses))
    existing_codes = set(str(x) for x in prestamos_df.get("display_id", pd.Series(dtype=str)).dropna().astype(str))
    display_id = generate_display_id(existing_codes, 5)
    nuevo = {"prestamo_id": pid, "display_id": display_id, "cliente_id": cliente_id, "monto": int(round(float(monto))), "tasa_mensual": float(tasa_mensual_decimal), "fecha_inicio": fecha_inicio, "plan_inicio": fecha_inicio, "plazo_meses": int(plazo_meses), "frecuencia": "mensual", "saldo_capital": int(round(float(monto))), "estado": "activo", "ultimo_calculo": fecha_inicio, "cuota_fija": cuota_fija, "creado_en": now, "actualizado_en": now}
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

    prestamos_df.loc[prestamos_df["prestamo_id"] == prestamo_id, ["saldo_capital","ultimo_calculo","actualizado_en"]] = [saldo_despues, fecha_pago, datetime.now()]
    if saldo_despues <= 0:
        prestamos_df.loc[prestamos_df["prestamo_id"] == prestamo_id, "estado"] = "cerrado"

    pago_row = {"pago_id": str(uuid.uuid4()), "prestamo_id": prestamo_id, "fecha_pago": fecha_pago, "monto_pago": monto_total, "interes_aplicado": interes_mes, "capital_aplicado": capital_total, "dias": 0, "observaciones": observaciones, "creado_en": datetime.now(), "metodo": "cuota_mensual", "saldo_antes": saldo_antes, "saldo_despues": saldo_despues, "dias_mora": dias_mora, "mora_aplicada": mora_aplicada, "tipo_pago": "cuota_normal_con_adelanto" if adelanto_extra > 0 else "cuota_normal", "adelanto_extra": adelanto_extra}
    pagos_df = pd.concat([pagos_df, pd.DataFrame([pago_row])], ignore_index=True)
    return prestamos_df, pagos_df, pago_row


# === Exportar simulación como imagen PNG (servidor) ===
from PIL import Image, ImageDraw, ImageFont

def build_sim_image(sim: dict, tabla_df: pd.DataFrame, logo_path: str = "assets/logo.png") -> bytes:
    """
    Genera una imagen PNG con:
    - Título y datos del cliente/fecha
    - Métricas clave (cuota, interés total, total pagado)
    - Tabla de amortización (primeras N filas)
    - Marca de agua con el logo (si existe)
    Retorna los bytes del PNG listo para descargar.
    """
    # --- Parámetros de layout
    W = 1200
    padding = 30
    row_h = 28
    max_rows = 30  # limita filas para que no quede gigante

    # --- Formateos
    P_fmt = format_cop(sim["P"])
    cuota_fmt = format_cop(sim["cuota"])
    tint_fmt = format_cop(sim["t_int"])
    tpag_fmt = format_cop(sim["t_pag"])
    tasa_pct = f"{round(sim['i_m']*100, 2)}%"
    f1 = (sim["f_inicio"].strftime("%Y-%m-%d") if isinstance(sim["f_inicio"], date) else str(sim["f_inicio"]))
    nombre = sim.get("nombre") or "—"

    # --- Altura estimada
    rows = min(len(tabla_df), max_rows)
    H = 260 + rows*row_h + 50  # header + métricas + tabla + margen

    # --- Canvas
    img = Image.new("RGBA", (W, H), (255, 255, 255, 255))
    draw = ImageDraw.Draw(img)

    # --- Fuentes (con fallback)
    try:
        font_title = ImageFont.truetype("DejaVuSans-Bold.ttf", 32)
        font_body  = ImageFont.truetype("DejaVuSans.ttf", 16)
        font_bold  = ImageFont.truetype("DejaVuSans-Bold.ttf", 16)
        font_small = ImageFont.truetype("DejaVuSans.ttf", 13)
    except Exception:
        font_title = font_body = font_bold = font_small = ImageFont.load_default()

    # --- Título y datos
    y = padding
    draw.text((padding, y), "Fondo Bonilla — Simulación", fill=(12, 122, 67, 255), font=font_title)
    y += 46
    draw.text((padding, y), f"Cliente: {nombre}", fill=(17, 24, 39, 255), font=font_body)
    y += 24
    draw.text((padding, y),
              f"Monto : {P_fmt}   ·   Tasa mensual: {tasa_pct}   ·   # Meses: {sim['n']}   ·   Fecha Desembolso: {f1}",
              fill=(17, 24, 39, 255), font=font_body)
    y += 28
    draw.text((padding, y), f"Cuota fija: {cuota_fmt}", fill=(17, 24, 39, 255), font=font_bold)
    y += 22
    draw.text((padding, y), f"Interés total: {tint_fmt}   ·   Total a pagar: {tpag_fmt}",
              fill=(17, 24, 39, 255), font=font_body)
    y += 28

    # --- Marca de agua (logo)
    try:
        lp = Path(logo_path)
        if lp.exists():
            logo = Image.open(lp).convert("RGBA")
            lw = 380
            ratio = lw / float(logo.width)
            lh = int(logo.height * ratio)
            logo = logo.resize((lw, lh), Image.LANCZOS)
            # opacidad sutil
            alpha = logo.split()[3].point(lambda a: int(a * 0.12))
            logo.putalpha(alpha)
            # centro aprox
            img.paste(logo, ((W - lw)//2, (H - lh)//3), logo)
    except Exception:
        pass

    # --- Tabla (cabecera + filas)
    cols = ["# CUOTA","MES","SALDO INICIAL","CUOTA","INTERES","CAPITAL","SALDO DESPUES DEL PAGO"]
    col_widths = [90, 140, 170, 140, 140, 140, 230]
    x0 = padding
    y0 = y + 8

    # cabecera
    draw.rectangle([x0-4, y0-6, x0 + sum(col_widths)+4, y0 + row_h], fill=(245,247,249,255), outline=(229,231,235,255))
    x = x0
    for i, c in enumerate(cols):
        draw.text((x+6, y0), c, fill=(17,24,39,255), font=font_bold)
        x += col_widths[i]
    y = y0 + row_h + 4

    # normalizar/formatos
    df = tabla_df.copy()
    df["MES"] = df["MES"].apply(lambda d: to_date(d).strftime("%Y-%m") if to_date(d) else str(d))

    for i in range(rows):
        x = x0
        r = df.iloc[i]
        vals = [
            str(r["# CUOTA"]),
            r["MES"],
            format_cop(r["SALDO INICIAL"]),
            format_cop(r["CUOTA"]),
            format_cop(r["INTERES"]),
            format_cop(r["CAPITAL"]),
            format_cop(r["SALDO DESPUES DEL PAGO"]),
        ]
        # zebra
        if i % 2 == 0:
            draw.rectangle([x-4, y-4, x + sum(col_widths)+4, y + row_h-6], fill=(250,250,250,255))
        for j, txt in enumerate(vals):
            draw.text((x+6, y), txt, fill=(17,24,39,255), font=font_small)
            x += col_widths[j]
        y += row_h

    if len(df) > rows:
        draw.text((x0, y+8), f"… {len(df)-rows} filas más", fill=(107,114,128,255), font=font_small)

    # --- A PNG
    out = BytesIO()
    img.convert("RGB").save(out, format="PNG", optimize=True)
    out.seek(0)
    return out.getvalue()



# -------------------- Layout (sin sidebar) --------------------
data = get_data()
(clientes, prestamos, pagos, parametros, integrantes, aportes_tarifas, aportes_pagos) = (
    data["clientes"], data["prestamos"], data["pagos"], data["parametros"], data["integrantes"], data["aportes_tarifas"], data["aportes_pagos"]
)

TABS = ["📊 Panel", "➕ Nuevo Préstamo", "💳 Registrar Pago", "📑 Reportes", "🧮 Simulador", "🗃️ Datos / Parámetros", "♻️ Re‑amortización", "👥 Aportes", "👤 Inversionista"]
if "nav_tabs" not in st.session_state:
    st.session_state["nav_tabs"] = TABS[0]
sel = st.radio("Navegación", options=TABS, key="nav_tabs")

# ---- Pestañas ----
if sel == TABS[0]:
    st.subheader("Panel general")

    def resumen_fondo_ext(prestamos_df, pagos_df, parametros_df, integ_df=None, aportes_pagos_df=None, inv_df=None, inv_movs_df=None):
        capital_inicial = 0.0
        row = parametros_df[parametros_df["clave"] == "capital_inicial"]
        if not row.empty:
            try:
                capital_inicial = float(row.iloc[0]["valor"])
            except Exception:
                capital_inicial = 0.0

        total_desembolsado = float(prestamos_df.get("monto", pd.Series(dtype=float)).sum()) if not prestamos_df.empty else 0.0
        saldo_capital_total = float(prestamos_df.get("saldo_capital", pd.Series(dtype=float)).sum()) if not prestamos_df.empty else 0.0
        intereses_cobrados = float(pagos_df.get("interes_aplicado", pd.Series(dtype=float)).sum()) if not pagos_df.empty else 0.0
        capital_recuperado = float(pagos_df.get("capital_aplicado", pd.Series(dtype=float)).sum()) if not pagos_df.empty else 0.0
        aportes_cobrados = float(aportes_pagos_df["monto_pagado"].sum()) if (aportes_pagos_df is not None and not aportes_pagos_df.empty) else 0.0

        # Egresos inversionista (dividendos + cancelación)
        pagos_a_inversionista = 0.0
        if inv_movs_df is not None and not inv_movs_df.empty:
            egresos_mask = (
                inv_movs_df["tipo"]
                .astype(str).str.strip().str.lower()
                .isin(["dividendo", "cancelacion"])
            )
            montos_egreso = pd.to_numeric(
                inv_movs_df.loc[egresos_mask, "monto"], errors="coerce"
            ).fillna(0)
            pagos_a_inversionista = float(montos_egreso.sum())

        # Caja neta (restando egresos)
        caja_actual = capital_inicial - total_desembolsado + (intereses_cobrados + capital_recuperado + aportes_cobrados) - pagos_a_inversionista
        total_prestado = saldo_capital_total
        total_fondo = caja_actual + total_prestado

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

        hoy = date.today()
        if not pagos_df.empty and "fecha_pago" in pagos_df.columns:
            mask_year = pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x).year == hoy.year)
            intereses_anio = float(pagos_df.loc[mask_year, "interes_aplicado"].sum()) if "interes_aplicado" in pagos_df.columns else 0.0
        else:
            intereses_anio = 0.0

        total_cupos = int(integ_df["cupos"].sum()) if (integ_df is not None and not integ_df.empty) else 0
        total_por_cupo = (total_sin_inversion / total_cupos) if total_cupos > 0 else 0.0

        return {"caja_actual": caja_actual, "total_prestado": total_prestado, "intereses_anio": intereses_anio, "total_fondo": total_fondo, "total_sin_inversion": total_sin_inversion, "pasivo_inversionista": pasivo_inv, "total_por_cupo": total_por_cupo, "total_cupos": total_cupos}

    inv_df, inv_movs_df = load_inversionista()
    R = resumen_fondo_ext(prestamos, pagos, parametros, integrantes, aportes_pagos, inv_df, inv_movs_df)

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
        activos_show = activos[["display_id","cliente","monto","tasa_%","plazo_meses","cuotas_restantes","saldo_capital","estado","cuota_fija","plan_inicio","fecha_inicio","ultimo_calculo","creado_en"]].copy()
        for c in ["monto","saldo_capital","cuota_fija"]:
            activos_show[c] = activos_show[c].apply(format_cop)
        activos_show = normalize_datetime_cols(activos_show, ["plan_inicio","fecha_inicio","ultimo_calculo","creado_en"], to_string=True)
        st.dataframe(activos_show, use_container_width=True)

elif sel == TABS[1]:
    st.subheader("Registrar nuevo préstamo (mensual)")
    if not can_edit():
        st.info(READ_ONLY_MSG)
    else:
        with st.expander("Crear/Seleccionar cliente", expanded=False):
            with st.form("form_cliente"):
                colA, colB, colC, colD = st.columns(4)
                nombre = colA.text_input("Nombre", key="newloan_cli_nombre")
                identificacion = colB.text_input("Identificación", key="newloan_cli_id")
                telefono = colC.text_input("Teléfono", key="newloan_cli_tel")
                email = colD.text_input("Email", key="newloan_cli_email")
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
            cliente_sel = st.selectbox("Cliente", options=labels, key="newloan_cli_select")
            cliente_id = id_map[cliente_sel]

            col1, col2, col3, col4 = st.columns(4)
            monto = col1.number_input("Monto (COP)", min_value=0.0, value=1_000_000.0, step=50_000.0, key="newloan_monto")
            tasa_text = col2.text_input("Tasa mensual (%)", value="3%", key="newloan_tasa")
            plazo_meses = col3.number_input("Plazo (meses)", min_value=1, value=6, step=1, key="newloan_plazo")
            fecha_inicio = col4.date_input("Fecha de inicio", value=date.today(), key="newloan_fecha")

            if st.button("➕ Crear préstamo", key="newloan_create"):
                tasa_mensual_decimal = parse_percent(tasa_text)
                prestamos, pid = crear_prestamo(prestamos, cliente_id, monto, tasa_mensual_decimal, fecha_inicio, plazo_meses)
                save_data(clientes, prestamos, pagos, parametros)
                pr = prestamos[prestamos["prestamo_id"] == pid].iloc[0]
                st.success(f"Préstamo creado — ID: {pr['display_id']} · Cuota fija: {format_cop(pr['cuota_fija'])}")

elif sel == TABS[2]:
    st.subheader("Registrar pago de cuota (fija) + adelanto extra (opcional)")
    if not can_edit():
        st.info(READ_ONLY_MSG)
    else:
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

            elegido_lbl = st.selectbox("Préstamo", options=opciones_labels, key="pay_select_prestamo")
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

                fecha_p = st.date_input("Fecha de pago", value=date.today(), key="pay_fecha")
                adelanto_extra = st.number_input("Adelanto extra a capital (COP)", min_value=0, value=0, step=50_000, key="pay_extra")
                observaciones = st.text_input("Observaciones", key="pay_obs")
                modo_reauto = st.selectbox("Re‑amortizar automáticamente el abono extra?", options=["No re‑amortizar","Reducir plazo (aumentar cuota)","Reducir cuota (aumentar plazo)"], key="pay_reauto")

                saldo_antes = safe_int(pr.get("saldo_capital"), default=0)
                interes_mes = int(round(saldo_antes * float(pr["tasa_mensual"]) if not pd.isna(pr["tasa_mensual"]) else 0.0))
                f_esp = fecha_esperada(pr, pagos)
                dias_mora, mora_aplicada = calcular_mora(saldo_antes, to_date(fecha_p) or date.today(), f_esp)
                cuota_fija = safe_int(pr.get("cuota_fija"), default=0)
                total_a_pagar = cuota_fija + mora_aplicada + safe_int(adelanto_extra, default=0)
                st.caption(f"Interés: {format_cop(interes_mes)} · Mora: {format_cop(mora_aplicada)} · Total: {format_cop(total_a_pagar)}")
                st.warning("La cuota normal no admite montos inferiores a la cuota fija.", icon="⚠️")

                if st.button("💾 Registrar pago", key="pay_save"):
                    try:
                        prestamos, pagos, pago_row = registrar_pago_cuota(prestamos, pagos, pid, fecha_p, adelanto_extra, observaciones)
                        saldo_despues = safe_int(pago_row["saldo_despues"], default=0)
                        tasa = float(pr["tasa_mensual"]) if not pd.isna(pr["tasa_mensual"]) else 0.0
                        cuota_actual = safe_int(pr.get("cuota_fija"), default=0)
                        plazo_actual = safe_int(pr.get("plazo_meses"), default=1)

                        if saldo_despues > 0 and safe_int(adelanto_extra, default=0) > 0 and modo_reauto != "No re‑amortizar":
                            if modo_reauto == "Reducir plazo (aumentar cuota)":
                                n_calc = resolver_plazo_por_cuota(saldo_despues, tasa, cuota_actual)
                                cuota_calc = calcular_cuota_fija(saldo_despues, tasa, n_calc)
                                prestamos.loc[prestamos["prestamo_id"] == pid, ["plazo_meses","cuota_fija","plan_inicio","actualizado_en"]] = [int(n_calc), int(cuota_calc), to_date(fecha_p) or date.today(), datetime.now()]
                                st.info(f"Re‑amortización automática aplicada: nuevo plazo {int(n_calc)} meses · cuota {format_cop(cuota_calc)}")
                            elif modo_reauto == "Reducir cuota (aumentar plazo)":
                                pr_tmp = pr.copy()
                                pr_tmp["plan_inicio"] = to_date(fecha_p) or date.today()
                                pagadas_prev = contar_cuotas_normales_desde_epoch(pagos, pr_tmp)
                                n_rest = max(plazo_actual - pagadas_prev, 1)
                                cuota_calc = calcular_cuota_fija(saldo_despues, tasa, n_rest)
                                prestamos.loc[prestamos["prestamo_id"] == pid, ["plazo_meses","cuota_fija","plan_inicio","actualizado_en"]] = [int(n_rest), int(cuota_calc), to_date(fecha_p) or date.today(), datetime.now()]
                                st.info(f"Re‑amortización automática aplicada: nuevo plazo {int(n_rest)} meses · nueva cuota {format_cop(cuota_calc)}")

                        save_data(clientes, prestamos, pagos, parametros)
                        st.success(
                            f"Pago OK. Interés: {format_cop(pago_row['interes_aplicado'])} · "
                            f"Capital: {format_cop(pago_row['capital_aplicado'])} · "
                            f"Mora: {format_cop(pago_row['mora_aplicada'])} · "
                            f"Saldo antes: {format_cop(pago_row['saldo_antes'])} → después: {format_cop(pago_row['saldo_despues'])}"
                        )
                    except Exception as e:
                        st.error(f"Error registrando pago: {e}")

elif sel == TABS[3]:
    st.subheader("Reportes y exportación")
    st.markdown("### Préstamos")
    prs = prestamos.copy()
    prs["cliente"] = prs["cliente_id"].apply(lambda cid: nombre_cliente_por_id(clientes, cid))
    prs["tasa_%"] = prs["tasa_mensual"].apply(percent_str)
    prs["cuotas_restantes"] = prs.apply(lambda r: cuotas_restantes(r, pagos), axis=1)
    nombres_opts = ["Todos"] + sorted(list(set(prs["cliente"].dropna().astype(str).tolist())))
    sel_cli = st.selectbox("Filtrar por cliente", options=nombres_opts, key="repo_cli_filter")
    if sel_cli != "Todos":
        prs = prs[prs["cliente"] == sel_cli]
    prs_show = prs[["display_id","cliente","monto","tasa_%","plazo_meses","cuotas_restantes","saldo_capital","estado","cuota_fija","plan_inicio","fecha_inicio","ultimo_calculo","creado_en"]].copy()
    for c in ["monto","saldo_capital","cuota_fija"]:
        prs_show[c] = prs_show[c].apply(format_cop)
    prs_show = normalize_datetime_cols(prs_show, ["plan_inicio","fecha_inicio","ultimo_calculo","creado_en"], to_string=True)
    st.dataframe(prs_show, use_container_width=True)

    st.markdown("### Pagos")
    pagos_show = pagos.copy()
    for c in ["monto_pago","interes_aplicado","capital_aplicado","mora_aplicada"]:
        if c in pagos_show.columns:
            pagos_show[c] = pagos_show[c].apply(format_cop)
    pagos_show = normalize_datetime_cols(pagos_show, ["fecha_pago","creado_en"], to_string=True)
    st.dataframe(pagos_show, use_container_width=True)

    def total_fondo_fin_mes(prestamos_df, pagos_df, parametros_df, aportes_pagos_df, fecha_corte):
        capital_inicial = 0.0
        row = parametros_df[parametros_df["clave"] == "capital_inicial"]
        if not row.empty:
            try:
                capital_inicial = float(row.iloc[0]["valor"])
            except:
                pass
        desembolsado_hasta = float(prestamos_df[prestamos_df["fecha_inicio"].apply(lambda x: to_date(x) and to_date(x) <= fecha_corte)]["monto"].sum())
        intereses_hasta = float(pagos_df[pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x) <= fecha_corte)]["interes_aplicado"].sum()) if ("interes_aplicado" in pagos_df.columns and not pagos_df.empty) else 0.0
        capital_rec_hasta = float(pagos_df[pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x) <= fecha_corte)]["capital_aplicado"].sum()) if ("capital_aplicado" in pagos_df.columns and not pagos_df.empty) else 0.0
        aportes_hasta = float(aportes_pagos_df[aportes_pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x) <= fecha_corte)]["monto_pagado"].sum()) if (aportes_pagos_df is not None and not aportes_pagos_df.empty) else 0.0
        caja = capital_inicial - desembolsado_hasta + (intereses_hasta + capital_rec_hasta + aportes_hasta)
        saldo_capital = 0
        for _, pr in prestamos_df.iterrows():
            monto0 = safe_int(pr.get("monto") if "monto" in pr else 0, default=0)
            pid = pr.get("prestamo_id")
            pagos_pr = pagos_df[(pagos_df["prestamo_id"] == pid) & (pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x) <= fecha_corte))]
            cap_rec = safe_int(pagos_pr["capital_aplicado"].sum() if not pagos_pr.empty else 0, default=0)
            saldo_capital += max(monto0 - cap_rec, 0)
        return caja + saldo_capital

    def rendimiento_anual(prestamos_df, pagos_df, parametros_df, aportes_pagos_df, anio):
        intereses_anio = float(pagos_df[pagos_df["fecha_pago"].apply(lambda x: to_date(x) and to_date(x).year == anio)]["interes_aplicado"].sum()) if ("interes_aplicado" in pagos_df.columns and not pagos_df.empty) else 0.0
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




elif sel == TABS[4]:
    st.subheader("Simulador de crédito — método francés")

    # --- Nombre de la persona/cliente
    nombre_persona = st.text_input("Nombre de la persona / cliente", key="sim_nombre")

    # ---- Entradas del simulador
    colA, colB, colC, colD = st.columns(4)
    principal = colA.number_input("Monto (P)", min_value=0.0, value=1_000_000.0, step=50_000.0, key="sim_monto")
    tasa_text = colB.text_input("Tasa mensual (%)", value="3%", key="sim_tasa")
    n = colC.number_input("Meses (n)", min_value=1, value=6, step=1, key="sim_n")
    f_inicio = colD.date_input("Fecha Desembolso", value=date.today(), key="sim_fecha")

    if st.button("🧮 Calcular simulación", key="sim_calc"):
        try:
            i_m = parse_percent(tasa_text)
            cuota, t_int, t_pag, tabla = simulador_cuotas_fijas(principal, i_m, int(n), f_inicio)
            st.session_state["sim"] = {
                "P": principal, "i_m": i_m, "n": int(n), "f_inicio": f_inicio,
                "cuota": cuota, "t_int": t_int, "t_pag": t_pag, "tabla": tabla,
                "nombre": nombre_persona
            }
        except Exception as e:
            st.error(f"Error en simulación: {e}")

    sim = st.session_state.get("sim")
    if sim:
        # --- Mostramos en la app normal (con marca de agua de sección)
        if _logo_b64():
            st.markdown('<div class="fb-watermark">', unsafe_allow_html=True)
        else:
            st.markdown('<div>', unsafe_allow_html=True)

        # Cabecera
        top_l, top_r = st.columns([3, 1])
        with top_l:
            st.markdown(
                f"**Cliente:** {sim.get('nombre') or (nombre_persona or '—')}  \n"
                f"**Fecha de simulación:** {date.today().strftime('%Y-%m-%d')}"
            )
        with top_r:
            if _logo_b64():
                st.image(LOGO_PATH, width=70)

        # Métricas
        c1, c2, c3 = st.columns(3)
        c1.metric("Cuota fija", format_cop(sim["cuota"]))
        c2.metric("Interés total", format_cop(sim["t_int"]))
        c3.metric("Total pagado", format_cop(sim["t_pag"]))

        # Tabla
        tabla_show = sim["tabla"].copy()
        for c in ["SALDO INICIAL","CUOTA","INTERES","CAPITAL","SALDO DESPUES DEL PAGO"]:
            tabla_show[c] = tabla_show[c].apply(format_cop)
        st.markdown("### Tabla de amortización")
        st.dataframe(tabla_show, use_container_width=True)

        st.markdown('</div>', unsafe_allow_html=True)  # cierra fb-watermark

        # --- PREPARAR HTML espejado para CAPTURA dentro del iframe (sin tocar el DOM padre)
        #     Así evitamos restricciones de iframe/sandbox y descargas bloqueadas.
        b64 = _logo_b64() or ""
        P_fmt = format_cop(sim["P"])
        cuota_fmt = format_cop(sim["cuota"])
        tint_fmt = format_cop(sim["t_int"])
        tpag_fmt = format_cop(sim["t_pag"])
        tasa_pct = f"{round(sim['i_m']*100, 2)}%"
        f1 = (sim["f_inicio"].strftime("%Y-%m-%d") if isinstance(sim["f_inicio"], date) else str(sim["f_inicio"]))
        nombre_html = sim.get("nombre") or (nombre_persona or "—")

        # Tabla formateada a HTML
        tabla_cap = sim["tabla"].copy()
        for c in ["SALDO INICIAL","CUOTA","INTERES","CAPITAL","SALDO DESPUES DEL PAGO"]:
            tabla_cap[c] = tabla_cap[c].apply(format_cop)
        tabla_html = tabla_cap.to_html(index=False, border=0, classes="fb-table")
        tabla_html = tabla_html.replace('"', '&quot;')  # escapar comillas para incrustar


        # === Botón de descarga confiable (servidor) ===
        try:
            img_bytes = build_sim_image(sim, sim["tabla"], LOGO_PATH)
            st.download_button(
                "⬇️ Descargar imagen del simulador (PNG)",
                data=img_bytes,
                file_name="Simulacion_Fondo_Bonilla.png",
                mime="image/png",
                key="dl_sim_png_server"
            )
        except Exception as e:
            st.warning(f"No fue posible generar la imagen en el servidor: {e}")



elif sel == TABS[5]:
    st.subheader("Parámetros del fondo")
    if not can_edit():
        st.info(READ_ONLY_MSG)
    else:
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

elif sel == TABS[6]:
    st.subheader("Re‑amortización de préstamo (actualiza plan_inicio)")
    if not can_edit():
        st.info(READ_ONLY_MSG)
    else:
        activos = prestamos[prestamos["estado"].fillna("activo") == "activo"]
        if activos.empty:
            st.info("No hay préstamos activos.")
        else:
            labels, id_map = [], {}
            for _, pr_item in activos.iterrows():
                nombre_cli = nombre_cliente_por_id(clientes, pr_item["cliente_id"])
                labels.append(f"{nombre_cli} · ID:{pr_item['display_id']} · Saldo:{format_cop(pr_item['saldo_capital'])}")
                id_map[labels[-1]] = pr_item["prestamo_id"]

            sel_label = st.selectbox("Préstamo", options=labels, key="rea_select")
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
                except Exception as e:
                    st.warning(f"No se puede estimar el plazo con la cuota indicada: {e}")

                if st.button("Aplicar re‑amortización", key="rea_apply_reduce_cuota"):
                    try:
                        cuota_in = safe_int(nueva_cuota_deseada, default=safe_int(pr.get("cuota_fija"), default=1))
                        if cuota_in <= 0:
                            raise ValueError("Cuota deseada inválida (debe ser > 0).")
                        n_calc = resolver_plazo_por_cuota(saldo_actual, i_m, cuota_in)
                        cuota_calc = calcular_cuota_fija(saldo_actual, i_m, n_calc)
                        prestamos.loc[prestamos["prestamo_id"] == pid_rea, ["tasa_mensual","plazo_meses","cuota_fija","plan_inicio","actualizado_en"]] = [i_m, int(n_calc), int(cuota_calc), to_date(fecha_rea) or date.today(), datetime.now()]
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
                except Exception as e:
                    st.warning(f"No se puede estimar la cuota con el plazo indicado: {e}")

                if st.button("Aplicar re‑amortización", key="rea_apply_reduce_plazo"):
                    try:
                        plazo_in = max(safe_int(nuevo_plazo, default=safe_int(pr.get("plazo_meses"), default=1)), 1)
                        nueva_cuota_calc = calcular_cuota_fija(saldo_actual, i_m, plazo_in)
                        prestamos.loc[prestamos["prestamo_id"] == pid_rea, ["tasa_mensual","plazo_meses","cuota_fija","plan_inicio","actualizado_en"]] = [i_m, int(plazo_in), int(nueva_cuota_calc), to_date(fecha_rea) or date.today(), datetime.now()]
                        save_data(clientes, prestamos, pagos, parametros)
                        st.success(f"Re‑amortizado: nuevo plazo {int(plazo_in)} meses · nueva cuota {format_cop(nueva_cuota_calc)} · plan desde {to_date(fecha_rea) or date.today()}")
                    except Exception as e:
                        st.error(f"Error al re‑amortizar: {e}")

elif sel == TABS[7]:
    st.subheader("Aportes de integrantes (registro por período)")
    if not can_edit():
        st.info(READ_ONLY_MSG)
    else:
        integ_edit = normalize_datetime_cols(integrantes.copy(), ["creado_en","actualizado_en"], to_string=True)
        st.dataframe(integ_edit, use_container_width=True)

        with st.form("form_integrantes_edit"):
            nombre_edit = st.text_input("Nombre del integrante a actualizar", key="ap_integ_edit_nombre")
            cupos_nuevo = st.number_input("Nuevo número de cupos", min_value=0, value=0, step=1, key="ap_integ_edit_cupos")
            submitted_edit = st.form_submit_button("Actualizar cupos")
        if submitted_edit:
            mask = integrantes["nombre"].str.lower() == nombre_edit.strip().lower()
            if mask.any():
                integrantes.loc[mask, ["cupos","actualizado_en"]] = [int(cupos_nuevo), datetime.now()]
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
                nuevos_aportes.append({"aporte_id": str(uuid.uuid4()), "integrante_id": row["integrante_id"], "periodo": periodo, "fecha_pago": fecha_pago_ap, "cupos_pagados": int(cupos_pag), "monto_pagado": monto, "observaciones": "", "creado_en": datetime.now()})

        if st.button("💾 Registrar aportes del período", key="ap_guardar_periodo"):
            if nuevos_aportes:
                aportes_pagos = pd.concat([aportes_pagos, pd.DataFrame(nuevos_aportes)], ignore_index=True)
                save_aportes_data(integrantes, aportes_tarifas, aportes_pagos)
                st.success(f"Aportes registrados: {len(nuevos_aportes)}")
            else:
                st.info("No se seleccionaron aportes.")

elif sel == TABS[8]:
    st.subheader("Inversionista — Teresa Pérez")
    if not can_edit():
        st.info(READ_ONLY_MSG)
    else:
        inv_df, inv_movs_df = load_inversionista()
        if inv_df.empty:
            st.warning("No hay datos del inversionista; se inicializarán al guardar.", icon="⚠️")
        else:
            inv = inv_df.iloc[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Capital inicial", format_cop(inv.get("capital_inicial", 0)))
            c2.metric("Ganancia anual", format_cop(inv.get("ganancia_anual", 0)))
            c3.metric("Ganancia pendiente", format_cop(inv.get("ganancia_pendiente", inv.get("ganancia_anual", 0))))
            st.caption(f"Total de referencia: {format_cop((inv.get('capital_inicial',0) or 0) + (inv.get('ganancia_pendiente', inv.get('ganancia_anual',0)) or 0))}")
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
                            mov = {"mov_id": str(uuid.uuid4()), "fecha": to_date(fecha_div) or date.today(), "tipo": "dividendo", "monto": pend, "observaciones": "entrega dividendo anual", "creado_en": datetime.now()}
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
                            mov = {"mov_id": str(uuid.uuid4()), "fecha": to_date(fecha_cap) or date.today(), "tipo": "cancelacion", "monto": cap, "observaciones": "devolución de capital", "creado_en": datetime.now()}
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
        movs_show = normalize_datetime_cols(movs_show, ["fecha","creado_en"], to_string=True)
        if not movs_show.empty:
            movs_show["monto"] = movs_show["monto"].apply(format_cop)
            st.dataframe(movs_show, use_container_width=True)

















