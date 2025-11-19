from __future__ import annotations

from datetime import datetime, timedelta
import csv, io, re, hashlib
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


# =========================
# CONFIG
# =========================

GOOGLE_CONN_ID = "GoogleSheets"
POSTGRES_CONN_ID = "Domidai-DB"

SPREADSHEET_ID = "1HSFqe6BE73cE0EqtFgVVcyv7_IFgZ02VteJARMFFwPI"
RANGE_VIVIENDAS = "VIVIENDAS!A1:Z"

TARGET_SCHEMA = "public"
TABLE_PROPIETARIOS = "propietarios"
TABLE_VIVIENDAS = "viviendas"
TABLE_VIVIENDA_PROP = "mapeo_vivienda_propietario"

EXCLUDED_REFS = {
    "Casa Pedro", "160.1", "160.3", "192", "185", "138", "142"
}


# =========================
# HELPERS
# =========================

def _normalize_col(col: str) -> str:
    # quita tildes, pasa a lower, sustituye espacios/caracteres raros por "_"
    rep = (
        ("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"), ("ñ", "n"),
        ("Á", "a"), ("É", "e"), ("Í", "i"), ("Ó", "o"), ("Ú", "u"), ("Ñ", "n")
    )
    for a, b in rep:
        col = col.replace(a, b)
    col = col.strip().lower()
    col = re.sub(r"[^a-z0-9]+", "_", col)
    col = re.sub(r"_+", "_", col).strip("_")
    return col


def read_sheet_as_df() -> pd.DataFrame:
    hook = GSheetsHook(gcp_conn_id=GOOGLE_CONN_ID)
    values = hook.get_values(spreadsheet_id=SPREADSHEET_ID, range_=RANGE_VIVIENDAS)

    if not values or len(values) < 2:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]

    # Aseguramos que todas las filas tengan mismo nº de columnas que el header
    n_cols = len(header)
    padded_rows = []
    for r in rows:
        if len(r) < n_cols:
            r = r + [""] * (n_cols - len(r))
        elif len(r) > n_cols:
            r = r[:n_cols]
        padded_rows.append(r)

    df = pd.DataFrame(padded_rows, columns=header).fillna("")
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Normalizar nombres de columnas
    df.columns = [_normalize_col(c) for c in df.columns]

    # Renombrado a nombres "canónicos"
    rename_hint = {
        "ref": "ref",
        "nombre": "nombre",
        "cuenta": "cuenta",
        "dni": "dni",
        "correo_electronico": "correo_electronico",
        "direccion_fiscal": "direccion_fiscal",
        "direccion_vivienda_alquilada": "direccion_vivienda_alquilada",
        "ingreso_limpieza": "ingreso_limpieza",
        "camas": "camas",
        "m2": "m2",
        "dormitorios": "dormitorios",
        "banos": "banos",
        "terrazas": "terrazas",
        "estimacion_horas": "estimacion_horas",
        "tipo_limpieza": "tipo_limpieza",
        "factura_plataforma": "factura_plataforma",
    }
    df = df.rename(columns={k: v for k, v in rename_hint.items() if k in df.columns})

    # Eliminar columnas que no queremos usar
    df = df.drop(columns=["estimacion_horas", "tipo_limpieza", "factura_plataforma"], errors="ignore")

    return df


def stable_owner_id(owner_key: str) -> int:
    """
    Genera un entero estable a partir de owner_key usando MD5.
    Mismo owner_key -> mismo id_propietario siempre.
    """
    h = hashlib.md5(owner_key.encode("utf-8")).hexdigest()
    return int(h[:15], 16)  # 15 hex ~ 60 bits, cabe en BIGINT


def build_owner_key(nombre: str, dni: str, cuenta: str, correo: str) -> str:
    dni = (dni or "").strip()
    nombre = (nombre or "").strip()
    cuenta = (cuenta or "").strip()
    correo = (correo or "").strip()

    if dni:
        return f"DNI::{dni}"
    return f"COMP::{nombre}|{cuenta}|{correo}"


def parse_ingreso(value):
    """
    Convierte la columna ingreso_limpieza:
    - "50-60" -> 55.0
    - "30 - 45" -> 37.5
    - "80" -> 80.0
    - Otros / no parseable -> None
    """
    if pd.isna(value):
        return None

    s = str(value).strip()
    if not s:
        return None

    # número simple
    if s.replace('.', '', 1).isdigit():
        return float(s)

    # rango "x-y"
    if "-" in s:
        parts = s.split("-")
        if len(parts) == 2:
            try:
                low = float(parts[0].strip())
                high = float(parts[1].strip())
                return (low + high) / 2
            except ValueError:
                return None

    return None


def create_tables(pg: PostgresHook):
    ddl = f"""
    DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{TABLE_VIVIENDA_PROP}" CASCADE;
    DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{TABLE_VIVIENDAS}" CASCADE;
    DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{TABLE_PROPIETARIOS}" CASCADE;

    CREATE TABLE "{TARGET_SCHEMA}"."{TABLE_PROPIETARIOS}" (
        id_propietario       BIGINT PRIMARY KEY,
        nombre               TEXT,
        dni                  TEXT,
        cuenta               TEXT,
        correo_electronico   TEXT,
        direccion_fiscal     TEXT
    );

    CREATE TABLE "{TARGET_SCHEMA}"."{TABLE_VIVIENDAS}" (
        ref                        TEXT PRIMARY KEY,
        direccion_vivienda_alquilada TEXT,
        ingreso_limpieza           NUMERIC(12,2),
        camas                      INTEGER,
        m2                         INTEGER,
        dormitorios                INTEGER,
        banos                      INTEGER,
        terrazas                   INTEGER
    );

    CREATE TABLE "{TARGET_SCHEMA}"."{TABLE_VIVIENDA_PROP}" (
        ref            TEXT REFERENCES "{TARGET_SCHEMA}"."{TABLE_VIVIENDAS}"(ref),
        id_propietario BIGINT REFERENCES "{TARGET_SCHEMA}"."{TABLE_PROPIETARIOS}"(id_propietario),
        PRIMARY KEY (ref, id_propietario)
    );
    """
    pg.run(ddl)


def dataframe_to_table_copy(pg: PostgresHook, df: pd.DataFrame, fqtn: str):
    """
    Carga un DataFrame en Postgres usando COPY y psycopg2, limpiando NaN/<NA> -> NULL.
    """
    if df.empty:
        return

    df = df.copy()
    cols = list(df.columns)

    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf, quoting=csv.QUOTE_MINIMAL)

    for row in df.itertuples(index=False, name=None):
        clean_row = []
        for v in row:
            if pd.isna(v):
                clean_row.append("")  # cadena vacía -> NULL en COPY CSV
            else:
                clean_row.append(v)
        writer.writerow(clean_row)

    csv_buf.seek(0)

    copy_sql = (
        f'COPY {fqtn} ({", ".join(f"{c}" for c in cols)}) '
        "FROM STDIN WITH (FORMAT CSV)"
    )

    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            cur.copy_expert(sql=copy_sql, file=csv_buf)
        conn.commit()
    finally:
        conn.close()


# =========================
# ETL PRINCIPAL
# =========================

def etl_viviendas_y_propietarios(**_):
    df = read_sheet_as_df()
    if df.empty:
        raise ValueError("La hoja 'VIVIENDAS' está vacía o no se pudo leer.")

    if "ref" not in df.columns:
        raise ValueError("No se encontró la columna 'REF' (como 'ref') en la hoja.")

    # Normalizar REF (string, trim)
    df["ref"] = df["ref"].astype(str).str.strip()

    # Normalización específica: 100.1 -> 100, 100.2 -> 101
    df["ref"] = df["ref"].replace({
        "100.1": "100",
        "100.2": "101",
    })

    # Normalizar INGRESO LIMPIEZA (rango -> media)
    if "ingreso_limpieza" in df.columns:
        df["ingreso_limpieza"] = df["ingreso_limpieza"].apply(parse_ingreso)

    # Excluir referencias obsoletas
    df = df[~df["ref"].isin(EXCLUDED_REFS)].copy()

    # Convertir numéricos (enteros)
    for c in ("camas", "m2", "dormitorios", "banos", "terrazas"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # =========================
    # Construir propietarios y relación vivienda_propietario
    # =========================

    owners_records = []
    vivienda_prop_records = []

    for _, row in df.iterrows():
        nombre_raw = (row.get("nombre") or "").strip()
        dni_raw = (row.get("dni") or "").strip()
        cuenta = row.get("cuenta") or ""
        correo = row.get("correo_electronico") or ""
        direccion_fiscal = row.get("direccion_fiscal") or ""
        ref = row["ref"]

        # Split por " y " para múltiples propietarios
        nombres = [n.strip() for n in nombre_raw.split(" y ")] if nombre_raw else [""]
        dnis = [d.strip() for d in dni_raw.split(" y ")] if dni_raw else [""]

        # igualar longitudes
        max_len = max(len(nombres), len(dnis))
        while len(nombres) < max_len:
            nombres.append("")
        while len(dnis) < max_len:
            dnis.append("")

        for nombre, dni in zip(nombres, dnis):
            if not nombre and not dni:
                continue

            owner_key = build_owner_key(nombre, dni, cuenta, correo)
            id_prop = stable_owner_id(owner_key)

            owners_records.append({
                "id_propietario": id_prop,
                "nombre": nombre,
                "dni": dni,
                "cuenta": cuenta,
                "correo_electronico": correo,
                "direccion_fiscal": direccion_fiscal,
            })

            vivienda_prop_records.append({
                "ref": ref,
                "id_propietario": id_prop,
            })

    propietarios_df = pd.DataFrame(owners_records)
    if not propietarios_df.empty:
        propietarios_df = propietarios_df.drop_duplicates(subset=["id_propietario"]).reset_index(drop=True)

    vivienda_prop_df = pd.DataFrame(vivienda_prop_records)
    if not vivienda_prop_df.empty:
        vivienda_prop_df = vivienda_prop_df.drop_duplicates(subset=["ref", "id_propietario"]).reset_index(drop=True)

    # =========================
    # Construir viviendas
    # =========================

    viv_cols = [
        "ref",
        "direccion_vivienda_alquilada",
        "ingreso_limpieza",
        "camas",
        "m2",
        "dormitorios",
        "banos",
        "terrazas",
    ]
    viv_cols = [c for c in viv_cols if c in df.columns]

    viviendas_df = df[viv_cols].drop_duplicates(subset=["ref"]).reset_index(drop=True)

    # =========================
    # Crear tablas y cargar datos
    # =========================

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    create_tables(pg)

    fq_prop = f'"{TARGET_SCHEMA}"."{TABLE_PROPIETARIOS}"'
    fq_viv = f'"{TARGET_SCHEMA}"."{TABLE_VIVIENDAS}"'
    fq_vp = f'"{TARGET_SCHEMA}"."{TABLE_VIVIENDA_PROP}"'

    if not propietarios_df.empty:
        dataframe_to_table_copy(pg, propietarios_df[
            ["id_propietario", "nombre", "dni", "cuenta", "correo_electronico", "direccion_fiscal"]
        ], fq_prop)

    if not viviendas_df.empty:
        dataframe_to_table_copy(pg, viviendas_df, fq_viv)

    if not vivienda_prop_df.empty:
        dataframe_to_table_copy(pg, vivienda_prop_df[["ref", "id_propietario"]], fq_vp)


# =========================
# DAG
# =========================

default_args = {
    "owner": "rafa",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="gsheets_viviendas_propietarios_to_postgres",
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule=None,  # ejecútalo manualmente; cambia a "@daily" si quieres planificarlo
    catchup=False,
    tags=["gsheets", "postgres", "viviendas", "propietarios"],
) as dag:

    cargar_todo = PythonOperator(
        task_id="extract_transform_load",
        python_callable=etl_viviendas_y_propietarios,
    )
