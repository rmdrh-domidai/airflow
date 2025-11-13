from __future__ import annotations

from datetime import datetime, timedelta
import csv, io, re, hashlib
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


GOOGLE_CONN_ID = "GoogleSheets"
POSTGRES_CONN_ID = "Domidai-DB"

SPREADSHEET_ID = "1HSFqe6BE73cE0EqtFgVVcyv7_IFgZ02VteJARMFFwPI"
RANGE_VIVIENDAS = "VIVIENDAS!A1:Z"

TARGET_SCHEMA = "public"
TABLE_PROPIETARIOS = "propietarios"
TABLE_VIVIENDAS = "viviendas"


def _normalize_col(col: str) -> str:
    rep = (
        ("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ñ","n"),
        ("Á","a"),("É","e"),("Í","i"),("Ó","o"),("Ú","u"),("Ñ","n")
    )
    for a,b in rep:
        col = col.replace(a,b)
    col = col.strip().lower()
    col = re.sub(r"[^a-z0-9]+", "_", col)
    col = re.sub(r"_+", "_", col).strip("_")
    return col


def read_sheet_as_df() -> pd.DataFrame:
    hook = GSheetsHook(gcp_conn_id=GOOGLE_CONN_ID)
    values = hook.get_values(spreadsheet_id=SPREADSHEET_ID, range_name=RANGE_VIVIENDAS)
    if not values or len(values) < 2:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]
    df = pd.DataFrame(rows, columns=header).fillna("")
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df.columns = [_normalize_col(c) for c in df.columns]

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
    return df


def create_tables(pg: PostgresHook):
    ddl = f"""
    DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{TABLE_VIVIENDAS}" CASCADE;
    DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{TABLE_PROPIETARIOS}" CASCADE;

    CREATE TABLE "{TARGET_SCHEMA}"."{TABLE_PROPIETARIOS}" (
        id_propietario       BIGINT PRIMARY KEY,
        nombre               TEXT,
        cuenta               TEXT,
        dni                  TEXT,
        correo_electronico   TEXT,
        direccion_fiscal     TEXT
    );

    CREATE TABLE "{TARGET_SCHEMA}"."{TABLE_VIVIENDAS}" (
        ref                       TEXT PRIMARY KEY,
        id_propietario            BIGINT NOT NULL
                                  REFERENCES "{TARGET_SCHEMA}"."{TABLE_PROPIETARIOS}"(id_propietario),
        direccion_vivienda_alquilada TEXT,
        ingreso_limpieza          NUMERIC(12,2),
        camas                     INTEGER,
        m2                        INTEGER,
        dormitorios               INTEGER,
        banos                     INTEGER,
        terrazas                  INTEGER,
        estimacion_horas          NUMERIC(6,2),
        tipo_limpieza             TEXT,
        factura_plataforma        TEXT
    );
    """
    pg.run(ddl)


def dataframe_to_table_copy(pg: PostgresHook, df: pd.DataFrame, fqtn: str):
    if df.empty:
        return
    cols = list(df.columns)
    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf, quoting=csv.QUOTE_MINIMAL)
    for row in df.itertuples(index=False, name=None):
        writer.writerow(["" if v is None else v for v in row])
    csv_buf.seek(0)
    copy_sql = (
        f'COPY {fqtn} ({", ".join(f"{c}" for c in cols)}) '
        "FROM STDIN WITH (FORMAT CSV)"
    )
    pg.copy_expert(sql=copy_sql, filename=None, data=csv_buf.getvalue())


def stable_owner_id(owner_key: str) -> int:
    """
    Genera un entero estable a partir de owner_key usando MD5.
    Mismo owner_key -> mismo id_propietario en todas las ejecuciones.
    """
    h = hashlib.md5(owner_key.encode("utf-8")).hexdigest()
    # 15 hex -> ~60 bits, cabe de sobra en BIGINT
    return int(h[:15], 16)


def etl_viviendas_y_propietarios(**_):
    df = read_sheet_as_df()
    if df.empty:
        raise ValueError("La hoja 'VIVIENDAS' está vacía o no se pudo leer.")

    for c in ("camas", "m2", "dormitorios", "banos", "terrazas"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in ("ingreso_limpieza", "estimacion_horas"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Clave de propietario: prioriza DNI
    def owner_key(row):
        dni = (row.get("dni") or "").strip()
        if dni:
            return f"DNI::{dni}"
        nombre = (row.get("nombre") or "").strip()
        cuenta = (row.get("cuenta") or "").strip()
        mail = (row.get("correo_electronico") or "").strip()
        return f"COMP::{nombre}|{cuenta}|{mail}"

    df["__owner_key__"] = df.apply(owner_key, axis=1)

    # Propietarios únicos
    owner_cols = [
        c
        for c in [
            "nombre",
            "cuenta",
            "dni",
            "correo_electronico",
            "direccion_fiscal",
            "__owner_key__",
        ]
        if c in df.columns
    ]
    propietarios_df = (
        df[owner_cols]
        .drop_duplicates(subset=["__owner_key__"])
        .reset_index(drop=True)
    )
    propietarios_df["id_propietario"] = propietarios_df["__owner_key__"].apply(
        stable_owner_id
    )

    # Viviendas
    if "ref" not in df.columns:
        raise ValueError("No se encontró la columna 'REF' (como 'ref') en la hoja.")

    viv_cols = [
        "ref",
        "direccion_vivienda_alquilada",
        "ingreso_limpieza",
        "camas",
        "m2",
        "dormitorios",
        "banos",
        "terrazas",
        "estimacion_horas",
        "tipo_limpieza",
        "factura_plataforma",
        "__owner_key__",
    ]
    viv_cols = [c for c in viv_cols if c in df.columns]
    viviendas_df = df[viv_cols].drop_duplicates(subset=["ref"]).copy()

    viviendas_df = viviendas_df.merge(
        propietarios_df[["id_propietario", "__owner_key__"]],
        on="__owner_key__",
        how="left",
    )
    viviendas_df = viviendas_df.drop(columns=["__owner_key__"])
    col_order = ["ref", "id_propietario"] + [
        c for c in viviendas_df.columns if c not in ("ref", "id_propietario")
    ]
    viviendas_df = viviendas_df[col_order]

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    create_tables(pg)

    fq_prop = f'"{TARGET_SCHEMA}"."{TABLE_PROPIETARIOS}"'
    fq_viv = f'"{TARGET_SCHEMA}"."{TABLE_VIVIENDAS}"'

    dataframe_to_table_copy(
        pg,
        propietarios_df[
            ["id_propietario", "nombre", "cuenta", "dni", "correo_electronico", "direccion_fiscal"]
        ],
        fq_prop,
    )
    dataframe_to_table_copy(pg, viviendas_df, fq_viv)


default_args = {
    "owner": "rafa",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="gsheets_viviendas_propietarios_to_postgres",
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gsheets", "postgres", "viviendas", "propietarios"],
) as dag:
    cargar_todo = PythonOperator(
        task_id="extract_transform_load",
        python_callable=etl_viviendas_y_propietarios,
    )
