from __future__ import annotations
from datetime import datetime, timedelta, timezone
import json
import logging
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.email import send_email

ID_CONEXION_POSTGRES = "Domidai-DB"
ID_CONEXION_HTTP_AVANTIO = "Avantio_API_test"

ESQUEMA_BD = "public"
TABLA_RESERVAS = "reservas"
TABLA_RESERVAS_EXTRAS = "cargos_extra_reservas"

def notificarFalloETL(context):
    try:
        correo = Variable.get("correo_notificaciones")
    except Exception:
        logging.error("No se pudo obtener la variable de Airflow 'correo_notificaciones'")
        return
    
    dag_id = context.get("dag").dag_id if context.get("dag") else "desconocido"
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "desconocido"
    fecha_ejecucion = context.get("execution_date") or context.get("ts")
    exception = context.get("exception")

    asunto = f"[Airflow] Fallo en DAG: {dag_id}"
    cuerpo = f"""
    <h3>Fallo en la ejecución del DAG: {dag_id}</h3>
    <p><strong>Tarea:</strong> {task_id}</p>
    <p><strong>Fecha de ejecución:</strong> {fecha_ejecucion}</p>
    <p><strong>ERROR:</strong> {str(exception)}</p>
    """
    send_email(to=correo, subject=asunto, html_content=cuerpo.replace("\n", "<br/>"))

def parseaFechaISOAware(fecha):
    if fecha is None:
        return None
    if isinstance(fecha, datetime):
        fecha = fecha.astimezone(timezone.utc)
        fecha = fecha.replace(tzinfo=None).isoformat(timespec='microseconds')
        return fecha
    if isinstance(fecha, str):
        try:
            if fecha.endswith("Z"):
                fecha = fecha[:-1] + "+00:00"
            fecha = datetime.fromisoformat(fecha)
            fecha = fecha.astimezone(timezone.utc)
            fecha = fecha.replace(tzinfo=None).isoformat(timespec='microseconds')
            return fecha
        except Exception:
            logging.warning("No se pudo parsear la fecha ISO: %s", fecha)
            return None

def obtieneConfiguracionAvantio():
    logging.info("Obteniendo configuración de Avantio desde la conexión Airflow.")
    conexion = BaseHook.get_connection(ID_CONEXION_HTTP_AVANTIO)
    host = conexion.host or ""
    esquema = conexion.schema or ""

    if host.startswith("http://") or host.startswith("https://"):
        url_base = host.rstrip("/")
    else:
        if esquema:
            url_base = f"{esquema}://{host}".rstrip("/")
        else:
            url_base = f"https://{host}".rstrip("/")
    
    extras = conexion.extra_dejson or {}
    api_key = extras.get("api_key") or conexion.password
    if not api_key:
        raise ValueError("La clave API no está configurada en la conexión Avantio.")
    
    return {"url_base": url_base, "api_key": api_key}

def construyeCabecerasAvantio(api_key):
    logging.info("Construyendo cabeceras para la API de Avantio.")
    return {"accept": "application/json", "X-Avantio-Auth": api_key}

def obtieneListadoReservas(url_base, cabeceras):
    logging.info("Obteniendo resumen de reservas desde Avantio.")
    sesion_http = requests.Session()
    url_actual = f"{url_base}/pms/v2/bookings"
    listado_reservas = []

    while url_actual:
        logging.info("Llamando al endpoint de listar reservas de Avantio: %s", url_actual)

        respuesta = sesion_http.get(url_actual, headers=cabeceras, timeout=30)
        respuesta.raise_for_status()

        cuerpo_respuesta = respuesta.json()
        reservas = cuerpo_respuesta.get("data", []) or []

        for reserva in reservas:
            reserva_id = reserva.get("id")
            if not reserva_id:
                continue

            listado_reservas.append(
                {
                    "id": str(reserva_id),
                    "updatedAt": reserva.get("updatedAt"),
                }
            )

        enlaces = cuerpo_respuesta.get("_links", {}) or {}
        url_actual = enlaces.get("next")

    logging.info("Total de reservas en listado: %d", len(listado_reservas))
    return listado_reservas


def obtieneDetallesReserva(id_reserva, url_base, cabeceras, sesion_http):
    logging.info("Obteniendo detalles de la reserva ID: %s", id_reserva)
    url = f"{url_base}/pms/v2/bookings/{id_reserva}"
    respuesta = sesion_http.get(url, headers=cabeceras, timeout=30)
    respuesta.raise_for_status()
    cuerpo_respuesta = respuesta.json()
    data = cuerpo_respuesta.get("data", {}) or {}
    if not isinstance(data, dict):
        logging.error(
            "Formato inesperado en 'data' para la reserva %s: tipo=%s, valor=%s",
            id_reserva, type(data).__name__, data
        )
        return {}
    return data

def sumaImportesImpuestos(diccionario_impuestos):
    logging.info("Sumando importes de impuestos.")
    if not isinstance(diccionario_impuestos, dict):
        return None, None
    
    suma_neto = 0.0
    suma_iva = 0.0
    hay_valores = False

    for valor in diccionario_impuestos.values():
        if not isinstance(valor, dict):
            continue

        neto = valor.get("net")
        if neto is not None:
            suma_neto += float(neto)
            hay_valores = True
        
        iva = valor.get("vat")
        if iva is not None:
            suma_iva += float(iva)
            hay_valores = True
        
    if not hay_valores:
        return None, None
    
    return round(suma_neto, 2), round(suma_iva, 2)

def construyeFilaReservaPrincipal(reserva):
    logging.info("Construyendo fila de reserva principal para ID: %s", reserva.get("id"))
    estancia = reserva.get("stayDates", {}) or {}
    cliente = reserva.get("customer", {}) or {}
    contacto_cliente = cliente.get("contact", {}) or {}
    emails_cliente = contacto_cliente.get("emails", []) or []

    telefonos_cliente = contacto_cliente.get("phones", []) or []
    telefono_cliente = None
    if telefonos_cliente and isinstance(telefonos_cliente[0], dict):
        telefono_cliente = telefonos_cliente[0].get("number")

    apellidos_cliente = cliente.get("surname", []) or []
    alojamiento = reserva.get("accommodation", {}) or {}

    importes = reserva.get("amounts", {}) or {}
    importe_total = importes.get("total", None) or None
    desglose = importes.get("breakdown", {}) or {}
    base = desglose.get("base", None) or None
    extras = desglose.get("extras", None) or None
    modificadores = desglose.get("modifiers", None) or None
    impuestos = desglose.get("taxes", None) or None

    impuestos_neto, impuestos_iva = sumaImportesImpuestos(impuestos)

    comision = importes.get("commission", None) or None
    deposito_seguridad = importes.get("securityDeposit", None) or None

    info_check_in = reserva.get("checkIn", None) or None
    info_check_out = reserva.get("checkOut", None) or None

    parametros_reserva = {
        "id_reserva": reserva.get("id"),
        "referencia": reserva.get("reference"),
        "estado": reserva.get("status"),
        "id_empresa": reserva.get("companyId"),
        "id_alojamiento": alojamiento.get("id") or reserva.get("accommodationId"),
        "fecha_creacion": reserva.get("creationDate"),
        "fecha_llegada": estancia.get("arrival"),
        "fecha_salida": estancia.get("departure"),
        "moneda": reserva.get("currency"),
        "importe_total_neto": importe_total.get("net"),
        "importe_total_iva": importe_total.get("vat"),
        "importe_base_neto": base.get("net"),
        "importe_base_iva": base.get("vat"),
        "importe_extras_neto": extras.get("net"),
        "importe_extras_iva": extras.get("vat"),
        "importe_modificadores_neto": modificadores.get("net"),
        "importe_modificadores_iva": modificadores.get("vat"),
        "importe_impuestos_neto": impuestos_neto,
        "importe_impuestos_iva": impuestos_iva,
        "importe_comision_portal": comision.get("portal"),
        "importe_deposito_seguridad": deposito_seguridad,
        "nombre_cliente": cliente.get("name"),
        "apellidos_cliente": " ".join(apellidos_cliente) if apellidos_cliente else None,
        "email_cliente": emails_cliente[0] if emails_cliente else None,
        "telefono_cliente": telefono_cliente,
        "check_in_realizado": info_check_in.get("done"),
        "estado_check_in": info_check_in.get("status"),
        "estado_check_out": info_check_out.get("status"),
        "hora_check_in": info_check_in.get("checkInTime"),
        "hora_check_out": info_check_out.get("checkOutTime"),
        "fecha_actualizacion": reserva.get("updatedAt")
    }

    return parametros_reserva

def construyeFilasCargosExtras(reserva):
    logging.info("Construyendo filas de cargos extras para la reserva ID: %s", reserva.get("id"))
    id_reserva = reserva.get("id")
    extras = reserva.get("extras", []) or []
    filas = []

    for extra in extras:
        info_extra = extra.get("info", {}) or {}
        precio_extra = extra.get("price", {}) or {}
        categoria_extra = info_extra.get("category", {}) or {}

        filas.append({
            "id_reserva": id_reserva,
            "referencia_extra": info_extra.get("reference"),
            "nombre_extra": info_extra.get("name"),
            "codigo_categoria": categoria_extra.get("code"),
            "cantidad": extra.get("quantity"),
            "importe_neto": precio_extra.get("net"),
            "importe_iva": precio_extra.get("vat"),
            "importe_impuesto": precio_extra.get("tax"),
            "fecha_aplicacion": extra.get("applicationDate")
        })

    return filas

def aseguraTablasEnBD(gancho_postgres):
    logging.info("Asegurando que las tablas existen en la base de datos.")
    comando_creacion_reservas = f"""
    CREATE TABLE IF NOT EXISTS {ESQUEMA_BD}.{TABLA_RESERVAS} (
        id_reserva TEXT PRIMARY KEY,
        referencia TEXT,
        estado TEXT,
        id_empresa TEXT,
        id_alojamiento TEXT,
        fecha_creacion TIMESTAMPTZ,
        fecha_llegada DATE,
        fecha_salida DATE,
        moneda TEXT,
        importe_total_neto NUMERIC,
        importe_total_iva NUMERIC,
        importe_base_neto NUMERIC,
        importe_base_iva NUMERIC,
        importe_extras_neto NUMERIC,
        importe_extras_iva NUMERIC,
        importe_modificadores_neto NUMERIC,
        importe_modificadores_iva NUMERIC,
        importe_impuestos_neto NUMERIC,
        importe_impuestos_iva NUMERIC,
        importe_comision_portal NUMERIC,
        importe_deposito_seguridad NUMERIC,
        nombre_cliente TEXT,
        apellidos_cliente TEXT,
        email_cliente TEXT,
        telefono_cliente TEXT,
        check_in_realizado BOOLEAN,
        estado_check_in TEXT,
        estado_check_out TEXT,
        hora_check_in TIME,
        hora_check_out TIME,
        fecha_actualizacion TIMESTAMPTZ
    );
    """

    comando_creacion_cargos_extras = f"""
    CREATE TABLE IF NOT EXISTS {ESQUEMA_BD}.{TABLA_RESERVAS_EXTRAS} (
        id BIGSERIAL PRIMARY KEY,
        id_reserva TEXT REFERENCES {ESQUEMA_BD}.{TABLA_RESERVAS}(id_reserva),
        referencia_extra TEXT,
        nombre_extra TEXT,
        codigo_categoria TEXT,
        cantidad INTEGER,
        importe_neto NUMERIC,
        importe_iva NUMERIC,
        importe_impuesto NUMERIC,
        fecha_aplicacion TIMESTAMPTZ
    );
    """

    gancho_postgres.run(comando_creacion_reservas)
    gancho_postgres.run(comando_creacion_cargos_extras)

def sincronizaReservasAvantio(**_):
    logging.info("Iniciando sincronización de reservas desde Avantio a PostgreSQL.")
    configuracion = obtieneConfiguracionAvantio()
    url_base = configuracion["url_base"]
    api_key = configuracion["api_key"]
    cabeceras = construyeCabecerasAvantio(api_key)

    # 1) Listado de reservas (ID + updatedAt)
    listado_resumen = obtieneListadoReservas(url_base, cabeceras)
    if not listado_resumen:
        logging.info("No se encontraron reservas para sincronizar.")
        return

    ids_todas = [r["id"] for r in listado_resumen]

    gancho_postgres = PostgresHook(postgres_conn_id=ID_CONEXION_POSTGRES)
    aseguraTablasEnBD(gancho_postgres)
    conexion = gancho_postgres.get_conn()

    # 2) Consultar en BD las reservas existentes y su fecha_actualizacion
    with conexion.cursor() as cursor:
        if ids_todas:
            cursor.execute(
                f"""
                SELECT id_reserva, fecha_actualizacion
                FROM {ESQUEMA_BD}.{TABLA_RESERVAS}
                WHERE id_reserva = ANY(%s)
                """,
                (ids_todas,),
            )
            filas_existentes = cursor.fetchall()
        else:
            filas_existentes = []

    mapa_existentes = {
        id_reserva: fecha_actualizacion
        for (id_reserva, fecha_actualizacion) in filas_existentes
    }

    # 3) Clasificar: nuevas, actualizadas, sin cambios (sin tocar detalle aún)
    ids_nuevos = set()
    ids_actualizar = set()

    for resumen in listado_resumen:
        id_reserva = resumen["id"]
        updated_api_str = resumen.get("updatedAt")

        fecha_api = parseaFechaISOAware(updated_api_str)
        fecha_bd = mapa_existentes.get(id_reserva)
        fecha_bd_norm = parseaFechaISOAware(fecha_bd)

        if id_reserva not in mapa_existentes:
            ids_nuevos.add(id_reserva)
        else:
            # Si alguna es None o difieren → la marcamos para actualizar
            if fecha_api is None or fecha_bd_norm is None or fecha_api != fecha_bd_norm:
                ids_actualizar.add(id_reserva)

    ids_necesitan_detalle = ids_nuevos | ids_actualizar

    logging.info(
        "Reservas nuevas: %d, reservas a actualizar: %d, reservas sin cambios: %d",
        len(ids_nuevos),
        len(ids_actualizar),
        len(ids_todas) - len(ids_necesitan_detalle),
    )

    if not ids_necesitan_detalle:
        logging.info("No hay reservas nuevas ni actualizadas. No se realizan cambios.")
        return

    # 4) Obtener detalle SOLO de las reservas nuevas + actualizadas
    filas_reservas = []
    filas_cargos_extras = []

    sesion_http = requests.Session()

    for id_reserva in ids_necesitan_detalle:
        try:
            reserva_json = obtieneDetallesReserva(
                id_reserva=id_reserva,
                url_base=url_base,
                cabeceras=cabeceras,
                sesion_http=sesion_http,
            )
            if not reserva_json:
                logging.warning(
                    "No se obtuvieron detalles para la reserva ID: %s",
                    id_reserva,
                )
                continue

            fila_reserva = construyeFilaReservaPrincipal(reserva_json)
            filas_reservas.append(fila_reserva)

            filas_extras_reserva = construyeFilasCargosExtras(reserva_json)
            filas_cargos_extras.extend(filas_extras_reserva)

        except Exception as e:
            logging.error(
                "Error al procesar la reserva ID %s: %s",
                id_reserva,
                str(e),
            )

    if not filas_reservas:
        logging.info("No hay datos de reservas para insertar tras filtrar por cambios.")
        return

    # IDs para los que realmente hemos obtenido detalle
    ids_con_detalle = {fila["id_reserva"] for fila in filas_reservas}

    ids_nuevos_efectivos = ids_nuevos & ids_con_detalle
    ids_actualizar_efectivos = ids_actualizar & ids_con_detalle
    ids_para_insertar = ids_nuevos_efectivos | ids_actualizar_efectivos

    if not ids_para_insertar:
        logging.info(
            "No se ha podido obtener detalle de ninguna reserva nueva/actualizada. No se realizan cambios."
        )
        return

    filas_reservas_insertar = [
        fila for fila in filas_reservas
        if fila["id_reserva"] in ids_para_insertar
    ]
    filas_cargos_extras_insertar = [
        fila for fila in filas_cargos_extras
        if fila["id_reserva"] in ids_para_insertar
    ]

    campos_reservas = list(filas_reservas_insertar[0].keys())
    campos_cargos_extras = (
        list(filas_cargos_extras_insertar[0].keys())
        if filas_cargos_extras_insertar
        else []
    )

    valores_reservas = [
        [fila[campo] for campo in campos_reservas]
        for fila in filas_reservas_insertar
    ]
    valores_cargos_extras = [
        [fila[campo] for campo in campos_cargos_extras]
        for fila in filas_cargos_extras_insertar
    ]

    logging.info(
        "Reservas nuevas efectivas: %d, reservas a actualizar efectivas: %d",
        len(ids_nuevos_efectivos),
        len(ids_actualizar_efectivos),
    )

    try:
        with conexion.cursor() as cursor:
            # 5) Para las actualizadas: borrar reservas + cargos extra previos
            if ids_actualizar_efectivos:
                logging.info(
                    "Eliminando reservas y cargos extra para %d reservas actualizadas.",
                    len(ids_actualizar_efectivos),
                )
                cursor.execute(
                    f"""
                    DELETE FROM {ESQUEMA_BD}.{TABLA_RESERVAS_EXTRAS}
                    WHERE id_reserva = ANY(%s)
                    """,
                    (list(ids_actualizar_efectivos),),
                )
                cursor.execute(
                    f"""
                    DELETE FROM {ESQUEMA_BD}.{TABLA_RESERVAS}
                    WHERE id_reserva = ANY(%s)
                    """,
                    (list(ids_actualizar_efectivos),),
                )

            # 6) Insertar nuevas + actualizadas
            sql_insertar_reservas = f"""
            INSERT INTO {ESQUEMA_BD}.{TABLA_RESERVAS}
            ({', '.join(campos_reservas)})
            VALUES ({', '.join(['%s'] * len(campos_reservas))})
            """
            cursor.executemany(sql_insertar_reservas, valores_reservas)

            if valores_cargos_extras:
                sql_insertar_cargos_extras = f"""
                INSERT INTO {ESQUEMA_BD}.{TABLA_RESERVAS_EXTRAS}
                ({', '.join(campos_cargos_extras)})
                VALUES ({', '.join(['%s'] * len(campos_cargos_extras))})
                """
                cursor.executemany(sql_insertar_cargos_extras, valores_cargos_extras)

        conexion.commit()
        logging.info(
            "Sincronización completada. Reservas insertadas/actualizadas: %d, cargos extras: %d",
            len(valores_reservas),
            len(valores_cargos_extras),
        )
    finally:
        conexion.close()

ARGUMENTOS_POR_DEFECTO = {
    "owner": "rafael martínez del río-hortega",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="avantio_reservas_to_postgres",
    default_args=ARGUMENTOS_POR_DEFECTO,
    start_date=datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["avantio", "postgres", "reservas", "pms", "cargos_extras"],
    on_failure_callback=notificarFalloETL,
) as dag:
    tarea_sincroniza_reservas = PythonOperator(
        task_id="sincroniza_reservas_avantio",
        python_callable=sincronizaReservasAvantio
    )
        