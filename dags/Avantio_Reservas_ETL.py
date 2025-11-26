from __future__ import annotations
from datetime import datetime, timedelta
import json
import logging
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

ID_CONEXION_POSTGRES = "Domidai-DB"
ID_CONEXION_HTTP_AVANTIO = "Avantio_API_test"

ESQUEMA_BD = "public"
TABLA_RESERVAS = "reservas"
TABLA_RESERVAS_EXTRAS = "cargos_extra_reservas"

def obtieneConfiguracionAvantio():
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
    return {"accept": "application/json", "X-Avantio-Auth": api_key}

def obtieneIdsReservas(url_base, cabeceras):
    sesion_http = requests.Session()
    url_actual = f"{url_base}/pms/v2/bookings"
    listado_id_reservas = []

    while url_actual:
        logging.info("Llamando al endpoint de lisatr reservas de Avantio: %s", url_actual)

        respuesta = sesion_http.get(url_actual, headers=cabeceras, timeout=30)
        respuesta.raise_for_status()

        cuerpo_respuesta = respuesta.json()
        reservas = cuerpo_respuesta.get("data", []) or []
        for reserva in reservas:
            if "id" in reserva:
                listado_id_reservas.append(str(reserva["id"]))
        
        enlaces = cuerpo_respuesta.get("_links", {}) or {}
        url_actual = enlaces.get("next")

    logging.info("Total de IDs de reservas obtenidas: %d", len(listado_id_reservas))
    return listado_id_reservas

def obtieneDetallesReserva(id_reserva, url_base, cabeceras):
    sesion_http = requests.Session()
    url = f"{url_base}/pms/v2/bookings/{id_reserva}"
    respuesta = sesion_http.get(url, headers=cabeceras, timeout=30)
    respuesta.raise_for_status()
    cuerpo_respuesta = respuesta.json()
    return cuerpo_respuesta.get("data", {}) or {}

def sumaImportesImpuestos(diccionario_impuestos):
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
    importe_total = importes.get("total", {}) or {}
    desglose = importes.get("breakdown", {}) or {}
    base = desglose.get("base", {}) or {}
    extras = desglose.get("extras", {}) or {}
    modificadores = desglose.get("modifiers", {}) or {}
    impuestos = desglose.get("taxes", {}) or {}

    impuestos_neto, impuestos_iva = sumaImportesImpuestos(impuestos)

    comision = importes.get("commission", {}) or {}
    deposito_seguridad = importes.get("securityDeposit", {}) or {}

    info_check_in = reserva.get("checkIn", {}) or {}
    info_check_out = reserva.get("checkOut", {}) or {}

    return {
        "id_reserva": reserva.get("id"),
        "referencia": reserva.get("reference"),
        "status": reserva.get("status"),
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
        "email_cliente": emails_cliente[0].get("email") if emails_cliente else None,
        "telefono_cliente": telefono_cliente,
        "check_in_realizado": info_check_in.get("done"),
        "estado_check_in": info_check_in.get("status"),
        "estado_check_out": info_check_out.get("status"),
        "hora_check_in": info_check_in.get("checkInTime"),
        "hora_check_out": info_check_out.get("checkOutTime"),
        "fecha_actualizacion": reserva.get("updatedAt"),
        "json_completo": json.dumps(reserva)
    }