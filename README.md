# 🏨 ETL Avantio PMS → PostgreSQL con Apache Airflow

Este repositorio contiene la implementación completa de un proceso ETL diseñado para sincronizar reservas desde la **API Avantio PMS** hacia una base de datos **PostgreSQL**, gestionado mediante **Apache Airflow**.

El objetivo principal es disponer de un sistema **robusto, incremental y eficiente**, capaz de mantener una copia local actualizada de todas las reservas, sus importes y sus cargos extra.

Este desarrollo forma parte del proceso de **certificación oficial de integración con Avantio**.

---

## 🚀 Características principales

- ✔ **Extracción programada** de reservas desde la API de Avantio  
- ✔ **Sincronización incremental** basada en el campo `updatedAt`  
- ✔ Identificación de nuevas, actualizadas y sin cambios  
- ✔ Persistencia en PostgreSQL (reservas y cargos extra)  
- ✔ Manejo robusto de errores y notificaciones email  
- ✔ Código limpio inspirado en principios SOLID  
- ✔ Credenciales seguras mediante Airflow Connections  

---

## 🧱 Arquitectura de la solución

```
Avantio API  →  Airflow (DAG ETL)  →  PostgreSQL
```

---

## 📦 Tablas generadas automáticamente

### 🗂 Tabla `reservas`
Contiene:
- Fechas
- Estado
- Cliente
- Contacto
- Check-in / check-out
- Importes desglosados
- Campo incremental: `fecha_actualizacion`

### 🗂 Tabla `cargos_extra_reservas`
Cada extra se guarda como fila independiente.

---

## ⚙️ Requisitos
- Python 3.10+
- Apache Airflow 3.x
- PostgreSQL 13+
- Dependencias:
```
apache-airflow
apache-airflow-providers-postgres
requests
```

---

## 🔧 Configuración en Airflow

### 1️⃣ Conexión Avantio API
- Conn ID: `Avantio_API_test`  
- Conn type: HTTP  
- Host: https://api.avantio.pro  
- Extra:
```json
{ "api_key": "TU_API_KEY_AQUI" }
```

### 2️⃣ Conexión PostgreSQL
- Conn ID: `Domidai-DB`

### 3️⃣ Variable de notificación
- `correo_notificaciones`

---

## 📜 DAG incluido
Archivo:
```
Avantio_Reservas_ETL.py
```

Implementa:
- Obtención de lista de reservas  
- Comparación incremental por `updatedAt`  
- Descarga de detalles solo cuando es necesario  
- Inserción y actualización en PostgreSQL  
- Envío automático de email ante errores  

---

## 🧠 Lógica Incremental

Avantio devuelve:
```
2023-06-25T01:26:09.577Z
```

PostgreSQL guarda:
```
TIMESTAMPTZ
```

Se normaliza a formato ISO UTC para comparación exacta.

---

## 🧪 Pruebas realizadas
- Procesadas +5000 reservas en entorno de test  
- Verificación manual del desglose  
- Comprobado comportamiento incremental  
- Optimización del rendimiento del DAG  

---

## 🐞 Troubleshooting

| Problema | Causa | Solución |
|---------|--------|-----------|
| Todas detectadas como actualizadas | Problema formato fecha | Se corrigió normalización |
| No se insertan extras | Estructura inesperada | Revisar campo extras |
| Error tabla no existe | Primera ejecución | Tablas autocreadas |

---

## 📈 Roadmap
- Sincronización de alojamientos  
- Webhooks  
- Pruebas unitarias  
- Dashboard analítico  

---

## 📄 Licencia
MIT

---

## 📬 Contacto
**Autor:** Rafael Martínez  
**Email:** rafamartinezdrh@gmail.com
