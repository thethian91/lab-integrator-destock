# Lab Integrator v2

Integrador de laboratorio (HL7 → XML/API) con interfaz gráfica multiplataforma.  
Permite recibir resultados desde analizadores, mapearlos, almacenarlos en SQLite y enviarlos a un LIS/ERP mediante API REST, con flujo automático y manual, reportes y trazabilidad completa.

Proyecto orientado a escenarios como Finecare, Icon, etc. integrados con sistemas tipo SOFIA / SNT.

---

## ✨ Características principales

- Ingesta HL7 desde archivos o socket TCP.
- Mapeo flexible mediante hl7_map.yaml y mapping.json.
- Persistencia en SQLite con tablas:
  - patients, exams
  - hl7_results (RAW)
  - hl7_obx_results (analitos)
  - trazabilidad de exportación
- Configuración centralizada en configs/settings.yaml.
- GUI (PySide6) con pestañas:
  - Monitor
  - Orders
  - Orders & Results (envío manual)
  - Reports (pendientes / enviados)
  - Traceability
  - SQL Viewer
  - Logs
  - Config / Maintenance
- Flujo automático con cierre de examen.
- Guardado opcional de XML enviados.
- Auditoría completa: HL7 → OBX → API.

---

## 🗂️ Estructura del proyecto

```text
lab-integrator-v2/
├─ lab_core/                 # Núcleo de negocio
│  ├─ db.py                  # Esquema + helpers SQLite
│  ├─ dispatcher.py          # Flujo automático (poller)
│  ├─ result_ingest.py       # Ingesta de HL7
│  ├─ result_sender.py       # Envío a la API (por OBX)
│  ├─ config.py              # Lectura de settings.yaml
│  └─ connectors/
│     └─ tcp.py              # Receptor TCP asíncrono
│
├─ apps/
│  └─ monitor/               # Aplicación GUI principal
│     ├─ main.py             # Punto de entrada de la GUI
│     ├─ net_server.py       # Embebido para recepción TCP
│     ├─ qt_logging.py       # Logging en la interfaz
│     └─ tabs/
│        ├─ monitor_tab.py
│        ├─ orders_tab.py
│        ├─ orders_results_tab.py
│        ├─ reports_tab.py
│        ├─ traceability_tab.py
│        ├─ sql_tab.py
│        ├─ logs_tab.py
│        ├─ config_tab.py
│        └─ maintenance_tab.py
│
├─ configs/
│  ├─ settings.yaml          # Configuración principal (API, paths, poller)
│  ├─ settings.yaml.example  # Ejemplo base
│  ├─ hl7_map.yaml           # Mapa de extracción HL7 (perfil por analizador)
│  └─ mapping.json           # Mapeo analyzer_code → client_code/title
│
├─ data/
│  └─ labintegrador.db       # Base SQLite (se crea automáticamente)
│
├─ inbox/                    # Archivos HL7 recibidos
├─ outbox_xml/               # XML/resultados enviados (si save_files está activo)
├─ resources/                # Iconos, imágenes, etc.
├─ samples/                  # HL7 y ejemplos de prueba
├─ scripts/                  # Utilidades para tests / debug
│
├─ README.md
├─ requirements.txt
├─ requirements-dev.txt
└─ pyproject.toml
```

---

## 🧩 Modelo de datos (resumen)

### patients

Documento, nombre, sexo, fecha nacimiento.

### exams

Orden, código de tubo, protocolo, fecha, estado.

### hl7_results

Registro RAW del HL7 con auditoría y estado de cierre.

### hl7_obx_results

Un analito por fila.
Estado individual: PENDING / SENT / ERROR / MAPPING_NOT_FOUND.
Incluye request/response del API, timestamp y mensaje de error.

---

## ✅ Requisitos

- Python 3.11+
- Instalación:

```

pip install -r requirements.txt

```

---

## 🚀 Puesta en marcha rápida

1. Clonar el repo:

```

git clone <url>
cd lab-integrator-v2

```

2. Opcional: entorno virtual

```

python -m venv .venv
source .venv/bin/activate

```

3. Instalar dependencias:

```

pip install -r requirements.txt

```

4. Copiar configuración:

```

cp configs/settings.yaml.example configs/settings.yaml

```

5. Configurar settings, hl7_map.yaml, mapping.json

6. Ejecutar monitor:

```

python -m apps.monitor.main

```

---

## 🧪 Flujo de trabajo

### Ingesta HL7

HL7 → result_ingest → SQLite (hl7_results + hl7_obx_results).

### Automático

dispatcher → envía OBX → si al menos uno OK → cierre de examen → auditoría.

### Manual

Orders & Results → filtrar, reenviar, ver XML, cerrar examen.

---

## 📊 Reportes y trazabilidad

### Reports tab

Pendientes, enviados, filtros por fecha y estado.

### Traceability tab

HL7 RAW, OBX, requests/responses API, timeline completo.

---

## 🛠️ Desarrollo

Formateo:

```

black .

```

Linter:

```

ruff check .

```

---

## 🧭 Roadmap

- Dashboard KPIs
- Exportación a Excel
- Modo simulación
- Editor gráfico del hl7_map.yaml

---

## 📄 Ejemplo XML enviado

```

<?xml version="1.0" encoding="utf-8" ?>

<log_envio>
<idexamen>412509-55</idexamen>
<paciente>288413</paciente>
<fecha>20250821</fecha>
<texto>PRUEBA</texto>
<valor_cualitativo>140.12</valor_cualitativo>
<valor_referencia>66-181</valor_referencia>
<valor_adicional>UNITS:nmol/L</valor_adicional>
</log_envio>

```

```

```
