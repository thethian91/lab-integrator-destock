
# Lab Integrator v2 (GUI Starter)

Multiplatform GUI starter for a lab integrator. **MVP included:** live TCP receiver that shows incoming payloads in a memo and saves them to disk. Two additional apps are included as stubs: a Configurator and a Dashboard.

## Structure
```
lab-integrator-v2/
├─ lab_core/
│  └─ connectors/
│     └─ tcp.py            # Async TCP receiver → saves files + emits events
├─ apps/
│  ├─ monitor/             # GUI monitor (ready to run)
│  ├─ configurator/        # GUI stub for settings
│  └─ dashboard/           # GUI stub for listing saved files
├─ configs/
│  └─ settings.yaml.example
├─ samples/                # HL7 example payloads
├─ scripts/                # Utilities (send HL7, PowerShell sender)
├─ resources/              # Icons/QSS (optional)
├─ requirements.txt
└─ README.md
```

## Quickstart

> Requires **Python 3.11+**. Tested with PySide6 + qasync.

### 1) Create venv & install
**Windows (PowerShell)**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**macOS / Linux**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Run the GUI Monitor
```bash
python apps/monitor/main.py
```
By default it listens on **0.0.0.0:5002** and writes files to `./inbox` (you can change it on the GUI).

### 3) Send a test HL7
Using Python script (cross‑platform):
```bash
python scripts/send_hl7.py --host 127.0.0.1 --port 5002 --file samples/sample.hl7
```

Or PowerShell one‑liner (Windows):
```powershell
scripts\send_hl7.ps1 -Host 127.0.0.1 -Port 5002 -Path "samples\sample.hl7"
```

### Packaging with PyInstaller

Install:
```bash
pip install pyinstaller
```

**Windows:**
```powershell
pyinstaller apps/monitor/main.py ^
  --name lab-monitor ^
  --onefile --windowed ^
  --add-data "configs;configs" ^
  --add-data "resources;resources"
```

**macOS/Linux:**
```bash
pyinstaller apps/monitor/main.py   --name lab-monitor   --onefile --windowed   --add-data "configs:configs"   --add-data "resources:resources"
```

### Notes
- This MVP **does not transform** messages. It simply receives and saves them.
- For future versions, you can add SQLite persistence, UDP/Serial/File‑Drop connectors, and a richer dashboard.


example result XML
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

