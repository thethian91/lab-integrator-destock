# test_result_sender.py
from lab_core.result_flow import (
    ResultSender,
    DefaultMappingRepo,
    DefaultExamRepo,
    DefaultXmlBuilder,
    DefaultApiClient,
)
from lab_core.config import load_settings

cfg = load_settings()
DB_PATH = "data/labintegrador.db"

# --- Inicialización del flujo ---
sender = ResultSender(
    mapping_repo=DefaultMappingRepo(mapping_path="configs/mapping.json"),
    exam_repo=DefaultExamRepo(db_path=DB_PATH),
    xml_builder=DefaultXmlBuilder(),
    api_client=DefaultApiClient(
        base_url=cfg.api.base_url,
        api_key=cfg.api.key,
        api_secret=cfg.api.secret,
        timeout=cfg.api.timeout or 30,
        default_resultado_global='',
        default_responsable='',
        default_notas='',
    ),
)

# --- OBX de prueba ---
# Usa datos reales de tu base (tubo_muestra existente en exams)
obx_record = {
    "analyzer": "ICON-3",  # o "FINECARE_FS114"
    "text": "WBC",  # analito que existe en el mapping
    "tubo_muestra": "11073123-16",  # DEBE existir en exams
    "value": "8.5",
    "unit": "x10^3/uL",
    "timestamp": "2025-11-11 13:25:00",
    "ultimo_del_examen": True,  # para que cierre el examen
    "paciente_id": "1098658028",  # opcional, si existe en DB
}

print("🔄 Enviando resultado de prueba...\n")

outcome = sender.process_obx(obx_record)

# --- Mostrar resultado ---
print("=" * 50)
print("✅ OK:", outcome.ok)
print(
    f"🧾 IDExamen: {outcome.id_examen} | ClientCode: {outcome.client_code} | Fecha: {outcome.order_date}"
)
print("-" * 50)
print("📋 LOGS:")
for line in outcome.logs:
    print("  ", line)
print("-" * 50)
print("⚠️ ERRORES:")
for code, msg in outcome.errors:
    print(f"  [{code}] {msg}")
print("=" * 50)
