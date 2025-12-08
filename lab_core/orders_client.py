# lab_core/orders_client.py
from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("lab.integrator.orders")


# ============= HELPER PARA FILTRAR EXAMENES
_ALLOWED_PROTOCOL_CODES: set[str] | None = None


def _load_allowed_protocolo_codigos_from_mapping() -> set[str]:
    """Lee configs/mapping.json y arma el set de códigos de protocolo permitidos.

    Usa el campo `client_code` (o `cliente_codigo` si existiera) de cada analito
    configurado por analizador.
    """
    try:
        base_dir = Path(__file__).resolve().parent.parent
        mapping_path = base_dir / "configs" / "mapping.json"
        data = json.loads(mapping_path.read_text(encoding="utf-8"))
    except Exception as e:  # fallback defensivo
        log.warning("No se pudo leer mapping.json para filtrar órdenes: %s", e)
        return set()

    analyzers = data.get("analyzers", {}) or {}
    allowed: set[str] = set()
    for _name, analyzer_cfg in analyzers.items():
        mapping = (analyzer_cfg or {}).get("map", {}) or {}
        for _analyzer_code, info in mapping.items():
            if not isinstance(info, dict):
                continue
            code = info.get("client_code") or info.get("cliente_codigo") or ""
            code = str(code).strip()
            if code:
                allowed.add(code)
    return allowed


# ==========================================


@dataclass
class Exam:
    id: str
    protocolo_codigo: str
    protocolo_titulo: str
    tubo: str
    tubo_muestra: str
    fecha: str
    hora: str
    paciente: str
    nombre: str
    sexo: str
    edad: str
    fecha_nacimiento: str


@dataclass
class OrderRecord:
    documento: str
    examenes: list[Exam]


def _build_session(timeout_s: float = 15.0) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET", "POST"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.request = _with_timeout(s.request, timeout_s)  # type: ignore
    return s


def _with_timeout(func, timeout_s: float):
    def wrapped(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout_s
        return func(method, url, **kwargs)

    return wrapped


def fetch_orders_xml(
    base_url: str,
    api_key: str,
    api_secret: str,
    action: str,
    fecha_exploracion: str,
) -> str:
    """Hace el POST y devuelve el XML (str)."""
    params = {
        "API_Key": api_key,
        "API_Secret": api_secret,
        "accion": action,
        "fecha_exploracion": fecha_exploracion,
    }
    # El servicio en tu ejemplo usa POST con params en querystring
    url = base_url
    session = _build_session()
    log.info(f"Descargando órdenes para fecha {fecha_exploracion}")
    resp = session.post(url, params=params, data={})
    # Forzamos UTF-8; si viniera mal codificado, intentamos fallback
    resp.encoding = resp.encoding or "utf-8"
    text = resp.text
    if "<resultado_ws" not in text:
        # pequeño fallback por si el servidor responde con latin-1
        try:
            text = resp.content.decode("latin-1")
        except Exception:
            pass
    resp.raise_for_status()
    return text


'''
def parse_orders(xml_text: str) -> list[OrderRecord]:
    """Parsea el XML a objetos OrderRecord -> Exam."""
    # Limpieza simple de caracteres sueltos
    xml_text = xml_text.strip()
    root = ET.fromstring(xml_text)

    # Estructura esperada: resultado_ws/detalle_respuesta/paciente/examen
    detalle = root.find(".//detalle_respuesta")
    if detalle is None:
        return []

    records: list[OrderRecord] = []
    for paciente in detalle.findall("./paciente"):
        doc = paciente.get("documento", "").strip()
        exams: list[Exam] = []
        for ex in paciente.findall("./examen"):

            def _t(tag: str) -> str:
                el = ex.find(tag)
                return (el.text or "").strip() if el is not None else ""

            exams.append(
                Exam(
                    id=_t("id"),
                    protocolo_codigo=_t("protocolo_codigo"),
                    protocolo_titulo=_t("protocolo_titulo"),
                    tubo=_t("tubo"),
                    tubo_muestra=_t("tubo_muestra"),
                    fecha=_t("fecha"),
                    hora=_t("hora"),
                    paciente=_t("paciente"),
                    nombre=_t("nombre"),
                    sexo=_t("sexo"),
                    edad=_t("edad"),
                    fecha_nacimiento=_t("fecha_nacimiento"),
                )
            )
        if exams:
            records.append(OrderRecord(documento=doc, examenes=exams))
    return records
'''


def parse_orders(xml_text: str) -> list[OrderRecord]:
    """Parsea el XML a objetos OrderRecord -> Exam.

    A partir de ahora **solo** conserva los exámenes cuyo `protocolo_codigo`
    esté mapeado en `configs/mapping.json` (campo `client_code`/`cliente_codigo`).
    De esta forma la base de datos no se llena con exámenes que nunca se van
    a procesar en la integración.
    """
    global _ALLOWED_PROTOCOL_CODES

    # Lazy-load del set de códigos permitidos para no leer el JSON en cada llamada
    if _ALLOWED_PROTOCOL_CODES is None:
        _ALLOWED_PROTOCOL_CODES = _load_allowed_protocolo_codigos_from_mapping()

    allowed = _ALLOWED_PROTOCOL_CODES or set()

    # Limpieza simple de caracteres sueltos
    xml_text = xml_text.strip()
    root = ET.fromstring(xml_text)

    # Estructura esperada: resultado_ws/detalle_respuesta/paciente/examen
    detalle = root.find(".//detalle_respuesta")
    if detalle is None:
        return []

    records: list[OrderRecord] = []
    for paciente in detalle.findall("./paciente"):
        doc = paciente.get("documento", "").strip()
        exams: list[Exam] = []

        for ex in paciente.findall("./examen"):

            def _t(tag: str) -> str:
                el = ex.find(tag)
                return (el.text or "").strip() if el is not None else ""

            protocolo_codigo = _t("protocolo_codigo")

            # Si hay lista de permitidos y este código no está -> lo saltamos
            if allowed and protocolo_codigo not in allowed:
                continue

            exams.append(
                Exam(
                    id=_t("id"),
                    protocolo_codigo=protocolo_codigo,
                    protocolo_titulo=_t("protocolo_titulo"),
                    tubo=_t("tubo"),
                    tubo_muestra=_t("tubo_muestra"),
                    fecha=_t("fecha"),
                    hora=_t("hora"),
                    paciente=_t("paciente"),
                    nombre=_t("nombre"),
                    sexo=_t("sexo"),
                    edad=_t("edad"),
                    fecha_nacimiento=_t("fecha_nacimiento"),
                )
            )
        if exams:
            records.append(OrderRecord(documento=doc, examenes=exams))
    return records


def save_orders(
    records: list[OrderRecord],
    out_dir: str,
    fecha_exploracion: str,
) -> list[str]:
    """Guarda un archivo por paciente (JSON) y retorna las rutas."""
    import json

    base = Path(out_dir) / fecha_exploracion
    base.mkdir(parents=True, exist_ok=True)
    saved_paths: list[str] = []
    for rec in records:
        # archivo por paciente; si necesitas uno por examen, se puede cambiar
        safe_doc = "".join(
            ch for ch in rec.documento if ch.isalnum() or ch in ("-", "_")
        )
        path = base / f"{safe_doc or 'sin_documento'}.json"
        data = {
            "fecha_exploracion": fecha_exploracion,
            "documento": rec.documento,
            "examenes": [asdict(e) for e in rec.examenes],
        }
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        saved_paths.append(str(path))
    return saved_paths


def download_and_store_orders(
    cfg: dict[str, Any],
    fecha_exploracion: str,
) -> dict[str, Any]:
    """Función de alto nivel: descarga → parsea → guarda."""
    api = cfg.get("api", {}) or {}
    orders_cfg = cfg.get("orders", {}) or {}
    base_url = api.get("base_url", "")
    key = api.get("key", "")
    secret = api.get("secret", "")
    action = api.get("action", "ordenes_laboratorio_fecha")
    out_dir = orders_cfg.get("out_dir", "inbox/orders")

    if not base_url or not key or not secret:
        raise RuntimeError(
            "Config incompleta: api.base_url/api.key/api.secret son obligatorios."
        )

    xml_text = fetch_orders_xml(base_url, key, secret, action, fecha_exploracion)
    recs = parse_orders(xml_text)

    # Validaciones simples
    if not recs:
        log.warning("No se encontraron órdenes para la fecha %s", fecha_exploracion)

    paths = save_orders(recs, out_dir, fecha_exploracion)
    log.info("Órdenes guardadas: %d archivos en %s", len(paths), out_dir)
    return {
        "count": len(recs),
        "files": paths,
    }


def get_orders_xml_from_cfg(cfg: dict, fecha_exploracion: str) -> str:
    """
    Si api.use_mock == true -> lee XML desde api.mock_file.
    De lo contrario -> llama al servicio real con fetch_orders_xml().
    """
    api = cfg.get("api", {}) or {}
    use_mock = bool(api.get("use_mock", False))
    if use_mock:
        mock_path = api.get("mock_file", "")
        if not mock_path:
            raise RuntimeError("api.use_mock=true pero api.mock_file no está definido.")
        p = Path(mock_path)
        if not p.exists():
            raise FileNotFoundError(f"Archivo mock no existe: {p}")
        return p.read_text(encoding="utf-8")

    # real service
    return fetch_orders_xml(
        api.get("base_url", ""),
        api.get("key", ""),
        api.get("secret", ""),
        api.get("action", "ordenes_laboratorio_fecha"),
        fecha_exploracion,
    )
