# lab_core/pipeline.py
from __future__ import annotations

import os
import json
import logging
from pathlib import Path
from datetime import datetime, date
from collections.abc import Mapping
from typing import Any, Tuple

from xml.etree.ElementTree import Element, SubElement, tostring

from lab_core.config import load_settings
from lab_core.sender import SNTClient
from lab_core.utils.dates import to_yyyymmdd

log = logging.getLogger("lab.integrator.pipeline")


# ===================== Helpers de configuración =====================


def _truthy(val: Any) -> bool:
    if isinstance(val, bool):
        return val
    s = str(val or "").strip().lower()
    return s in {"1", "true", "yes", "on", "si", "sí"}


def _exports_enabled(cfg) -> bool:
    """
    Regla de activación:
      1) Si SAVE_SENT_XML está en el entorno (1/true/yes/on), gana.
      2) Si cfg.export.save_xml existe y es True.
      3) Sino, False.
    """
    if _truthy(os.getenv("SAVE_SENT_XML")):
        return True
    try:
        return bool(getattr(getattr(cfg, "export", object()), "save_xml", False))
    except Exception:
        return False


def _exports_base_dir(cfg) -> Path:
    """
    Prioridades:
      1) EXPORTS_DIR en el entorno
      2) cfg.export.dir
      3) 'data/exports'
    """
    env_dir = os.getenv("EXPORTS_DIR")
    if env_dir:
        return Path(env_dir)
    try:
        d = getattr(getattr(cfg, "export", object()), "dir", None)
        if d:
            return Path(d)
    except Exception:
        pass
    return Path("data/exports")


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _today_dir(base: Path) -> Path:
    d = base / date.today().strftime("%Y-%m-%d")
    _ensure_dir(d)
    return d


def _safe_filename(s: Any) -> str:
    s = str(s or "")
    return "".join(c for c in s if c.isalnum() or c in ("-", "_", "."))[:80]


# ===================== Construcción del cliente =====================


def build_snt_client() -> SNTClient:
    cfg = load_settings()
    return SNTClient(
        base_url=cfg.api.base_url,
        api_key=cfg.api.key,
        api_secret=cfg.api.secret,
        timeout=cfg.api.timeout,
    )


# ===================== Serialización a XML (solo para logging) =====================


def _build_xml_from_item(
    item: Mapping[str, Any], fecha_api: str, valor_adicional: str | None
) -> str:
    """
    XML de auditoría (no afecta el envío real). Ajusta etiquetas si tu integración
    necesita un formato distinto para el log.
    """
    root = Element("log_envio")

    # Mantenemos las claves que tú ya utilizas en item
    pairs = {
        "idexamen": item.get("idexamen", ""),
        "paciente": item.get("paciente_doc", ""),
        "fecha": fecha_api,  # ya normalizada a YYYYMMDD
        "texto": item.get("texto", ""),
        "valor": item.get("valor", ""),  # lo que llamas 'valor_cualitativo' en la API
        "ref": item.get("ref", ""),
        "units": item.get("units", ""),
    }

    for k, v in pairs.items():
        node = SubElement(root, k)
        node.text = "" if v is None else str(v)

    if valor_adicional is not None:
        node = SubElement(root, "valor_adicional")
        node.text = str(valor_adicional)

    xml_text = '<?xml version="1.0" encoding="utf-8"?>\n' + tostring(
        root, encoding="unicode"
    )
    return xml_text


# ===================== API principal =====================


def enviar_resultado_item(item: Mapping[str, Any]) -> str:
    """
    Envía un resultado usando SNTClient y, si está habilitado, guarda
    el XML (de auditoría) y la respuesta en disco.
    Retorna el texto de respuesta.
    """
    cfg = load_settings()
    client = build_snt_client()

    # Normalizaciones previas
    fecha_api = to_yyyymmdd(item.get("fecha"))
    valor_adicional = f"UNITS:{item['units']}" if item.get("units") else None

    # ---- (1) Guardado previo del XML si está habilitado ----
    save_logs = _exports_enabled(cfg)
    base_dir = _exports_base_dir(cfg)
    req_path: Path | None = None
    resp_path: Path | None = None
    meta_path: Path | None = None

    if save_logs:
        try:
            out_dir = _today_dir(base_dir)
            ts = datetime.now().strftime("%H%M%S_%f")
            idex = _safe_filename(item.get("idexamen", "NA"))
            doc = _safe_filename(item.get("paciente_doc", "NA"))
            base = f"{idex}_{doc}_{ts}"

            req_path = out_dir / f"request_{base}.xml"
            resp_path = out_dir / f"response_{base}.txt"
            meta_path = out_dir / f"request_{base}.json"

            # Construir XML de auditoría (lo que "representa" lo que enviamos)
            xml_text = _build_xml_from_item(item, fecha_api, valor_adicional)
            req_path.write_text(xml_text, encoding="utf-8")

            # Guardar metadatos del item tal cual
            meta_payload = {
                "item": dict(item),
                "fecha_api": fecha_api,
                "valor_adicional": valor_adicional,
                "endpoint": getattr(getattr(cfg, "api", object()), "base_url", None),
                "ts": ts,
            }
            meta_path.write_text(
                json.dumps(meta_payload, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception as e:
            log.exception("No se pudo guardar el XML/metadata de auditoría: %s", e)

    # ---- (2) Envío real al servicio ----
    resp_text = ""
    try:
        resp = client.agregar_item_examenlab(
            idexamen=item["idexamen"],
            paciente=item["paciente_doc"],
            fecha=fecha_api,
            texto=item.get("texto", ""),
            valor_cualitativo=item.get("valor"),
            valor_referencia=item.get("ref"),
            valor_adicional=valor_adicional,
        )
        resp_text = resp.text or ""
    except Exception:
        # Si falla, intentamos también persistir la excepción
        log.exception(
            "Error enviando resultado (idexamen=%s, paciente=%s)",
            item.get("idexamen"),
            item.get("paciente_doc"),
        )
        if save_logs and resp_path:
            try:
                resp_path.write_text("EXCEPTION DURING SEND\n", encoding="utf-8")
            except Exception:
                pass
        # Propaga para que arriba tu UI pueda marcar ERROR si corresponde
        raise
    else:
        # ---- (3) Guardar respuesta si está habilitado ----
        if save_logs and resp_path:
            try:
                resp_path.write_text(resp_text, encoding="utf-8")
            except Exception as e:
                log.exception("No se pudo guardar la respuesta: %s", e)

    return resp_text
