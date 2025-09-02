# lab_core/pipeline.py
from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from lab_core.config import load_settings
from lab_core.sender import SNTClient
from lab_core.utils.dates import to_yyyymmdd

log = logging.getLogger("lab.integrator.pipeline")


def build_snt_client():
    cfg = load_settings()
    return SNTClient(
        base_url=cfg.api.base_url,
        api_key=cfg.api.key,
        api_secret=cfg.api.secret,
        timeout=cfg.api.timeout,
    )


def enviar_resultado_item(item: Mapping[str, Any]) -> str:
    """
    item esperado:
      idexamen, paciente_doc, fecha, texto, valor, ref, units
    """
    client = build_snt_client()
    fecha_api = to_yyyymmdd(item.get("fecha"))
    valor_adicional = f"UNITS:{item['units']}" if item.get("units") else None

    resp = client.agregar_item_examenlab(
        idexamen=item["idexamen"],
        paciente=item["paciente_doc"],
        fecha=fecha_api,
        texto=item.get("texto", ""),
        valor_cualitativo=item.get("valor"),
        valor_referencia=item.get("ref"),
        valor_adicional=valor_adicional,
    )
    return resp.text or ""
