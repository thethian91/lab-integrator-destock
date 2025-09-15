from __future__ import annotations

import os
import json
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, date
from collections.abc import Mapping
from typing import Any

from xml.etree.ElementTree import Element, SubElement, tostring

from lab_core.config import load_settings, read_cfg_safe
from lab_core.sender import SNTClient
from lab_core.utils.dates import to_yyyymmdd
from lab_core.db import get_conn, mark_obx_exported, mark_obx_error
from lab_core.utils.mapping_json import load_mapping, lookup_client_code


log = logging.getLogger("lab.integrator.pipeline")

MAX_RESULT_LEN = 250


def exportar_examen_concatenado(result_id: int) -> tuple[bool, str]:
    """
    Lee header + OBX de hl7_results/hl7_obx_results y envía:
      - 1 OBX  -> valor/unidades/rango/flags (resultado “normal”) en 'valor'
      - 2+ OBX -> concatenado en 'texto' (valor/ref/units en None)
    Retorna (ok, mensaje).
    """
    try:
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row

            # Header principal del examen
            header_data = conn.execute(
                """
                SELECT id
                     , analyzer_name
                     , patient_id    AS paciente_doc
                     , patient_name  AS paciente_nombre
                     , exam_code     AS codigo_examen
                     , exam_title    AS titulo_examen
                     , received_at   AS fecha_hora
                  FROM hl7_results
                 WHERE id = ?
                """,
                (result_id,),
            ).fetchone()

            if not header_data:
                return False, f"Examen {result_id} no existe"

            # Todos los OBX del examen (normalizados a dict)
            obx_rows = [
                dict(r)
                for r in conn.execute(
                    """
                    SELECT id        AS id
                         , result_id AS obr_id
                         , id        AS seq
                         , code      AS obx_code
                         , text      AS obx_name
                         , value     AS obx_value
                         , units     AS obx_unit   
                         , ref_range AS obx_refrange
                         , flags
                         , obs_dt
                      FROM hl7_obx_results
                     WHERE result_id = ?
                     ORDER BY id ASC
                    """,
                    (result_id,),
                ).fetchall()
            ]
            total = len(obx_rows)
            oks = 0
            errs = 0
            last_resp: str | None = None
            id_examen: str | None = None
            fecha_order: str | None = None

            for item_raw in obx_rows:

                # get idExamen
                data, alias_idx = load_mapping("configs/mapping.json")
                protocolo_code, protocolo_name = lookup_client_code(
                    data,
                    alias_idx,
                    header_data["analyzer_name"],
                    item_raw.get("obx_name"),
                )

                order_info = conn.execute(
                    ''' 
                      SELECT id    AS id_examen
                           , fecha AS fecha_orden
                        FROM exams 
                       WHERE paciente_doc = ?
                         AND protocolo_codigo = ?
                    ORDER BY fecha DESC 
                    LIMIT 1
                ''',
                    (header_data["paciente_doc"], protocolo_code),
                ).fetchone()

                if not order_info:
                    return False, f"Info order no encontrada {result_id}"

                id_examen = order_info[0]
                fecha_order = str(order_info[1]).split(" ")[0]

                item = {
                    "idexamen": id_examen,
                    "paciente_doc": header_data["paciente_doc"],
                    "fecha": fecha_order,
                    "texto": (item_raw.get("obx_name") or None),
                    "valor": (item_raw.get("obx_value") or None),
                    "ref": (item_raw.get("obx_refrange") or None),
                    "units": (item_raw.get("obx_unit") or None),
                }
                try:
                    resp_text = enviar_resultado_item(item)
                    last_resp = resp_text or last_resp
                    mark_obx_exported(conn, item_raw["id"])
                    oks += 1
                except Exception as e:
                    print(e)
                    mark_obx_error(conn, item_raw["id"], str(e))
                    errs += 1

            conn.commit()
            ok_global = errs == 0
            resumen = f"OBX enviados: {total} | OK: {oks} | ERROR: {errs}"
            if last_resp and ok_global:
                resumen += f" | Última respuesta: {last_resp[:120]}"

            # =======================================================
            # NEW: notificar examen completo si todo salió bien
            cfg_raw = read_cfg_safe()  # dict crudo del YAML
            upd = (cfg_raw.get("api") or {}).get("update_exam") or {}
            if upd.get("enabled", True):
                resultado_global = upd.get("resultado_global", "Normal")
                responsable = upd.get("responsable", "PENDIENTEVALIDAR")
                notas = upd.get("notas", "Enviado desde integracion")

                client = build_snt_client()
                # order_info y header_data ya existen en el scope de esta función
                paciente = header_data["paciente_doc"]

                try:
                    resp = client.actualizar_examenlab_fecha(
                        idexamen=id_examen,
                        paciente=paciente,
                        fecha=fecha_order,
                        resultado_global=resultado_global,
                        responsable=responsable,
                        notas=notas,
                    )
                    print(f"resp: {resp}")
                    log.info(
                        "Examen %s marcado como COMPLETADO (respuesta: %s)",
                        id_examen,
                        (resp.text[:200] if hasattr(resp, "text") else ""),
                    )
                except Exception as e:
                    log.error(
                        "Fallo notificando examen completo id=%s: %s", id_examen, e
                    )
            # =======================================================

            return ok_global, resumen

    except Exception as err:
        log.error(err)
        return False, f"ERROR inesperado: {err}"


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
        "fecha": fecha_api,
        "texto": item.get("texto", ""),
        "valor": item.get("valor", ""),
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
