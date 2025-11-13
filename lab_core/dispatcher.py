# lab_core/dispatcher.py
from __future__ import annotations

import sqlite3
import json
from pathlib import Path
from typing import Dict, List

from lab_core.db import (
    ensure_obx_dispatch_cols,
    get_conn,
    mark_obx_error,
    mark_obx_exported,
    mark_obx_request_response,
    mark_obx_mapping_not_found,
)
from lab_core.xml_builder import build_log_envio_for_result
from lab_core.config import load_settings

# enviar_resultado_item debe aceptar dict con "client_code" si se lo pasamos
from lab_core.pipeline import enviar_resultado_item
from lab_core.results_store import find_exam_id_by_keys
import logging

log = logging.getLogger("lab.integrator.dispatcher")

# ================== Cache interno para mapping ================== #

_mapping_cache: dict[str, dict] | None = None  # cache estructurada


def _as_bool(x, default: bool = False) -> bool:
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return x != 0
    if isinstance(x, str):
        return x.strip().lower() in ("1", "true", "yes", "y", "on")
    return default


def _norm_key(x: str | None) -> str:
    """
    Normaliza claves para matching (case-insensitive, trim, sin espacios/guiones/underscores).
    """
    if not x:
        return ""
    s = str(x).strip().lower()
    s = s.replace("-", "").replace("_", "").replace(" ", "")
    return s


def _safe_iter(x):
    """Convierte a iterable de forma segura."""
    if x is None:
        return []
    if isinstance(x, (list, tuple, set)):
        return list(x)
    return [x]


def _load_mapping_json() -> dict[str, dict]:
    """
    Carga 'configs/mapping.json' con el esquema:

    {
      "version": 1,
      "updated_at": "...",
      "analyzers": {
        "<NAME>": {
          "aliases": ["ALIAS1", "ALIAS-2", ...],
          "map": {
            "<OBX_CODE>": { "client_code": "...", "client_title": "..." },
            ...
          }
        }
      }
    }

    Devuelve un cache estructurado:
    {
      "alias_to_analyzer": { "<alias_norm>": "<canonical_analyzer_norm>", ... },
      "by_analyzer": {
        "<canonical_analyzer_norm>": {
          "map": { "<obx_code_norm>": {"client_code": "...", "client_title": "..."} , ... },
          "raw_name": "<NAME tal como aparece en el json>"
        },
        ...
      }
    }
    """
    global _mapping_cache
    if _mapping_cache is not None:
        return _mapping_cache

    mapping_file = Path("configs/mapping.json")
    cache: dict[str, dict] = {
        "alias_to_analyzer": {},
        "by_analyzer": {},
    }

    if not mapping_file.exists():
        _mapping_cache = cache
        return cache

    try:
        raw = json.loads(mapping_file.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("mapping.json inválido: %s", e)
        _mapping_cache = cache
        return cache

    analyzers = {}
    if isinstance(raw, dict):
        analyzers = raw.get("analyzers", {})
    if not isinstance(analyzers, dict):
        log.warning("mapping.json -> 'analyzers' no es dict; ignorado")
        _mapping_cache = cache
        return cache

    alias_to_an = cache["alias_to_analyzer"]
    by_an = cache["by_analyzer"]

    for name, node in analyzers.items():
        canonical = _norm_key(name)
        if not canonical:
            log.warning("Analyzer con nombre vacío; saltando")
            continue

        if not isinstance(node, dict):
            log.warning(
                "Analyzer '%s' no es dict; saltando (type=%s)",
                name,
                type(node).__name__,
            )
            continue

        aliases = node.get("aliases", [])
        if isinstance(aliases, (str, int, float)):
            aliases = [aliases]
        if not isinstance(aliases, list):
            aliases = []

        codes = node.get("map", {})
        if not isinstance(codes, dict):
            log.warning("Analyzer '%s' -> 'map' no es dict; usando vacío", name)
            codes = {}

        code_map_norm: dict[str, dict] = {}
        for obx_code, payload in codes.items():
            key = _norm_key(str(obx_code))
            if not key:
                continue
            if not isinstance(payload, dict):
                log.warning(
                    "Analyzer '%s' code '%s': payload no dict; saltando", name, obx_code
                )
                continue
            code_map_norm[key] = {
                "client_code": payload.get("client_code"),
                "client_title": payload.get("client_title"),
            }

        by_an[canonical] = {
            "map": code_map_norm,
            "raw_name": name,
        }

        # registrar alias → canonical (incluye el nombre crudo y el canonical)
        for a in [name, canonical, *_safe_iter(aliases)]:
            alias_to_an[_norm_key(str(a))] = canonical

    _mapping_cache = cache
    return cache


def _resolve_analyzer_key(analyzer_name: str | None) -> str | None:
    """
    Resuelve analyzer_name (con aliases) al canonical key.
    """
    name_norm = _norm_key(analyzer_name or "")
    if not name_norm:
        return None
    cache = _load_mapping_json()
    return cache["alias_to_analyzer"].get(name_norm)


# ------------------ Helpers internos de dispatch ------------------ #


def _select_pending_obx(conn: sqlite3.Connection, limit: int) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT o.id AS obx_id, o.result_id
          FROM hl7_obx_results o
         WHERE COALESCE(o.export_status,'') NOT IN ('SENT','SKIPPED', 'ERROR')
         ORDER BY o.id
         LIMIT ?
        """,
        (limit,),
    )
    return cur.fetchall()


def _write_xml(out_dir: Path, result_id: int, obx_id: int, xml: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"log_envio_{result_id}_{obx_id}.xml"
    path.write_text(xml, encoding="utf-8")
    return path


def _safe_text(obj) -> str:
    try:
        if obj is None:
            return ""
        if hasattr(obj, "text"):
            return obj.text or ""
        return str(obj)
    except Exception:
        return ""


def _normalize_units(u: str | None) -> str | None:
    if not u:
        return u
    u = u.strip()
    u = u.replace("μ", "µ")  # greek mu -> micro sign
    # Si el cliente exige ASCII:
    # u = u.replace("µ", "u")
    return u


def _is_mapped_obx_via_json(
    conn: sqlite3.Connection, result_id: int, obx_id: int
) -> tuple[bool, str | None]:
    """
    Devuelve (is_mapped, client_code | None) según mapping.json (match por obx_code + analyzer alias).
    """
    conn.row_factory = sqlite3.Row
    raw = conn.execute(
        """SELECT r.analyzer_name, r.exam_code AS obr_code, o.code AS obx_code, o.text AS obx_text
           FROM hl7_results r JOIN hl7_obx_results o ON o.result_id=r.id
           WHERE r.id=? AND o.id=? LIMIT 1""",
        (result_id, obx_id),
    ).fetchone()
    if not raw:
        return (False, None)

    canonical = _resolve_analyzer_key(raw["analyzer_name"])
    if not canonical:
        # analyzer no reconocido por alias
        return (False, None)

    cache = _load_mapping_json()
    entry = cache["by_analyzer"].get(canonical, {})
    code_map = entry.get("map", {})

    obx_code_norm = _norm_key(raw["obx_text"])
    # obx_code_norm = raw["obx_text"]
    hit = code_map.get(obx_code_norm)
    if hit and (hit.get("client_code") or hit.get("client_title")):
        return (True, hit.get("client_code"))

    return (False, None)


def _build_item_from_db(
    conn: sqlite3.Connection, result_id: int, obx_id: int
) -> Dict[str, str]:
    """
    Construye el payload mínimo por analito. Ajusta llaves si tu API requiere otras.
    """
    conn.row_factory = sqlite3.Row
    r = conn.execute(
        """
        SELECT
            r.patient_id,
            r.order_number,
            r.exam_code,
            r.exam_title,
            r.exam_date,
            r.exam_time,
            o.id         AS obx_pk,
            o.obx_id     AS obx_id,
            o.code       AS obx_code,
            o.text       AS obx_text,
            o.value      AS obx_value,
            o.units      AS obx_units,
            o.ref_range  AS obx_ref_range,
            o.flags      AS obx_flags,
            o.obs_dt     AS obx_obs_dt
        FROM hl7_results r
        JOIN hl7_obx_results o ON o.result_id = r.id
        WHERE r.id = ? AND o.id = ?
        LIMIT 1
        """,
        (result_id, obx_id),
    ).fetchone()

    if not r:
        raise RuntimeError("Registro OBX no encontrado para construir payload.")

    # === Resolver idExamen usando la BD local (tabla exams) ===
    paciente_doc = (r["patient_id"] or "").strip()
    protocolo_code = (r["exam_code"] or "").strip()
    nombre_pac = (r["patient_name"] or "").strip()

    exam_id = find_exam_id_by_keys(
        paciente_doc=paciente_doc,
        protocolo_codigo=protocolo_code,
        tubo_muestra=paciente_doc,  # si luego quieres usar 'order_number' como tubo_muestra, pásalo aquí
        nombre_paciente=nombre_pac,
        db_path="data/labintegrador.db",
    )

    # Traer la fecha real de la orden desde exams (la que te exige el cliente)
    fecha_order = None
    if exam_id:
        row_fecha = conn.execute(
            "SELECT fecha FROM exams WHERE id = ?", (exam_id,)
        ).fetchone()
        if row_fecha:
            fecha_order = (row_fecha["fecha"] or "").strip()

    item = {
        "patient_id": r["patient_id"] or "",
        "order_number": r["order_number"] or "",
        "exam_code": r["exam_code"] or "",
        "exam_title": r["exam_title"] or "",
        "exam_date": r["exam_date"] or "",
        "exam_time": r["exam_time"] or "",
        "obx_id": r["obx_id"] or "",
        "code": r["obx_code"] or "",
        "text": r["obx_text"] or "",
        "value": r["obx_value"] or "",
        "units": _normalize_units(r["obx_units"]),
        "ref_range": r["obx_ref_range"] or "",
        "flags": r["obx_flags"] or "",
        "obs_dt": r["obx_obs_dt"] or "",
    }
    return item


# ------------------ Ciclo principal ------------------ #


def dispatch_cycle(db_path: str, out_dir: str, batch_size: int = 200) -> dict[str, int]:
    """
    Un ciclo:
      - Selecciona OBX pendientes
      - Verifica mapeo via mapping.json (por analyzer/alias + obx_code).
        Si NO está mapeado -> mapping_not_found y NO envía ni genera XML.
      - Genera XML (uno por OBX) con build_log_envio_for_result
      - Según 'delivery_mode': escribe archivo, envía a SNT, o ambos
      - Guarda request/response en BD por analito
    Retorna stats: {"picked": N, "sent": A, "error": B}
    """
    cfg = load_settings()
    mode = cfg.result_export.delivery_mode
    save_files = cfg.result_export.save_files

    log.info(
        "Dispatch cycle start | mode=%s save_files=%s batch=%s db=%s out=%s",
        mode,
        save_files,
        batch_size,
        db_path,
        out_dir,
    )

    ensure_obx_dispatch_cols(db_path)
    sent = err = 0

    out = Path(out_dir)
    conn = get_conn(db_path)
    try:
        pend = _select_pending_obx(conn, batch_size)
        log.info("Pending OBX picked=%s", len(pend))
        if not pend:
            return {"picked": 0, "sent": 0, "error": 0}

        # Agrupar pending por result_id para no recalcular múltiples veces
        by_result: Dict[int, List[int]] = {}
        for r in pend:
            by_result.setdefault(int(r["result_id"]), []).append(int(r["obx_id"]))

        for result_id, obx_ids in by_result.items():
            # Construye todos los XML de ese resultado (luego filtramos por mapping)
            pairs = build_log_envio_for_result(conn, result_id)  # [(obx_id, xml), ...]
            xml_map = {oid: xml for (oid, xml) in pairs}

            for obx_id in obx_ids:
                try:
                    # 0) Filtrar por mapping ANTES de todo
                    is_mapped, client_code = _is_mapped_obx_via_json(
                        conn, result_id, obx_id
                    )
                    if not is_mapped:
                        log.info("OBX %s -> SKIP (mapping_not_found)", obx_id)
                        mark_obx_mapping_not_found(conn, obx_id)
                        conn.commit()
                        err += 1  # si quieres distinguir, crea 'skipped' y no sumes a 'err'
                        continue

                    if obx_id not in xml_map:
                        # Si por alguna razón no vino XML (debería estar si está mapeado)
                        log.warning("OBX %s -> XML missing after mapping OK", obx_id)
                        mark_obx_error(conn, obx_id, "xml_generation_missing")
                        conn.commit()
                        err += 1
                        continue

                    xml = xml_map[obx_id]
                    path_written = None

                    # 1) Guardar XML a disco si corresponde
                    if save_files or mode in ("xml_only", "both"):
                        path_written = _write_xml(out, result_id, obx_id, xml)
                        log.debug("OBX %s -> XML written: %s", obx_id, path_written)

                    # 2) Enviar a SNT si corresponde
                    if mode in ("http_direct", "both"):
                        item = _build_item_from_db(conn, result_id, obx_id)
                        # Inyectamos client_code si lo tenemos
                        if client_code:
                            item["client_code"] = client_code

                        log.info(
                            "OBX %s -> sending to API (order=%s code=%s client_code=%s text=%s)",
                            obx_id,
                            item.get("order_number"),
                            item.get("code"),
                            item.get("client_code"),
                            item.get("text"),
                        )

                        print(f'------------------------')
                        print(f'item: {item}')
                        print(f'------------------------')
                        resp = enviar_resultado_item(item)
                        resp_text = _safe_text(resp)
                        status = getattr(resp, "status_code", None)
                        log.info(
                            "OBX %s -> API response status=%s preview=%s",
                            obx_id,
                            status,
                            (resp_text[:200] if resp_text else ""),
                        )

                        mark_obx_request_response(
                            conn, obx_id, request_xml=xml, response_text=resp_text
                        )
                        mark_obx_exported(
                            conn, obx_id, str(path_written) if path_written else ""
                        )

                    elif mode == "xml_only":
                        mark_obx_exported(
                            conn, obx_id, str(path_written) if path_written else ""
                        )

                    else:
                        raise RuntimeError(f"delivery_mode inválido: {mode}")

                    conn.commit()
                    sent += 1

                except Exception:
                    log.exception("OBX %s -> ERROR", obx_id)
                    mark_obx_error(conn, obx_id, "see logs")
                    conn.commit()
                    err += 1

        log.info(
            "Dispatch cycle end | picked=%s sent=%s error=%s", len(pend), sent, err
        )
        return {"picked": len(pend), "sent": sent, "error": err}
    finally:
        conn.close()
