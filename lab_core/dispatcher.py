# lab_core/dispatcher.py
from __future__ import annotations

import sqlite3
import json
from pathlib import Path
from typing import Dict, List
from datetime import datetime
import logging

from lab_core.db import (
    ensure_obx_dispatch_cols,
    get_conn,
    mark_obx_error,
    mark_obx_exported,
    mark_obx_request_response,
    mark_obx_mapping_not_found,
)
from lab_core.config import load_settings
from lab_core.file_tracer import FileTraceWriter
from lab_core.result_flow import (
    ResultSender,
    DefaultMappingRepo,
    DefaultExamRepo,
    DefaultXmlBuilder,
    DefaultApiClient,
    ErrorCode,
)

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
    s = x.strip().lower()
    for ch in (" ", "-", "_"):
        s = s.replace(ch, "")
    return s


def _safe_iter(o):
    if isinstance(o, dict):
        return o.items()
    if isinstance(o, (list, tuple)):
        return enumerate(o)
    return []


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
            "<OBX_TEXT>": { "client_code": "...", "client_title": "..." }
          }
        },
        ...
      }
    }
    """
    global _mapping_cache
    if _mapping_cache is not None:
        return _mapping_cache

    mapping_path = Path("configs/mapping.json")
    if not mapping_path.exists():
        log.warning(
            "mapping.json no encontrado en %s; mapping via JSON deshabilitado",
            mapping_path,
        )
        _mapping_cache = {}
        return _mapping_cache

    try:
        data = json.loads(mapping_path.read_text(encoding="utf-8"))
    except Exception:
        log.exception("Error leyendo configs/mapping.json")
        _mapping_cache = {}
        return _mapping_cache

    analyzers = data.get("analyzers") or {}
    norm_analyzers: dict[str, dict] = {}

    for name, cfg in analyzers.items():
        base = _norm_key(name)
        if not base:
            continue

        aliases = cfg.get("aliases") or []
        # "map" es dict { key: { client_code, ... } }
        amap = cfg.get("map") or {}
        entry = {
            "name": name,
            "aliases": aliases,
            "map": amap,
        }

        # idx por nombre base
        norm_analyzers[base] = entry

        # idx por alias
        for al in aliases:
            k = _norm_key(al)
            if k and k not in norm_analyzers:
                norm_analyzers[k] = entry

    _mapping_cache = norm_analyzers
    log.info("mapping.json cargado; analyzers en cache=%s", list(norm_analyzers.keys()))
    return _mapping_cache


def _resolve_analyzer_key(analyzer_name: str) -> dict | None:
    """
    Dado el analyzer_name (p.e. 'ICON3') intenta resolverlo en mapping.json
    usando nombre y aliases.
    """
    if not analyzer_name:
        return None
    cache = _load_mapping_json()
    k = _norm_key(analyzer_name)
    return cache.get(k)


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


def _build_obx_record_from_db(
    conn: sqlite3.Connection, result_id: int, obx_id: int
) -> Dict[str, str]:
    """Construye un obx_record compatible con ResultSender
    a partir de hl7_results (header) y hl7_obx_results (detalle).
    """
    conn.row_factory = sqlite3.Row

    header = conn.execute(
        """
        SELECT
            analyzer_name,
            patient_id,
            exam_date
        FROM hl7_results
        WHERE id = ?
        LIMIT 1
        """,
        (result_id,),
    ).fetchone()

    if not header:
        raise RuntimeError(f"No se encontró hl7_results.id={result_id}")

    obx = conn.execute(
        """
        SELECT
            code,
            text,
            value,
            units,
            ref_range
        FROM hl7_obx_results
        WHERE id = ?
        LIMIT 1
        """,
        (obx_id,),
    ).fetchone()

    if not obx:
        raise RuntimeError(f"No se encontró hl7_obx_results.id={obx_id}")

    analyzer = (header["analyzer_name"] or "").strip()
    tubo_muestra = (header["patient_id"] or "").strip()
    # fecha_ref = (header["exam_date"] or "").strip()  # por si se usa en el futuro

    obx_code = obx["code"] or ""
    obx_text = obx["text"] or ""
    obx_value = obx["value"]
    obx_units = _normalize_units(obx["units"])
    obx_ref = obx["ref_range"] or ""

    obx_record: Dict[str, str] = {
        "analyzer": analyzer,
        "code": obx_code,
        "text": obx_text,
        "value": obx_value,
        "unit": obx_units,
        "ref_range": obx_ref,
        "tubo_muestra": tubo_muestra,
        # No enviamos paciente_id aquí para forzar que ResultSender
        # tome el documento desde la tabla exams (paciente_doc).
        # "paciente_id": tubo_muestra,
        "ultimo_del_examen": False,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return obx_record


def _write_xml(out_dir: Path, result_id: int, obx_id: int, xml_text: str) -> Path:
    """
    Escribe XML a disco como:
      <out_dir>/xml_result_<RESULTID>_<OBXID>.xml
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"xml_result_{result_id}_{obx_id}.xml"
    fpath = out_dir / fname
    fpath.write_text(xml_text, encoding="utf-8")
    return fpath


def _safe_text(s) -> str:
    if s is None:
        return ""
    return str(s)


def _normalize_units(units: str | None) -> str:
    if not units:
        return ""
    # Aquí puedes normalizar símbolos si SOFIA da problemas con Unicode
    return units


def _is_mapped_obx_via_json(
    conn: sqlite3.Connection, result_id: int, obx_id: int
) -> tuple[bool, str | None]:
    """
    Retorna (is_mapped, client_code) para un OBX específico, usando mapping.json:

    - Identifica analyzer_name desde hl7_results
    - Busca analyzer en mapping.json (nombre + aliases)
    - Usa obx_code (o text) para buscar en "map" del analyzer
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        SELECT r.analyzer_name, o.code AS obx_code, o.text AS obx_text
          FROM hl7_obx_results o
          JOIN hl7_results r ON r.id = o.result_id
         WHERE o.id = ?
           AND o.result_id = ?
        """,
        (obx_id, result_id),
    )
    row = cur.fetchone()
    if not row:
        log.warning("No se encontró row para obx_id=%s result_id=%s", obx_id, result_id)
        return False, None

    analyzer_name = row["analyzer_name"] or ""
    obx_code = (row["obx_code"] or "").strip()
    obx_text = (row["obx_text"] or "").strip()

    if not analyzer_name:
        log.warning("analyzer_name vacío para obx_id=%s", obx_id)
        return False, None

    analyzer_cfg = _resolve_analyzer_key(analyzer_name)
    if not analyzer_cfg:
        # No existe ese analyzer (ni como alias) en mapping.json
        log.info(
            "Analyzer '%s' no encontrado en mapping.json (obx_id=%s)",
            analyzer_name,
            obx_id,
        )
        return False, None

    amap = analyzer_cfg.get("map") or {}
    if not amap:
        log.info(
            "Analyzer '%s' no tiene 'map' definido en mapping.json (obx_id=%s)",
            analyzer_cfg.get("name"),
            obx_id,
        )
        return False, None

    # --- Intentar primero por código exacto ---
    if obx_code:
        if obx_code in amap:
            info = amap[obx_code] or {}
            code = info.get("client_code")
            if code:
                return True, str(code)

    # --- Luego por texto exacto (no parcial) ---
    if obx_text:
        tx_norm = _norm_key(obx_text)
        for key, info in _safe_iter(amap):
            k_norm = _norm_key(key)
            if tx_norm == k_norm:
                code = info.get("client_code")
                if code:
                    return True, str(code)

    # Si no hay coincidencia exacta, no hay mapping
    log.info(
        "No mapping para analyzer='%s' (map-name='%s') obx_code='%s' obx_text='%s'",
        analyzer_name,
        analyzer_cfg.get("name"),
        obx_code,
        obx_text,
    )
    return False, None


def _build_item_from_db(
    conn: sqlite3.Connection, result_id: int, obx_id: int
) -> Dict[str, str]:
    """
    Construye dict con la data necesaria para enviar a SNT.
    Se usa para debug y compatibilidad con pipeline.enviar_resultado_item.
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            r.id          AS result_id,
            r.patient_id  AS patient_id,
            r.exam_date   AS exam_date,
            r.exam_time   AS exam_time,
            r.order_number AS order_number,
            r.exam_code   AS exam_code,
            r.exam_title  AS exam_title,
            o.id          AS obx_id,
            o.code        AS obx_code,
            o.text        AS obx_text,
            o.value       AS obx_value,
            o.units       AS obx_units,
            o.ref_range   AS obx_ref_range,
            o.flags       AS obx_flags
        FROM hl7_results r
        JOIN hl7_obx_results o ON o.result_id = r.id
        WHERE r.id = ?
          AND o.id = ?
        """,
        (result_id, obx_id),
    )
    r = cur.fetchone()
    if not r:
        raise RuntimeError(
            f"No se encontró result/obx para result_id={result_id}, obx_id={obx_id}"
        )

    # Opcional: buscar idexamen y fecha en otra tabla (exams) si aplica.
    # NOTA: en este nuevo flujo usamos ResultSender + DefaultExamRepo para esto.
    fecha_order = None
    try:
        cur.execute(
            """
            SELECT fecha
              FROM exams
             WHERE codigo_tubo = ?
             ORDER BY fecha DESC
             LIMIT 1
            """,
            (r["patient_id"],),
        )
        row_fecha = cur.fetchone()
        if row_fecha:
            fecha_order = (row_fecha["fecha"] or "").strip()
    except Exception:
        # En caso que la tabla 'exams' no exista o falle, no rompemos el flujo.
        log.debug(
            "No se pudo recuperar fecha desde exams para patient_id=%s", r["patient_id"]
        )

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
        "units": r["obx_units"] or "",
        "ref_range": r["obx_ref_range"] or "",
        "flags": r["obx_flags"] or "",
        "fecha_order_real": fecha_order or "",
    }
    return item


def dispatch_cycle(db_path: str, out_dir: str, batch_size: int = 200) -> dict[str, int]:
    """Ciclo de despacho automático usando el mismo flujo que el envío manual.

    Flujo:
      - Selecciona OBX pendientes desde hl7_obx_results
      - Para cada OBX construye un obx_record desde la BD
      - Llama a ResultSender.process_obx(obx_record)
      - Marca export_status según el resultado (EXPORTED / MAPPING_NOT_FOUND / ERROR)
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

    # --- Construir el sender igual que en el proceso manual ---
    api_cfg = cfg.api
    api_client = DefaultApiClient(
        base_url=api_cfg.base_url,
        api_key=api_cfg.key,
        api_secret=api_cfg.secret,
        timeout=getattr(api_cfg, "timeout", 30),
        default_resultado_global=getattr(api_cfg, "resultado_global", "Normal"),
        default_responsable=getattr(api_cfg, "responsable", "PENDIENTEVALIDAR"),
        default_notas=getattr(api_cfg, "notas", "Enviado desde integracion"),
    )

    trace_writer = FileTraceWriter(
        enabled=bool(getattr(cfg.result_export, "save_files", False)),
        base_dir=str(getattr(cfg.result_export, "save_dir", "outbox_xml")),
    )

    sender = ResultSender(
        mapping_repo=DefaultMappingRepo(mapping_path="configs/mapping.json"),
        exam_repo=DefaultExamRepo(db_path=db_path),
        xml_builder=DefaultXmlBuilder(),
        api_client=api_client,
        logger=log,
        trace_writer=trace_writer,
    )

    ensure_obx_dispatch_cols(db_path)
    sent = err = 0

    conn = get_conn(db_path)
    try:
        pend = _select_pending_obx(conn, batch_size)
        log.info("Pending OBX picked=%s", len(pend))
        if not pend:
            return {"picked": 0, "sent": 0, "error": 0}

        # Agrupar pending por result_id para mantener trazabilidad por examen
        by_result: Dict[int, List[int]] = {}
        for r in pend:
            by_result.setdefault(int(r["result_id"]), []).append(int(r["obx_id"]))

        for result_id, obx_ids in by_result.items():
            for obx_id in obx_ids:
                try:
                    obx_record = _build_obx_record_from_db(conn, result_id, obx_id)

                    log.info(
                        "OBX %s -> sending via ResultSender (analyzer=%s text=%s)",
                        obx_id,
                        obx_record.get("analyzer"),
                        obx_record.get("text"),
                    )

                    outcome = sender.process_obx(obx_record)

                    if outcome.ok:
                        # Exportado correctamente
                        mark_obx_exported(conn, obx_id, "")
                        sent += 1
                    else:
                        # Si hay error de mapping, marcamos específicamente
                        any_mapping_err = any(
                            code == ErrorCode.MAPPING_NOT_FOUND
                            for code, _ in outcome.errors
                        )
                        if any_mapping_err:
                            mark_obx_mapping_not_found(conn, obx_id)
                        else:
                            msg = (
                                "; ".join(
                                    f"{code}: {text}" for code, text in outcome.errors
                                )
                                or "UNKNOWN_ERROR"
                            )
                            mark_obx_error(conn, obx_id, msg)
                        err += 1

                    conn.commit()

                except Exception as ex:
                    log.exception("OBX %s -> ERROR en dispatch_cycle", obx_id)
                    mark_obx_error(conn, obx_id, f"EXCEPTION: {ex}")
                    conn.commit()
                    err += 1

        log.info(
            "Dispatch cycle end | picked=%s sent=%s error=%s", len(pend), sent, err
        )
        return {"picked": len(pend), "sent": sent, "error": err}
    finally:
        conn.close()
