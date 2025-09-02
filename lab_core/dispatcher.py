# lab_core/dispatcher.py
from __future__ import annotations

import sqlite3
from pathlib import Path

from lab_core.db import (
    ensure_obx_dispatch_cols,
    get_conn,
    mark_obx_error,
    mark_obx_exported,
)
from lab_core.xml_builder import build_log_envio_for_result


def _select_pending_obx(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT o.id AS obx_id, o.result_id
          FROM hl7_obx_results o
         WHERE COALESCE(o.export_status,'') NOT IN ('SENT','SKIPPED')
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


def dispatch_cycle(db_path: str, out_dir: str, batch_size: int = 200) -> dict[str, int]:
    """
    Un ciclo:
      - Selecciona OBX pendientes
      - Genera XML (uno por OBX) con build_log_envio_for_result
      - Escribe a disco y marca SENT, o ERROR si falla
    Retorna stats: {"picked": N, "sent": A, "error": B}
    """
    ensure_obx_dispatch_cols(db_path)
    sent = err = 0

    out = Path(out_dir)
    conn = get_conn(db_path)
    try:
        pend = _select_pending_obx(conn, batch_size)
        if not pend:
            return {"picked": 0, "sent": 0, "error": 0}

        # Agrupar pending por result_id para no recalcular múltiples veces
        by_result: dict[int, list[int]] = {}
        for r in pend:
            by_result.setdefault(int(r["result_id"]), []).append(int(r["obx_id"]))

        for result_id, obx_ids in by_result.items():
            # Construye todos los XML de ese resultado
            pairs = build_log_envio_for_result(conn, result_id)  # [(obx_id, xml), ...]
            xml_map = {oid: xml for (oid, xml) in pairs}

            for obx_id in obx_ids:
                try:
                    if obx_id not in xml_map:
                        raise RuntimeError("XML no generado: OBX no presente en build.")
                    xml = xml_map[obx_id]
                    path = _write_xml(out, result_id, obx_id, xml)
                    mark_obx_exported(conn, obx_id, str(path))
                    conn.commit()
                    sent += 1
                except Exception as e:
                    mark_obx_error(conn, obx_id, str(e))
                    conn.commit()
                    err += 1

        return {"picked": len(pend), "sent": sent, "error": err}
    finally:
        conn.close()
