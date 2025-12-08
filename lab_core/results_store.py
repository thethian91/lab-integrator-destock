# lab_core/results_store.py
from __future__ import annotations

import sqlite3

from .db import get_conn

DB_PATH = "data/labintegrador.db"


def find_exam_id_by_keys(
    paciente_doc: str | None,
    protocolo_codigo: str | None,
    tubo_muestra: str | None,
    nombre_paciente: str | None = None,
    db_path: str = DB_PATH,
) -> int | None:
    """
    Matching flexible:
    1) Por tubo_muestra (exacto)
    2) Por (documento + protocolo)
    3) Por (nombre + protocolo) [cuando no hay documento, ej. Icon-3]
    Devuelve exams.id o None si no encuentra.
    """
    conn = get_conn(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1) tubo_muestra
    if tubo_muestra:
        cur.execute(
            "SELECT id FROM exams WHERE tubo_muestra = ? ORDER BY id DESC LIMIT 1",
            (tubo_muestra,),
        )
        r = cur.fetchone()
        if r:
            conn.close()
            return r["id"]

    # 2) documento + protocolo
    if paciente_doc and protocolo_codigo:
        cur.execute(
            """
          SELECT e.id
          FROM exams e
          WHERE e.paciente_doc = ? AND e.protocolo_codigo = ?
          ORDER BY e.fecha DESC, e.hora DESC, e.id DESC
          LIMIT 1
        """,
            (paciente_doc, protocolo_codigo),
        )
        r = cur.fetchone()
        if r:
            conn.close()
            return r["id"]

    # 3) nombre + protocolo (join patients) — útil para Icon-3
    if nombre_paciente and protocolo_codigo:
        cur.execute(
            """
          SELECT e.id
          FROM exams e
          JOIN patients p ON p.documento = e.paciente_doc
          WHERE UPPER(p.nombre) = UPPER(?) AND e.protocolo_codigo = ?
          ORDER BY e.fecha DESC, e.hora DESC, e.id DESC
          LIMIT 1
        """,
            (nombre_paciente, protocolo_codigo),
        )
        r = cur.fetchone()
        if r:
            conn.close()
            return r["id"]

    conn.close()
    return None


def attach_result_by_id(
    exam_id: int,
    result_xml: str | None,
    result_value: str | None,
    db_path: str = DB_PATH,
):
    """Adjunta resumen/valor y marca como RESULTED (si no lo estaba)."""
    conn = get_conn(db_path)
    with conn:
        conn.execute(
            """
          UPDATE exams
          SET result_xml = COALESCE(?, result_xml),
              result_value = COALESCE(?, result_value),
              status = 'RESULTED',
              resulted_at = datetime('now')
          WHERE id = ?
        """,
            (result_xml, result_value, exam_id),
        )
    conn.close()


def mark_sent(exam_id: int, db_path: str = DB_PATH):
    """Marca examen como enviado (SENT)."""
    conn = get_conn(db_path)
    with conn:
        conn.execute(
            """
          UPDATE exams
          SET status = 'SENT',
              sent_at = datetime('now')
          WHERE id = ?
        """,
            (exam_id,),
        )
    conn.close()
