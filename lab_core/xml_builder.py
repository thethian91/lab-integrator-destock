# lab_core/xml_builder.py
from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping
from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.sax.saxutils import escape

from lab_core.db import code_map_lookup  # mapeo analizador -> cliente

from .transform import to_yyyymmdd


def _pretty_xml(elem: Element) -> str:
    rough = tostring(elem, encoding="utf-8")
    parsed = minidom.parseString(rough)
    return parsed.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")


def _compose_fecha(date_str: str, time_str: str) -> str:
    date_str = (date_str or "").strip()
    time_str = (time_str or "").strip()
    if date_str and time_str:
        return f"{date_str} {time_str}"
    return date_str or time_str or ""


def build_log_envio_xml_single(
    idexamen: str | int | None,
    paciente: str | None,
    fecha: str | None,
    texto: str | None,
    valor_cualitativo: str | None,
    valor_referencia: str | None,
    valor_adicional: str | None,
) -> str:
    """
    Construye el XML solicitado:
    <?xml version="1.0" encoding="utf-8"?>
    <log_envio>
      <idexamen>...</idexamen>
      <paciente>...</paciente>
      <fecha>...</fecha>
      <texto>...</texto>
      <valor_cualitativo>...</valor_cualitativo>
      <valor_referencia>...</valor_referencia>
      <valor_adicional>...</valor_adicional>
    </log_envio>
    """
    root = Element("log_envio")
    SubElement(root, "idexamen").text = "" if idexamen is None else str(idexamen)
    SubElement(root, "paciente").text = paciente or ""
    SubElement(root, "fecha").text = to_yyyymmdd(fecha) or ""
    SubElement(root, "texto").text = texto or ""
    SubElement(root, "valor_cualitativo").text = valor_cualitativo or ""
    SubElement(root, "valor_referencia").text = valor_referencia or ""
    SubElement(root, "valor_adicional").text = valor_adicional or ""
    return _pretty_xml(root)


# --------------------------------------------------------------------
# High-level: leer BD (hl7_results + hl7_obx_results) y cruzar con órdenes
# --------------------------------------------------------------------


def resolve_exam_for_result(
    conn: sqlite3.Connection,
    patient_id: str | None,
    exam_code: str | None,
    exam_title: str | None,
    exam_date: str | None,
) -> sqlite3.Row | None:
    """
    Intenta encontrar el examen (orden) en tu tabla de órdenes (asumo 'exams'):
    1) por paciente + protocolo_codigo (match exacto con exam_code)
    2) por paciente + protocolo_titulo (LIKE exam_title)
    3) por paciente + proximidad de fecha (±2 días)
    Devuelve la fila de 'exams' o None.
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1) match por código
    if patient_id and exam_code:
        cur.execute(
            """
            SELECT *
            FROM exams
            WHERE paciente_doc = ?
              AND protocolo_codigo = ?
            ORDER BY fecha DESC, hora DESC
            LIMIT 1
        """,
            (patient_id, exam_code),
        )
        row = cur.fetchone()
        if row:
            return row

    # 2) match por título
    if patient_id and exam_title:
        cur.execute(
            """
            SELECT *
            FROM exams
            WHERE paciente_doc = ?
              AND UPPER(protocolo_titulo) LIKE UPPER(?)
            ORDER BY fecha DESC, hora DESC
            LIMIT 1
        """,
            (patient_id, f"%{exam_title}%"),
        )
        row = cur.fetchone()
        if row:
            return row

    # 3) por proximidad de fecha (±2 días)
    if patient_id and exam_date:
        cur.execute(
            """
            SELECT *
            FROM exams
            WHERE paciente_doc = ?
              AND fecha BETWEEN date(?, '-2 day') AND date(?, '+2 day')
            ORDER BY ABS(julianday(fecha) - julianday(?)) ASC, hora DESC
            LIMIT 1
        """,
            (patient_id, exam_date, exam_date, exam_date),
        )
        row = cur.fetchone()
        if row:
            return row

    # Nada encontrado
    return None


def build_log_envio_for_result(
    conn: sqlite3.Connection,
    result_id: int,
) -> list[tuple[int, str]]:
    """
    Construye una lista de (obx_row_id, xml_string) para cada OBX del resultado indicado.
    Usa hl7_results + hl7_obx_results, cruza con 'exams' y aplica mapeo (code_map).
    """

    # ----------------- helpers internos -----------------
    def _normalize_date_if_compact(d: str | None) -> str | None:
        """'YYYYMMDD...' -> 'YYYY-MM-DD' (si ya tiene '-', se deja igual)."""
        if not d:
            return d
        s = d.strip()
        if "-" in s:
            return s
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) >= 8:
            return f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"
        return s

    def _hl7_ts_to_datetime_str(s: str | None) -> str:
        """
        Convierte HL7 TS (YYYYMMDD[HH[MM[SS]]]) o ISO a 'YYYY-MM-DD HH:MM:SS'.
        Si no se puede, devuelve ''.
        """
        if not s:
            return ""
        raw = s.strip()
        # Intento ISO
        try:
            from datetime import datetime

            ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return f"{ts.date().isoformat()} {ts.time().strftime('%H:%M:%S')}"
        except Exception:
            pass
        # HL7 TS
        digits = "".join(ch for ch in raw if ch.isdigit())
        if len(digits) >= 8:
            yyyy, mm, dd = digits[0:4], digits[4:6], digits[6:8]
            hh = digits[8:10] if len(digits) >= 10 else "00"
            mi = digits[10:12] if len(digits) >= 12 else "00"
            ss = digits[12:14] if len(digits) >= 14 else "00"
            return f"{yyyy}-{mm}-{dd} {hh}:{mi}:{ss}"
        return ""

    def _resolve_exam(
        cur: sqlite3.Cursor,
        patient_id: str | None,
        exam_code: str | None,
        exam_title: str | None,
        exam_date: str | None,
    ) -> sqlite3.Row | None:
        """
        Busca en 'exams' por:
          1) paciente + protocolo_codigo
          2) paciente + LIKE protocolo_titulo
          3) paciente + cercanía de fecha (±2 días)
        """
        # 1) por código
        if patient_id and exam_code:
            cur.execute(
                """
                SELECT * FROM exams
                WHERE paciente_doc = ? AND protocolo_codigo = ?
                ORDER BY fecha DESC, hora DESC
                LIMIT 1
            """,
                (patient_id, exam_code),
            )
            row = cur.fetchone()
            if row:
                return row
        # 2) por título
        if patient_id and exam_title:
            cur.execute(
                """
                SELECT * FROM exams
                WHERE paciente_doc = ? AND UPPER(protocolo_titulo) LIKE UPPER(?)
                ORDER BY fecha DESC, hora DESC
                LIMIT 1
            """,
                (patient_id, f"%{exam_title}%"),
            )
            row = cur.fetchone()
            if row:
                return row
        # 3) por proximidad de fecha
        if patient_id and exam_date:
            d = _normalize_date_if_compact(exam_date)
            if d:
                cur.execute(
                    """
                    SELECT *
                    FROM exams
                    WHERE paciente_doc = ?
                      AND fecha BETWEEN date(?, '-2 day') AND date(?, '+2 day')
                    ORDER BY ABS(julianday(fecha) - julianday(?)) ASC, hora DESC
                    LIMIT 1
                """,
                    (patient_id, d, d, d),
                )
                row = cur.fetchone()
                if row:
                    return row
        return None

    # ----------------- cabecera del resultado -----------------
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, analyzer_name, patient_id, patient_name, exam_code, exam_title, exam_date, exam_time
        FROM hl7_results
        WHERE id = ?
    """,
        (result_id,),
    )
    hdr = cur.fetchone()
    if not hdr:
        return []

    analyzer = (hdr["analyzer_name"] or "").strip()
    patient_id = (hdr["patient_id"] or "").strip()
    exam_code = (hdr["exam_code"] or "").strip()
    exam_title = (hdr["exam_title"] or "").strip()
    exam_date = _normalize_date_if_compact(hdr["exam_date"])
    exam_time = (hdr["exam_time"] or "").strip()
    fecha_base = _compose_fecha(exam_date, exam_time)

    # Resolver orden inicialmente con lo que vino de HL7
    exam_row = _resolve_exam(cur, patient_id, exam_code, exam_title, exam_date)

    # ----------------- cargar OBX -----------------
    cur.execute(
        """
        SELECT id, code, text, value, units, ref_range, flags, obs_dt
        FROM hl7_obx_results
        WHERE result_id = ?
        ORDER BY id
    """,
        (result_id,),
    )
    obx_rows = cur.fetchall()

    out: list[tuple[int, str]] = []

    for obx in obx_rows:
        obx_code = (obx["code"] or "").strip()
        obx_text = (obx["text"] or "").strip()
        obx_value = (obx["value"] or "").strip()
        obx_units = (obx["units"] or "").strip()
        obx_ref = (obx["ref_range"] or "").strip()
        obx_flags = (obx["flags"] or "").strip()
        obx_obs_dt = (obx["obs_dt"] or "").strip()

        # -------- mapeo a código del cliente --------
        client_code, client_title = code_map_lookup(
            conn,
            analyzer_name=analyzer,
            obr_code=exam_code,
            obx_code=obx_code,
            obx_text=obx_text,
        )

        # Si aún no hay orden resuelta y tenemos client_code, reintenta con ese código
        exam_effective = exam_row
        if not exam_effective and patient_id and client_code:
            cur2 = conn.cursor()
            cur2.execute(
                """
                SELECT * FROM exams
                WHERE paciente_doc = ? AND protocolo_codigo = ?
                ORDER BY fecha DESC, hora DESC
                LIMIT 1
            """,
                (patient_id, client_code),
            )
            exam_effective = cur2.fetchone()

        # -------- campos para el XML --------
        idexamen = exam_effective["id"] if exam_effective else None
        paciente = patient_id or (
            exam_effective["paciente_doc"] if exam_effective else ""
        )
        # Texto preferente: OBX.text/OBX.code; si faltara, usa título del mapeo
        texto = obx_text or obx_code or (client_title or "")
        valor_cualitativo = obx_value
        valor_referencia = obx_ref
        # Puedes concatenar flags si quieres más contexto:
        valor_adicional = obx_units if obx_units else obx_flags

        # Fecha efectiva: cabecera; si no hay, intenta con OBS datetime del OBX
        fecha_eff = fecha_base
        if (not fecha_eff) and obx_obs_dt:
            fecha_eff = _hl7_ts_to_datetime_str(obx_obs_dt)

        xml = build_log_envio_xml_single(
            idexamen=idexamen,
            paciente=paciente,
            fecha=fecha_eff,
            texto=texto,
            valor_cualitativo=valor_cualitativo,
            valor_referencia=valor_referencia,
            valor_adicional=valor_adicional,
        )
        out.append((obx["id"], xml))

    return out


def build_log_envio_for_result_range(
    conn: sqlite3.Connection,
    date_from: str,
    date_to: str,
    analyzer: str | None = None,
) -> list[tuple[int, int, str]]:
    """
    Genera XML para todos los resultados en un rango de fechas (por hl7_results.exam_date o received_at).
    Devuelve lista de (result_id, obx_id, xml).
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    sql = """
        SELECT id
        FROM hl7_results
        WHERE COALESCE(NULLIF(exam_date,''), substr(received_at,1,10)) BETWEEN ? AND ?
    """
    params = [date_from, date_to]
    if analyzer:
        sql += " AND analyzer_name = ?"
        params.append(analyzer)

    cur.execute(sql, params)
    results = [r["id"] for r in cur.fetchall()]

    out: list[tuple[int, int, str]] = []
    for rid in results:
        pairs = build_log_envio_for_result(conn, rid)
        for obx_id, xml in pairs:
            out.append((rid, obx_id, xml))
    return out


def _val(x) -> str:
    """Convierte None a '', y asegura str para el XML."""
    if x is None:
        return ""
    return str(x)


def _e(text: str) -> str:
    """Escapa caracteres especiales para XML."""
    return escape(_val(text), {'"': "&quot;", "'": "&apos;"})


def build_result_xml_multi(
    exam_row: Mapping, patient_row: Mapping, obx_rows: Iterable[Mapping]
) -> str:
    """
    Construye un XML de resultado con múltiples analitos (OBX).
    - exam_row: fila de 'exams' (dict-like)
    - patient_row: fila de 'patients' (dict-like)
    - obx_rows: lista de filas de 'obx_results' (dict-like)
    Devuelve string XML (UTF-8).
    """
    analitos_xml = []
    for r in obx_rows:
        code = r.get("client_code") or r.get("analyzer_code") or ""
        text = r.get("client_text") or r.get("analyzer_text") or ""
        analitos_xml.append(
            f"""    <analito>
      <codigo>{_e(code)}</codigo>
      <descripcion>{_e(text)}</descripcion>
      <valor>{_e(r.get('value',''))}</valor>
      <unidades>{_e(r.get('units',''))}</unidades>
      <rango>{_e(r.get('ref_range',''))}</rango>
      <bandera>{_e(r.get('flags',''))}</bandera>
      <fecha_observacion>{_e((r.get('obs_dt') or '').replace('T',' '))}</fecha_observacion>
    </analito>"""
        )

    analitos_str = "\n".join(analitos_xml)

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<resultado>
  <paciente documento="{_e(patient_row.get('documento',''))}">
    <nombre>{_e(patient_row.get('nombre',''))}</nombre>
    <sexo>{_e(patient_row.get('sexo',''))}</sexo>
    <fecha_nacimiento>{_e(patient_row.get('fecha_nacimiento',''))}</fecha_nacimiento>
  </paciente>
  <examen>
    <id>{_e(exam_row.get('id',''))}</id>
    <protocolo_codigo>{_e(exam_row.get('protocolo_codigo',''))}</protocolo_codigo>
    <protocolo_titulo>{_e(exam_row.get('protocolo_titulo',''))}</protocolo_titulo>
    <tubo>{_e(exam_row.get('tubo',''))}</tubo>
    <tubo_muestra>{_e(exam_row.get('tubo_muestra',''))}</tubo_muestra>
    <fecha>{_e(exam_row.get('fecha',''))}</fecha>
    <hora>{_e(exam_row.get('hora',''))}</hora>
{analitos_str}
  </examen>
</resultado>
"""
    return xml


def build_result_xml_single(
    exam_row: Mapping, patient_row: Mapping, obx_row: Mapping
) -> str:
    """Conveniencia: XML de un solo analito."""
    return build_result_xml_multi(exam_row, patient_row, [obx_row])
