# lab_core/result_ingest.py
from __future__ import annotations

import sqlite3
import traceback
from datetime import datetime
from pathlib import Path

from lab_core.hl7_parser import load_hl7_map_yaml, parse_hl7_configurable
from lab_core.hl7_reader import HL7Message, parse_hl7

HL7_MAP = load_hl7_map_yaml("configs/hl7_map.yaml")

# ============
# Ingesta Inbox
# ============


def ingest_inbox(inbox_path: str = "inbox"):
    """Procesa todos los archivos HL7 de la carpeta `inbox/`"""
    inbox = Path(inbox_path)
    processed = inbox / "processed"
    failed = inbox / "failed"
    processed.mkdir(exist_ok=True, parents=True)
    failed.mkdir(exist_ok=True, parents=True)

    files = sorted(inbox.glob("*.hl7"))
    count = 0
    for file in files:
        try:
            ingest_result_file(file)
            file.rename(processed / file.name)
            print(f"[Inbox] Procesado: {file.name}")
            count += 1
        except Exception as e:
            (failed / (file.stem + ".error.txt")).write_text(
                traceback.format_exc(), encoding="utf-8", errors="ignore"
            )
            file.rename(failed / file.name)
            print(
                f"[Inbox] ERROR al procesar {file.name}: {e} (ver {failed / (file.stem + '.error.txt')})"
            )
    print(f"[Inbox] Total procesados: {count}")


# =====================
# Ingesta archivo HL7
# =====================


def _split_iso(dt_iso: str) -> tuple[str, str]:
    if not dt_iso:
        return "", ""
    s = (dt_iso or "").strip()

    # 1) ISO (permite 'Z')
    try:
        ts = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return ts.date().isoformat(), ts.time().strftime("%H:%M:%S")
    except Exception:
        pass

    # 2) HL7 TS: YYYYMMDD[HH[MM[SS]]]
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 8:
        yyyy, mm, dd = digits[0:4], digits[4:6], digits[6:8]
        hh = digits[8:10] if len(digits) >= 10 else "00"
        mi = digits[10:12] if len(digits) >= 12 else "00"
        ss = digits[12:14] if len(digits) >= 14 else "00"
        return f"{yyyy}-{mm}-{dd}", f"{hh}:{mi}:{ss}"
    return "", ""


def ingest_result_file(path: Path) -> None:
    """
    Lee un archivo HL7 de resultados, lo parsea y guarda un registro RAW en la tabla hl7_results.
    Además, guarda los OBX en hl7_obx_results usando el parser configurable (YAML).
    """
    stage = "read"
    try:
        raw_bytes = path.read_bytes()  # bytes
        raw_text = raw_bytes.decode("utf-8", errors="replace")

        # 1) Parser clásico (aprovechamos analyzer_name, demográficos y fechas)
        stage = "parse_hl7(reader)"
        msg: HL7Message = parse_hl7(raw_bytes)

        # 2) Parser configurable (YAML) para campos críticos y OBX
        stage = "parse_hl7(configurable)"
        parsed = parse_hl7_configurable(raw_text, HL7_MAP)

        # Fecha/hora del examen: primero OBR/OBX del reader; si no, usar OBX.when del configurable
        base_dt = msg.obr.obr_dt or (msg.obx_list[0].obs_dt if msg.obx_list else "")
        if not base_dt:
            base_dt = parsed.get("exam_when") or ""
        if not base_dt and parsed.get("obx_list"):
            base_dt = parsed["obx_list"][0].get("when") or ""
        exam_date, exam_time = _split_iso(base_dt)

        # Campos base del registro RAW (reader) + overrides del configurable
        patient_id = parsed.get("patient_id") or (msg.pid.patient_id or "")
        exam_code = parsed.get("exam_code") or (msg.obr.proto_codigo or "")
        exam_title = parsed.get("exam_title") or (msg.obr.proto_texto or "")

        record = {
            "received_at": datetime.now().isoformat(timespec="seconds"),
            "analyzer_name": (msg.analyzer_name or "UNKNOWN"),
            "raw_hl7": raw_text,
            # PID
            "patient_id": patient_id,
            "patient_name": (parsed.get("patient_name") or msg.pid.nombre or ""),
            "birth_date": (msg.pid.f_nac or ""),
            "sex": (msg.pid.sexo or ""),
            # OBR
            "order_number": (msg.obr.placer or msg.obr.filler or ""),
            "exam_code": exam_code,
            "exam_title": exam_title,
            "exam_date": exam_date,
            "exam_time": exam_time,
            # Metadata
            "source_file": str(path),
            "status": "RAW",
        }

        stage = "save_result_record"
        save_result_record(record, parsed_obx=parsed.get("obx_list", []))

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[ERROR] Ingesta resultados en stage={stage}: {e}\n{tb}")
        raise


# ==============================
# Persistencia (con fallback)
# ==============================


def _get_conn_fallback() -> sqlite3.Connection:
    """
    Intenta usar lab_core.db si existe. Si no, crea/usa SQLite local ./data/labintegrador.db
    con la tabla hl7_results y hl7_obx_results.
    """
    try:
        # Si tienes tu propia capa de DB, úsala aquí:
        # from lab_core.db import get_conn, init_db
        # conn = get_conn()
        # return conn
        raise ImportError  # fuerza a ir al fallback si no tienes la capa anterior disponible
    except Exception:
        data_dir = Path("data")
        data_dir.mkdir(exist_ok=True, parents=True)
        db_path = data_dir / "labintegrador.db"
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        # tablas mínimas
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hl7_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at TEXT,
                analyzer_name TEXT,
                raw_hl7 TEXT,

                patient_id TEXT,
                patient_name TEXT,
                birth_date TEXT,
                sex TEXT,

                order_number TEXT,
                exam_code TEXT,
                exam_title TEXT,
                exam_date TEXT,
                exam_time TEXT,

                source_file TEXT,
                status TEXT
            )
        """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hl7_obx_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                result_id INTEGER,
                obx_id TEXT,
                code TEXT,
                text TEXT,
                value TEXT,
                units TEXT,
                ref_range TEXT,
                flags TEXT,
                obs_dt TEXT,
                FOREIGN KEY(result_id) REFERENCES hl7_results(id)
            )
        """
        )
        conn.commit()
        return conn


def save_result_record(data: dict, parsed_obx: list[dict] | None = None) -> None:
    """
    Guarda el registro base en hl7_results y, si se entrega parsed_obx, guarda los OBX en hl7_obx_results.
    (parsed_obx proviene de parse_hl7_configurable -> lista de dicts con keys: code,text,value,units,ref_range,status,when,raw)
    """
    # Blindaje de claves
    payload = {
        "received_at": data.get("received_at", ""),
        "analyzer_name": data.get("analyzer_name", "UNKNOWN"),
        "raw_hl7": data.get("raw_hl7", ""),
        "patient_id": data.get("patient_id", ""),
        "patient_name": data.get("patient_name", ""),
        "birth_date": data.get("birth_date", ""),
        "sex": data.get("sex", ""),
        "order_number": data.get("order_number", ""),
        "exam_code": data.get("exam_code", ""),
        "exam_title": data.get("exam_title", ""),
        "exam_date": data.get("exam_date", ""),
        "exam_time": data.get("exam_time", ""),
        "source_file": data.get("source_file", ""),
        "status": data.get("status", "RAW"),
    }

    conn = _get_conn_fallback()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO hl7_results (
            received_at, analyzer_name, raw_hl7,
            patient_id, patient_name, birth_date, sex,
            order_number, exam_code, exam_title, exam_date, exam_time,
            source_file, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload["received_at"],
            payload["analyzer_name"],
            payload["raw_hl7"],
            payload["patient_id"],
            payload["patient_name"],
            payload["birth_date"],
            payload["sex"],
            payload["order_number"],
            payload["exam_code"],
            payload["exam_title"],
            payload["exam_date"],
            payload["exam_time"],
            payload["source_file"],
            payload["status"],
        ),
    )
    result_id = cur.lastrowid

    # Guarda OBX desde el parser configurable
    if parsed_obx:
        for idx, obx in enumerate(parsed_obx):
            code = (obx.get("code") or "").strip()
            text = (obx.get("text") or "").strip()
            value = (obx.get("value") or "").strip()
            units = (obx.get("units") or "").strip()
            ref_range = (obx.get("ref_range") or "").strip()
            flags = (obx.get("status") or "").strip()
            obs_dt = (obx.get("when") or "").strip()

            # Genera un obx_id estable (si tienes otra regla, cámbiala aquí)
            obx_id = f"CODE-{code}" if code else f"OBX-{idx}"

            cur.execute(
                """
                INSERT INTO hl7_obx_results (
                    result_id, obx_id, code, text, value, units, ref_range, flags, obs_dt
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_id,
                    obx_id,
                    code,
                    text,
                    value,
                    units,
                    ref_range,
                    flags,
                    obs_dt,
                ),
            )

    conn.commit()
    cur.close()
    conn.close()
