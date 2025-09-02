# lab_core/maintenance.py
import sqlite3
from pathlib import Path
from shutil import copy2
from datetime import datetime

DB_PATH = "data/labintegrador.db"

def _connect(db_path: str = DB_PATH):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def get_stats(db_path: str = DB_PATH) -> dict:
    """Totales y tamaño del archivo."""
    p = Path(db_path)
    size_mb = round(p.stat().st_size / (1024*1024), 3) if p.exists() else 0.0
    out = {"size_mb": size_mb, "patients": 0, "exams_total": 0, "by_status": {}}
    if not p.exists():
        return out
    conn = _connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM patients")
    out["patients"] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM exams")
    out["exams_total"] = cur.fetchone()[0]
    cur.execute("SELECT status, COUNT(*) FROM exams GROUP BY status")
    out["by_status"] = {s if s is not None else "NULL": n for s, n in cur.fetchall()}
    conn.close()
    return out

def vacuum(db_path: str = DB_PATH):
    """Compacta el archivo y libera espacio."""
    if not Path(db_path).exists():
        return
    conn = _connect(db_path)
    with conn:
        conn.execute("VACUUM;")
    conn.close()

def backup(out_dir: str = "backups", db_path: str = DB_PATH) -> str:
    """Copia el .db con timestamp a /backups."""
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    dst = Path(out_dir) / f"orders-{ts}.db"
    if Path(db_path).exists():
        copy2(db_path, dst)
    return str(dst)

def purge(date_before: str | None = None, status: str | None = None, db_path: str = DB_PATH) -> int:
    """
    Elimina exámenes por criterios:
    - date_before: borra e.fecha < 'YYYY-MM-DD'
    - status: 'PENDING'|'RESULTED'|'SENT'
    Devuelve cantidad de exámenes eliminados. Limpia pacientes huérfanos.
    """
    if not Path(db_path).exists():
        return 0
    conn = _connect(db_path)
    cur = conn.cursor()
    wh = []
    params = []
    if date_before:
        wh.append("fecha < ?")
        params.append(date_before)
    if status:
        wh.append("status = ?")
        params.append(status)
    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""
    with conn:
        # contar antes
        cur.execute(f"SELECT COUNT(*) FROM exams {where_sql}", params)
        to_delete = cur.fetchone()[0]
        # borrar exams
        cur.execute(f"DELETE FROM exams {where_sql}", params)
        # borrar pacientes sin exams
        cur.execute("""
            DELETE FROM patients
            WHERE documento NOT IN (SELECT DISTINCT paciente_doc FROM exams)
        """)
    conn.close()
    return to_delete

def purge_all(db_path: str = DB_PATH) -> tuple[int,int]:
    """Borra TODO (exams + patients). Devuelve (exams_eliminados, patients_eliminados)."""
    if not Path(db_path).exists():
        return (0,0)
    conn = _connect(db_path)
    cur = conn.cursor()
    with conn:
        cur.execute("SELECT COUNT(*) FROM exams")
        ex = cur.fetchone()[0]
        cur.execute("DELETE FROM exams")
        cur.execute("SELECT COUNT(*) FROM patients")
        pa = cur.fetchone()[0]
        cur.execute("DELETE FROM patients")
    conn.close()
    return (ex, pa)

def purge_results():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Borrar resultados (tabla hl7_results o como la hayas llamado)
    cur.execute("DELETE FROM hl7_results")
    rows_affected = cur.rowcount

    conn.commit()
    conn.close()

    return rows_affected
