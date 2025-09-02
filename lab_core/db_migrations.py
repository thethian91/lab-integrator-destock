import sqlite3
from pathlib import Path


def migrate_hl7_results():
    db_path = Path("data/labintegrador.db")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        # Crear nueva tabla para resultados crudos desde HL7
        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS hl7_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT NOT NULL,
            analyzer_name TEXT NOT NULL,
            raw_hl7 TEXT NOT NULL,

            -- Info del paciente (PID)
            patient_id TEXT,
            patient_name TEXT,
            birth_date TEXT,
            sex TEXT,

            -- Info del examen (OBR)
            order_number TEXT,
            exam_code TEXT,
            exam_title TEXT,
            exam_date TEXT,
            exam_time TEXT,

            -- Info general del mensaje
            source_file TEXT,
            status TEXT DEFAULT 'RAW' -- RAW | MAPPED | SENT
        )
        """
        )

        conn.commit()


if __name__ == "__main__":
    migrate_hl7_results()
    print("Migración completada: hl7_results")
