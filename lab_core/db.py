from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import datetime  # <- necesario para mark_obx_exported
from pathlib import Path

# === Ruta única y consistente en toda la app ===
DEFAULT_DB_PATH = "data/labintegrador.db"

# =============== DDLs base ===============

LEGACY_DDL = r"""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS patients (
  documento TEXT PRIMARY KEY,
  nombre TEXT,
  sexo TEXT,
  fecha_nacimiento TEXT
);

CREATE TABLE IF NOT EXISTS exams (
  id INTEGER PRIMARY KEY,              -- ID del XML
  paciente_doc TEXT NOT NULL,
  protocolo_codigo TEXT,
  protocolo_titulo TEXT,
  tubo TEXT,
  tubo_muestra TEXT,
  fecha TEXT,
  hora TEXT,
  status TEXT DEFAULT 'PENDING',       -- PENDING|RESULTED|SENT
  result_value TEXT,
  result_xml  TEXT,
  resulted_at TEXT,
  sent_at TEXT,
  UNIQUE(id) ON CONFLICT REPLACE,
  FOREIGN KEY (paciente_doc) REFERENCES patients(documento)
);

CREATE INDEX IF NOT EXISTS idx_exams_doc     ON exams(paciente_doc);
CREATE INDEX IF NOT EXISTS idx_exams_tubo    ON exams(tubo);
CREATE INDEX IF NOT EXISTS idx_exams_tubo_m  ON exams(tubo_muestra);
CREATE INDEX IF NOT EXISTS idx_exams_status  ON exams(status);
"""

RESULTS_DDL = r"""
PRAGMA foreign_keys = ON;

-- hl7_results: registros RAW
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
);

-- hl7_obx_results: detalle por analito (OBX)
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

  -- columnas de exportación/polling (pueden no existir en BD antiguas; hay migración)
  export_status   TEXT,                -- PENDING|SENT|ERROR|SKIPPED
  export_attempts INTEGER DEFAULT 0,
  export_error    TEXT,
  export_path     TEXT,
  exported_at     TEXT,

  FOREIGN KEY(result_id) REFERENCES hl7_results(id)
);

-- Índices útiles
CREATE INDEX IF NOT EXISTS idx_hl7_results_received_at   ON hl7_results(received_at);
CREATE INDEX IF NOT EXISTS idx_hl7_results_analyzer_name ON hl7_results(analyzer_name);
CREATE INDEX IF NOT EXISTS idx_hl7_results_patient_id    ON hl7_results(patient_id);
CREATE INDEX IF NOT EXISTS idx_hl7_results_order_number  ON hl7_results(order_number);
CREATE INDEX IF NOT EXISTS idx_hl7_results_exam_code     ON hl7_results(exam_code);

CREATE INDEX IF NOT EXISTS idx_obx_result_id             ON hl7_obx_results(result_id);
CREATE INDEX IF NOT EXISTS idx_obx_code                  ON hl7_obx_results(code);
CREATE INDEX IF NOT EXISTS idx_obx_text                  ON hl7_obx_results(text);
-- OJO: NO crear aquí idx_obx_export_status (en BDs viejas falla si no existe la columna)
"""

# code_map: mapeo de códigos (analyzer -> cliente)
CODE_MAP_DDL = r"""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS code_map (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  analyzer_name   TEXT NOT NULL,
  signature_type  TEXT NOT NULL,       -- 'OBR_CODE' | 'OBX_CODE' | 'OBX_TEXT'
  signature_value TEXT NOT NULL,
  client_code     TEXT NOT NULL,
  client_title    TEXT,
  is_active       INTEGER NOT NULL DEFAULT 1,
  updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_code_map
  ON code_map(analyzer_name, signature_type, signature_value);
"""

SCHEMA_META_DDL = r"""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
  key TEXT PRIMARY KEY,
  value TEXT
);

INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('schema_version', '1');
"""

# =============== Conexión y helpers ===============


def get_conn(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _exec_script(conn: sqlite3.Connection, sql_script: str) -> None:
    with conn:
        conn.executescript(sql_script)


def get_schema_version(conn: sqlite3.Connection) -> int:
    try:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key='schema_version'"
        ).fetchone()
        return int(row["value"]) if row and row["value"] else 0
    except Exception:
        return 0


def set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    with conn:
        conn.execute(
            "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(version),),
        )


# =============== Vista de compatibilidad ===============
def _build_obx_results_view_sql(extra_aliases: Iterable[str] = ()) -> str:
    extras = [a.strip() for a in extra_aliases if a and a.strip()]
    base_aliases = [
        "'' AS client_code",
        "COALESCE(r.order_number, r.exam_code, o.code, '') AS exam_id",
        "o.export_status AS export_status",
        "o.export_attempts AS export_attempts",
        "o.export_error AS export_error",
        "o.export_path AS export_path",
        "o.exported_at AS exported_at",
    ]
    all_aliases = base_aliases + extras
    extra = ",\n  " + ",\n  ".join(all_aliases) if all_aliases else ""

    return f"""
DROP VIEW IF EXISTS obx_results;
CREATE VIEW obx_results AS
SELECT
  o.id                AS id,
  r.id                AS result_id,
  r.analyzer_name     AS analyzer_name,
  r.received_at       AS received_at,
  r.patient_id        AS patient_id,
  r.patient_name      AS patient_name,
  r.order_number      AS order_number,
  r.exam_code         AS exam_code,
  r.exam_title        AS exam_title,
  r.exam_date         AS exam_date,
  r.exam_time         AS exam_time,
  o.obx_id            AS obx_id,
  o.code              AS code,
  o.text              AS text,
  o.value             AS value,
  o.units             AS units,
  o.ref_range         AS ref_range,
  o.flags             AS flags,
  o.obs_dt            AS obs_dt{extra}
FROM hl7_obx_results o
JOIN hl7_results r ON r.id = o.result_id;
"""


def recreate_obx_view(
    conn: sqlite3.Connection, extra_aliases: Iterable[str] = ()
) -> None:
    sql = _build_obx_results_view_sql(extra_aliases)
    _exec_script(conn, sql)


# =============== Migraciones ligeras ===============


def _table_has_col(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info('{table}')")
    return any(r[1] == col for r in cur.fetchall())


def ensure_obx_dispatch_cols(db_path: str = DEFAULT_DB_PATH) -> None:
    """Garantiza columnas export_* en hl7_obx_results para BD ya existentes."""
    conn = get_conn(db_path)
    try:
        cols = {
            "export_status": "TEXT",
            "export_attempts": "INTEGER DEFAULT 0",
            "export_error": "TEXT",
            "export_path": "TEXT",
            "exported_at": "TEXT",
        }
        for c, ddl in cols.items():
            if not _table_has_col(conn, "hl7_obx_results", c):
                conn.execute(f"ALTER TABLE hl7_obx_results ADD COLUMN {c} {ddl}")
        # crea el índice ahora que la columna existe
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_obx_export_status ON hl7_obx_results(export_status)"
        )
        conn.commit()
    finally:
        conn.close()


# =============== Inicialización completa ===============


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    """
    Inicializa TODAS las estructuras (idempotente):
    - Esquema legacy (patients/exams)
    - Esquema resultados HL7 (hl7_results/hl7_obx_results)
    - code_map (mapeos)
    - schema_meta (versionado)
    - Vista de compatibilidad obx_results
    - Migración de columnas export_* si faltaran
    """
    conn = get_conn(db_path)
    try:
        _exec_script(conn, LEGACY_DDL)
        _exec_script(conn, RESULTS_DDL)
        _exec_script(conn, CODE_MAP_DDL)
        _exec_script(conn, SCHEMA_META_DDL)

        version = get_schema_version(conn)
        if version < 1:
            set_schema_version(conn, 1)

        recreate_obx_view(conn)
        conn.commit()
    finally:
        conn.close()

    # Migración out-of-band: asegurar columnas export_* (e índice) aunque la tabla ya existiera
    ensure_obx_dispatch_cols(db_path)


def ensure_schema(db_path: str = DEFAULT_DB_PATH) -> None:
    """Llama esto al arrancar tu servicio y tu UI (misma ruta)."""
    init_db(db_path)


# =============== Utilidades de verificación ===============


def debug_dump(db_path: str = DEFAULT_DB_PATH) -> None:
    conn = get_conn(db_path)
    try:
        print(f"DB: {db_path}")
        print("\nObjetos:")
        for r in conn.execute(
            "SELECT type, name FROM sqlite_master WHERE type IN ('table','view') ORDER BY type, name;"
        ):
            print(f" - {r['type']}: {r['name']}")

        print("\nConteos:")
        for name in [
            "patients",
            "exams",
            "hl7_results",
            "hl7_obx_results",
            "obx_results",
            "code_map",
        ]:
            try:
                cnt = conn.execute(f"SELECT COUNT(1) AS c FROM {name}").fetchone()["c"]
                print(f"{name}: {cnt}")
            except sqlite3.OperationalError as e:
                print(f"{name}: (no existe) {e}")
    finally:
        conn.close()


# --- Mapeo de códigos (code_map) ---------------------------------------------


def ensure_code_map_schema(db_path: str = DEFAULT_DB_PATH):
    """Idempotente: crea code_map e índice único (si no existieran)."""
    conn = get_conn(db_path)
    try:
        _exec_script(conn, CODE_MAP_DDL)
        conn.commit()
    finally:
        conn.close()


def code_map_upsert(
    analyzer_name: str,
    signature_type: str,
    signature_value: str,
    client_code: str,
    client_title: str | None = None,
    db_path: str = DEFAULT_DB_PATH,
):
    conn = get_conn(db_path)
    try:
        conn.execute(
            """
            INSERT INTO code_map (analyzer_name, signature_type, signature_value, client_code, client_title, is_active)
            VALUES (?, ?, ?, ?, ?, 1)
            ON CONFLICT(analyzer_name, signature_type, signature_value)
            DO UPDATE SET client_code=excluded.client_code,
                          client_title=excluded.client_title,
                          is_active=1,
                          updated_at=datetime('now')
        """,
            (analyzer_name, signature_type, signature_value, client_code, client_title),
        )
        conn.commit()
    finally:
        conn.close()


def code_map_delete(
    analyzer_name: str,
    signature_type: str,
    signature_value: str,
    db_path: str = DEFAULT_DB_PATH,
):
    conn = get_conn(db_path)
    try:
        conn.execute(
            """
            DELETE FROM code_map
            WHERE analyzer_name=? AND signature_type=? AND signature_value=?
        """,
            (analyzer_name, signature_type, signature_value),
        )
        conn.commit()
    finally:
        conn.close()


def code_map_lookup(
    conn: sqlite3.Connection,
    analyzer_name: str,
    obr_code: str | None = None,
    obx_code: str | None = None,
    obx_text: str | None = None,
) -> tuple[str | None, str | None]:
    """
    Devuelve (client_code, client_title) si encuentra un mapeo activo.
    Prioridad: OBR_CODE -> OBX_CODE -> OBX_TEXT
    """
    cur = conn.cursor()

    def _q(sig_type: str, value: str | None):
        if not value:
            return None
        cur.execute(
            """
            SELECT client_code, client_title
            FROM code_map
            WHERE analyzer_name=? AND signature_type=? AND signature_value=? AND is_active=1
            LIMIT 1
        """,
            (analyzer_name, sig_type, value),
        )
        return cur.fetchone()

    for sig_type, value in (
        ("OBR_CODE", obr_code),
        ("OBX_CODE", obx_code),
        ("OBX_TEXT", obx_text),
    ):
        row = _q(sig_type, value)
        if row:
            return row[0], row[1]
    return None, None


# --- Helpers de despacho de XML por OBX --------------------------------------


def mark_obx_exported(conn: sqlite3.Connection, obx_id: int, path: str) -> None:
    conn.execute(
        """
        UPDATE hl7_obx_results
           SET export_status='SENT',
               export_error=NULL,
               export_path=?,
               exported_at=?,
               export_attempts=export_attempts+1
         WHERE id=?
    """,
        (path, datetime.now().isoformat(timespec="seconds"), obx_id),
    )


def mark_obx_error(conn: sqlite3.Connection, obx_id: int, err: str) -> None:
    conn.execute(
        """
        UPDATE hl7_obx_results
           SET export_status='ERROR',
               export_error=?,
               export_attempts=export_attempts+1
         WHERE id=?
    """,
        (err[:500], obx_id),
    )
