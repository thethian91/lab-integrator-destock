# db_obx_upsert.py
from typing import Any

from sqlalchemy import Column, Integer, MetaData, Table, Text, create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

# Ajusta tu URL de conexión:
# SQLite: 'sqlite:///lab.db'
# Postgres: 'postgresql+psycopg2://user:pass@host:5432/dbname'
ENGINE_URL = "sqlite:///labintegrador.db"

engine = create_engine(ENGINE_URL, future=True)
md = MetaData()

hl7_obx_results = Table(
    "hl7_obx_results",
    md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("message_uid", Text, nullable=False),
    Column("obx_index", Integer, nullable=False),
    Column("patient_id", Text),
    Column("exam_code", Text),
    Column("exam_title", Text),
    Column("code", Text),
    Column("text", Text),
    Column("value", Text),
    Column("units", Text),
    Column("ref_range", Text),
    Column("status", Text),
    Column("when_ts", Text),
    Column("raw_segment", Text),
)


def upsert_obx_rows(message_uid: str, parsed: dict[str, Any]):
    """
    parsed es el dict que devuelve parse_hl7_configurable(...)
    Inserta/actualiza cada OBX con clave única (message_uid, obx_index).
    """
    rows = []
    for idx, obx in enumerate(parsed.get("obx_list", [])):
        rows.append(
            {
                "message_uid": message_uid,
                "obx_index": idx,
                "patient_id": parsed.get("patient_id"),
                "exam_code": parsed.get("exam_code"),
                "exam_title": parsed.get("exam_title"),
                "code": obx.get("code"),
                "text": obx.get("text"),
                "value": obx.get("value"),
                "units": obx.get("units"),
                "ref_range": obx.get("ref_range"),
                "status": obx.get("status"),
                "when_ts": obx.get("when"),
                "raw_segment": obx.get("raw"),
            }
        )

    if not rows:
        return 0

    with engine.begin() as conn:
        # Detectar dialecto
        if conn.dialect.name == "postgresql":
            stmt = pg_insert(hl7_obx_results).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["message_uid", "obx_index"],
                set_={
                    "patient_id": stmt.excluded.patient_id,
                    "exam_code": stmt.excluded.exam_code,
                    "exam_title": stmt.excluded.exam_title,
                    "code": stmt.excluded.code,
                    "text": stmt.excluded.text,
                    "value": stmt.excluded.value,
                    "units": stmt.excluded.units,
                    "ref_range": stmt.excluded.ref_range,
                    "status": stmt.excluded.status,
                    "when_ts": stmt.excluded.when_ts,
                    "raw_segment": stmt.excluded.raw_segment,
                },
            )
            res = conn.execute(stmt)
            return res.rowcount or 0

        elif conn.dialect.name == "sqlite":
            stmt = sqlite_insert(hl7_obx_results).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["message_uid", "obx_index"],
                set_={
                    "patient_id": stmt.excluded.patient_id,
                    "exam_code": stmt.excluded.exam_code,
                    "exam_title": stmt.excluded.exam_title,
                    "code": stmt.excluded.code,
                    "text": stmt.excluded.text,
                    "value": stmt.excluded.value,
                    "units": stmt.excluded.units,
                    "ref_range": stmt.excluded.ref_range,
                    "status": stmt.excluded.status,
                    "when_ts": stmt.excluded.when_ts,
                    "raw_segment": stmt.excluded.raw_segment,
                },
            )
            res = conn.execute(stmt)
            return res.rowcount or 0

        else:
            # Fallback genérico: borra e inserta (no ideal, pero seguro)
            deleted = 0
            for r in rows:
                conn.execute(
                    hl7_obx_results.delete()
                    .where(hl7_obx_results.c.message_uid == r["message_uid"])
                    .where(hl7_obx_results.c.obx_index == r["obx_index"])
                )
                deleted += 1
            conn.execute(hl7_obx_results.insert(), rows)
            return len(rows)
