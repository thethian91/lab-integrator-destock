from .db import get_conn


def upsert_orders(records, db_path: str = "data/labintegrador.db"):
    """
    records: lista de OrderRecord (de orders_client.parse_orders)
    """
    conn = get_conn(db_path)
    with conn:
        for rec in records:
            # paciente
            nombre = rec.examenes[0].nombre if rec.examenes else None
            sexo = rec.examenes[0].sexo if rec.examenes else None
            fnac = rec.examenes[0].fecha_nacimiento if rec.examenes else None
            conn.execute(
                "INSERT INTO patients(documento, nombre, sexo, fecha_nacimiento) "
                "VALUES(?,?,?,?) "
                "ON CONFLICT(documento) DO UPDATE SET nombre=excluded.nombre, sexo=excluded.sexo, fecha_nacimiento=excluded.fecha_nacimiento",
                (rec.documento, nombre, sexo, fnac),
            )
            # exámenes
            for e in rec.examenes:
                conn.execute(
                    "INSERT INTO exams(id, paciente_doc, protocolo_codigo, protocolo_titulo, tubo, tubo_muestra, fecha, hora, status) "
                    "VALUES(?,?,?,?,?,?,?,?,COALESCE((SELECT status FROM exams WHERE id=?),'PENDING')) "
                    "ON CONFLICT(id) DO UPDATE SET "
                    "paciente_doc=excluded.paciente_doc, protocolo_codigo=excluded.protocolo_codigo, protocolo_titulo=excluded.protocolo_titulo, "
                    "tubo=excluded.tubo, tubo_muestra=excluded.tubo_muestra, fecha=excluded.fecha, hora=excluded.hora",
                    (
                        int(e.id),
                        rec.documento,
                        e.protocolo_codigo,
                        e.protocolo_titulo,
                        e.tubo,
                        e.tubo_muestra,
                        e.fecha,
                        e.hora,
                        int(e.id),
                    ),
                )
    conn.close()
