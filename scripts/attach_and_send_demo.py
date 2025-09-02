from lab_core.db import get_conn
from lab_core.results_store import attach_result_by_id, mark_sent
from lab_core.xml_builder import build_result_xml

def get_row_dicts(exam_id: int, db_path="data/labintegrador.db"):
    conn = get_conn(db_path)
    conn.row_factory = lambda c, r: {d[0]: r[i] for i, d in enumerate(c.description)}
    cur = conn.cursor()
    cur.execute("SELECT * FROM exams WHERE id=?", (exam_id,))
    exam = cur.fetchone()
    if not exam:
        conn.close(); raise SystemExit(f"Exam {exam_id} no existe.")
    cur.execute("SELECT * FROM patients WHERE documento=?", (exam["paciente_doc"],))
    patient = cur.fetchone()
    conn.close()
    return exam, patient

if __name__ == "__main__":
    exam_id = 288379  # cambia por uno existente en tu DB
    result_value = "4.23"  # ejemplo
    # 1) adjuntar resultado
    attach_result_by_id(exam_id, result_xml="", result_value=result_value)
    # 2) construir XML final para enviar
    exam, patient = get_row_dicts(exam_id)
    xml = build_result_xml(exam, patient, result_value=result_value)
    # aquí harías el POST al cliente con `xml`
    print(xml)
    # 3) marcar enviado
    mark_sent(exam_id)
    print("Marcado como SENT.")
