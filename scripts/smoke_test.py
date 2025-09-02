# scripts/smoke_test.py
from __future__ import annotations
import json
from lab_core.pipeline import enviar_resultado_item

if __name__ == "__main__":
    sample = {
        "idexamen": 288396,
        "paciente_doc": "1017198585",
        "fecha": "2025-08-21",  # se normaliza a YYYYMMDD
        "texto": "prueba de resultado",
        "valor": "140.12",
        "ref": "66-181",
        "units": "nmol/L",
    }
    try:
        resp = enviar_resultado_item(sample)
        print("OK:", resp[:300])
    except Exception as e:
        print("ERROR:", e)
        raise
