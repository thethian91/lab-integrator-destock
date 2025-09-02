# app/transform.py (o el módulo donde generas tu XML)
from datetime import datetime


def to_yyyymmdd(value: str | datetime) -> str:
    """
    Acepta datetime o string en formatos comunes ('YYYY-MM-DD', 'YYYY/MM/DD', 'DD/MM/YYYY', etc.)
    y retorna 'YYYYMMDD'. Si no puede parsear, deja solo dígitos y hace el mejor esfuerzo.
    """
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")

    s = str(value).strip()
    # intentos comunes
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y%m%d")
        except ValueError:
            pass

    # fallback: quita no dígitos y trata de rearmar
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return digits  # último recurso


def build_xml_log(registro: dict) -> str:
    """
    Ejemplo: usa to_yyyymmdd para la etiqueta <fecha>.
    Adapta a tu estructura real de XML si ya la tienes.
    """
    from xml.etree.ElementTree import Element, SubElement, tostring  # simple y rápido

    root = Element("resultado_ws")
    detalle = SubElement(root, "detalle_respuesta")
    pac = SubElement(
        detalle, "paciente", {"documento": str(registro.get("paciente_doc", ""))}
    )
    exam = SubElement(pac, "examen")

    SubElement(exam, "id").text = str(registro.get("idexamen", ""))
    SubElement(exam, "protocolo_codigo").text = str(
        registro.get("protocolo_codigo", "")
    )
    SubElement(exam, "protocolo_titulo").text = str(
        registro.get("protocolo_titulo", "")
    )

    # ⬇️ FECHA ajustada a YYYYMMDD
    fecha_original = registro.get("fecha")  # p.ej. '2025-08-25' o datetime
    SubElement(exam, "fecha").text = to_yyyymmdd(fecha_original)

    # ... agrega los demás campos que ya tenías
    return tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")
