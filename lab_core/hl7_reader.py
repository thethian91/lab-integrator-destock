# lab_core/hl7_reader.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

SEG = "\r"
FIELD = "|"
COMP = "^"


def _get(component: str, idx: int, default: str = "") -> str:
    parts = component.split(COMP)
    return parts[idx] if idx < len(parts) else default


def _to_iso(dt_str: str) -> str:
    """Convierte fecha HL7 a ISO si es posible"""
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y%m%d"):
        try:
            return datetime.strptime(dt_str, fmt).isoformat()
        except Exception:
            pass
    return ""


@dataclass
class OBX:
    id: str
    analyzer_code: str
    analyzer_text: str
    value: str
    units: str
    ref_range: str
    flags: str
    obs_dt: str
    raw: list[str] = field(default_factory=list)


@dataclass
class OBR:
    placer: str
    filler: str
    proto_codigo: str
    proto_texto: str
    tubo_muestra: str
    obr_dt: str
    raw: list[str] = field(default_factory=list)


@dataclass
class PID:
    patient_id: str
    doc: str
    nombre: str
    sexo: str
    f_nac: str
    raw: list[str] = field(default_factory=list)


@dataclass
class HL7Message:
    pid: PID
    obr: OBR
    obx_list: list[OBX]
    sending_app: str
    analyzer_name: str


def _guess_analyzer_alias(msh3: str) -> str:
    m = (msh3 or "").upper()
    if "ICON-3" in m:
        return "ICON3"
    if "QIANALYZER" in m or "FS114" in m or "F114" in m:
        return "FINECARE"
    return "DEFAULT"


def parse_hl7(text: str | bytes, analyzer_alias: str | None = None) -> HL7Message:
    """Parsea un mensaje HL7 a un objeto HL7Message"""
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="ignore")

    segs = [s for s in text.replace("\n", "\r").split(SEG) if s]
    fields_by_seg: dict[str, list[list[str]]] = {}

    for line in segs:
        typ = (line[:3] or "").strip()
        fields = line.split(FIELD) if line else []
        # normaliza Nones a "", evita indexaciones raras río abajo
        fields_by_seg.setdefault(typ, []).append([f or "" for f in fields])

    # MSH
    msh = fields_by_seg.get("MSH", [[]])[0]
    sending_app = msh[2] if len(msh) > 2 else ""
    alias = analyzer_alias or _guess_analyzer_alias(sending_app)

    # PID
    if "PID" in fields_by_seg:
        pidf = fields_by_seg.get("PID", [[]])[0]
        pid3 = pidf[3] if len(pidf) > 3 else ""
        pid5 = pidf[5] if len(pidf) > 5 else ""
        pid7 = pidf[7] if len(pidf) > 7 else ""
        pid8 = pidf[8] if len(pidf) > 8 else ""
        pid19 = pidf[19] if len(pidf) > 19 else ""
        nombre = _get(pid5, 0) or pid5
        doc = pid19 or _get(pid3, 1) or pid3
        pid = PID(
            patient_id=pid3, doc=doc, nombre=nombre, sexo=pid8, f_nac=pid7, raw=pidf
        )
    else:
        # Icon-3: paciente en NTE
        patient_name = ""
        for nte in fields_by_seg.get("NTE", []):
            val = nte[3] if len(nte) > 3 else ""
            qual = nte[4] if len(nte) > 4 else ""
            if qual.endswith("^Name"):
                patient_name = val
        pid = PID(patient_id="", doc="", nombre=patient_name, sexo="", f_nac="", raw=[])

    # OBR (primer OBR)
    obrf = fields_by_seg.get("OBR", [[]])[0]
    obr2 = obrf[2] if len(obrf) > 2 else ""
    obr3 = obrf[3] if len(obrf) > 3 else ""
    obr4 = obrf[4] if len(obrf) > 4 else ""
    # protocolo: preferir .1/.2; si vacío (Icon-3), usar .4/.5
    proto_cod = _get(obr4, 0) or _get(obr4, 3)
    proto_txt = _get(obr4, 1) or _get(obr4, 4)
    obr7 = obrf[7] if len(obrf) > 7 else ""  # fecha/hora observación
    obr_dt = _to_iso(obr7)
    obr20 = obrf[20] if len(obrf) > 20 else ""  # tubo_muestra
    obr = OBR(
        placer=obr2,
        filler=obr3,
        proto_codigo=proto_cod,
        proto_texto=proto_txt,
        tubo_muestra=obr20,
        obr_dt=obr_dt,
        raw=obrf,
    )

    # OBX
    obx_list: list[OBX] = []
    for obxf in fields_by_seg.get("OBX", []):
        # Finecare: OBX-3 = code; OBX-4 = text
        # Icon-3:   OBX-3 vacío; OBX-4 = code^text (e.g., 1^HGB)
        obx3 = obxf[3] if len(obxf) > 3 else ""
        obx4 = obxf[4] if len(obxf) > 4 else ""
        id_comp = obx3 if obx3 else obx4

        analyzer_code = _get(id_comp, 0)
        analyzer_text = _get(id_comp, 1)

        # Si el "código" es solo un índice (Icon-3), usa el texto como código lógico
        if analyzer_code.isdigit() and analyzer_text:
            analyzer_code = analyzer_text

        val = obxf[5] if len(obxf) > 5 else ""
        obx6 = obxf[6] if len(obxf) > 6 else ""
        units = _get(obx6, 1) or obx6  # en Icon-3 viene como ^g/L
        ref_range = obxf[7] if len(obxf) > 7 else ""
        flags = obxf[8] if len(obxf) > 8 else ""
        obx14 = obxf[14] if len(obxf) > 14 else ""
        obs_dt = _to_iso(obx14)

        obx_list.append(
            OBX(
                id=obxf[1] if len(obxf) > 1 else "",
                analyzer_code=analyzer_code,
                analyzer_text=analyzer_text,
                value=val,
                units=units,
                ref_range=ref_range,
                flags=flags,
                obs_dt=obs_dt,
                raw=obxf,
            )
        )

    return HL7Message(
        pid=pid,
        obr=obr,
        obx_list=obx_list,
        sending_app=sending_app,
        analyzer_name=alias,
    )
