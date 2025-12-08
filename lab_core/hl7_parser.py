import re
from typing import Any

import yaml

# ---------------------------
# Utilidades HL7
# ---------------------------


def split_segments(hl7_text: str) -> list[list[str]]:
    lines = [ln.strip() for ln in hl7_text.splitlines() if ln.strip()]
    segs = [ln.split("|") for ln in lines]
    return segs


def get_field(
    segs: list[list[str]],
    seg_name: str,
    field_idx: int,
    comp_idx: int | None,
    first_only=True,
) -> str | None:
    """
    seg_name: 'MSH','PID','OBR','OBX'
    field_idx: N (1-based HL7 field index)
    comp_idx:  M (1-based component), None si no aplica
    """
    seg_name = seg_name.upper()
    for seg in segs:
        if not seg or seg[0].upper() != seg_name:
            continue

        # --- Índice real en la lista 'seg' ---
        # Segments normales: HL7-1 -> seg[1]
        # MSH es especial: HL7-2 -> seg[1], HL7-3 -> seg[2], ...  (o sea index = field_idx - 1 cuando N>=2)
        if seg_name == "MSH":
            if field_idx < 2:
                # MSH-1 es el separador '|' y no aparece tras split, no hay valor que devolver
                continue
            real_idx = field_idx - 1
        else:
            real_idx = field_idx  # para PID/OBR/OBX y demás

        if real_idx >= len(seg):
            continue

        val = seg[real_idx]
        if comp_idx is not None:
            comps = val.split("^")
            if 1 <= comp_idx <= len(comps):
                return comps[comp_idx - 1].strip() or None
            else:
                continue
        return (val or "").strip() or None

    return None


def parse_path(segs: list[list[str]], path: str) -> str | None:
    """
    Path estilo 'OBX-8.2' o 'PID-3.2' o 'OBX-5'
    """
    # Formato: SEG-<field>[.<component>]
    # Ej: "OBX-8.2"
    try:
        seg_part, rest = path.split("-", 1)
        seg_name = seg_part.strip().upper()
        if "." in rest:
            f_str, c_str = rest.split(".", 1)
            f_idx = int(f_str)
            c_idx = int(c_str)
        else:
            f_idx = int(rest)
            c_idx = None
        return get_field(segs, seg_name, f_idx, c_idx)
    except Exception:
        return None


def first_non_empty(values: list[str], segs: list[list[str]]) -> str | None:
    """
    Ejecuta rutas en orden y devuelve la primera no vacía.
    """
    for v in values:
        out = parse_path(segs, v)
        if out:
            return out
    return None


def _nte_find_by_label(segs, wanted_label: str) -> str | None:
    """
    Busca NTE donde NTE-5.2 == wanted_label y devuelve NTE-4 (el valor).
    Ejemplo:
      NTE|Comment1||juancho correlon|1^Name  -> label='Name', valor='juancho correlon'
      NTE|Comment2||55|2^Age                -> label='Age',  valor='55'
    """
    wanted_label = (wanted_label or "").strip()
    for seg in segs:
        if not seg or seg[0].upper() != "NTE":
            continue
        # NTE-4 = comentario/valor; NTE-5 = 'idx^Label'
        nte4 = seg[3] if len(seg) > 3 else ""
        nte5 = seg[4] if len(seg) > 4 else ""
        comps = nte5.split("^")
        label = comps[1].strip() if len(comps) >= 2 else ""
        if label == wanted_label and nte4:
            return nte4.strip() or None
    return None


# --- parche ligero a parse_path para soportar paths especiales ---
_NTE_LABEL_RE = re.compile(r"^NTE\[label=(?P<label>[A-Za-z0-9 _\-\#]+)\]$")


def parse_path(segs: list[list[str]], path: str) -> str | None:
    """
    Soporta:
      - 'SEG-<field>'            (ej: 'OBX-5')
      - 'SEG-<field>.<comp>'     (ej: 'OBX-6.2')
      - 'NTE[label=Name]'        (especial ICON3: devuelve NTE-4 donde NTE-5.2 == 'Name')
    """
    # 1) NTE por etiqueta
    m = _NTE_LABEL_RE.match(path.strip())
    if m:
        return _nte_find_by_label(segs, m.group("label"))

    # 2) Rutas estándar SEG-campo(.comp)
    try:
        seg_part, rest = path.split("-", 1)
        seg_name = seg_part.strip().upper()
        if "." in rest:
            f_str, c_str = rest.split(".", 1)
            f_idx = int(f_str)
            c_idx = int(c_str)
        else:
            f_idx = int(rest)
            c_idx = None
        return get_field(segs, seg_name, f_idx, c_idx)
    except Exception:
        return None


# ---------------------------
# Selección de perfil
# ---------------------------


def field_contains(segs: list[list[str]], expr: str) -> bool:
    """
    expr: 'MSH-3 contains QIAnalyzer' -> verifica si el valor contiene el substring (case-sensitive)
    """
    try:
        left, needle = expr.split(" contains ", 1)
        seg_field = left.strip()
        needle = needle.strip()
        val = parse_path(segs, seg_field)
        return val is not None and needle in val
    except Exception:
        return False


def pick_profile(
    cfg: dict[str, Any], segs: list[list[str]]
) -> dict[str, Any] | None:
    defaults = cfg.get("defaults", {})
    profiles = cfg.get("profiles", {})
    for name, prof in profiles.items():
        any_of = (prof.get("match") or {}).get("any_of", [])
        if any_of and any(field_contains(segs, expr) for expr in any_of):
            # Combina defaults con perfil (sin mutar)
            merged = {
                "separators": defaults.get(
                    "separators", {"field": "|", "component": "^"}
                ),
                **{k: v for k, v in prof.items() if k != "match"},
            }
            merged["_name"] = name
            return merged
    return None


# ---------------------------
# Parser principal
# ---------------------------


def parse_hl7_configurable(hl7_text: str, cfg: dict[str, Any]) -> dict[str, Any]:
    segs = split_segments(hl7_text)
    prof = pick_profile(cfg, segs)
    if not prof:
        raise ValueError("No se encontró perfil coincidente en el YAML para este HL7.")

    extract = prof.get("extract", {})
    norm = prof.get("normalize", {}) or {}

    # Campos simples (PID/OBR)
    patient_id_paths = extract.get("patient_id", [])
    if isinstance(patient_id_paths, str):
        patient_id_paths = [patient_id_paths]
    patient_id = first_non_empty(patient_id_paths, segs)

    exam_code_paths = extract.get("exam_code", [])
    if isinstance(exam_code_paths, str):
        exam_code_paths = [exam_code_paths]
    exam_code = first_non_empty(exam_code_paths, segs)

    exam_title_paths = extract.get("exam_title", [])
    if isinstance(exam_title_paths, str):
        exam_title_paths = [exam_title_paths]
    exam_title = first_non_empty(exam_title_paths, segs)

    # NUEVO: fecha/hora a nivel examen (OBR/MSH)
    exam_when_paths = extract.get("exam_when", [])
    if isinstance(exam_when_paths, str):
        exam_when_paths = [exam_when_paths]
    exam_when = first_non_empty(exam_when_paths, segs)

    # Normalizaciones
    if norm.get("patient_id_strip_carets") and patient_id:
        patient_id = patient_id.strip("^")

    # OBX: recoge todos los OBX
    obx_spec = extract.get("obx", {})
    obx_list = []
    for seg in segs:
        if not seg or seg[0].upper() != "OBX":
            continue

        # Construimos un “mini parser” para este OBX específico
        def p_local(path: str) -> str | None:
            # Fuerza a usar el primer OBX coincidente de 'segs'; necesitamos sólo este 'seg'
            # Reutilizamos parse_path pero sobre el mismo 'seg' aislado:
            try:
                seg_name, rest = path.split("-", 1)
                if seg_name.upper() != "OBX":
                    # Si el config mete otra cosa por error, ignoramos
                    return None
                if "." in rest:
                    f_str, c_str = rest.split(".", 1)
                    f_idx = int(f_str)
                    c_idx = int(c_str)
                else:
                    f_idx = int(rest)
                    c_idx = None
                # Aplicamos sobre 'seg' actual
                val = seg[f_idx] if f_idx < len(seg) else ""
                if c_idx is not None:
                    comps = val.split("^")
                    if 1 <= c_idx <= len(comps):
                        val = comps[c_idx - 1]
                    else:
                        val = ""
                return (val or "").strip() or None
            except Exception:
                return None

        def take(paths_or_str: Any) -> str | None:
            if not paths_or_str:
                return None
            paths = paths_or_str if isinstance(paths_or_str, list) else [paths_or_str]
            for p in paths:
                v = p_local(p)
                if v:
                    return v
            return None

        code = take(obx_spec.get("code"))
        text = take(obx_spec.get("text"))
        value = take(obx_spec.get("value"))
        units = take(obx_spec.get("units"))
        ref_range = take(obx_spec.get("ref_range"))
        status = take(obx_spec.get("status"))
        when = take(obx_spec.get("when"))
        if not when:
            when = exam_when  # hereda la fecha/hora del examen

        if text and norm.get("text_upper"):
            text = text.upper()

        obx_list.append(
            {
                "code": code,
                "text": text,
                "value": value,
                "units": units,
                "ref_range": ref_range,
                "status": status,
                "when": when,
                "raw": "|".join(seg),  # trazabilidad
            }
        )

    return {
        "profile": prof.get("_name"),
        "patient_id": patient_id,
        "exam_code": exam_code,
        "exam_title": (
            exam_title.upper() if exam_title and norm.get("text_upper") else exam_title
        ),
        "obx_list": obx_list,
    }


# ---------------------------
# Cargador de YAML
# ---------------------------


def load_hl7_map_yaml(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)
