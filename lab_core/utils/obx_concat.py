# lab_core/utils/obx_concat.py
from typing import Iterable, Mapping, Optional


def concat_obx_rows(
    obx_rows: Iterable[Mapping],
    sep: str = " | ",
    mark_flags: bool = True,
    replace_pipes: bool = True,
) -> str:
    """
    Recibe filas OBX con campos típicos:
      seq, code, text, value, units, ref_range, flags
    Devuelve un único string con todos los analitos en orden por seq.
    """

    def fmt_one(r: Mapping) -> Optional[str]:
        name = (r.get("text") or r.get("code") or "").strip()
        val = (r.get("value") or "").strip()
        uni = (r.get("units") or "").strip()
        ref = (r.get("ref_range") or "").strip()
        flg = (r.get("flags") or "").strip().upper()  # H/L/HH/LL/A...

        if not name and not val and not uni:
            return None

        parts = []
        if name:
            parts.append(f"{name}:")
        if val:
            parts.append(val)
        if uni:
            parts[-1] = f"{parts[-1]} {uni}"
        if mark_flags and flg in {"H", "L", "A", "HH", "LL"} and parts:
            parts[-1] = f"{parts[-1]}*"

        s = " ".join(parts).strip()
        if ref:
            s += f" ({ref})"

        if replace_pipes:
            s = s.replace("|", "/")
        return s.replace("\n", " ").strip()

    try:
        rows = sorted(
            list(obx_rows), key=lambda r: (r.get("seq") is None, r.get("seq"))
        )
        items = [fmt_one(r) for r in rows]
        items = [i for i in items if i]
        return sep.join(items)
    except Exception as err:
        return err
