from __future__ import annotations
from typing import Iterable, Mapping, Optional, Tuple, Dict, Any, List


def _as_dict(r: Mapping) -> Dict[str, Any]:
    try:
        return {k: r[k] for k in r.keys()}  # sqlite3.Row -> dict
    except Exception:
        return dict(r)


def _safe_str(x: Any) -> str:
    return (str(x) if x is not None else "").strip()


def concat_obx_rows(
    obx_rows: Iterable[Mapping],
    sep: str = " | ",
    mark_flags: bool = True,
    replace_pipes: bool = True,
) -> str:
    rows: List[Dict[str, Any]] = [_as_dict(r) for r in obx_rows]
    rows.sort(key=lambda r: int(_safe_str(r.get("seq") or "0") or "0"))
    parts = []
    for r in rows:
        name = _safe_str(r.get("text") or r.get("code"))
        val = _safe_str(r.get("value"))
        uni = _safe_str(r.get("units"))
        ref = _safe_str(r.get("ref_range"))
        flg = _safe_str(r.get("flags"))
        if replace_pipes:
            name = name.replace("|", "/")
            val = val.replace("|", "/")
            uni = uni.replace("|", "/")
            ref = ref.replace("|", "/")
            flg = flg.replace("|", "/")
        core = f"{name}: {val}" if name else val
        if uni:
            core += f" {uni}"
        if ref:
            core += f" (ref: {ref})"
        if mark_flags and flg:
            core += f" [{flg}]"
        if core:
            parts.append(core)
    return sep.join(parts)
