# lab_core/mapping_json.py
from __future__ import annotations
import json
from pathlib import Path
from typing import Optional, Tuple, Dict, Any


def _norm(text: str) -> str:
    """
    Normaliza nombres (analyzer/alias/test_code) para búsquedas robustas:
    - Uppercase
    - Quita espacios y símbolos no alfanuméricos
    """
    if text is None:
        return ""
    t = text.upper()
    return "".join(ch for ch in t if ch.isalnum())


def _build_alias_index(data: Dict[str, Any]) -> Dict[str, str]:
    """
    Devuelve un índice {alias_normalizado: nombre_canonico_analyzer}
    Incluye el propio nombre del analizador como alias.
    """
    idx: Dict[str, str] = {}
    analyzers = (data or {}).get("analyzers", {})
    for canon_name, payload in analyzers.items():
        idx[_norm(canon_name)] = canon_name  # el propio
        for alias in (payload or {}).get("aliases", []) or []:
            idx[_norm(alias)] = canon_name
    return idx


def load_mapping(path: str | Path) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """
    Carga el JSON y prepara el índice de alias.
    Retorna: (data, alias_index)
    """
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    alias_idx = _build_alias_index(data)
    return data, alias_idx


def lookup_client_code(
    data: Dict[str, Any],
    alias_idx: Dict[str, str],
    analyzer_name: str,
    test_code: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Busca client_code/client_title a partir del analizador (con alias) y el test_code.
    Retorna (client_code, client_title) o (None, None) si no hay match.
    """
    if not analyzer_name or not test_code:
        return None, None

    canon = alias_idx.get(_norm(analyzer_name))
    if not canon:
        return None, None

    analyzers = (data or {}).get("analyzers", {})
    payload = analyzers.get(canon) or {}
    amap = payload.get("map") or {}

    entry = amap.get(test_code.upper())
    if not entry:
        # Si el JSON tuviera claves en distintos cases, normalizamos por si acaso:
        # (generalmente no hace falta, pero cuesta poco probar)
        for k, v in amap.items():
            if _norm(k) == _norm(test_code):
                entry = v
                break

    if not entry:
        return None, None

    return entry.get("client_code"), entry.get("client_title")
