import unicodedata

_SUPERSCRIPTS = {
    "⁰": "^0",
    "¹": "^1",
    "²": "^2",
    "³": "^3",
    "⁴": "^4",
    "⁵": "^5",
    "⁶": "^6",
    "⁷": "^7",
    "⁸": "^8",
    "⁹": "^9",
}


def normalize_units_for_sofia(unit: str | None) -> str | None:
    if not unit:
        return unit

    # Normalizar μ / µ → u
    unit = unit.replace("μ", "u").replace("µ", "u")

    # Reemplazar superíndices
    for k, v in _SUPERSCRIPTS.items():
        unit = unit.replace(k, v)

    # Opcional: x10^n → 10^n
    unit = unit.replace("×", "x")

    # Limpieza final (solo ASCII)
    unit = unit.encode("ascii", "ignore").decode("ascii")

    return unit.strip()
