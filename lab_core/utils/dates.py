# lab_core/utils/dates.py
from __future__ import annotations

from datetime import datetime


def to_yyyymmdd(value: str | datetime) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    s = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y%m%d")
        except ValueError:
            pass
        digits = "".join(ch for ch in s if ch.isdigit())
        return digits[:8] if len(digits) >= 8 else digits
