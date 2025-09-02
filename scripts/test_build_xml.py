#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sys
import sqlite3
from pathlib import Path
from typing import Optional

# importa tus builders
from lab_core.xml_builder import (
    build_log_envio_for_result,
    build_log_envio_for_result_range,
)

DEFAULT_DB = "data/labintegrador.db"
DEFAULT_OUT = "outbox"


def ensure_outdir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def run_single(db_path: str, result_id: int, outdir: str, quiet: bool = False) -> int:
    conn = sqlite3.connect(db_path)
    try:
        pairs = build_log_envio_for_result(conn, result_id=result_id)
    finally:
        conn.close()

    out = ensure_outdir(outdir)
    if not pairs:
        if not quiet:
            print(f"[test_xml] No se generó ningún XML para result_id={result_id}")
        return 1

    if not quiet:
        print(f"[test_xml] Generando {len(pairs)} XML(s) para result_id={result_id} → {out}")

    for obx_id, xml in pairs:
        fp = out / f"log_envio_{result_id}_{obx_id}.xml"
        fp.write_text(xml, encoding="utf-8")
        if not quiet:
            print(f"  -> {fp}")

    return 0


def run_range(
    db_path: str,
    date_from: str,
    date_to: str,
    analyzer: Optional[str],
    outdir: str,
    quiet: bool = False,
) -> int:
    conn = sqlite3.connect(db_path)
    try:
        triples = build_log_envio_for_result_range(
            conn, date_from=date_from, date_to=date_to, analyzer=analyzer
        )
    finally:
        conn.close()

    out = ensure_outdir(outdir)
    if not triples:
        if not quiet:
            print(f"[test_xml] No se generaron XMLs para el rango {date_from}..{date_to}"
                  + (f" (analyzer={analyzer})" if analyzer else ""))
        return 1

    if not quiet:
        print(f"[test_xml] Generando {len(triples)} XML(s) para rango {date_from}..{date_to}"
              + (f" (analyzer={analyzer})" if analyzer else "")
              + f" → {out}")

    for result_id, obx_id, xml in triples:
        fp = out / f"log_envio_{result_id}_{obx_id}.xml"
        fp.write_text(xml, encoding="utf-8")
        if not quiet:
            print(f"  -> {fp}")

    return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Generar XML <log_envio> desde la BD (hl7_results + hl7_obx_results)."
    )
    ap.add_argument("--db", default=DEFAULT_DB, help=f"Ruta de la BD SQLite (default: {DEFAULT_DB})")
    ap.add_argument("--out", default=DEFAULT_OUT, help=f"Carpeta de salida (default: {DEFAULT_OUT})")
    ap.add_argument("--quiet", action="store_true", help="Menos salida en consola")

    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--result-id", type=int, help="Generar XML(s) solo para este result_id")
    mode.add_argument("--from", dest="date_from", help="Fecha desde (YYYY-MM-DD)")
    ap.add_argument("--to", dest="date_to", help="Fecha hasta (YYYY-MM-DD)")
    ap.add_argument("--analyzer", help="Filtrar por analyzer_name (opcional en modo rango)")

    args = ap.parse_args()

    # Validación simple para modo rango
    if args.date_from and not args.date_to:
        ap.error("--from requiere también --to")

    return args


def main() -> int:
    args = parse_args()

    if args.result_id is not None:
        return run_single(db_path=args.db, result_id=args.result_id, outdir=args.out, quiet=args.quiet)

    # modo rango
    return run_range(
        db_path=args.db,
        date_from=args.date_from,
        date_to=args.date_to,
        analyzer=args.analyzer,
        outdir=args.out,
        quiet=args.quiet,
    )


if __name__ == "__main__":
    sys.exit(main())
