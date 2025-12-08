# lab_core/config.py
from __future__ import annotations
import os
from pathlib import Path
from dataclasses import dataclass, field
import yaml


# ---------- utilidades simples para leer/escribir YAML suelto ----------


def config_dir() -> Path:
    base = os.getenv("LAB_INTEGRADOR_HOME") or os.getcwd()
    return Path(base) / "configs"


def config_path() -> Path:
    return config_dir() / "settings.yaml"


def read_cfg_safe() -> dict:
    try:
        p = config_path()
        if not p.exists():
            return {}
        with p.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def write_cfg_safe(cfg: dict) -> bool:
    try:
        d = config_dir()
        d.mkdir(parents=True, exist_ok=True)
        with config_path().open("w", encoding="utf-8") as f:
            yaml.safe_dump(cfg, f, sort_keys=False, allow_unicode=True)
        return True
    except Exception:
        return False


# ---------- modelos usados por lab_core.load_settings() ----------


@dataclass
class ApiCfg:
    base_url: str
    key: str
    secret: str
    timeout: int = 20


@dataclass
class TcpCfg:
    host: str = "0.0.0.0"
    port: int = 5002


@dataclass
class PathsCfg:
    inbox: Path = Path("./inbox")
    outbox_xml: Path = Path("./outbox_xml")
    logs: Path = Path("./logs")


@dataclass
class ExportCfg:
    """Sección para el guardado de XML/respuestas del pipeline."""

    save_xml: bool = False
    dir: Path = Path("data/exports")


@dataclass
class ResultExportCfg:
    """Seccion para confg de resultados"""

    enabled: bool = False
    interval_ms: int = 0
    batch_size: int = 200
    outbox: Path = Path('./outbox_xml')
    delivery_mode: str = 'http_direct'
    save_files: bool = False
    save_dir: Path = Path('./outbox_xml')


@dataclass
class Settings:
    api: ApiCfg
    tcp: TcpCfg
    paths: PathsCfg
    result_export: ResultExportCfg
    # 👇 Importante: usar default_factory en vez de ExportCfg() para evitar el error
    export: ExportCfg = field(default_factory=ExportCfg)


# ---------- loader principal ----------


def _read_yaml(path: Path) -> dict:
    txt = path.read_text(encoding="utf-8")
    data = yaml.safe_load(txt)
    return data or {}


def load_settings(path: str | Path = "configs/settings.yaml") -> Settings:
    p = Path(path)
    data = _read_yaml(p)

    # --- API ---
    api_d = data.get("api", {})
    api = ApiCfg(
        base_url=os.getenv("SNT_BASE_URL", api_d.get("base_url", "")),
        key=os.getenv("SNT_API_KEY", api_d.get("key", "")),
        secret=os.getenv("SNT_API_SECRET", api_d.get("secret", "")),
        timeout=int(os.getenv("SNT_TIMEOUT", api_d.get("timeout", 20))),
    )

    # --- TCP ---
    tcp_d = data.get("tcp", {})
    tcp = TcpCfg(
        host=os.getenv("TCP_HOST", tcp_d.get("host", "0.0.0.0")),
        port=int(os.getenv("TCP_PORT", tcp_d.get("port", 5002))),
    )

    # --- PATHS ---
    paths_d = data.get("paths", {}) or {}
    file_d = data.get("file", {}) or {}  # fallback para YAML que usa file.inbox_dir
    inbox_val = paths_d.get("inbox") or file_d.get("inbox_dir", "./inbox")

    paths = PathsCfg(
        inbox=Path(inbox_val),
        outbox_xml=Path(paths_d.get("outbox_xml", "./outbox_xml")),
        logs=Path(paths_d.get("logs", "./logs")),
    )

    # --- EXPORT (nuevo) ---
    export_d = data.get("export", {}) or {}
    export = ExportCfg(
        save_xml=bool(export_d.get("save_xml", False)),
        dir=Path(export_d.get("dir", "data/exports")),
    )

    # --- RESULT EXPORT ---
    results_export_d = data.get('results_export', {}) or {}
    result_export = ResultExportCfg(
        enabled=bool(results_export_d.get('enabled', False)),
        interval_ms=int(results_export_d.get('interval_ms', 0)),
        batch_size=int(results_export_d.get('batch_size', 0)),
        outbox=Path(results_export_d.get('outbox', './outbox')),
        delivery_mode=results_export_d.get('delivery_mode', 'xml_only'),
        save_files=bool(results_export_d.get('save_files', False)),
        save_dir=Path(results_export_d.get('outbox', './outbox')),
    )

    return Settings(
        api=api, tcp=tcp, paths=paths, export=export, result_export=result_export
    )
