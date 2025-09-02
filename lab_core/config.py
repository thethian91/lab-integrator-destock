from __future__ import annotations
import os
from pathlib import Path
from dataclasses import dataclass
from pathlib import Path

import yaml


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


# lab_core/config.py


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
class Settings:
    api: ApiCfg
    tcp: TcpCfg
    paths: PathsCfg


def _read_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_settings(path: str | Path = "configs/settings.yaml") -> Settings:
    p = Path(path)
    data = _read_yaml(p)

    api = ApiCfg(
        base_url=os.getenv("SNT_BASE_URL", data["api"]["base_url"]),
        key=os.getenv("SNT_API_KEY", data["api"]["key"]),
        secret=os.getenv("SNT_API_SECRET", data["api"]["secret"]),
        timeout=int(os.getenv("SNT_TIMEOUT", data["api"].get("timeout", 20))),
    )

    tcp = TcpCfg(
        host=os.getenv("TCP_HOST", data["tcp"].get("host", "0.0.0.0")),
        port=int(os.getenv("TCP_PORT", data["tcp"].get("port", 5002))),
    )

    paths_cfg = data.get("paths", {})
    paths = PathsCfg(
        inbox=Path(paths_cfg.get("inbox", "./inbox")),
        outbox_xml=Path(paths_cfg.get("outbox_xml", "./outbox_xml")),
        logs=Path(paths_cfg.get("logs", "./logs")),
    )

    return Settings(api=api, tcp=tcp, paths=paths)
