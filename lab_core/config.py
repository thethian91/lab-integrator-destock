from pathlib import Path
import os, yaml

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
