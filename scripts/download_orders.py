# scripts/download_orders.py (opcional)
from lab_core.config import read_cfg_safe
from lab_core.orders_client import download_and_store_orders

if __name__ == "__main__":
    cfg = read_cfg_safe()
    # YYYYMMDD de exploración — ejemplo fijo:
    fecha = "20250825"
    result = download_and_store_orders(cfg, fecha)
    print(result)
