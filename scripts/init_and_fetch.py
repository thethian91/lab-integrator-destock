from datetime import datetime

from lab_core.config import read_cfg_safe
from lab_core.db import init_db
from lab_core.orders_client import get_orders_xml_from_cfg, parse_orders
from lab_core.orders_store import upsert_orders

if __name__ == "__main__":
    init_db()
    cfg = read_cfg_safe()
    fecha = datetime.now().strftime("%Y%m%d")  # o fijo: "20250825"

    xml_text = get_orders_xml_from_cfg(cfg, fecha)
    records = parse_orders(xml_text)
    upsert_orders(records)
    print(f"OK - {len(records)} pacientes cargados para {fecha}")
