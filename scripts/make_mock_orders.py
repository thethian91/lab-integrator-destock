from pathlib import Path
from datetime import datetime

TEMPLATE = """<?xml version="1.0" encoding="utf-8" ?>
<resultado_ws>
  <respuesta>OK</respuesta>
  <detalle_respuesta>
    <paciente documento="12345678">
      <examen>
        <id>288379</id>
        <protocolo_codigo>412509-53</protocolo_codigo>
        <protocolo_titulo>Trydoyotironina libre (T3L) - Remitido</protocolo_titulo>
        <tubo>08250465</tubo>
        <tubo_muestra>08250465-24</tubo_muestra>
        <fecha>{YYYY}-{MM}-{DD}</fecha>
        <hora>09:43:25</hora>
        <paciente>12345678</paciente>
        <nombre>pepito perez</nombre>
        <sexo>M</sexo>
        <edad>42</edad>
        <fecha_nacimiento>1983-02-14</fecha_nacimiento>
      </examen>
      <examen>
        <id>288380</id>
        <protocolo_codigo>412509-55</protocolo_codigo>
        <protocolo_titulo>Tiroxina libre T4L - Remitido</protocolo_titulo>
        <tubo>08250465</tubo>
        <tubo_muestra>08250465-24</tubo_muestra>
        <fecha>{YYYY}-{MM}-{DD}</fecha>
        <hora>09:43:25</hora>
        <paciente>12345678</paciente>
        <nombre>pepito perez</nombre>
        <sexo>M</sexo>
        <edad>42</edad>
        <fecha_nacimiento>1983-02-14</fecha_nacimiento>
      </examen>
    </paciente>

    <paciente documento="1016025677">
      <examen>
        <id>288382</id>
        <protocolo_codigo>412509-53</protocolo_codigo>
        <protocolo_titulo>Trydoyotironina libre (T3L) - Remitido</protocolo_titulo>
        <tubo>08250466</tubo>
        <tubo_muestra>08250466-24</tubo_muestra>
        <fecha>{YYYY}-{MM}-{DD}</fecha>
        <hora>09:56:31</hora>
        <paciente>1016025677</paciente>
        <nombre>JHONNATAN ANDREI VELASCO NAVARRETE</nombre>
        <sexo>M</sexo>
        <edad>32</edad>
        <fecha_nacimiento>1993-01-21</fecha_nacimiento>
      </examen>
      <examen>
        <id>288383</id>
        <protocolo_codigo>412509-55</protocolo_codigo>
        <protocolo_titulo>Tiroxina libre T4L - Remitido</protocolo_titulo>
        <tubo>08250466</tubo>
        <tubo_muestra>08250466-24</tubo_muestra>
        <fecha>{YYYY}-{MM}-{DD}</fecha>
        <hora>09:56:31</hora>
        <paciente>1016025677</paciente>
        <nombre>JHONNATAN ANDREI VELASCO NAVARRETE</nombre>
        <sexo>M</sexo>
        <edad>32</edad>
        <fecha_nacimiento>1993-01-21</fecha_nacimiento>
      </examen>
    </paciente>
  </detalle_respuesta>
</resultado_ws>
"""

if __name__ == "__main__":
    today = datetime.now().strftime("%Y%m%d")
    YYYY, MM, DD = today[:4], today[4:6], today[6:]
    xml = TEMPLATE.replace("{YYYY}", YYYY).replace("{MM}", MM).replace("{DD}", DD)

    out_dir = Path("samples")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"orders_{today}.xml"
    out_path.write_text(xml, encoding="utf-8")
    print(f"Mock creado: {out_path}")
