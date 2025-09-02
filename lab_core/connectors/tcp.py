
# lab_core/connectors/tcp.py
import asyncio
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional, Tuple

MLLP_PATTERN = re.compile(rb"\x0b(.*?)\x1c\r", re.DOTALL)

def split_messages(payload: bytes) -> list[bytes]:
    # 1) Intenta MLLP
    msgs = [m for m in MLLP_PATTERN.findall(payload)]
    if msgs:
        return msgs
    # 2) Fallback simple (opcional): separar por doble newline si quieres
    # parts = [p.strip() for p in payload.split(b"\n\n") if p.strip()]
    # if parts: return parts
    # 3) Por defecto: 1 mensaje = toda la conexión
    return [payload]

@dataclass
class TCPConfig:
    host: str = "0.0.0.0"
    port: int = 5002
    outbox: Path = Path("./inbox")

class TCPReceiver:
    def __init__(self, cfg: TCPConfig, on_message: Callable[[bytes, str], None]):
        self.cfg = cfg
        self.on_message = on_message
        self._server: Optional[asyncio.AbstractServer] = None
        self.cfg.outbox.mkdir(parents=True, exist_ok=True)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        chunks = []
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                chunks.append(data)

            payload = b"".join(chunks)
            messages = split_messages(payload)

            ts = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
            peer_ip = (peer[0] if peer else "unknown").replace(":", "_")
            peer_port = (str(peer[1]) if peer else "0")

            saved_paths = []
            for idx, msg in enumerate(messages, start=1):
                # nombre único por mensaje
                fname = f"{ts}_tcp_{peer_ip}_{peer_port}_m{idx}_{uuid.uuid4().hex[:8]}.hl7"
                fpath = self.cfg.outbox / fname
                fpath.write_bytes(msg)
                saved_paths.append(str(fpath))

                # callback por cada mensaje (para la UI/memo)
                self.on_message(msg, str(fpath))

        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def start(self):
        self._server = await asyncio.start_server(self._handle, self.cfg.host, self.cfg.port)

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
