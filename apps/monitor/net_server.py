import logging
import socket
import threading
import time
from pathlib import Path

from PySide6.QtCore import QObject, Signal

VT, FS, CR = b"\x0b", b"\x1c", b"\x0d"


class MLLPServer(QObject):
    received = Signal(bytes)
    started = Signal(str, int)
    stopped = Signal()
    error = Signal(str)  # opcional: reportar errores a la UI

    def __init__(self, host: str, port: int, save_dir: str):
        super().__init__()
        self.log = logging.getLogger("lab.integrator.net")
        self.host = host
        self.port = port
        self.save_dir = Path(save_dir)
        self._sock = None
        self._stop = threading.Event()
        self._thread = None
        self._counter = 0

    def start(self):
        # Evita doble inicio
        if self._thread and self._thread.is_alive():
            return
        self.log.info("Solicitado inicio de servidor MLLP")
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        # “Poke” al accept() para que despierte:
        try:
            with socket.create_connection((self.host, self.port), timeout=0.2):
                pass
        except Exception:
            pass
        # Cierra socket principal
        try:
            if self._sock:
                self._sock.close()
        except Exception:
            pass
        self._sock = None
        # Espera a que el hilo termine (pequeño join no bloqueante)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)
        self._thread = None
        self.log.info("Servidor detenido")
        self.stopped.emit()

    def _save_hl7(self, payload: bytes):
        self.save_dir.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d-%H%M%S")
        self._counter += 1
        path = self.save_dir / f"{ts}-{self._counter:04d}.hl7"
        path.write_bytes(payload)
        return str(path)

    def _run(self):
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Ayuda con TIME_WAIT en macOS/Linux
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Si quieres, puedes probar con REUSEPORT en macOS; NO es necesario normalmente:
            # try: self._sock.setsockopt(socket.SOL_SOCKET, 0x200, 1)  # SO_REUSEPORT
            # except Exception: pass

            self._sock.bind((self.host, self.port))
            self._sock.listen(5)

            # Si usaste puerto 0, obtén el real
            actual_port = self._sock.getsockname()[1]
            self.log.info(f"Servidor iniciado en {self.host}:{actual_port}")
            self.started.emit(self.host, actual_port)

            self._sock.settimeout(0.5)
            while not self._stop.is_set():
                try:
                    conn, addr = self._sock.accept()
                except TimeoutError:
                    continue
                except OSError:
                    break  # socket cerrado
                threading.Thread(
                    target=self._handle_client, args=(conn, addr), daemon=True
                ).start()

        except OSError as e:
            # Errno 48/98: puerto en uso
            self.error.emit(f"OSError: {e}")
            self.log.error(f"OSError al iniciar: {e}")
            self.error.emit(f"OSError: {e}")
        except Exception as e:
            self.error.emit(f"Server error: {e}")
        finally:
            try:
                if self._sock:
                    self._sock.close()
            except Exception:
                pass
            self._sock = None

    def _handle_client(self, conn: socket.socket, addr):
        buf = b""
        try:
            conn.settimeout(10)
            self.log.debug(f"Conexión desde {addr[0]}:{addr[1]}")
            while not self._stop.is_set():
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buf += chunk
                while True:
                    start = buf.find(VT)
                    if start == -1:
                        break
                    end = buf.find(FS + CR, start + 1)
                    if end == -1:
                        break
                    frame = buf[start + 1 : end]
                    buf = buf[end + 2 :]
                    self.received.emit(frame)
                    self._save_hl7(frame)
                    ack = (
                        VT
                        + b"MSH|^~\\&|LIM|SERVER|||"
                        + time.strftime("%Y%m%d%H%M%S").encode()
                        + b"||ACK^A01|1|P|2.3\rMSA|AA|1\r"
                        + FS
                        + CR
                    )
                    try:
                        conn.sendall(ack)
                    except Exception:
                        pass
        finally:
            try:
                conn.close()
            except Exception:
                pass
