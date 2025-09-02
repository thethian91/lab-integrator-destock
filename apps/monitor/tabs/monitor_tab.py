import logging
from datetime import datetime
from pathlib import Path

from PySide6 import QtWidgets, QtCore
from PySide6.QtCore import QDate, QTimer

from ..net_server import MLLPServer

# Descarga de órdenes (mock o real)
from lab_core.orders_client import (
    get_orders_xml_from_cfg,
    parse_orders,
    download_and_store_orders,
)
from lab_core.orders_store import upsert_orders
from lab_core.result_ingest import ingest_inbox
from lab_core.db import init_db

class MonitorTab(QtWidgets.QWidget):
    def __init__(self, read_cfg_safe, write_cfg_safe):
        super().__init__()

        self.ui_log = logging.getLogger("lab.integrator.ui")
        self.read_cfg = read_cfg_safe
        self.write_cfg = write_cfg_safe
        self.server = None
        self._orders_timer: QTimer | None = None
        self._polling_now = False

        v = QtWidgets.QVBoxLayout(self)

        # Top bar
        top = QtWidgets.QHBoxLayout()
        v.addLayout(top)

        top.addWidget(QtWidgets.QLabel("Host:"))
        self.host = QtWidgets.QLineEdit()
        self.host.setPlaceholderText("0.0.0.0")
        self.host.setFixedWidth(180)
        top.addWidget(self.host)

        top.addWidget(QtWidgets.QLabel("Puerto:"))
        self.port = QtWidgets.QSpinBox()
        self.port.setRange(1, 65535)
        self.port.setValue(5002)
        self.port.setFixedWidth(100)
        top.addWidget(self.port)

        top.addWidget(QtWidgets.QLabel("Modo:"))
        self.mode = QtWidgets.QComboBox()
        self.mode.addItems(["tcp", "file"])
        top.addWidget(self.mode)

        self.dir_label = QtWidgets.QLabel("Carpeta:")
        self.filedir = QtWidgets.QLineEdit()
        self.filedir.setPlaceholderText("C:/lab-integrador/inbox")
        self.filedir.setFixedWidth(340)
        self.btn_browse = QtWidgets.QPushButton("Examinar…")
        self.btn_browse.clicked.connect(self._pick_folder)

        top.addWidget(self.dir_label)
        top.addWidget(self.filedir)
        top.addWidget(self.btn_browse)
        top.addStretch(1)

        # Log en vivo
        grp = QtWidgets.QGroupBox("Eventos en tiempo real")
        v.addWidget(grp)
        gvl = QtWidgets.QVBoxLayout(grp)
        self.monitor_log = QtWidgets.QPlainTextEdit()
        self.monitor_log.setReadOnly(True)
        gvl.addWidget(self.monitor_log)

        # -----------------------------
        # Barra de botones inferior
        # -----------------------------
        btns = QtWidgets.QHBoxLayout()
        v.addLayout(btns)

        # Controles de fecha / descarga
        btns.addWidget(QtWidgets.QLabel("Fecha:"))
        self.order_date = QtWidgets.QDateEdit()
        self.order_date.setCalendarPopup(True)
        self.order_date.setDate(QDate.currentDate())
        self.order_date.setDisplayFormat("yyyy-MM-dd")
        btns.addWidget(self.order_date)

        self.btn_fetch_date = QtWidgets.QPushButton("Descargar órdenes (fecha)")
        self.btn_fetch_date.setToolTip("Usa mock si api.use_mock=true; si no, llama servicio real.")
        btns.addWidget(self.btn_fetch_date)

        # Botones de acciones varias
        self.btn_fetch = QtWidgets.QPushButton("Descargar órdenes (hoy)")
        self.btn_start = QtWidgets.QPushButton("Iniciar servidor")
        self.btn_stop = QtWidgets.QPushButton("Detener")
        self.btn_save = QtWidgets.QPushButton("Guardar configuración")
        self.btn_stop.setEnabled(False)

        btns.addWidget(self.btn_fetch)
        btns.addWidget(self.btn_start)
        btns.addWidget(self.btn_stop)
        btns.addStretch(1)
        btns.addWidget(self.btn_save)

        # Señales UI / config
        self.btn_fetch.clicked.connect(self._fetch_today_orders)
        self.btn_fetch_date.clicked.connect(self._fetch_orders_for_date)
        self.btn_save.clicked.connect(self.save_now)
        self.mode.currentIndexChanged.connect(self._on_mode_change)
        self.host.editingFinished.connect(self.save_now)
        self.port.valueChanged.connect(self.save_now)
        self.filedir.editingFinished.connect(self.save_now)

        # Start/Stop server
        self.btn_start.clicked.connect(self._on_start)
        self.btn_stop.clicked.connect(self._on_stop)

        # 1) asegurar la BD to update
        init_db()

        # 2) timer para resultados
        self.results_timer = QtCore.QTimer(self)
        self.results_timer.setInterval(10_000)   # cada 10 s (ajústalo)
        self.results_timer.timeout.connect(self._poll_results)
        self.results_timer.start()

        # Visibilidad inicial
        self._on_mode_change()

        # Autorun: iniciar server y programar descargas
        self._setup_autorun()

    def _poll_results(self):
        try:
            res = ingest_inbox(inbox_path=self.filedir.text().strip() or "inbox")
            if res:
                if res["processed"] or res["errors"]:
                    self._append_monitor(f"[RESULTADOS] Ingesta: ok={res['processed']} err={res['errors']}")
        except Exception as e:
            self._append_monitor(f"[ERROR] Ingesta resultados: {e}")

    # ---------- Descarga manual: hoy ----------
    def _fetch_today_orders(self):
        # Usa la fecha de hoy (forzado)
        fecha = datetime.now().strftime("%Y%m%d")
        try:
            cfg = self.read_cfg()
            # Reutilizamos la función de alto nivel que guarda en disco,
            # pero si usas MOCK, preferible usar _fetch_orders_for_today_silent()
            result = download_and_store_orders(cfg, fecha)
            self._append_monitor(
                f"[ORDENES] {result['count']} pacientes – "
                f"archivos guardados en {cfg.get('orders', {}).get('out_dir', 'inbox/orders')}/{fecha}"
            )
        except Exception as e:
            self._append_monitor(f"[ERROR] Descarga de órdenes (hoy): {e}")

    # ---------- Descarga manual: fecha del selector ----------
    def _fetch_orders_for_date(self):
        """Descarga y almacena órdenes para la fecha elegida.
        - Si api.use_mock=true -> lee samples/orders_YYYYMMDD.xml (y ajusta settings.api.mock_file)
        - Si api.use_mock=false -> llama al endpoint real.
        """
        qd = self.order_date.date()
        fecha = f"{qd.year():04d}{qd.month():02d}{qd.day():02d}"

        try:
            # init_db()
            cfg = self.read_cfg() or {}
            api = cfg.get("api", {}) or {}
            use_mock = bool(api.get("use_mock", False))

            # Si está en mock y el mock_file no corresponde a la fecha seleccionada, actualízalo
            if use_mock:
                mock_path = api.get("mock_file") or ""
                desired = f"samples/orders_{fecha}.xml"
                if mock_path != desired:
                    api["mock_file"] = desired
                    cfg["api"] = api
                    if not self.write_cfg(cfg):
                        self._append_monitor("[WARN] No se pudo actualizar mock_file en settings.yaml.")
                    else:
                        self._append_monitor(f"[MOCK] Usando archivo: {desired}")

            xml_text = get_orders_xml_from_cfg(cfg, fecha)
            records = parse_orders(xml_text)
            upsert_orders(records)

            self._append_monitor(f"[ORDENES] {len(records)} pacientes cargados para {fecha}")
            self._append_monitor("[INFO] Fuente: MOCK (archivo local)" if use_mock else "[INFO] Fuente: Servicio real")

        except FileNotFoundError as e:
            self._append_monitor(f"[ERROR] Mock no encontrado: {e}")
        except Exception as e:
            self._append_monitor(f"[ERROR] Descarga de órdenes ({fecha}): {e}")

    # ---------- Autorun & Polling ----------
    def _setup_autorun(self):
        """Arranca server y programador de descargas según settings."""
        cfg = self.read_cfg() or {}

        # Autostart del servidor
        if (cfg.get("monitor", {}) or {}).get("autostart_server", False):
            if self.mode.currentText() == "tcp" and self.server is None:
                self._on_start()

        # Timer de descargas periódicas
        poll_sec = int((cfg.get("orders", {}) or {}).get("poll_every_sec", 0) or 0)
        if poll_sec > 0:
            self._start_orders_timer(poll_sec)

    def _start_orders_timer(self, seconds: int):
        """Programa descargas automáticas cada 'seconds' segundos."""
        if self._orders_timer is not None:
            self._orders_timer.stop()
            self._orders_timer.deleteLater()
            self._orders_timer = None

        self._orders_timer = QTimer(self)
        self._orders_timer.setInterval(max(5, seconds) * 1000)  # mínimo 5s
        self._orders_timer.timeout.connect(self._poll_orders_tick)
        self._orders_timer.start()
        self._append_monitor(f"[ORDENES] Polling programado cada {seconds}s")

    def _poll_orders_tick(self):
        """Tick del timer: descarga órdenes de HOY, evitando solapamientos."""
        if self._polling_now:
            return
        self._polling_now = True
        try:
            self._fetch_orders_for_today_silent()
        finally:
            self._polling_now = False

    def _fetch_orders_for_today_silent(self):
        """Descarga órdenes para HOY (mock o real), sin bloquear la UI."""
        fecha = datetime.now().strftime("%Y%m%d")
        try:
            init_db()
            cfg = self.read_cfg() or {}
            api = cfg.get("api", {}) or {}
            use_mock = bool(api.get("use_mock", False))

            # Si MOCK, apuntar mock_file del día automáticamente (si quieres)
            if use_mock:
                desired = f"samples/orders_{fecha}.xml"
                if (api.get("mock_file") or "") != desired:
                    api["mock_file"] = desired
                    cfg["api"] = api
                    self.write_cfg(cfg)

            xml_text = get_orders_xml_from_cfg(cfg, fecha)
            records = parse_orders(xml_text)
            upsert_orders(records)

            self._append_monitor(
                f"[ORDENES/AUTO] {len(records)} pacientes cargados para {fecha}" +
                (" [MOCK]" if use_mock else " [REAL]")
            )
        except FileNotFoundError as e:
            self._append_monitor(f"[ERROR] Mock no encontrado ({fecha}): {e}")
        except Exception as e:
            self._append_monitor(f"[ERROR] Polling de órdenes ({fecha}): {e}")

    # ---------- Config & UI ----------
    def load_settings(self):
        cfg = self.read_cfg()
        self.host.setText(cfg.get("tcp", {}).get("host", "0.0.0.0"))
        self.port.setValue(int(cfg.get("tcp", {}).get("port", 5002)))
        mode_val = cfg.get("input", {}).get("mode", "tcp")
        idx = self.mode.findText(mode_val)
        if idx >= 0:
            self.mode.setCurrentIndex(idx)
        self.filedir.setText(cfg.get("file", {}).get("inbox_dir", ""))

    @QtCore.Slot()
    def save_now(self):
        cfg = {
            "tcp": {
                "host": self.host.text().strip() or "0.0.0.0",
                "port": int(self.port.value()),
            },
            "input": {"mode": self.mode.currentText()},
            "file": {"inbox_dir": self.filedir.text().strip()},
        }
        existing = self.read_cfg()
        existing.update(cfg)
        if not self.write_cfg(existing):
            QtWidgets.QMessageBox.warning(self, "Configuración", "No se pudo guardar settings.yaml.")

    def _on_mode_change(self):
        is_file = (self.mode.currentText() == "file")
        for w in (self.dir_label, self.filedir, self.btn_browse):
            w.setVisible(is_file)

    def _pick_folder(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Selecciona carpeta de entrada")
        if d:
            self.filedir.setText(d)
            self.save_now()

    # ---------- Monitor helpers ----------
    def _append_monitor(self, text: str):
        self.monitor_log.appendPlainText(text)

    # ---------- Server wiring ----------
    def _connect_server_signals(self):
        # Hacia el memo de Monitor
        self.server.started.connect(lambda h, p: self._append_monitor(f"[START] Escuchando {h}:{p} (MLLP)"))
        self.server.stopped.connect(lambda: self._append_monitor("[STOP] Servidor detenido"))
        self.server.received.connect(self._on_payload)
        self.server.error.connect(lambda msg: self._append_monitor(f"[ERROR] {msg}"))
        # (Opcional) hacia logger
        self.server.started.connect(lambda h, p: self.ui_log.info(f"Escuchando {h}:{p} (MLLP)"))
        self.server.stopped.connect(lambda: self.ui_log.info("Servidor detenido"))
        self.server.received.connect(lambda _: self.ui_log.info("HL7 recibido"))
        self.server.error.connect(lambda msg: self.ui_log.error(msg))

    # ---------- Start/Stop ----------
    def _on_start(self):
        if self.server is not None:
            self._append_monitor("[INFO] Servidor ya iniciado.")
            return

        host = self.host.text().strip() or "0.0.0.0"
        port = int(self.port.value())
        mode = self.mode.currentText()

        if mode != "tcp":
            self._append_monitor("[INFO] El modo actual no es TCP; nada que iniciar.")
            return

        inbox = self.filedir.text().strip() or str(Path.cwd() / "inbox")

        # Crear servidor ahora
        self.server = MLLPServer(host, port, inbox)

        # Conectar señales ahora que existe
        self._connect_server_signals()

        # Arrancar
        self.server.start()
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)

    def _on_stop(self):
        if self.server:
            self.server.stop()
            self.server = None
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)

    # ---------- Payload recibido ----------
    @QtCore.Slot(bytes)
    def _on_payload(self, payload: bytes):
        preview = payload.decode(errors="ignore").splitlines()[:3]
        self._append_monitor(f"[RECV] {len(payload)} bytes\n" + "\n".join(preview))
