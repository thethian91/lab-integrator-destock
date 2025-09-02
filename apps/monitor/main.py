import logging
import os
from pathlib import Path

from PySide6 import QtWidgets, QtGui, QtCore
from PySide6.QtCore import Qt, QSettings
from PySide6.QtGui import QGuiApplication

from lab_core.config import read_cfg_safe, write_cfg_safe
from lab_core.db import ensure_schema, DEFAULT_DB_PATH
from lab_core.dispatcher import dispatch_cycle  # <- core del poller

from .tabs.monitor_tab import MonitorTab
from .tabs.config_tab import ConfigTab
from .tabs.logs_tab import LogsTab
from .tabs.orders_tab import OrdersTab
from .tabs.maintenance_tab import MaintenanceTab
from .tabs.orders_results_tab import OrdersResultsTab
from .qt_logging import QtLogEmitter, QtLogHandler

APP_ORG = "Vitronix"
APP_NAME = "LabIntegratorMonitor"
RES = Path(__file__).resolve().parents[2] / "resources"

# ------------------- Utilidades de configuración -------------------
def _export_defaults() -> dict:
    # Defaults seguros si el YAML no define algo
    return {
        "enabled": True,
        "interval_ms": 5000,
        "batch_size": 200,
        "outbox": "outbox_xml",
    }

def _read_export_cfg() -> dict:
    cfg = read_cfg_safe() or {}
    node = (cfg.get("results_export") or {})
    dft = _export_defaults()
    return {
        "enabled": bool(node.get("enabled", dft["enabled"])),
        "interval_ms": int(node.get("interval_ms", dft["interval_ms"])),
        "batch_size": int(node.get("batch_size", dft["batch_size"])),
        "outbox": str(node.get("outbox", dft["outbox"])),
    }

# Asegura esquema de DB al cargar la app (idempotente)
ensure_schema(DEFAULT_DB_PATH)

# ------------------- Worker de despacho (hilo) -------------------
class DispatchWorker(QtCore.QObject):
    finished = QtCore.Signal(dict)
    errored = QtCore.Signal(str)

    def __init__(self, db_path: str, out_dir: str, batch_size: int):
        super().__init__()
        self.db_path = db_path
        self.out_dir = out_dir
        self.batch_size = batch_size

    @QtCore.Slot()
    def run(self):
        try:
            stats = dispatch_cycle(self.db_path, self.out_dir, self.batch_size)
            # stats ejemplo: {"picked": .., "sent": .., "error": ..}
            self.finished.emit(stats)
        except Exception as e:
            self.errored.emit(str(e))

# ------------------- Ventana principal -------------------
class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Lab Integrator • Monitor")
        self.resize(980, 640)
        self.setMinimumSize(720, 480)
        self.setWindowState(Qt.WindowNoState)

        # Centrar
        screen = QGuiApplication.primaryScreen().availableGeometry()
        self.move(
            screen.center().x() - self.width() // 2,
            screen.center().y() - self.height() // 2,
        )

        # QSettings para recordar geometría
        self.settings = QSettings(APP_ORG, APP_NAME)
        self._restore_geometry()

        # Asegura esquema (idempotente)
        ensure_schema(DEFAULT_DB_PATH)

        # Tabs
        self.tabs = QtWidgets.QTabWidget()
        self.setCentralWidget(self.tabs)

        # Instancias de pestañas
        self.monitor_tab = MonitorTab(read_cfg_safe, write_cfg_safe)
        self.config_tab = ConfigTab(read_cfg_safe, write_cfg_safe)
        self.orders_tab = OrdersTab()                   # Órdenes
        self.orders_results_tab = OrdersResultsTab()    # Resultados
        self.maint_tab = MaintenanceTab()
        self.logs_tab = LogsTab()

        # Icono de la app
        self.setWindowIcon(QtGui.QIcon(str(RES / "app.png")))

        # Agregar tabs (solo una vez, con iconos)
        self.tabs.addTab(self.monitor_tab, QtGui.QIcon(str(RES / "monitor.png")), "Monitor")
        self.tabs.addTab(self.config_tab,  QtGui.QIcon(str(RES / "config.png")),  "Configuración")
        self.tabs.addTab(self.orders_tab,  QtGui.QIcon(str(RES / "tests.png")),   "Órdenes")
        self.tabs.addTab(self.orders_results_tab, QtGui.QIcon(str(RES / "tests.png")), "Resultados")
        self.tabs.addTab(self.maint_tab,   QtGui.QIcon(str(RES / "tests.png")),   "Mantenimiento")
        self.tabs.addTab(self.logs_tab,    QtGui.QIcon(str(RES / "logs.png")),    "Logs")

        # Status de exportación en la barra de estado
        self._xml_status = QtWidgets.QLabel("XML: idle")
        self.statusBar().addPermanentWidget(self._xml_status)

        # Estado del ciclo/exportación
        self._dispatch_timer = QtCore.QTimer(self)
        self._dispatch_timer.timeout.connect(self._kick_dispatch)
        self._dispatch_running = False

        # Cargar settings después de construir UI
        self._apply_log_level_from_settings()
        self.monitor_tab.load_settings()
        self.config_tab.load_settings()

        # Aplicar configuración de exportación desde YAML
        self.refresh_export_settings()

        # ---- Logger → LogsTab
        self._setup_logs_bridge()

    # ------------------- Helpers -------------------
    def _setup_logs_bridge(self):
        self.log_emitter = QtLogEmitter()
        self.log_emitter.log.connect(self.logs_tab.append_log)
        handler = QtLogHandler(self.log_emitter)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        handler.setLevel(logging.DEBUG)
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        root.addHandler(handler)

    def _apply_log_level_from_settings(self):
        cfg = read_cfg_safe()
        level_name = (cfg.get("logging", {}) or {}).get("level", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)
        logging.getLogger().setLevel(level)

    def _restore_geometry(self):
        geo = self.settings.value("window/geometry")
        if geo is not None:
            self.restoreGeometry(geo)
        self.setWindowState(Qt.WindowNoState)

    # ------------------- API pública para re-aplicar YAML -------------------
    def refresh_export_settings(self):
        """
        Lee config/settings.yaml y aplica:
          - results_export.enabled
          - results_export.interval_ms
          - results_export.batch_size
          - results_export.outbox
        """
        exp = _read_export_cfg()
        self._export_enabled = exp["enabled"]
        self._export_interval_ms = exp["interval_ms"]
        self._export_batch_size = exp["batch_size"]
        self._export_outbox = exp["outbox"]

        # Aplicar al timer
        if self._export_enabled:
            self._dispatch_timer.setInterval(self._export_interval_ms)
            if not self._dispatch_timer.isActive():
                self._dispatch_timer.start()
            self._xml_status.setText(f"XML: enabled • every {self._export_interval_ms} ms")
        else:
            if self._dispatch_timer.isActive():
                self._dispatch_timer.stop()
            self._xml_status.setText("XML: disabled")

    # ------------------- Eventos -------------------
    def closeEvent(self, ev: QtGui.QCloseEvent):
        # Detener timer al cerrar
        if hasattr(self, "_dispatch_timer") and self._dispatch_timer.isActive():
            self._dispatch_timer.stop()
        # Guardar geometría y ajustes
        self.settings.setValue("window/geometry", self.saveGeometry())
        # Pide a las tabs que guarden si es necesario
        self.monitor_tab.save_now()
        self.config_tab.save_now()
        super().closeEvent(ev)

    # ------------------- Ciclo de despacho -------------------
    @QtCore.Slot()
    def _kick_dispatch(self):
        # Si está deshabilitado por YAML, no correr
        if not getattr(self, "_export_enabled", True):
            return

        if self._dispatch_running:
            return
        self._dispatch_running = True
        self._xml_status.setText("XML: running...")

        # hilo efímero por ciclo
        self._dispatch_thread = QtCore.QThread(self)
        self._dispatch_worker = DispatchWorker(
            DEFAULT_DB_PATH,
            self._export_outbox,
            self._export_batch_size
        )
        self._dispatch_worker.moveToThread(self._dispatch_thread)

        self._dispatch_thread.started.connect(self._dispatch_worker.run)
        self._dispatch_worker.finished.connect(self._on_dispatch_done)
        self._dispatch_worker.errored.connect(self._on_dispatch_err)

        # limpieza
        self._dispatch_worker.finished.connect(self._dispatch_thread.quit)
        self._dispatch_worker.errored.connect(self._dispatch_thread.quit)
        self._dispatch_thread.finished.connect(self._dispatch_worker.deleteLater)
        self._dispatch_thread.finished.connect(self._dispatch_thread.deleteLater)

        self._dispatch_thread.start()

    @QtCore.Slot(dict)
    def _on_dispatch_done(self, stats: dict):
        self._xml_status.setText(
            f"XML: picked={stats.get('picked', 0)} "
            f"sent={stats.get('sent', 0)} "
            f"err={stats.get('error', 0)}"
        )
        self._dispatch_running = False

    @QtCore.Slot(str)
    def _on_dispatch_err(self, msg: str):
        self._xml_status.setText(f"XML: error: {msg[:60]}")
        self._dispatch_running = False

# ------------------- bootstrap -------------------
def main():
    import sys
    app = QtWidgets.QApplication(sys.argv)
    app.setWindowIcon(QtGui.QIcon(str(RES / "app.png")))
    app.setOrganizationName(APP_ORG)
    app.setApplicationName(APP_NAME)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
