from PySide6 import QtWidgets, QtCore
from lab_core.config import config_path

class ConfigTab(QtWidgets.QWidget):
    # Señal para notificar que se guardó settings.yaml (p/ refrescar en MainWindow)
    settings_saved = QtCore.Signal()

    def __init__(self, read_cfg_safe, write_cfg_safe):
        super().__init__()
        self.read_cfg = read_cfg_safe
        self.write_cfg = write_cfg_safe

        # ---- Layout raíz
        root = QtWidgets.QVBoxLayout(self)

        # Ruta del YAML (solo lectura)
        hdr = QtWidgets.QHBoxLayout()
        self.path_lbl = QtWidgets.QLineEdit(str(config_path()))
        self.path_lbl.setReadOnly(True)
        hdr.addWidget(QtWidgets.QLabel("Ruta settings.yaml:"))
        hdr.addWidget(self.path_lbl)
        root.addLayout(hdr)

        # ---- Tabs por secciones
        self.tabs = QtWidgets.QTabWidget()
        root.addWidget(self.tabs)

        # Secciones (cada una arma su propio formulario)
        self._build_tcp_tab()
        self._build_input_tab()
        self._build_paths_tab()
        self._build_memo_tab()
        self._build_api_tab()
        self._build_orders_tab()
        self._build_monitor_tab()
        self._build_results_export_tab()
        self._build_logging_tab()

        # ---- Botones
        btns = QtWidgets.QHBoxLayout()
        btns.addStretch(1)

        self.btn_reload = QtWidgets.QPushButton("Recargar")
        self.btn_save = QtWidgets.QPushButton("Guardar cambios")
        btns.addWidget(self.btn_reload)
        btns.addWidget(self.btn_save)

        root.addLayout(btns)

        # Conexiones
        self.btn_reload.clicked.connect(self.load_settings)
        self.btn_save.clicked.connect(self.save_now)

        # Carga inicial
        self.load_settings()

    # --------------- Construcción de pestañas ---------------

    def _build_tcp_tab(self):
        w = QtWidgets.QWidget()
        form = QtWidgets.QFormLayout(w)

        self.tcp_host = QtWidgets.QLineEdit()
        self.tcp_port = QtWidgets.QSpinBox()
        self.tcp_port.setRange(1, 65535)

        form.addRow("Host:", self.tcp_host)
        form.addRow("Port:", self.tcp_port)

        self.tabs.addTab(w, "TCP")

    def _build_input_tab(self):
        w = QtWidgets.QWidget()
        form = QtWidgets.QFormLayout(w)

        # input.mode
        self.input_mode = QtWidgets.QComboBox()
        self.input_mode.addItems(["tcp", "file"])

        # file.inbox_dir
        inbox_row = QtWidgets.QHBoxLayout()
        self.file_inbox_dir = QtWidgets.QLineEdit()
        self.btn_pick_inbox = QtWidgets.QToolButton()
        self.btn_pick_inbox.setText("…")
        self.btn_pick_inbox.clicked.connect(self._pick_dir_inbox)
        inbox_row.addWidget(self.file_inbox_dir)
        inbox_row.addWidget(self.btn_pick_inbox)

        form.addRow("Modo de entrada:", self.input_mode)
        form.addRow("Carpeta de entrada (file.inbox_dir):", self._wrap(inbox_row))

        self.tabs.addTab(w, "Entrada")

    def _build_paths_tab(self):
        w = QtWidgets.QWidget()
        form = QtWidgets.QFormLayout(w)

        # paths.outbox
        outbox_row = QtWidgets.QHBoxLayout()
        self.paths_outbox = QtWidgets.QLineEdit()
        self.btn_pick_outbox = QtWidgets.QToolButton()
        self.btn_pick_outbox.setText("…")
        self.btn_pick_outbox.clicked.connect(self._pick_dir_outbox)
        outbox_row.addWidget(self.paths_outbox)
        outbox_row.addWidget(self.btn_pick_outbox)

        form.addRow("Carpeta paths.outbox:", self._wrap(outbox_row))

        self.tabs.addTab(w, "Rutas")

    def _build_memo_tab(self):
        w = QtWidgets.QWidget()
        form = QtWidgets.QFormLayout(w)
        self.memo_limit = QtWidgets.QSpinBox()
        self.memo_limit.setRange(1, 1_000_000_000)
        form.addRow("memo.limit:", self.memo_limit)
        self.tabs.addTab(w, "Memo")

    def _build_api_tab(self):
        w = QtWidgets.QWidget()
        form = QtWidgets.QFormLayout(w)

        self.api_base_url = QtWidgets.QLineEdit()
        self.api_key = QtWidgets.QLineEdit()
        self.api_secret = QtWidgets.QLineEdit()
        self.api_secret.setEchoMode(QtWidgets.QLineEdit.Password)
        self.api_action = QtWidgets.QLineEdit()

        self.api_use_mock = QtWidgets.QCheckBox("Usar mock")
        mock_row = QtWidgets.QHBoxLayout()
        self.api_mock_file = QtWidgets.QLineEdit()
        self.btn_pick_mock = QtWidgets.QToolButton()
        self.btn_pick_mock.setText("…")
        self.btn_pick_mock.clicked.connect(self._pick_file_mock)
        mock_row.addWidget(self.api_mock_file)
        mock_row.addWidget(self.btn_pick_mock)

        form.addRow("Base URL:", self.api_base_url)
        form.addRow("API Key:", self.api_key)
        form.addRow("API Secret:", self.api_secret)
        form.addRow("Acción:", self.api_action)
        form.addRow("", self.api_use_mock)
        form.addRow("Mock file:", self._wrap(mock_row))

        self.tabs.addTab(w, "API")

    def _build_orders_tab(self):
        w = QtWidgets.QWidget()
        form = QtWidgets.QFormLayout(w)
        self.orders_poll_sec = QtWidgets.QDoubleSpinBox()
        self.orders_poll_sec.setDecimals(2)
        self.orders_poll_sec.setRange(0.0, 86400.0)
        form.addRow("orders.poll_every_sec:", self.orders_poll_sec)
        self.tabs.addTab(w, "Órdenes")

    def _build_monitor_tab(self):
        w = QtWidgets.QWidget()
        form = QtWidgets.QFormLayout(w)
        self.monitor_autostart = QtWidgets.QCheckBox("Autostart server al abrir")
        form.addRow("", self.monitor_autostart)
        self.tabs.addTab(w, "Monitor")

    def _build_results_export_tab(self):
        w = QtWidgets.QWidget()
        form = QtWidgets.QFormLayout(w)

        self.exp_enabled = QtWidgets.QCheckBox("Habilitar exportación de resultados")
        self.exp_interval = QtWidgets.QSpinBox()
        self.exp_interval.setRange(100, 3_600_000)
        self.exp_interval.setSuffix(" ms")

        self.exp_batch = QtWidgets.QSpinBox()
        self.exp_batch.setRange(1, 1_000_000)

        exp_outbox_row = QtWidgets.QHBoxLayout()
        self.exp_outbox = QtWidgets.QLineEdit()
        self.btn_pick_exp_outbox = QtWidgets.QToolButton()
        self.btn_pick_exp_outbox.setText("…")
        self.btn_pick_exp_outbox.clicked.connect(self._pick_dir_exp_outbox)
        exp_outbox_row.addWidget(self.exp_outbox)
        exp_outbox_row.addWidget(self.btn_pick_exp_outbox)

        form.addRow("", self.exp_enabled)
        form.addRow("Intervalo:", self.exp_interval)
        form.addRow("Tamaño de lote:", self.exp_batch)
        form.addRow("Carpeta outbox:", self._wrap(exp_outbox_row))

        self.tabs.addTab(w, "Exportación")

    def _build_logging_tab(self):
        w = QtWidgets.QWidget()
        form = QtWidgets.QFormLayout(w)

        self.log_to_file = QtWidgets.QCheckBox("Guardar logs en archivo")
        self.log_level = QtWidgets.QComboBox()
        self.log_level.addItems(["DEBUG", "INFO", "WARNING", "ERROR"])

        form.addRow("", self.log_to_file)
        form.addRow("Nivel de logs:", self.log_level)

        self.tabs.addTab(w, "Logging")

    # --------------- Helpers UI ---------------

    def _wrap(self, layout: QtWidgets.QLayout) -> QtWidgets.QWidget:
        w = QtWidgets.QWidget()
        w.setLayout(layout)
        return w

    def _pick_dir_inbox(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Seleccionar carpeta de entrada")
        if d:
            self.file_inbox_dir.setText(d)

    def _pick_dir_outbox(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Seleccionar carpeta paths.outbox")
        if d:
            self.paths_outbox.setText(d)

    def _pick_dir_exp_outbox(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Seleccionar carpeta de exportación")
        if d:
            self.exp_outbox.setText(d)

    def _pick_file_mock(self):
        f, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Seleccionar mock XML", filter="XML (*.xml);;Todos (*)")
        if f:
            self.api_mock_file.setText(f)

    # --------------- Carga / Guardado ---------------

    def load_settings(self):
        cfg = self.read_cfg() or {}

        # tcp
        tcp = cfg.get("tcp", {}) or {}
        self.tcp_host.setText(str(tcp.get("host", "")))
        self.tcp_port.setValue(int(tcp.get("port", 5002)))

        # input / file
        input_ = cfg.get("input", {}) or {}
        mode = str(input_.get("mode", "tcp")).lower()
        idx = self.input_mode.findText(mode)
        self.input_mode.setCurrentIndex(idx if idx >= 0 else 0)

        file_ = cfg.get("file", {}) or {}
        self.file_inbox_dir.setText(str(file_.get("inbox_dir", "")))

        # paths
        paths = cfg.get("paths", {}) or {}
        self.paths_outbox.setText(str(paths.get("outbox", "")))

        # memo
        memo = cfg.get("memo", {}) or {}
        self.memo_limit.setValue(int(memo.get("limit", 200000)))

        # api
        api = cfg.get("api", {}) or {}
        self.api_base_url.setText(str(api.get("base_url", "")))
        self.api_key.setText(str(api.get("key", "")))
        self.api_secret.setText(str(api.get("secret", "")))
        self.api_action.setText(str(api.get("action", "")))
        self.api_use_mock.setChecked(bool(api.get("use_mock", False)))
        self.api_mock_file.setText(str(api.get("mock_file", "")))

        # orders
        orders = cfg.get("orders", {}) or {}
        self.orders_poll_sec.setValue(float(orders.get("poll_every_sec", 0)))

        # monitor
        monitor = cfg.get("monitor", {}) or {}
        self.monitor_autostart.setChecked(bool(monitor.get("autostart_server", True)))

        # results_export
        exp = cfg.get("results_export", {}) or {}
        self.exp_enabled.setChecked(bool(exp.get("enabled", True)))
        self.exp_interval.setValue(int(exp.get("interval_ms", 5000)))
        self.exp_batch.setValue(int(exp.get("batch_size", 200)))
        self.exp_outbox.setText(str(exp.get("outbox", "outbox_xml")))

        # logging
        logging = cfg.get("logging", {}) or {}
        self.log_to_file.setChecked(bool(logging.get("to_file", False)))
        lvl = str(logging.get("level", "INFO")).upper()
        i = self.log_level.findText(lvl)
        self.log_level.setCurrentIndex(i if i >= 0 else self.log_level.findText("INFO"))

    @QtCore.Slot()
    def save_now(self):
        cfg = self.read_cfg() or {}

        # Asegura sub-dicts
        for key in ["tcp", "paths", "memo", "input", "file", "logging", "api", "orders", "monitor", "results_export"]:
            if key not in cfg or not isinstance(cfg[key], dict):
                cfg[key] = {}

        # tcp
        cfg["tcp"]["host"] = self.tcp_host.text().strip()
        cfg["tcp"]["port"] = int(self.tcp_port.value())

        # input / file
        cfg["input"]["mode"] = self.input_mode.currentText()
        cfg["file"]["inbox_dir"] = self.file_inbox_dir.text().strip()

        # paths
        cfg["paths"]["outbox"] = self.paths_outbox.text().strip()

        # memo
        cfg["memo"]["limit"] = int(self.memo_limit.value())

        # api
        cfg["api"]["base_url"] = self.api_base_url.text().strip()
        cfg["api"]["key"] = self.api_key.text().strip()
        cfg["api"]["secret"] = self.api_secret.text().strip()
        cfg["api"]["action"] = self.api_action.text().strip()
        cfg["api"]["use_mock"] = bool(self.api_use_mock.isChecked())
        cfg["api"]["mock_file"] = self.api_mock_file.text().strip()

        # orders
        cfg["orders"]["poll_every_sec"] = float(self.orders_poll_sec.value())

        # monitor
        cfg["monitor"]["autostart_server"] = bool(self.monitor_autostart.isChecked())

        # results_export
        cfg["results_export"]["enabled"] = bool(self.exp_enabled.isChecked())
        cfg["results_export"]["interval_ms"] = int(self.exp_interval.value())
        cfg["results_export"]["batch_size"] = int(self.exp_batch.value())
        cfg["results_export"]["outbox"] = self.exp_outbox.text().strip()

        # logging
        cfg["logging"]["to_file"] = bool(self.log_to_file.isChecked())
        cfg["logging"]["level"] = self.log_level.currentText()

        # Guardar
        ok = self.write_cfg(cfg)
        if not ok:
            QtWidgets.QMessageBox.warning(self, "Configuración", "No se pudo guardar settings.yaml.")
            return

        # Aviso OK + señal para refrescar en caliente
        QtWidgets.QMessageBox.information(self, "Configuración", "Cambios guardados.")
        self.settings_saved.emit()
