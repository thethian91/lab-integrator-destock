# apps/configurator/main.py
import sys
from pathlib import Path

import yaml
from PySide6 import QtWidgets

DEFAULT_PATH = Path("configs/settings.yaml")


class Configurator(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Lab Integrator • Configurator")
        self.resize(720, 360)

        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        layout = QtWidgets.QFormLayout(central)

        self.host = QtWidgets.QLineEdit("0.0.0.0")
        self.port = QtWidgets.QSpinBox()
        self.port.setRange(1, 65535)
        self.port.setValue(5002)
        self.outbox = QtWidgets.QLineEdit(str(Path("./inbox").absolute()))
        self.btn_browse = QtWidgets.QPushButton("...")
        self.btn_save = QtWidgets.QPushButton("Guardar")
        self.btn_load = QtWidgets.QPushButton("Cargar")

        row = QtWidgets.QHBoxLayout()
        row.addWidget(self.outbox)
        row.addWidget(self.btn_browse)

        layout.addRow("Host", self.host)
        layout.addRow("Puerto", self.port)
        layout.addRow("Destino", row)
        layout.addRow(self.btn_load, self.btn_save)

        self.btn_browse.clicked.connect(self.pick_dir)
        self.btn_save.clicked.connect(self.save)
        self.btn_load.clicked.connect(self.load)

    def pick_dir(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Destino")
        if d:
            self.outbox.setText(d)

    def save(self):
        data = {
            "tcp": {"host": self.host.text(), "port": int(self.port.value())},
            "paths": {"outbox": self.outbox.text()},
        }
        DEFAULT_PATH.parent.mkdir(parents=True, exist_ok=True)
        DEFAULT_PATH.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
        QtWidgets.QMessageBox.information(self, "OK", f"Guardado en {DEFAULT_PATH}")

    def load(self):
        if not DEFAULT_PATH.exists():
            QtWidgets.QMessageBox.warning(self, "Aviso", f"No existe {DEFAULT_PATH}")
            return
        data = yaml.safe_load(DEFAULT_PATH.read_text(encoding="utf-8"))
        self.host.setText(str(data.get("tcp", {}).get("host", "0.0.0.0")))
        self.port.setValue(int(data.get("tcp", {}).get("port", 5002)))
        self.outbox.setText(str(data.get("paths", {}).get("outbox", "./inbox")))


def main():
    app = QtWidgets.QApplication(sys.argv)
    win = Configurator()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
