
# apps/dashboard/main.py
import sys
from pathlib import Path
from PySide6 import QtWidgets, QtGui, QtCore

class Dashboard(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Lab Integrator • Dashboard")
        self.resize(900, 560)

        central = QtWidgets.QWidget(); self.setCentralWidget(central)
        v = QtWidgets.QVBoxLayout(central)

        top = QtWidgets.QHBoxLayout()
        self.folder = QtWidgets.QLineEdit(str(Path("./inbox").absolute()))
        self.btn_browse = QtWidgets.QPushButton("...")
        self.btn_refresh = QtWidgets.QPushButton("Refrescar")
        top.addWidget(QtWidgets.QLabel("Carpeta de resultados:"))
        top.addWidget(self.folder); top.addWidget(self.btn_browse); top.addWidget(self.btn_refresh)
        v.addLayout(top)

        self.table = QtWidgets.QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Archivo", "Tamaño (bytes)", "Modificado"])
        self.table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        v.addWidget(self.table)

        self.btn_browse.clicked.connect(self.pick_dir)
        self.btn_refresh.clicked.connect(self.refresh)
        self.table.itemDoubleClicked.connect(self.open_file)

        self.refresh()

    def pick_dir(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Seleccionar carpeta")
        if d:
            self.folder.setText(d)
            self.refresh()

    def refresh(self):
        p = Path(self.folder.text())
        self.table.setRowCount(0)
        if not p.exists():
            return
        for f in sorted(p.glob("*.hl7")):
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.table.setItem(row, 0, QtWidgets.QTableWidgetItem(f.name))
            self.table.setItem(row, 1, QtWidgets.QTableWidgetItem(str(f.stat().st_size)))
            self.table.setItem(row, 2, QtWidgets.QTableWidgetItem(QtCore.QDateTime.fromSecsSinceEpoch(int(f.stat().st_mtime)).toString(QtCore.Qt.ISODate)))

    def open_file(self, item):
        row = item.row()
        name = self.table.item(row, 0).text()
        p = Path(self.folder.text()) / name
        if p.exists():
            dlg = QtWidgets.QDialog(self)
            dlg.setWindowTitle(name)
            dlg.resize(800, 600)
            layout = QtWidgets.QVBoxLayout(dlg)
            txt = QtWidgets.QPlainTextEdit()
            txt.setReadOnly(True)
            txt.setPlainText(p.read_text(errors="replace"))
            layout.addWidget(txt)
            dlg.exec()

def main():
    app = QtWidgets.QApplication(sys.argv)
    win = Dashboard()
    win.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
