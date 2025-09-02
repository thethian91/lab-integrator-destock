from pathlib import Path

from PySide6 import QtWidgets


class TestsTab(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        v = QtWidgets.QVBoxLayout(self)
        hl = QtWidgets.QHBoxLayout()
        v.addLayout(hl)

        self.file = QtWidgets.QLineEdit()
        self.file.setPlaceholderText("Selecciona un archivo HL7…")
        self.btn_pick = QtWidgets.QPushButton("Seleccionar…")
        self.btn_send = QtWidgets.QPushButton("Enviar")

        hl.addWidget(self.file)
        hl.addWidget(self.btn_pick)
        hl.addWidget(self.btn_send)

        grp = QtWidgets.QGroupBox("Respuesta")
        v.addWidget(grp)
        gvl = QtWidgets.QVBoxLayout(grp)
        self.out = QtWidgets.QPlainTextEdit()
        self.out.setReadOnly(True)
        gvl.addWidget(self.out)

        v.addStretch(1)

        self.btn_pick.clicked.connect(self._pick_file)
        self.btn_send.clicked.connect(self._send)

    def _pick_file(self):
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "Selecciona archivo HL7", "", "HL7 (*.hl7 *.txt);;Todos (*)"
        )
        if path:
            self.file.setText(path)

    def _send(self):
        p = self.file.text().strip()
        if not p:
            QtWidgets.QMessageBox.information(
                self, "Pruebas", "Selecciona primero un archivo HL7."
            )
            return
        if not Path(p).exists():
            QtWidgets.QMessageBox.warning(
                self, "Pruebas", "El archivo seleccionado no existe."
            )
            return
        # TODO: conectar con tu pipeline real
        self.out.setPlainText(
            "200 OK - Mensaje aceptado\nJSON enviado a SOFIA con ID=12345"
        )
