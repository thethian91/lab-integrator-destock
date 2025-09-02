from PySide6 import QtWidgets

class LogsTab(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        v = QtWidgets.QVBoxLayout(self)
        self.system_log = QtWidgets.QPlainTextEdit()
        self.system_log.setReadOnly(True)
        v.addWidget(self.system_log)

    # Puedes exponer un método para que otras partes escriban aquí
    def append_log(self, text: str):
        self.system_log.appendPlainText(text)
