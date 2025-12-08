# apps/monitor/tabs/orders_tab.py
import sqlite3
from pathlib import Path

from PySide6 import QtCore, QtGui, QtWidgets
from PySide6.QtCore import QDate

DB_PATH = "data/labintegrador.db"

COLUMNS = [
    ("id", "Examen ID"),
    ("documento", "Documento"),
    ("nombre", "Paciente"),
    ("protocolo_codigo", "Código"),
    ("protocolo_titulo", "Examen"),
    ("tubo", "Tubo"),
    ("tubo_muestra", "Tubo Muestra"),
    ("fecha", "Fecha"),
    ("hora", "Hora"),
    ("status", "Estado"),
    ("resulted_at", "Resultó"),
    ("sent_at", "Enviado"),
]


class OrdersTab(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        v = QtWidgets.QVBoxLayout(self)

        # ---------- Filtros ----------
        filt = QtWidgets.QHBoxLayout()
        v.addLayout(filt)

        filt.addWidget(QtWidgets.QLabel("Documento:"))
        self.doc = QtWidgets.QLineEdit()
        self.doc.setPlaceholderText("CC/NIT")
        self.doc.setFixedWidth(180)
        filt.addWidget(self.doc)

        filt.addWidget(QtWidgets.QLabel("Desde:"))
        self.since = QtWidgets.QDateEdit()
        self.since.setCalendarPopup(True)
        self.since.setDisplayFormat("yyyy-MM-dd")
        self.since.setDate(QDate.currentDate().addMonths(-1))
        filt.addWidget(self.since)

        filt.addWidget(QtWidgets.QLabel("Hasta:"))
        self.until = QtWidgets.QDateEdit()
        self.until.setCalendarPopup(True)
        self.until.setDisplayFormat("yyyy-MM-dd")
        self.until.setDate(QDate.currentDate())
        filt.addWidget(self.until)

        filt.addWidget(QtWidgets.QLabel("Estado:"))
        self.status = QtWidgets.QComboBox()
        self.status.addItems(["(Todos)", "PENDING", "RESULTED", "SENT"])
        self.status.setFixedWidth(120)
        filt.addWidget(self.status)

        self.btn_refresh = QtWidgets.QPushButton("Actualizar")
        filt.addWidget(self.btn_refresh)

        filt.addStretch(1)

        # ---------- Tabla ----------
        self.table = QtWidgets.QTableView()
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.table.doubleClicked.connect(self._open_detail)
        v.addWidget(self.table)

        # ---------- Barra inferior ----------
        bottom = QtWidgets.QHBoxLayout()
        v.addLayout(bottom)
        self.lbl_count = QtWidgets.QLabel("0 registros")
        bottom.addWidget(self.lbl_count)
        bottom.addStretch(1)

        # señales
        self.btn_refresh.clicked.connect(self.refresh)
        self.doc.returnPressed.connect(self.refresh)
        self.status.currentIndexChanged.connect(self.refresh)
        self.since.dateChanged.connect(self.refresh)
        self.until.dateChanged.connect(self.refresh)

        # primer llenado
        self.refresh()

    # ------------------------------- UI actions -------------------------------

    def refresh(self):
        rows = self._query_orders(
            documento=self.doc.text().strip() or None,
            date_from=self._qdate_to_str(self.since.date()),
            date_to=self._qdate_to_str(self.until.date()),
            status=(
                None
                if self.status.currentText() == "(Todos)"
                else self.status.currentText()
            ),
            limit=1000,
        )
        self._fill_table(rows)
        self.lbl_count.setText(f"{len(rows)} registros")

    def _fill_table(self, rows):
        model = QtGui.QStandardItemModel(len(rows), len(COLUMNS))
        # headers
        for j, (_, title) in enumerate(COLUMNS):
            model.setHeaderData(j, QtCore.Qt.Horizontal, title)

        # data
        for i, r in enumerate(rows):
            for j, (key, _) in enumerate(COLUMNS):
                val = r.get(key, "")
                item = QtGui.QStandardItem("" if val is None else str(val))
                if key in ("id", "documento", "status"):
                    item.setFont(QtGui.QFont("", weight=QtGui.QFont.Bold))
                model.setItem(i, j, item)

        self.table.setModel(model)
        self.table.resizeColumnsToContents()
        self.table.horizontalHeader().setStretchLastSection(True)

    def _open_detail(self, index: QtCore.QModelIndex):
        if not index.isValid():
            return
        row = index.row()
        model = self.table.model()
        # reconstruir dict desde la fila
        r = {key: model.index(row, j).data() for j, (key, _) in enumerate(COLUMNS)}
        dlg = self._make_detail_dialog(r)
        dlg.exec()

    def _make_detail_dialog(self, row: dict) -> QtWidgets.QDialog:
        d = QtWidgets.QDialog(self)
        d.setWindowTitle(f"Detalle examen #{row.get('id')}")
        layout = QtWidgets.QVBoxLayout(d)

        # resumen
        form = QtWidgets.QFormLayout()
        layout.addLayout(form)
        form.addRow("Documento:", QtWidgets.QLabel(row.get("documento", "")))
        form.addRow("Paciente:", QtWidgets.QLabel(row.get("nombre", "")))
        form.addRow("Código:", QtWidgets.QLabel(row.get("protocolo_codigo", "")))
        form.addRow("Examen:", QtWidgets.QLabel(row.get("protocolo_titulo", "")))
        form.addRow("Tubo:", QtWidgets.QLabel(row.get("tubo", "")))
        form.addRow("Tubo muestra:", QtWidgets.QLabel(row.get("tubo_muestra", "")))
        form.addRow(
            "Fecha/Hora:",
            QtWidgets.QLabel(f"{row.get('fecha','')} {row.get('hora','')}"),
        )
        form.addRow("Estado:", QtWidgets.QLabel(row.get("status", "")))
        form.addRow("Resultó:", QtWidgets.QLabel(row.get("resulted_at", "")))
        form.addRow("Enviado:", QtWidgets.QLabel(row.get("sent_at", "")))

        # botones
        btns = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Close)
        btns.rejected.connect(d.reject)
        btns.accepted.connect(d.accept)
        layout.addWidget(btns)
        return d

    # ------------------------------- Data access ------------------------------

    def _qdate_to_str(self, qd: QDate) -> str:
        return f"{qd.year():04d}-{qd.month():02d}-{qd.day():02d}"

    def _query_orders(
        self,
        documento: str | None,
        date_from: str,
        date_to: str,
        status: str | None,
        limit: int = 1000,
    ):
        """Devuelve filas (JOIN patients + exams) filtradas."""
        path = Path(DB_PATH)
        if not path.exists():
            return []

        conn = sqlite3.connect(str(path))
        # rows como dict
        conn.row_factory = lambda c, r: {
            d[0]: r[i] for i, d in enumerate(c.description)
        }
        cur = conn.cursor()

        wh = []
        params = []

        # rango de fecha sobre exams.fecha (que viene 'YYYY-MM-DD')
        if date_from:
            wh.append("e.fecha >= ?")
            params.append(date_from)
        if date_to:
            wh.append("e.fecha <= ?")
            params.append(date_to)

        if documento:
            wh.append("p.documento = ?")
            params.append(documento)

        if status:
            wh.append("e.status = ?")
            params.append(status)

        where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""
        sql = f"""
        SELECT
          e.id, p.documento, p.nombre, e.protocolo_codigo, e.protocolo_titulo,
          e.tubo, e.tubo_muestra, e.fecha, e.hora, e.status, e.resulted_at, e.sent_at
        FROM exams e
        JOIN patients p ON p.documento = e.paciente_doc
        {where_sql}
        ORDER BY e.fecha DESC, e.hora DESC, e.id DESC
        LIMIT ?
        """
        params.append(limit)

        cur.execute(sql, params)
        rows = cur.fetchall()
        conn.close()
        return rows
