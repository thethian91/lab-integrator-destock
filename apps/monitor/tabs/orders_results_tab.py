from __future__ import annotations

import sqlite3

from PySide6 import QtCore, QtGui, QtWidgets
from PySide6.QtCore import QDate, Qt

from lab_core.db import get_conn  # reutilizamos tu helper

DB_PATH = "data/labintegrador.db"

# Columnas visibles (coinciden con hl7_results)
VISIBLE_COLUMNS = [
    ("id", "ID"),
    ("analyzer_name", "Analizador"),
    ("patient_id", "Documento"),
    ("patient_name", "Paciente"),
    ("exam_code", "Código Examen"),
    ("exam_title", "Nombre Examen"),
    ("fecha_ref", "Fecha"),  # calculada
]


class OrdersResultsTab(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)

        # ------------------ Filtros ------------------
        filt_box = QtWidgets.QGroupBox("Filtros")
        fgrid = QtWidgets.QGridLayout(filt_box)
        self.cmb_analyzer = QtWidgets.QComboBox()
        self.txt_doc = QtWidgets.QLineEdit()
        self.txt_name = QtWidgets.QLineEdit()
        self.dt_from = QtWidgets.QDateEdit()
        self.dt_to = QtWidgets.QDateEdit()

        self.cmb_analyzer.setMinimumWidth(160)
        self.txt_doc.setPlaceholderText("Documento")
        self.txt_name.setPlaceholderText("Nombre (contiene)")

        self.dt_from.setCalendarPopup(True)
        self.dt_from.setDisplayFormat("yyyy-MM-dd")
        self.dt_from.setDate(QDate.currentDate().addMonths(-1))

        self.dt_to.setCalendarPopup(True)
        self.dt_to.setDisplayFormat("yyyy-MM-dd")
        self.dt_to.setDate(QDate.currentDate())

        self.btn_refresh = QtWidgets.QPushButton("Actualizar")
        self.btn_clear = QtWidgets.QPushButton("Limpiar")

        fgrid.addWidget(QtWidgets.QLabel("Analizador:"), 0, 0)
        fgrid.addWidget(self.cmb_analyzer, 0, 1)
        fgrid.addWidget(QtWidgets.QLabel("Documento:"), 0, 2)
        fgrid.addWidget(self.txt_doc, 0, 3)
        fgrid.addWidget(QtWidgets.QLabel("Nombre:"), 0, 4)
        fgrid.addWidget(self.txt_name, 0, 5)

        fgrid.addWidget(QtWidgets.QLabel("Desde:"), 1, 0)
        fgrid.addWidget(self.dt_from, 1, 1)
        fgrid.addWidget(QtWidgets.QLabel("Hasta:"), 1, 2)
        fgrid.addWidget(self.dt_to, 1, 3)
        fgrid.addWidget(self.btn_refresh, 1, 5)
        fgrid.addWidget(self.btn_clear, 1, 6)

        layout.addWidget(filt_box)

        # ------------------ Tabla ------------------
        self.table = QtWidgets.QTableView()
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.table.verticalHeader().setVisible(False)
        self.table.setSortingEnabled(True)
        layout.addWidget(self.table)

        # Doble clic -> detalle OBX
        self.table.doubleClicked.connect(self._open_detail_for_row)

        # Pie
        self.lbl_count = QtWidgets.QLabel("0 resultados")
        layout.addWidget(self.lbl_count)

        # Señales
        self.btn_refresh.clicked.connect(self.refresh)
        self.btn_clear.clicked.connect(self._clear_filters)

        # Inicial
        self._fill_analyzers()
        self.refresh()

    # ------------------ Helpers UI ------------------

    def _clear_filters(self):
        self.cmb_analyzer.setCurrentIndex(0)
        self.txt_doc.clear()
        self.txt_name.clear()
        self.dt_from.setDate(QDate.currentDate().addMonths(-1))
        self.dt_to.setDate(QDate.currentDate())
        self.refresh()

    def _qdate_to_str(self, qd: QDate) -> str:
        return f"{qd.year():04d}-{qd.month():02d}-{qd.day():02d}"

    # ------------------ Datos ------------------

    def _fill_analyzers(self):
        self.cmb_analyzer.blockSignals(True)
        self.cmb_analyzer.clear()
        self.cmb_analyzer.addItem("(Todos)", "")
        conn = get_conn(DB_PATH)
        cur = conn.cursor()
        # analyzers desde hl7_results
        cur.execute(
            """
            SELECT DISTINCT analyzer_name
            FROM hl7_results
            WHERE IFNULL(analyzer_name,'') <> ''
            ORDER BY analyzer_name
        """
        )
        for r in cur.fetchall():
            self.cmb_analyzer.addItem(r[0], r[0])
        conn.close()
        self.cmb_analyzer.blockSignals(False)

    def refresh(self):
        rows = self._query_results()
        self._fill_table(rows)
        self.lbl_count.setText(f"{len(rows)} resultados")

    def _query_results(self) -> list[dict]:
        analyzer = self.cmb_analyzer.currentData()
        doc = self.txt_doc.text().strip()
        name = self.txt_name.text().strip()
        date_from = self._qdate_to_str(self.dt_from.date())
        date_to = self._qdate_to_str(self.dt_to.date())

        conn = get_conn(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # fecha_ref: si exam_date viene vacío, tomamos fecha de received_at
        sql = """
            SELECT
                r.id,
                r.analyzer_name,
                r.patient_id,
                r.patient_name,
                r.exam_code,
                r.exam_title,
                COALESCE(NULLIF(r.exam_date,''), substr(r.received_at,1,10)) AS fecha_ref
            FROM hl7_results r
            WHERE 1=1
        """
        params: list = []

        if analyzer:
            sql += " AND r.analyzer_name = ?"
            params.append(analyzer)
        if doc:
            sql += " AND r.patient_id = ?"
            params.append(doc)
        if name:
            sql += " AND UPPER(r.patient_name) LIKE UPPER(?)"
            params.append(f"%{name}%")
        if date_from:
            sql += (
                " AND COALESCE(NULLIF(r.exam_date,''), substr(r.received_at,1,10)) >= ?"
            )
            params.append(date_from)
        if date_to:
            sql += (
                " AND COALESCE(NULLIF(r.exam_date,''), substr(r.received_at,1,10)) <= ?"
            )
            params.append(date_to)

        sql += " ORDER BY fecha_ref DESC, r.id DESC LIMIT 1000"

        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def _fill_table(self, rows: list[dict]):
        model = QtGui.QStandardItemModel(len(rows), len(VISIBLE_COLUMNS))
        for j, (_, title) in enumerate(VISIBLE_COLUMNS):
            model.setHeaderData(j, Qt.Horizontal, title)

        for i, r in enumerate(rows):
            for j, (key, _) in enumerate(VISIBLE_COLUMNS):
                val = r.get(key, "")
                item = QtGui.QStandardItem("" if val is None else str(val))
                # alineación para ID/Fecha/Códigos
                if key in ("id", "fecha_ref", "exam_code"):
                    item.setTextAlignment(Qt.AlignCenter)
                model.setItem(i, j, item)

        self.table.setModel(model)
        self.table.resizeColumnsToContents()
        self._rows = rows  # cache para doble clic

    # ------------------ Detalle OBX ------------------

    def _open_detail_for_row(self, index: QtCore.QModelIndex):
        if not index.isValid():
            return
        row = index.row()
        data = self._rows[row]
        result_id = data.get("id")
        if not result_id:
            return

        obx_rows = self._load_obx(result_id)
        self._show_obx_dialog(data, obx_rows)

    def _load_obx(self, result_id: int) -> list[dict]:
        conn = get_conn(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                code,
                text,
                value,
                units,
                ref_range,
                flags,
                obs_dt
            FROM hl7_obx_results
            WHERE result_id = ?
            ORDER BY id
        """,
            (result_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def _show_obx_dialog(self, header: dict, obx_rows: list[dict]):
        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle(
            f"Detalle resultado #{header.get('id')} — {header.get('patient_name')}"
        )
        dlg.resize(820, 480)
        v = QtWidgets.QVBoxLayout(dlg)

        # Encabezado corto
        info = QtWidgets.QLabel(
            f"<b>Paciente:</b> {header.get('patient_name','')} "
            f"(<code>{header.get('patient_id','')}</code>) &nbsp;&nbsp; "
            f"<b>Examen:</b> {header.get('exam_code','')} — {header.get('exam_title','')} &nbsp;&nbsp; "
            f"<b>Fecha:</b> {header.get('fecha_ref','')}"
        )
        info.setTextFormat(Qt.RichText)
        v.addWidget(info)

        # Tabla OBX
        tbl = QtWidgets.QTableWidget(0, 7)
        tbl.setHorizontalHeaderLabels(
            [
                "Código",
                "Texto",
                "Valor",
                "Unidades",
                "Rango Ref.",
                "Flags",
                "Fecha/Hora",
            ]
        )
        tbl.verticalHeader().setVisible(False)
        tbl.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        tbl.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        tbl.setAlternatingRowColors(True)
        tbl.horizontalHeader().setStretchLastSection(True)

        tbl.setRowCount(len(obx_rows))
        for i, r in enumerate(obx_rows):
            tbl.setItem(i, 0, QtWidgets.QTableWidgetItem(str(r.get("code") or "")))
            tbl.setItem(i, 1, QtWidgets.QTableWidgetItem(str(r.get("text") or "")))
            tbl.setItem(i, 2, QtWidgets.QTableWidgetItem(str(r.get("value") or "")))
            tbl.setItem(i, 3, QtWidgets.QTableWidgetItem(str(r.get("units") or "")))
            tbl.setItem(i, 4, QtWidgets.QTableWidgetItem(str(r.get("ref_range") or "")))
            tbl.setItem(i, 5, QtWidgets.QTableWidgetItem(str(r.get("flags") or "")))
            tbl.setItem(i, 6, QtWidgets.QTableWidgetItem(str(r.get("obs_dt") or "")))

        tbl.resizeColumnsToContents()
        v.addWidget(tbl)

        btns = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Close)
        btns.rejected.connect(dlg.reject)
        v.addWidget(btns)

        dlg.exec()
