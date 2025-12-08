from __future__ import annotations

import datetime as _dt
from typing import Dict, Any, List

from PySide6 import QtWidgets, QtCore, QtGui

from lab_core.db import get_conn


# ----------------------------------------------------------------------
# Helpers genéricos
# ----------------------------------------------------------------------
def _to_iso_range(dfrom: QtCore.QDate, dto: QtCore.QDate) -> tuple[str, str]:
    """
    Convierte QDate a rangos ISO string (inicio y fin del día).
    Asume que received_at/close_exam_at usan formato ISO con fecha+hora.
    """
    py_from = _dt.datetime(dfrom.year(), dfrom.month(), dfrom.day(), 0, 0, 0)
    py_to = _dt.datetime(dto.year(), dto.month(), dto.day(), 23, 59, 59)
    return (
        py_from.isoformat(timespec="seconds"),
        py_to.isoformat(timespec="seconds"),
    )


def _make_table_model(
    headers: List[str], rows: List[tuple]
) -> QtGui.QStandardItemModel:
    model = QtGui.QStandardItemModel()
    if headers:
        model.setHorizontalHeaderLabels(headers)
    for r in rows:
        items = [QtGui.QStandardItem("" if v is None else str(v)) for v in r]
        model.appendRow(items)
    return model


# ----------------------------------------------------------------------
# Página base
# ----------------------------------------------------------------------
class BaseReportPage(QtWidgets.QWidget):
    def reload_data(self, filters: Dict[str, Any]) -> None:  # override en hijos
        pass


# ----------------------------------------------------------------------
# Reporte 1: Pendientes por enviar
# ----------------------------------------------------------------------
class PendingReportPage(BaseReportPage):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QtWidgets.QVBoxLayout(self)
        self.table = QtWidgets.QTableView()
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        layout.addWidget(self.table)

    def reload_data(self, filters: Dict[str, Any]) -> None:
        d_from_iso: str = filters["from_iso"]
        d_to_iso: str = filters["to_iso"]
        analyzer: str | None = filters["analyzer"]

        sql = """
        SELECT 
            r.id AS result_id,
            r.received_at,
            r.analyzer_name,
            r.patient_id,
            r.patient_name,
            r.exam_code,
            r.exam_title,
            SUM(
              CASE 
                WHEN ob.export_status IS NULL OR ob.export_status = 'PENDING' 
                THEN 1 ELSE 0 
              END
            ) AS pending_obx
        FROM hl7_results r
        JOIN hl7_obx_results ob ON ob.result_id = r.id
        WHERE r.received_at BETWEEN ? AND ?
        """
        params: list[Any] = [d_from_iso, d_to_iso]

        if analyzer and analyzer != "__ALL__":
            sql += " AND r.analyzer_name = ?"
            params.append(analyzer)

        sql += """
        GROUP BY r.id
        HAVING pending_obx > 0
        ORDER BY r.received_at ASC
        """

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
        finally:
            conn.close()

        headers = [
            "result_id",
            "Recibido",
            "Analizador",
            "Paciente ID",
            "Paciente",
            "Código examen",
            "Título examen",
            "OBX pendientes",
        ]
        model = _make_table_model(headers, rows)
        self.table.setModel(model)
        self.table.resizeColumnsToContents()


# ----------------------------------------------------------------------
# Reporte 2: Errores de envío
# ----------------------------------------------------------------------
class ErrorReportPage(BaseReportPage):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QtWidgets.QVBoxLayout(self)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        layout.addWidget(splitter)

        # Tabla superior con errores
        self.table = QtWidgets.QTableView()
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.table.selectionModelChanged = None

        # Panel inferior para ver detalle (export_error / export_response)
        detail_widget = QtWidgets.QWidget()
        detail_layout = QtWidgets.QVBoxLayout(detail_widget)

        self.txt_error = QtWidgets.QTextEdit()
        self.txt_error.setReadOnly(True)
        self.txt_error.setPlaceholderText("Detalle de error / export_error...")

        self.txt_response = QtWidgets.QTextEdit()
        self.txt_response.setReadOnly(True)
        self.txt_response.setPlaceholderText(
            "Respuesta completa del servicio (export_response)..."
        )

        detail_layout.addWidget(QtWidgets.QLabel("Error"))
        detail_layout.addWidget(self.txt_error)
        detail_layout.addWidget(QtWidgets.QLabel("Respuesta servicio"))
        detail_layout.addWidget(self.txt_response)

        splitter.addWidget(self.table)
        splitter.addWidget(detail_widget)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)

        # Conectar selección para cargar detalle
        self.table.selectionModelChanged = self._on_sel_model_changed

    def _on_sel_model_changed(self, old, new):
        # No se usa, pero dejamos el nombre por compatibilidad
        pass

    def setSelectionModel(self, model):
        # Hook opcional si quisieras, pero aquí no hace falta
        pass

    def reload_data(self, filters: Dict[str, Any]) -> None:
        d_from_iso: str = filters["from_iso"]
        d_to_iso: str = filters["to_iso"]
        analyzer: str | None = filters["analyzer"]

        sql = """
        SELECT
            r.id          AS result_id,
            ob.id         AS obx_id,
            r.received_at,
            ob.exported_at,
            r.analyzer_name,
            r.patient_id,
            r.exam_code,
            r.exam_title,
            ob.code       AS obx_code,
            ob.text       AS obx_text,
            ob.value,
            ob.export_status,
            ob.export_error,
            ob.export_response
        FROM hl7_obx_results ob
        JOIN hl7_results r ON ob.result_id = r.id
        WHERE ob.export_status = 'ERROR'
          AND r.received_at BETWEEN ? AND ?
        """
        params: list[Any] = [d_from_iso, d_to_iso]

        if analyzer and analyzer != "__ALL__":
            sql += " AND r.analyzer_name = ?"
            params.append(analyzer)

        sql += " ORDER BY ob.exported_at DESC NULLS LAST"

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
        finally:
            conn.close()

        headers = [
            "result_id",
            "obx_id",
            "Recibido",
            "Enviado",
            "Analizador",
            "Paciente ID",
            "Código examen",
            "Título examen",
            "OBX code",
            "OBX texto",
            "Valor",
            "Estado",
            "Error",
            "Respuesta",
        ]
        model = _make_table_model(headers, rows)
        self.table.setModel(model)
        self.table.resizeColumnsToContents()

        # Re-wire selección para mostrar detalle
        sel_model = self.table.selectionModel()
        sel_model.selectionChanged.connect(self._on_selection_changed)

        # Limpia panel detalle
        self.txt_error.clear()
        self.txt_response.clear()

    def _on_selection_changed(self, selected, _deselected):
        if not selected.indexes():
            self.txt_error.clear()
            self.txt_response.clear()
            return

        index = selected.indexes()[0]
        model = self.table.model()

        # Columnas de error y respuesta según headers de arriba
        col_error = 12
        col_resp = 13

        error_idx = model.index(index.row(), col_error)
        resp_idx = model.index(index.row(), col_resp)

        error_txt = model.data(error_idx) or ""
        resp_txt = model.data(resp_idx) or ""

        self.txt_error.setPlainText(str(error_txt))
        self.txt_response.setPlainText(str(resp_txt))


# ----------------------------------------------------------------------
# Reporte 3: Volumen por analizador
# ----------------------------------------------------------------------
class AnalyzerSummaryPage(BaseReportPage):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QtWidgets.QVBoxLayout(self)
        self.table = QtWidgets.QTableView()
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        layout.addWidget(self.table)

    def reload_data(self, filters: Dict[str, Any]) -> None:
        d_from_iso: str = filters["from_iso"]
        d_to_iso: str = filters["to_iso"]

        sql = """
        SELECT
            r.analyzer_name,
            COUNT(DISTINCT r.id) AS results_count,
            COUNT(ob.id)         AS obx_count,
            SUM(CASE WHEN ob.export_status='SENT' THEN 1 ELSE 0 END) AS ok_obx,
            SUM(CASE WHEN ob.export_status='ERROR' THEN 1 ELSE 0 END) AS error_obx,
            SUM(
                CASE 
                  WHEN ob.export_status IS NULL OR ob.export_status='PENDING'
                  THEN 1 ELSE 0 
                END
            ) AS pending_obx
        FROM hl7_results r
        LEFT JOIN hl7_obx_results ob ON ob.result_id = r.id
        WHERE r.received_at BETWEEN ? AND ?
        GROUP BY r.analyzer_name
        ORDER BY results_count DESC
        """

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql, [d_from_iso, d_to_iso])
            rows = cur.fetchall()
        finally:
            conn.close()

        headers = [
            "Analizador",
            "Resultados",
            "OBX totales",
            "OBX OK",
            "OBX ERROR",
            "OBX pendientes",
        ]
        model = _make_table_model(headers, rows)
        self.table.setModel(model)
        self.table.resizeColumnsToContents()


# ----------------------------------------------------------------------
# Tab principal de Reportes
# ----------------------------------------------------------------------
class ReportsTab(QtWidgets.QWidget):
    """
    Tab de reportes básicos:
      - Pendientes por enviar
      - Errores de envío
      - Resumen por analizador
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        main_layout = QtWidgets.QVBoxLayout(self)

        # --- Filtros arriba ---
        filter_layout = QtWidgets.QHBoxLayout()

        # Rango de fechas
        lbl_from = QtWidgets.QLabel("Desde:")
        lbl_to = QtWidgets.QLabel("Hasta:")

        self.date_from = QtWidgets.QDateEdit()
        self.date_to = QtWidgets.QDateEdit()

        self.date_from.setCalendarPopup(True)
        self.date_to.setCalendarPopup(True)

        today = QtCore.QDate.currentDate()
        # Por defecto: últimos 7 días
        self.date_to.setDate(today)
        self.date_from.setDate(today.addDays(-6))

        # Analizador
        lbl_anal = QtWidgets.QLabel("Analizador:")
        self.cbo_analyzer = QtWidgets.QComboBox()
        self.cbo_analyzer.addItem("Todos", "__ALL__")  # se llena luego

        # Botón actualizar
        self.btn_refresh = QtWidgets.QPushButton("Actualizar")

        filter_layout.addWidget(lbl_from)
        filter_layout.addWidget(self.date_from)
        filter_layout.addWidget(lbl_to)
        filter_layout.addWidget(self.date_to)
        filter_layout.addSpacing(20)
        filter_layout.addWidget(lbl_anal)
        filter_layout.addWidget(self.cbo_analyzer)
        filter_layout.addStretch()
        filter_layout.addWidget(self.btn_refresh)

        main_layout.addLayout(filter_layout)

        # --- Sub-tabs de reportes ---
        self.subtabs = QtWidgets.QTabWidget()
        main_layout.addWidget(self.subtabs, 1)

        self.pending_page = PendingReportPage()
        self.error_page = ErrorReportPage()
        self.summary_page = AnalyzerSummaryPage()

        self.subtabs.addTab(self.pending_page, "Pendientes")
        self.subtabs.addTab(self.error_page, "Errores")
        self.subtabs.addTab(self.summary_page, "Por analizador")

        # Conexiones
        self.btn_refresh.clicked.connect(self.reload_reports)

        # Inicialización
        self._load_analyzers()
        self.reload_reports()

    # ------------------------------------------------------------------
    # Ayudas internas
    # ------------------------------------------------------------------
    def _load_analyzers(self) -> None:
        """Carga lista de analizadores distintos de hl7_results en el combo."""
        # Limpia, deja siempre "Todos"
        self.cbo_analyzer.clear()
        self.cbo_analyzer.addItem("Todos", "__ALL__")

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT analyzer_name
                  FROM hl7_results
                 WHERE analyzer_name IS NOT NULL AND analyzer_name <> ''
                 ORDER BY analyzer_name
                """
            )
            rows = cur.fetchall()
        finally:
            conn.close()

        for (name,) in rows:
            self.cbo_analyzer.addItem(name, name)

    def _gather_filters(self) -> Dict[str, Any]:
        """Construye el diccionario de filtros para pasar a las páginas."""
        d_from = self.date_from.date()
        d_to = self.date_to.date()
        from_iso, to_iso = _to_iso_range(d_from, d_to)

        analyzer_data = self.cbo_analyzer.currentData()
        return {
            "from_iso": from_iso,
            "to_iso": to_iso,
            "analyzer": analyzer_data,
        }

    def reload_reports(self) -> None:
        filters = self._gather_filters()
        self.pending_page.reload_data(filters)
        self.error_page.reload_data(filters)
        self.summary_page.reload_data(filters)
