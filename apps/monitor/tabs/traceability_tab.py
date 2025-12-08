from __future__ import annotations

import datetime as _dt
from typing import Any, Dict, List, Optional

from PySide6 import QtWidgets, QtCore, QtGui

from lab_core.db import get_conn


def _to_iso_range(dfrom: QtCore.QDate, dto: QtCore.QDate) -> tuple[str, str]:
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


class TraceabilityTab(QtWidgets.QWidget):
    """
    Tab de trazabilidad completa:
      - Lista de resultados (hl7_results)
      - OBX asociados (hl7_obx_results)
      - Paneles de texto:
          * HL7 RAW (por resultado)
          * Petición / respuesta de envío de item (por OBX)
          * Petición / respuesta de cierre de examen (por resultado)
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self._col_hl7_raw: Optional[str] = None
        self._col_close_req: Optional[str] = None
        self._col_close_resp: Optional[str] = None
        self._col_export_req: Optional[str] = None
        self._col_export_resp: Optional[str] = None

        self._detect_columns()

        main_layout = QtWidgets.QVBoxLayout(self)

        # --- Filtros arriba ---
        filter_layout = QtWidgets.QHBoxLayout()

        lbl_from = QtWidgets.QLabel("Desde:")
        lbl_to = QtWidgets.QLabel("Hasta:")

        self.date_from = QtWidgets.QDateEdit()
        self.date_to = QtWidgets.QDateEdit()
        self.date_from.setCalendarPopup(True)
        self.date_to.setCalendarPopup(True)

        today = QtCore.QDate.currentDate()
        self.date_to.setDate(today)
        self.date_from.setDate(today.addDays(-6))

        lbl_anal = QtWidgets.QLabel("Analizador:")
        self.cbo_analyzer = QtWidgets.QComboBox()

        lbl_search = QtWidgets.QLabel("Buscar (paciente / doc):")
        self.txt_search = QtWidgets.QLineEdit()

        self.btn_refresh = QtWidgets.QPushButton("Actualizar")

        filter_layout.addWidget(lbl_from)
        filter_layout.addWidget(self.date_from)
        filter_layout.addWidget(lbl_to)
        filter_layout.addWidget(self.date_to)
        filter_layout.addSpacing(10)
        filter_layout.addWidget(lbl_anal)
        filter_layout.addWidget(self.cbo_analyzer)
        filter_layout.addSpacing(10)
        filter_layout.addWidget(lbl_search)
        filter_layout.addWidget(self.txt_search, 1)
        filter_layout.addStretch()
        filter_layout.addWidget(self.btn_refresh)

        main_layout.addLayout(filter_layout)

        # --- Splitter principal: izquierda resultados, derecha detalle ---
        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        main_layout.addWidget(splitter, 1)

        # -------- Panel izquierdo: resultados --------
        left_panel = QtWidgets.QWidget()
        left_layout = QtWidgets.QVBoxLayout(left_panel)

        self.table_results = QtWidgets.QTableView()
        self.table_results.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table_results.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        left_layout.addWidget(QtWidgets.QLabel("Resultados (hl7_results)"))
        left_layout.addWidget(self.table_results, 1)

        splitter.addWidget(left_panel)

        # -------- Panel derecho: OBX + textos --------
        right_panel = QtWidgets.QWidget()
        right_layout = QtWidgets.QVBoxLayout(right_panel)

        # Splitter vertical: OBX arriba, textos abajo
        v_splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        right_layout.addWidget(v_splitter)

        # Tabla de OBX
        obx_panel = QtWidgets.QWidget()
        obx_layout = QtWidgets.QVBoxLayout(obx_panel)

        self.table_obx = QtWidgets.QTableView()
        self.table_obx.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table_obx.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        obx_layout.addWidget(QtWidgets.QLabel("Detalle OBX (hl7_obx_results)"))
        obx_layout.addWidget(self.table_obx, 1)

        v_splitter.addWidget(obx_panel)

        # Tabs de textos
        text_tabs = QtWidgets.QTabWidget()

        # HL7 RAW
        self.txt_hl7 = QtWidgets.QPlainTextEdit()
        self.txt_hl7.setReadOnly(True)
        self.txt_hl7.setPlaceholderText("HL7 RAW del resultado seleccionado...")

        hl7_widget = QtWidgets.QWidget()
        hl7_layout = QtWidgets.QVBoxLayout(hl7_widget)
        hl7_layout.addWidget(self.txt_hl7)
        text_tabs.addTab(hl7_widget, "HL7 RAW")

        # Envío de item (OBX)
        self.txt_obx_request = QtWidgets.QPlainTextEdit()
        self.txt_obx_request.setReadOnly(True)
        self.txt_obx_request.setPlaceholderText("XML/Request de envío de este OBX...")

        self.txt_obx_response = QtWidgets.QPlainTextEdit()
        self.txt_obx_response.setReadOnly(True)
        self.txt_obx_response.setPlaceholderText(
            "Respuesta del servicio para este OBX..."
        )

        obx_text_widget = QtWidgets.QWidget()
        obx_text_layout = QtWidgets.QVBoxLayout(obx_text_widget)
        obx_text_layout.addWidget(QtWidgets.QLabel("Petición (export_request)"))
        obx_text_layout.addWidget(self.txt_obx_request, 1)
        obx_text_layout.addWidget(QtWidgets.QLabel("Respuesta (export_response)"))
        obx_text_layout.addWidget(self.txt_obx_response, 1)

        text_tabs.addTab(obx_text_widget, "Envio item (OBX)")

        # Cierre de examen
        self.txt_close_request = QtWidgets.QPlainTextEdit()
        self.txt_close_request.setReadOnly(True)
        self.txt_close_request.setPlaceholderText("XML/Request de cierre de examen...")

        self.txt_close_response = QtWidgets.QPlainTextEdit()
        self.txt_close_response.setReadOnly(True)
        self.txt_close_response.setPlaceholderText(
            "Respuesta del servicio al cierre de examen..."
        )

        close_widget = QtWidgets.QWidget()
        close_layout = QtWidgets.QVBoxLayout(close_widget)
        close_layout.addWidget(
            QtWidgets.QLabel("Petición de cierre (close_exam_request)")
        )
        close_layout.addWidget(self.txt_close_request, 1)
        close_layout.addWidget(
            QtWidgets.QLabel("Respuesta de cierre (close_exam_response)")
        )
        close_layout.addWidget(self.txt_close_response, 1)

        text_tabs.addTab(close_widget, "Cierre de examen")

        v_splitter.addWidget(text_tabs)
        v_splitter.setStretchFactor(0, 3)
        v_splitter.setStretchFactor(1, 2)

        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 4)

        # --- Conexiones ---
        self.btn_refresh.clicked.connect(self.reload_all)
        self.table_results.selectionModelChanged = None  # placeholder
        self.table_obx.selectionModelChanged = None

        # Cargar filtros iniciales
        self._load_analyzers()
        self.reload_all()

    # ------------------------------------------------------------------
    # Detección de columnas existentes (para ser robustos)
    # ------------------------------------------------------------------
    def _detect_columns(self) -> None:
        conn = get_conn()
        try:
            # hl7_results
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(hl7_results)")
            cols_res = {row[1] for row in cur.fetchall()}  # row[1] = name

            # hl7_obx_results
            cur.execute("PRAGMA table_info(hl7_obx_results)")
            cols_obx = {row[1] for row in cur.fetchall()}
        finally:
            conn.close()

        # Intentamos encontrar columna de HL7 RAW por nombre típico
        for cand in ("raw_message", "raw_hl7", "hl7_raw", "raw_data"):
            if cand in cols_res:
                self._col_hl7_raw = cand
                break

        # Columnas de cierre (si existen)
        if "close_exam_request" in cols_res:
            self._col_close_req = "close_exam_request"
        if "close_exam_response" in cols_res:
            self._col_close_resp = "close_exam_response"

        # Columnas de request/response de OBX (si existen)
        if "export_request" in cols_obx:
            self._col_export_req = "export_request"
        if "export_response" in cols_obx:
            self._col_export_resp = "export_response"

    # ------------------------------------------------------------------
    # Filtros
    # ------------------------------------------------------------------
    def _load_analyzers(self) -> None:
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
        d_from = self.date_from.date()
        d_to = self.date_to.date()
        from_iso, to_iso = _to_iso_range(d_from, d_to)
        search = self.txt_search.text().strip()
        analyzer = self.cbo_analyzer.currentData()

        return {
            "from_iso": from_iso,
            "to_iso": to_iso,
            "search": search,
            "analyzer": analyzer,
        }

    # ------------------------------------------------------------------
    # Carga de datos
    # ------------------------------------------------------------------
    def reload_all(self) -> None:
        filters = self._gather_filters()
        self._reload_results(filters)
        # limpia detalle
        self._load_obx_for_result(None)
        self._load_texts_for_result(None)

    def _reload_results(self, filters: Dict[str, Any]) -> None:
        d_from_iso: str = filters["from_iso"]
        d_to_iso: str = filters["to_iso"]
        analyzer: Optional[str] = filters["analyzer"]
        search: str = filters["search"]

        sql = """
        SELECT
            id,
            received_at,
            analyzer_name,
            patient_id,
            patient_name,
            exam_code,
            exam_title,
            close_exam_status,
            close_exam_at
        FROM hl7_results
        WHERE received_at BETWEEN ? AND ?
        """
        params: List[Any] = [d_from_iso, d_to_iso]

        if analyzer and analyzer != "__ALL__":
            sql += " AND analyzer_name = ?"
            params.append(analyzer)

        if search:
            sql += " AND (patient_id LIKE ? OR patient_name LIKE ?)"
            like = f"%{search}%"
            params.extend([like, like])

        sql += " ORDER BY received_at DESC"

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
        finally:
            conn.close()

        headers = [
            "id",
            "Recibido",
            "Analizador",
            "Paciente ID",
            "Paciente",
            "Código examen",
            "Título examen",
            "Estado cierre",
            "Cierre en",
        ]
        model = _make_table_model(headers, rows)
        self.table_results.setModel(model)
        self.table_results.resizeColumnsToContents()

        sel_model = self.table_results.selectionModel()
        sel_model.selectionChanged.connect(self._on_result_selection_changed)

    def _on_result_selection_changed(self, selected, _deselected) -> None:
        if not selected.indexes():
            self._load_obx_for_result(None)
            self._load_texts_for_result(None)
            return

        index = selected.indexes()[0]
        model = self.table_results.model()
        result_id_idx = model.index(index.row(), 0)
        result_id = model.data(result_id_idx)
        if result_id is None:
            self._load_obx_for_result(None)
            self._load_texts_for_result(None)
            return

        rid = int(result_id)
        self._load_obx_for_result(rid)
        self._load_texts_for_result(rid)

    def _load_obx_for_result(self, result_id: Optional[int]) -> None:
        if result_id is None:
            empty_model = _make_table_model([], [])
            self.table_obx.setModel(empty_model)
            self.table_obx.resizeColumnsToContents()
            self.txt_obx_request.clear()
            self.txt_obx_response.clear()
            return

        # Construimos lista de columnas base y opcionales
        cols = [
            "id",
            "code",
            "text",
            "value",
            "units",
            "ref_range",
            "flags",
            "export_status",
            "export_error",
        ]
        if self._col_export_req:
            cols.append(self._col_export_req)
        if self._col_export_resp:
            cols.append(self._col_export_resp)

        sql = f"""
        SELECT {', '.join(cols)}
          FROM hl7_obx_results
         WHERE result_id = ?
         ORDER BY id ASC
        """

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql, [result_id])
            rows = cur.fetchall()
        finally:
            conn.close()

        headers = [
            "obx_id",
            "Código",
            "Texto",
            "Valor",
            "Unidades",
            "Rango ref",
            "Flags",
            "Estado",
            "Error",
        ]
        if self._col_export_req:
            headers.append("Request")
        if self._col_export_resp:
            headers.append("Response")

        model = _make_table_model(headers, rows)
        self.table_obx.setModel(model)
        self.table_obx.resizeColumnsToContents()

        sel_model = self.table_obx.selectionModel()
        sel_model.selectionChanged.connect(self._on_obx_selection_changed)

        # limpiar panel OBX request/response
        self.txt_obx_request.clear()
        self.txt_obx_response.clear()

    def _on_obx_selection_changed(self, selected, _deselected) -> None:
        if not selected.indexes():
            self.txt_obx_request.clear()
            self.txt_obx_response.clear()
            return

        index = selected.indexes()[0]
        model = self.table_obx.model()

        # Columnas base: 0..8
        col_req = None
        col_resp = None
        base_cols = 9

        if self._col_export_req:
            col_req = base_cols
            base_cols += 1
        if self._col_export_resp:
            col_resp = base_cols

        if col_req is not None:
            req_idx = model.index(index.row(), col_req)
            self.txt_obx_request.setPlainText(str(model.data(req_idx) or ""))
        else:
            self.txt_obx_request.setPlainText(
                "No hay columna export_request en hl7_obx_results."
            )

        if col_resp is not None:
            resp_idx = model.index(index.row(), col_resp)
            self.txt_obx_response.setPlainText(str(model.data(resp_idx) or ""))
        else:
            self.txt_obx_response.setPlainText(
                "No hay columna export_response en hl7_obx_results."
            )

    def _load_texts_for_result(self, result_id: Optional[int]) -> None:
        # HL7 RAW + cierre examen para un resultado
        self.txt_hl7.clear()
        self.txt_close_request.clear()
        self.txt_close_response.clear()

        if result_id is None:
            return

        cols: List[str] = ["id"]
        if self._col_hl7_raw:
            cols.append(self._col_hl7_raw)
        if self._col_close_req:
            cols.append(self._col_close_req)
        if self._col_close_resp:
            cols.append(self._col_close_resp)

        if len(cols) == 1:
            # sólo id => no tenemos nada que mostrar
            self.txt_hl7.setPlainText(
                "No se encontró columna de HL7 RAW en hl7_results."
            )
            return

        sql = f"SELECT {', '.join(cols)} FROM hl7_results WHERE id = ?"

        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql, [result_id])
            row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            return

        # row[0] = id
        idx = 1
        if self._col_hl7_raw:
            hl7_raw = row[idx]
            idx += 1
            self.txt_hl7.setPlainText(str(hl7_raw or ""))

        if self._col_close_req:
            close_req = row[idx]
            idx += 1
            self.txt_close_request.setPlainText(str(close_req or ""))

        if self._col_close_resp:
            close_resp = row[idx]
            self.txt_close_response.setPlainText(str(close_resp or ""))
