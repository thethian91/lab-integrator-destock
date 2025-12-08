from __future__ import annotations

import sqlite3
from datetime import datetime

from PySide6 import QtCore, QtGui, QtWidgets
from PySide6.QtCore import QDate, Qt, QObject, Signal, QThread

from lab_core.config import load_settings
from lab_core.db import (
    get_conn,
    mark_obx_error,
    mark_obx_exported,
    DEFAULT_DB_PATH as DB_PATH,
)
from lab_core.file_tracer import FileTraceWriter
from lab_core.result_flow import (
    ResultSender,
    DefaultMappingRepo,
    DefaultExamRepo,
    DefaultXmlBuilder,
    DefaultApiClient,
)
import logging

logger = logging.getLogger("lab_integrador.gui")

cfg = load_settings()

trace_writer = FileTraceWriter(
    enabled=bool(getattr(cfg.result_export, "save_files", False)),
    base_dir=str(getattr(cfg.result_export, "save_dir", "outbox_xml")),
)

# --- ApiClient usando las mismas claves que el flujo ---
api_client = DefaultApiClient(
    base_url=cfg.api.base_url,
    api_key=cfg.api.key,
    api_secret=cfg.api.secret,
    timeout=getattr(cfg.api, "timeout", 30) or 30,
    default_resultado_global=(getattr(cfg.api, "resultado_global", None) or "Normal"),
    default_responsable=getattr(cfg.api, "responsable", 'PENDIENTEVALIDAR'),
    default_notas=getattr(cfg.api, "notas", 'Enviado desde integracion'),
)

# --- Sender apuntando al mapping del cliente ---
sender = ResultSender(
    mapping_repo=DefaultMappingRepo(mapping_path="configs/mapping.json"),
    exam_repo=DefaultExamRepo(db_path=DB_PATH),
    xml_builder=DefaultXmlBuilder(),  # normaliza ASCII en unidades
    api_client=api_client,
    logger=logger,
    trace_writer=trace_writer,  # Activar el guardado del XML si esta activo
)


def get_paciente_doc_by_exam_id(id_examen: int, db_path: str = DB_PATH) -> str:
    """
    Devuelve el paciente_doc de la tabla exams para un id_examen dado.
    Si no existe o está vacío, devuelve "".
    """
    conn = get_conn(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT paciente_doc FROM exams WHERE id = ?",
            (id_examen,),
        )
        row = cur.fetchone()
        if row and row["paciente_doc"]:
            return str(row["paciente_doc"])
        return ""
    finally:
        try:
            conn.close()
        except Exception:
            pass


# Columnas visibles (coinciden con hl7_results)
VISIBLE_COLUMNS = [
    ("id", "ID"),
    ("analyzer_name", "Analizador"),
    ("patient_id", "Documento"),
    ("patient_name", "Paciente"),
    ("exam_code", "Código Examen"),
    ("exam_title", "Nombre Examen"),
    ("fecha_ref", "Fecha"),
    ("export_status", "Estado"),
    ("exported_at", "Exportado"),
]


class ExportWorker(QObject):
    progress = Signal(int, int)  # done, total
    item_done = Signal(dict, str, str)  # header_row, status, detail
    finished = Signal()

    def __init__(self, rows: list[dict], export_fn):
        super().__init__()
        self._export_queue: list[dict] = []
        self._export_total = 0
        self._export_done = 0
        self._export_dlg: QtWidgets.QProgressDialog | None = None
        self._export_cancelled = False
        self.rows = rows
        self.export_fn = export_fn

    def run(self):
        total = len(self.rows)
        done = 0
        for header in self.rows:
            try:
                status, detail = self.export_fn(header)
                self.item_done.emit(header, status, detail or "")
            except Exception as e:
                self.item_done.emit(header, "ERROR", str(e))
            finally:
                done += 1
                self.progress.emit(done, total)
        self.finished.emit()


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

        # ------------------ Acciones ------------------
        actions = QtWidgets.QHBoxLayout()
        self.btn_export_one = QtWidgets.QPushButton("Exportar seleccionado")
        self.btn_export_filtered = QtWidgets.QPushButton("Exportar filtrados")
        self.btn_export_one.setIcon(
            self.style().standardIcon(QtWidgets.QStyle.SP_ArrowRight)
        )
        self.btn_export_filtered.setIcon(
            self.style().standardIcon(QtWidgets.QStyle.SP_ArrowRight)
        )
        actions.addWidget(self.btn_export_one)
        actions.addWidget(self.btn_export_filtered)
        actions.addStretch()
        layout.addLayout(actions)

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
        self.btn_export_one.clicked.connect(self._export_selected_row)
        self.btn_export_filtered.clicked.connect(self._export_all_filtered)

        # Inicial
        self._rows: list[dict] = []
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

    def _selected_row_index(self) -> int | None:
        idxs = self.table.selectionModel().selectedRows()
        if not idxs:
            return None
        return idxs[0].row()

    # ------------------ Datos ------------------

    def _fill_analyzers(self):
        self.cmb_analyzer.blockSignals(True)
        self.cmb_analyzer.clear()
        self.cmb_analyzer.addItem("(Todos)", "")
        conn = get_conn(DB_PATH)
        cur = conn.cursor()
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
                COALESCE(NULLIF(r.exam_date,''), substr(r.received_at,1,10)) AS fecha_ref,

                (
                SELECT
                    CASE
                    WHEN EXISTS(SELECT 1 FROM hl7_obx_results xo WHERE xo.result_id=r.id AND xo.export_status='ERROR') THEN 'ERROR'
                    WHEN EXISTS(SELECT 1 FROM hl7_obx_results xo WHERE xo.result_id=r.id AND (xo.export_status='SENT' OR xo.export_status='EXPORTED')) THEN 'EXPORTED'
                    WHEN EXISTS(SELECT 1 FROM hl7_obx_results xo WHERE xo.result_id=r.id AND xo.export_attempts>0) THEN 'PENDING'
                    ELSE ''
                    END
                ) AS export_status,

                (SELECT MAX(xo.exported_at) FROM hl7_obx_results xo WHERE xo.result_id=r.id) AS exported_at

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
                if key in (
                    "id",
                    "fecha_ref",
                    "exam_code",
                    "export_status",
                    "exported_at",
                ):
                    item.setTextAlignment(Qt.AlignCenter)
                if key == "export_status":
                    st = (val or "").upper()
                    if st == "EXPORTED":
                        item.setBackground(QtGui.QBrush(QtGui.QColor("#d4edda")))
                    elif st == "ERROR":
                        item.setBackground(QtGui.QBrush(QtGui.QColor("#f8d7da")))
                model.setItem(i, j, item)

        self.table.setModel(model)
        self.table.resizeColumnsToContents()
        self._rows = rows

    # ------------------ Exportación ------------------

    def _start_export_safe(self, rows: list[dict]):
        self.btn_export_one.setEnabled(False)
        self.btn_export_filtered.setEnabled(False)
        self.btn_refresh.setEnabled(False)
        self.setCursor(Qt.BusyCursor)

        self._export_queue = list(rows)
        self._export_total = len(rows)
        self._export_done = 0
        self._export_cancelled = False

        self._export_dlg = QtWidgets.QProgressDialog(
            "Exportando resultados...", "", 0, self._export_total, self
        )
        self._export_dlg.setWindowTitle("Exportación")
        self._export_dlg.setAutoClose(False)
        self._export_dlg.setAutoReset(False)
        self._export_dlg.setMinimumDuration(300)
        self._export_dlg.setWindowModality(Qt.ApplicationModal)

        try:
            self._export_dlg.setCancelButton(None)
        except Exception:
            self._export_dlg.setCancelButtonText("")
        self._export_dlg.setWindowFlag(Qt.WindowCloseButtonHint, False)
        self._export_dlg.setValue(0)

        QtCore.QTimer.singleShot(0, self._process_next_export_item)

    def _cancel_export_safe(self):
        if self._export_queue:
            self._export_cancelled = True

    def _process_next_export_item(self):
        if self._export_cancelled or not self._export_queue:
            return self._finish_export_safe()

        header = self._export_queue.pop(0)

        status, detail = "ERROR", ""
        try:
            status, detail = self.export_fn(header)
        except Exception as e:
            status, detail = "ERROR", str(e)

        self._export_done += 1
        if self._export_dlg:
            self._export_dlg.setValue(self._export_done)
            self._export_dlg.setLabelText(
                f"Exportando resultados... {self._export_done}/{self._export_total}"
            )

        if not self._export_cancelled and self._export_queue:
            QtCore.QTimer.singleShot(0, self._process_next_export_item)
        else:
            self._finish_export_safe()

    def _finish_export_safe(self):
        if self._export_dlg:
            try:
                self._export_dlg.close()
            except Exception:
                pass
            self._export_dlg = None

        self.btn_export_one.setEnabled(True)
        self.btn_export_filtered.setEnabled(True)
        self.btn_refresh.setEnabled(True)
        self.unsetCursor()

        try:
            self.refresh()
        except Exception:
            pass

        QtWidgets.QMessageBox.information(self, "Exportación", "Proceso finalizado.")

    def _export_selected_row(self):
        idx = self._selected_row_index()
        if idx is None:
            QtWidgets.QMessageBox.information(
                self, "Exportar", "Selecciona una fila primero."
            )
            return
        self._start_export_safe([self._rows[idx]])

    def _export_all_filtered(self):
        if not self._rows:
            QtWidgets.QMessageBox.information(
                self, "Exportar", "No hay resultados filtrados para exportar."
            )
            return
        confirm = QtWidgets.QMessageBox.question(
            self,
            "Exportar",
            f"¿Exportar {len(self._rows)} registros filtrados?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
        )
        if confirm != QtWidgets.QMessageBox.Yes:
            return
        self._start_export_safe(self._rows)

    # ------------------ Exportación (callback único) ------------------

    def export_fn(self, header_row: dict) -> tuple[str, str]:
        """
        Envía cada OBX con ResultSender y retorna (status_global, detail).
        Cierra el examen al final SOLO si al menos un analito se envió OK.
        """
        result_id = int(header_row["id"])

        # 1) Cargar OBX del resultado
        obx_rows = self._load_obx(result_id)
        if not obx_rows:
            return ("ERROR", "No hay OBX para exportar.")

        # 2) Resolver el código de barras (tubo_muestra)
        barcode = self._resolve_barcode(header_row)
        if header_row.get("analyzer_name") == 'FINECARE':
            barcode = header_row.get("patient_name")

        if not barcode:
            return ("ERROR", "No se pudo resolver 'tubo_muestra' (código de barras).")

        analyzer = header_row.get("analyzer_name") or ""

        # Flags para lógica de cierre
        any_ok = False
        last_ok_outcome = None

        # Usamos SIEMPRE la misma conexión para marcar los OBX
        conn = get_conn(DB_PATH)
        try:
            all_ok = True
            last_index = len(obx_rows) - 1

            for idx, r in enumerate(obx_rows):
                obx_id = r.get("id")
                if not obx_id:
                    continue

                obx_code = r.get("code") or ""
                obx_text = r.get("text") or ""
                obx_value = r.get("value")
                obx_units = r.get("units") or ""
                obx_ref_range = r.get("ref_range") or ""

                obx_record = {
                    "analyzer": analyzer,
                    "code": obx_code,
                    "text": obx_text,
                    "value": obx_value,
                    "unit": obx_units,
                    "ref_range": obx_ref_range,
                    "tubo_muestra": barcode,
                    # OJO: ahora NO delegamos el cierre aquí
                    "ultimo_del_examen": False,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                outcome = sender.process_obx(obx_record)

                if outcome.ok:
                    any_ok = True
                    last_ok_outcome = outcome
                    try:
                        # path lo dejamos vacío porque usamos export_request/export_response
                        mark_obx_exported(conn, obx_id, "")
                    except Exception:
                        logger.exception("Error marcando OBX exportado (id=%s)", obx_id)
                else:
                    all_ok = False
                    try:
                        detail_err = (
                            " | ".join([f"{c}:{m}" for c, m in outcome.errors])
                            or "Fallo no especificado"
                        )
                        mark_obx_error(conn, obx_id, detail_err)
                    except Exception:
                        logger.exception("Error marcando OBX con ERROR (id=%s)", obx_id)

            conn.commit()
        finally:
            conn.close()

        # 3) Cerrar examen SOLO si al menos un analito se envió OK
        if any_ok and last_ok_outcome and last_ok_outcome.id_examen:
            try:
                paciente_doc = get_paciente_doc_by_exam_id(last_ok_outcome.id_examen)
                resp = api_client.close_exam(
                    id_examen=last_ok_outcome.id_examen,
                    order_date=last_ok_outcome.order_date,
                    paciente=paciente_doc,
                )

                # Guardar también la trazabilidad del cierre en hl7_results
                conn2 = get_conn(DB_PATH)
                try:
                    conn2.execute(
                        """
                        UPDATE hl7_results
                        SET close_exam_request  = ?,
                            close_exam_response = ?,
                            close_exam_status   = 'OK',
                            close_exam_at       = ?
                        WHERE id = ?
                        """,
                        (
                            resp.get("url", ""),
                            resp.get("raw", ""),
                            datetime.now().isoformat(timespec="seconds"),
                            result_id,
                        ),
                    )
                    conn2.commit()
                finally:
                    conn2.close()

                logger.info(
                    "Cierre de examen OK (result_id=%s, id_examen=%s)",
                    result_id,
                    last_ok_outcome.id_examen,
                )
            except Exception as ex:
                logger.exception(
                    "Error cerrando examen para result_id=%s: %s", result_id, ex
                )

        # 4) Resultado global para la UI
        if all_ok:
            return ("OK", f"Exportados {len(obx_rows)} OBX.")
        elif any_ok:
            return (
                "ERROR",
                "Algunos OBX fallaron pero al menos uno fue enviado. Revisa la tabla/estado.",
            )
        else:
            return (
                "ERROR",
                "Todos los OBX fallaron. El examen NO se cerró.",
            )

    def export_fn_old(self, header_row: dict) -> tuple[str, str]:
        """
        Envía cada OBX con ResultSender y retorna (status_global, detail).
        Cierra el examen automáticamente en el último OBX.
        """
        result_id = int(header_row["id"])

        # 1) Cargar OBX del resultado
        obx_rows = self._load_obx(result_id)
        if not obx_rows:
            return ("ERROR", "No hay OBX para exportar.")

        # 2) Resolver el código de barras (tubo_muestra)
        # barcode = self._resolve_barcode(header_row)
        barcode = header_row.get("patient_id")
        if header_row.get("analyzer_name") == 'FINECARE':
            barcode = header_row.get("patient_name")

        # 3) Contexto base desde el header
        analyzer = header_row.get("analyzer_name") or ""
        paciente_id = header_row.get("patient_id") or ""

        # 4) Enviar cada OBX
        all_ok = True
        last_index = len(obx_rows) - 1
        for idx, r in enumerate(obx_rows):
            obx_id = r.get("id")
            obx_code = r.get("code") or ""
            obx_text = r.get("text") or ""
            obx_value = r.get("value")
            obx_units = r.get("units") or ""
            obx_ref_range = r.get("ref_range") or ""

            obx_record = {
                "analyzer": analyzer,
                "code": obx_code,
                "text": obx_text,
                "value": obx_value,
                "unit": obx_units,
                "ref_range": obx_ref_range,
                "tubo_muestra": barcode,
                "ultimo_del_examen": (idx == last_index),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            outcome = sender.process_obx(obx_record)

            conn = get_conn(DB_PATH)
            try:
                if outcome.ok:
                    try:
                        mark_obx_exported(
                            conn,
                            obx_id,
                            detail=f"id_examen={outcome.id_examen}, client_code={outcome.client_code}",
                        )
                    except Exception:
                        pass
                else:
                    all_ok = False
                    try:
                        detail_err = (
                            " | ".join([f"{c}:{m}" for c, m in outcome.errors])
                            or "Fallo no especificado"
                        )
                        mark_obx_error(conn, obx_id, detail=detail_err)
                    except Exception:
                        pass
                conn.commit()
            finally:
                conn.close()

        if all_ok:
            return ("OK", f"Exportados {len(obx_rows)} OBX.")
        else:
            return (
                "ERROR",
                "Uno o más OBX fallaron. Revisa el detalle en la tabla/estado.",
            )

    # ------------------ Resolución de tubo ------------------

    def _resolve_barcode(self, header_row: dict) -> str | None:
        """
        1) Si el header ya lo trae (tubo_muestra/barcode/sample_id/tube_code), úsalo.
        2) Si no, primero intentamos usar patient_id como tubo_muestra (caso ICON3 actual).
        3) Como fallback opcional, usar patient_id como documento + fecha (compatibilidad).
        """
        # 1) En header (por si en el futuro se guarda ahí)
        for k in ("tubo_muestra", "barcode", "sample_id", "tube_code"):
            val = header_row.get(k)
            if val:
                return str(val)

        conn = get_conn(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        pid = header_row.get("patient_id")
        fecha = header_row.get("fecha_ref")  # yyyy-mm-dd

        # 2) Intentar primero como tubo_muestra = hl7_results.patient_id
        if pid:
            cur.execute(
                """
                SELECT tubo_muestra
                FROM exams
                WHERE tubo_muestra = ?
                ORDER BY fecha DESC
                LIMIT 1
                """,
                (pid,),
            )
            row = cur.fetchone()
            if row and row["tubo_muestra"]:
                conn.close()
                return str(row["tubo_muestra"])

        # 3) (Opcional) fallback: tratar patient_id como documento + fecha
        if pid and fecha:
            cur.execute(
                """
                SELECT tubo_muestra
                FROM exams
                WHERE paciente_doc = ?
                AND date(fecha) = date(?)
                ORDER BY fecha DESC
                LIMIT 1
                """,
                (pid, fecha),
            )
            row = cur.fetchone()
            conn.close()
            if row and row["tubo_muestra"]:
                return str(row["tubo_muestra"])

        conn.close()
        return None

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
            SELECT id
                 , code
                 , text
                 , value
                 , units
                 , ref_range
                 , flags
              FROM hl7_obx_results
             WHERE result_id = ?
            ORDER BY id ASC
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
        dlg.resize(900, 520)
        v = QtWidgets.QVBoxLayout(dlg)

        info = QtWidgets.QLabel(
            f"<b>Paciente:</b> {header.get('patient_name','')} "
            f"(<code>{header.get('patient_id','')}</code>) &nbsp;&nbsp; "
            f"<b>Examen:</b> {header.get('exam_code','')} — {header.get('exam_title','')} &nbsp;&nbsp; "
            f"<b>Fecha:</b> {header.get('fecha_ref','')} &nbsp;&nbsp; "
            f"<b>Estado:</b> {header.get('export_status','') or '-'}"
        )
        info.setTextFormat(Qt.RichText)
        v.addWidget(info)

        tbl = QtWidgets.QTableWidget(0, 8)
        tbl.setHorizontalHeaderLabels(
            [
                "Sec.",
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
            tbl.setItem(
                i,
                0,
                QtWidgets.QTableWidgetItem(
                    "" if r.get("seq") is None else str(r.get("seq"))
                ),
            )
            tbl.setItem(i, 1, QtWidgets.QTableWidgetItem(str(r.get("code") or "")))
            tbl.setItem(i, 2, QtWidgets.QTableWidgetItem(str(r.get("text") or "")))
            tbl.setItem(i, 3, QtWidgets.QTableWidgetItem(str(r.get("value") or "")))
            tbl.setItem(i, 4, QtWidgets.QTableWidgetItem(str(r.get("units") or "")))
            tbl.setItem(i, 5, QtWidgets.QTableWidgetItem(str(r.get("ref_range") or "")))
            tbl.setItem(i, 6, QtWidgets.QTableWidgetItem(str(r.get("flags") or "")))
            tbl.setItem(i, 7, QtWidgets.QTableWidgetItem(str(r.get("obs_dt") or "")))

        tbl.resizeColumnsToContents()
        v.addWidget(tbl)

        btns = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Close)
        btns.rejected.connect(dlg.reject)
        v.addWidget(btns)

        dlg.exec()
