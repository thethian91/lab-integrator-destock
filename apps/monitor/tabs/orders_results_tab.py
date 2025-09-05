from __future__ import annotations

import sqlite3
from datetime import datetime

from PySide6 import QtCore, QtGui, QtWidgets
from PySide6.QtCore import QDate, Qt, QObject, Signal, QThread

from lab_core.db import (
    get_conn,
    mark_obx_error,
    mark_obx_exported,
)  # helper de conexión
from lab_core.pipeline import enviar_resultado_item

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
    ("export_status", "Estado"),
    ("exported_at", "Exportado"),
]


class ExportWorker(QObject):
    progress = Signal(int, int)  # done, total
    item_done = Signal(dict, str, str)  # header_row, status, detail
    finished = Signal()

    def __init__(self, rows: list[dict], export_fn):
        super().__init__()
        # --- Export state (modo seguro sin threads) ---
        self._export_queue: list[dict] = []
        self._export_total = 0
        self._export_done = 0
        self._export_dlg: QtWidgets.QProgressDialog | None = None
        self._export_cancelled = False
        self.rows = rows
        self.export_fn = (
            export_fn  # callback que exporta 1 item y devuelve (status, detail)
        )

    def run(self):
        total = len(self.rows)
        done = 0
        for header in self.rows:
            try:
                status, detail = self.export_fn(header)  # no UI aquí
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
        # Devuelve índice en el modelo (respetando sort)
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

                -- ⬇️ agregado desde OBX (por resultado)
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
                # Alineación para ID/Fecha/Códigos/Estado
                if key in (
                    "id",
                    "fecha_ref",
                    "exam_code",
                    "export_status",
                    "exported_at",
                ):
                    item.setTextAlignment(Qt.AlignCenter)
                # Colorear estado
                if key == "export_status":
                    st = (val or "").upper()
                    if st == "EXPORTED":
                        item.setBackground(QtGui.QBrush(QtGui.QColor("#d4edda")))
                    elif st == "ERROR":
                        item.setBackground(QtGui.QBrush(QtGui.QColor("#f8d7da")))
                model.setItem(i, j, item)

        self.table.setModel(model)
        self.table.resizeColumnsToContents()
        self._rows = rows  # cache para doble clic / export

    # ------------------ Exportación ------------------
    # =============== Exportación: modo seguro sin threads ===============

    def _start_export_safe(self, rows: list[dict]):
        # Desactivar UI mientras corre
        self.btn_export_one.setEnabled(False)
        self.btn_export_filtered.setEnabled(False)
        self.btn_refresh.setEnabled(False)
        self.setCursor(Qt.BusyCursor)

        self._export_queue = list(rows)
        self._export_total = len(rows)
        self._export_done = 0
        self._export_cancelled = False

        # QProgressDialog SIN botón Cancelar ni cierre por 'X'
        self._export_dlg = QtWidgets.QProgressDialog(
            "Exportando resultados...", "", 0, self._export_total, self
        )
        self._export_dlg.setWindowTitle("Exportación")
        self._export_dlg.setAutoClose(False)
        self._export_dlg.setAutoReset(False)
        self._export_dlg.setMinimumDuration(300)
        self._export_dlg.setWindowModality(Qt.ApplicationModal)

        # Quitar botón Cancelar y deshabilitar botón de cierre de ventana
        try:
            self._export_dlg.setCancelButton(None)  # PySide6: elimina el botón Cancelar
        except Exception:
            self._export_dlg.setCancelButtonText("")  # Fallback
        self._export_dlg.setWindowFlag(Qt.WindowCloseButtonHint, False)

        # IMPORTANTE: NO conectar self._export_dlg.canceled -> _cancel_export_safe
        # para evitar "cancelaciones fantasmas".

        self._export_dlg.setValue(0)

        # Iniciar procesamiento item por item en el hilo principal
        QtCore.QTimer.singleShot(0, self._process_next_export_item)

    def _cancel_export_safe(self):
        # Marcar cancelado únicamente si quedan items por procesar
        if self._export_queue:
            self._export_cancelled = True

    def _process_next_export_item(self):
        # Si ya no hay elementos (o se marcó cancelado de forma programática), finalizar
        if self._export_cancelled or not self._export_queue:
            return self._finish_export_safe()

        header = self._export_queue.pop(0)

        # Ejecutar UNA export dentro del hilo principal, con try/except defensivo
        status, detail = "ERROR", ""
        try:
            status, detail = self._export_one_core(
                header
            )  # usa tu pipeline y marca OBX
        except Exception as e:
            status, detail = "ERROR", str(e)

        # Avanzar progreso
        self._export_done += 1
        if self._export_dlg:
            self._export_dlg.setValue(self._export_done)
            self._export_dlg.setLabelText(
                f"Exportando resultados... {self._export_done}/{self._export_total}"
            )

        # Si quedan elementos, procesa el siguiente sin congelar la UI
        if not self._export_cancelled and self._export_queue:
            QtCore.QTimer.singleShot(0, self._process_next_export_item)
        else:
            self._finish_export_safe()

    def _finish_export_safe(self):
        # Cerrar diálogo si sigue abierto
        if self._export_dlg:
            try:
                self._export_dlg.close()
            except Exception:
                pass
            self._export_dlg = None

        # Reactivar UI
        self.btn_export_one.setEnabled(True)
        self.btn_export_filtered.setEnabled(True)
        self.btn_refresh.setEnabled(True)
        self.unsetCursor()

        # Refrescar tabla para ver estados actualizados
        try:
            self.refresh()
        except Exception:
            pass

        # Como no conectamos 'canceled', sólo mostramos "finalizado"
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

    ## --------lanzador
    def _run_export_async(self, rows: list[dict]):
        # Desactivar UI mientras corre
        self.btn_export_one.setEnabled(False)
        self.btn_export_filtered.setEnabled(False)
        self.btn_refresh.setEnabled(False)
        self.setCursor(Qt.BusyCursor)

        total = len(rows)
        dlg = QtWidgets.QProgressDialog(
            "Exportando resultados...", "Cerrar", 0, total, self
        )
        dlg.setWindowTitle("Exportación")
        dlg.setAutoClose(False)
        dlg.setAutoReset(False)
        dlg.setMinimumDuration(300)
        dlg.setValue(0)

        # Thread + worker
        self._export_thread = QThread(self)
        self._export_worker = ExportWorker(rows, self._export_one_core)
        self._export_worker.moveToThread(self._export_thread)

        # Señales → slots locales
        self._export_thread.started.connect(self._export_worker.run)

        def on_progress(done, tot):
            dlg.setValue(done)
            dlg.setLabelText(f"Exportando resultados... {done}/{tot}")

        def on_item_done(header, status, detail):
            # opcional: logging por-item
            pass

        def on_finished():
            dlg.setValue(total)
            dlg.close()
            # Reactivar UI
            self.btn_export_one.setEnabled(True)
            self.btn_export_filtered.setEnabled(True)
            self.btn_refresh.setEnabled(True)
            self.unsetCursor()
            # Limpiar thread
            self._export_worker.deleteLater()
            self._export_thread.quit()
            self._export_thread.wait()
            self._export_thread.deleteLater()
            # Refrescar la tabla para ver estados
            self.refresh()
            QtWidgets.QMessageBox.information(
                self, "Exportación", "Proceso finalizado."
            )

        dlg.canceled.connect(lambda: None)  # solo cierra el diálogo (no cancela worker)
        self._export_worker.progress.connect(on_progress)
        self._export_worker.item_done.connect(on_item_done)
        self._export_worker.finished.connect(on_finished)

        # Inicia
        self._export_thread.start()

    ## --------end lanzador

    def _export_one_core(self, header_row: dict) -> tuple[str, str]:
        """
        Exporta 1 resultado (sin tocar UI).
        Devuelve (status, detail) donde status ∈ {'EXPORTED','ERROR'}.
        """
        from lab_core.db import get_conn, mark_obx_exported, mark_obx_error
        from lab_core.pipeline import enviar_resultado_item

        result_id = header_row.get("id")
        if not result_id:
            return "ERROR", "ID de resultado inválido."

        obx_rows = self._load_obx(result_id)

        valor, units, ref, txt_extra = "", "", "", ""
        obx_id_used = None
        for obx in obx_rows:
            if (obx.get("value") or "").strip():
                obx_id_used = obx.get("id")
                valor = str(obx.get("value"))
                units = str(obx.get("units") or "")
                ref = str(obx.get("ref_range") or "")
                txt_extra = str(obx.get("text") or "")
                break

        item = {
            "idexamen": header_row.get("exam_code") or header_row.get("id"),
            "paciente_doc": header_row.get("patient_id"),
            "fecha": header_row.get("fecha_ref"),
            "texto": (header_row.get("exam_title") or "")
            + (f" — {txt_extra}" if txt_extra else ""),
            "valor": valor,
            "ref": ref,
            "units": units,
        }

        try:
            resp_text = enviar_resultado_item(item)  # hace requests.post con timeout
            if obx_id_used:
                conn = get_conn(DB_PATH)
                try:
                    mark_obx_exported(conn, obx_id_used, "API:SNT")
                    conn.commit()
                finally:
                    conn.close()
            return "EXPORTED", (resp_text or "")[:500]
        except Exception as e:
            if obx_id_used:
                conn = get_conn(DB_PATH)
                try:
                    mark_obx_error(conn, obx_id_used, str(e))
                    conn.commit()
                finally:
                    conn.close()
            return "ERROR", str(e)

    def _export_one(self, header_row: dict, show_messages: bool = True):
        """
        Toma la cabecera (hl7_results) y arma el 'item' para el pipeline usando OBX.
        Reglas simples:
          - Toma el primer OBX con valor no vacío como 'valor' principal.
          - Usa 'units' y 'ref_range' si existen.
          - 'texto' = exam_title (y si hay texto OBX se concatena).
        """
        result_id = header_row.get("id")
        if not result_id:
            if show_messages:
                QtWidgets.QMessageBox.warning(
                    self, "Exportar", "ID de resultado inválido."
                )
            return

        obx_rows = self._load_obx(result_id)
        valor, units, ref, txt_extra = "", "", "", ""

        obx_id_used = None
        for obx in obx_rows:
            if (obx.get("value") or "").strip():
                obx_id_used = obx.get("id")  # ⬅️ guarda la PK
                valor = str(obx.get("value"))
                units = str(obx.get("units") or "")
                ref = str(obx.get("ref_range") or "")
                txt_extra = str(obx.get("text") or "")
                break

        for obx in obx_rows:
            if (obx.get("value") or "").strip():
                valor = str(obx.get("value"))
                units = str(obx.get("units") or "")
                ref = str(obx.get("ref_range") or "")
                txt_extra = str(obx.get("text") or "")
                break

        # construir payload esperado por pipeline
        item = {
            "idexamen": header_row.get("exam_code")
            or header_row.get("id"),  # <- ajusta si tu mapping define otro campo
            "paciente_doc": header_row.get("patient_id"),
            "fecha": header_row.get("fecha_ref"),
            "texto": (header_row.get("exam_title") or "")
            + (f" — {txt_extra}" if txt_extra else ""),
            "valor": valor,
            "ref": ref,
            "units": units,
        }

        # Llamar pipeline
        try:
            resp_text = enviar_resultado_item(item)

            # Marca SOLO el OBX que exportaste
            if obx_id_used:
                conn = get_conn(DB_PATH)
                try:
                    mark_obx_exported(conn, obx_id_used, "API:SNT")
                    conn.commit()
                finally:
                    conn.close()

            # marcar exportado en BD
            self._mark_export_status(result_id, "EXPORTED", resp_text)
            if show_messages:
                QtWidgets.QMessageBox.information(
                    self, "Exportar", f"Exportado OK.\n{resp_text[:250]}"
                )
        except Exception as e:
            if obx_id_used:
                conn = get_conn(DB_PATH)
                try:
                    mark_obx_error(conn, obx_id_used, str(e))
                    conn.commit()
                finally:
                    conn.close()
            # marcar error en BD con mensaje
            self._mark_export_status(result_id, "ERROR", str(e))
            if show_messages:
                QtWidgets.QMessageBox.critical(
                    self, "Exportar", f"Error al exportar:\n{e}"
                )
        finally:
            self.refresh()

    def _mark_export_status(self, result_id: int, status: str, detail: str = ""):
        conn = get_conn(DB_PATH)
        cur = conn.cursor()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Guarda también un breve detalle si tienes columna (si no la tienes, ignora)
        try:
            cur.execute(
                """
                UPDATE hl7_results
                SET export_status = ?, exported_at = ?
                WHERE id = ?
                """,
                (status.upper(), now, result_id),
            )
            conn.commit()
        finally:
            conn.close()

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
                id,
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
        dlg.resize(900, 520)
        v = QtWidgets.QVBoxLayout(dlg)

        # Encabezado corto
        info = QtWidgets.QLabel(
            f"<b>Paciente:</b> {header.get('patient_name','')} "
            f"(<code>{header.get('patient_id','')}</code>) &nbsp;&nbsp; "
            f"<b>Examen:</b> {header.get('exam_code','')} — {header.get('exam_title','')} &nbsp;&nbsp; "
            f"<b>Fecha:</b> {header.get('fecha_ref','')} &nbsp;&nbsp; "
            f"<b>Estado:</b> {header.get('export_status','') or '-'}"
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
