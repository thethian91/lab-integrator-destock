# apps/monitor/tabs/maintenance_tab.py
from PySide6 import QtWidgets, QtCore
from PySide6.QtCore import QDate

from lab_core.maintenance import get_stats, vacuum, backup, purge, purge_all, purge_results

class MaintenanceTab(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        v = QtWidgets.QVBoxLayout(self)

        # ---- Stats ----
        grp_stats = QtWidgets.QGroupBox("Estado de la base de datos")
        v.addWidget(grp_stats)
        gl = QtWidgets.QGridLayout(grp_stats)

        self.lbl_size = QtWidgets.QLabel("Tamaño: - MB")
        self.lbl_pat  = QtWidgets.QLabel("Pacientes: -")
        self.lbl_exam = QtWidgets.QLabel("Exámenes: -")
        self.lbl_by   = QtWidgets.QLabel("Por estado: -")

        gl.addWidget(self.lbl_size, 0, 0)
        gl.addWidget(self.lbl_pat,  0, 1)
        gl.addWidget(self.lbl_exam, 1, 0)
        gl.addWidget(self.lbl_by,   1, 1)

        self.btn_refresh = QtWidgets.QPushButton("Actualizar")
        gl.addWidget(self.btn_refresh, 0, 2, 2, 1)

        # ---- Backup / Compact ----
        grp_ops = QtWidgets.QGroupBox("Operaciones")
        v.addWidget(grp_ops)
        hb = QtWidgets.QHBoxLayout(grp_ops)

        self.backup_dir = QtWidgets.QLineEdit("backups")
        self.btn_pick   = QtWidgets.QPushButton("Destino…")
        self.btn_backup = QtWidgets.QPushButton("Backup ahora")
        self.btn_vacuum = QtWidgets.QPushButton("Compactar (VACUUM)")

        hb.addWidget(QtWidgets.QLabel("Carpeta de backup:"))
        hb.addWidget(self.backup_dir)
        hb.addWidget(self.btn_pick)
        hb.addStretch(1)
        hb.addWidget(self.btn_backup)
        hb.addWidget(self.btn_vacuum)

        # ---- Purga selectiva ----
        grp_purge = QtWidgets.QGroupBox("Depuración selectiva")
        v.addWidget(grp_purge)
        fp = QtWidgets.QHBoxLayout(grp_purge)

        self.date_before = QtWidgets.QDateEdit()
        self.date_before.setCalendarPopup(True)
        self.date_before.setDisplayFormat("yyyy-MM-dd")
        self.date_before.setDate(QDate.currentDate().addMonths(-3))

        self.status = QtWidgets.QComboBox()
        self.status.addItems(["(Todos)", "PENDING", "RESULTED", "SENT"])

        self.btn_purge = QtWidgets.QPushButton("Purgar")

        fp.addWidget(QtWidgets.QLabel("Eliminar antes de:"))
        fp.addWidget(self.date_before)
        fp.addSpacing(12)
        fp.addWidget(QtWidgets.QLabel("Estado:"))
        fp.addWidget(self.status)
        fp.addStretch(1)
        fp.addWidget(self.btn_purge)

        # ---- Danger zone ----
        grp_danger = QtWidgets.QGroupBox("Zona peligrosa")
        v.addWidget(grp_danger)
        hd = QtWidgets.QHBoxLayout(grp_danger)
        self.btn_purge_all = QtWidgets.QPushButton("BORRAR TODO (exámenes + pacientes)")
        self.btn_purge_all.setStyleSheet("QPushButton{background:#c0392b;color:white;font-weight:bold;}")
        hd.addStretch(1)
        hd.addWidget(self.btn_purge_all)

        # ---- Depurar resultados OBX ----
        grp_res = QtWidgets.QGroupBox("Resultados (OBX)")
        v.addWidget(grp_res)
        hr = QtWidgets.QHBoxLayout(grp_res)
        self.btn_purge_results = QtWidgets.QPushButton("Eliminar resultados (OBX)")
        self.btn_purge_results.setStyleSheet("QPushButton{background:#e67e22;color:white;font-weight:bold;}")
        hr.addStretch(1)
        hr.addWidget(self.btn_purge_results)

        v.addStretch(1)

        # Señales
        self.btn_refresh.clicked.connect(self.refresh)
        self.btn_pick.clicked.connect(self._pick_dir)
        self.btn_backup.clicked.connect(self._do_backup)
        self.btn_vacuum.clicked.connect(self._do_vacuum)
        self.btn_purge.clicked.connect(self._do_purge)
        self.btn_purge_all.clicked.connect(self._do_purge_all)
        self.btn_purge_results.clicked.connect(self._do_purge_results)

        # Cargar
        self.refresh()

    # ----------------- slots -----------------

    @QtCore.Slot()
    def refresh(self):
        s = get_stats()
        self.lbl_size.setText(f"Tamaño: {s['size_mb']} MB")
        self.lbl_pat.setText(f"Pacientes: {s['patients']}")
        self.lbl_exam.setText(f"Exámenes: {s['exams_total']}")
        state_str = ", ".join([f"{k}:{v}" for k, v in s.get("by_status", {}).items()]) or "-"
        self.lbl_by.setText(f"Por estado: {state_str}")

    def _pick_dir(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, "Selecciona carpeta de backup")
        if d:
            self.backup_dir.setText(d)

    def _do_backup(self):
        try:
            dest = backup(self.backup_dir.text().strip() or "backups")
            QtWidgets.QMessageBox.information(self, "Backup", f"Respaldo creado:\n{dest}")
            self.refresh()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Backup", f"Error al respaldar:\n{e}")

    def _do_vacuum(self):
        try:
            vacuum()
            QtWidgets.QMessageBox.information(self, "Compactar", "VACUUM completado.")
            self.refresh()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Compactar", f"Error en VACUUM:\n{e}")

    def _do_purge(self):
        df = self.date_before.date()
        date_str = f"{df.year():04d}-{df.month():02d}-{df.day():02d}"
        s = self.status.currentText()
        status = None if s == "(Todos)" else s

        ok = QtWidgets.QMessageBox.question(
            self, "Confirmar purga",
            f"¿Eliminar exámenes con fecha < {date_str}"
            + (f" y estado {status}" if status else "")
            + "?\nEsta acción no se puede deshacer.",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
        )
        if ok != QtWidgets.QMessageBox.Yes:
            return

        try:
            n = purge(date_before=date_str, status=status)
            QtWidgets.QMessageBox.information(self, "Purga", f"Eliminados {n} exámenes.\nPacientes huérfanos limpiados.")
            self.refresh()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Purga", f"Error al depurar:\n{e}")

    def _do_purge_all(self):
        ok = QtWidgets.QMessageBox.warning(
            self, "BORRAR TODO",
            "Vas a ELIMINAR TODOS los exámenes y pacientes.\n¿Seguro?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
        )
        if ok != QtWidgets.QMessageBox.Yes:
            return
        try:
            ex, pa = purge_all()
            QtWidgets.QMessageBox.information(self, "BORRAR TODO", f"Exámenes eliminados: {ex}\nPacientes eliminados: {pa}")
            self.refresh()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "BORRAR TODO", f"Error:\n{e}")

    def _do_purge_results(self):
        ok = QtWidgets.QMessageBox.warning(
            self, "Eliminar resultados (OBX)",
            "¿Estás seguro de eliminar todos los resultados almacenados (OBX)?\nEsta acción no se puede deshacer.",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
        )
        if ok != QtWidgets.QMessageBox.Yes:
            return
        try:
            n = purge_results()
            QtWidgets.QMessageBox.information(self, "Resultados", f"Resultados eliminados: {n}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Resultados", f"Error:\n{e}")
