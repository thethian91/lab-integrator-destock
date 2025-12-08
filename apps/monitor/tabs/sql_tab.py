from __future__ import annotations

import sqlite3

from PySide6 import QtWidgets, QtCore, QtGui

from lab_core.db import get_conn


class SqlTab(QtWidgets.QWidget):
    """
    Pestaña para ejecutar consultas SQL sobre la base SQLite
    y ver:
      - Lista de tablas/vistas
      - Estructura de la tabla seleccionada (PRAGMA table_info)
      - Resultados de la consulta
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        main_layout = QtWidgets.QVBoxLayout(self)

        # --- Barra superior: info y botones ---
        top_layout = QtWidgets.QHBoxLayout()
        self.lbl_info = QtWidgets.QLabel("Consola SQL (SQLite)")
        self.btn_reload_tables = QtWidgets.QPushButton("Refrescar tablas")
        self.btn_test_conn = QtWidgets.QPushButton("Probar conexión")

        top_layout.addWidget(self.lbl_info)
        top_layout.addStretch()
        top_layout.addWidget(self.btn_test_conn)
        top_layout.addWidget(self.btn_reload_tables)

        main_layout.addLayout(top_layout)

        # --- Splitter: izquierda tablas, derecha editor/resultados ---
        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        main_layout.addWidget(splitter, 1)

        # -------- Panel izquierdo: tablas + estructura --------
        left_panel = QtWidgets.QWidget()
        left_layout = QtWidgets.QVBoxLayout(left_panel)

        left_layout.addWidget(QtWidgets.QLabel("Tablas / Vistas"))

        self.lst_tables = QtWidgets.QListWidget()
        self.lst_tables.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)

        left_layout.addWidget(self.lst_tables, 2)

        # Tabla de estructura (PRAGMA table_info)
        left_layout.addWidget(
            QtWidgets.QLabel("Estructura de tabla (PRAGMA table_info)")
        )
        self.table_structure = QtWidgets.QTableView()
        self.table_structure.setSelectionBehavior(
            QtWidgets.QAbstractItemView.SelectRows
        )
        self.table_structure.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        left_layout.addWidget(self.table_structure, 3)

        splitter.addWidget(left_panel)

        # -------- Panel derecho: editor SQL + resultados --------
        right_panel = QtWidgets.QWidget()
        right_layout = QtWidgets.QVBoxLayout(right_panel)

        self.txt_sql = QtWidgets.QPlainTextEdit()
        self.txt_sql.setPlaceholderText(
            "-- Escribe tu consulta SQL aquí.\n"
            "-- Ejemplo:\n"
            "-- SELECT * FROM hl7_results LIMIT 50;"
        )

        self.btn_exec = QtWidgets.QPushButton("Ejecutar SQL")

        self.table_view = QtWidgets.QTableView()
        self.table_view.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table_view.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        self.txt_log = QtWidgets.QTextEdit()
        self.txt_log.setReadOnly(True)
        self.txt_log.setFixedHeight(80)

        right_layout.addWidget(QtWidgets.QLabel("Consulta SQL"))
        right_layout.addWidget(self.txt_sql)
        right_layout.addWidget(self.btn_exec)
        right_layout.addWidget(QtWidgets.QLabel("Resultados"))
        right_layout.addWidget(self.table_view, 1)
        right_layout.addWidget(QtWidgets.QLabel("Mensajes"))
        right_layout.addWidget(self.txt_log)

        splitter.addWidget(right_panel)
        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 4)

        # --- Conexiones ---
        self.btn_exec.clicked.connect(self.run_sql)
        self.btn_reload_tables.clicked.connect(self.load_tables)
        self.btn_test_conn.clicked.connect(self.test_connection)
        self.lst_tables.itemClicked.connect(self.on_table_clicked)

        # --- Inicialización ---
        self.test_connection()
        self.load_tables()

    # ------------------------------------------------------------------
    # Utilidades de conexión / tablas
    # ------------------------------------------------------------------
    def test_connection(self):
        """Verifica que la conexión a la DB funcione."""
        try:
            conn = get_conn()
            with conn:
                conn.execute("SELECT 1")
            self._log("Conexión OK.")
        except Exception as e:
            self._log(f"Error de conexión: {e}")

    def load_tables(self):
        """Carga lista de tablas y vistas en el panel izquierdo."""
        self.lst_tables.clear()
        # Limpia estructura
        self._show_structure([], [])

        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT name, type
                  FROM sqlite_master
                 WHERE type IN ('table', 'view')
                   AND name NOT LIKE 'sqlite_%'
                 ORDER BY type, name
                """
            )
            rows = cur.fetchall()
        except Exception as e:
            self._log(f"Error listando tablas: {e}")
            return

        for name, ttype in rows:
            item = QtWidgets.QListWidgetItem(f"{ttype.upper()} • {name}")
            item.setData(QtCore.Qt.UserRole, name)
            self.lst_tables.addItem(item)

        self._log(f"{len(rows)} objetos cargados (tablas/vistas).")

    def on_table_clicked(self, item: QtWidgets.QListWidgetItem):
        """Cuando el usuario hace clic en una tabla, armamos un SELECT y cargamos estructura."""
        table_name = item.data(QtCore.Qt.UserRole)
        if not table_name:
            return

        # Preparamos consulta rápida
        sql = f"SELECT * FROM {table_name} LIMIT 100;"
        self.txt_sql.setPlainText(sql)
        self._log(f"Consulta preparada para tabla: {table_name}")

        # Cargamos estructura de la tabla
        self._load_table_structure(table_name)

    def _load_table_structure(self, table_name: str):
        """Carga la estructura de la tabla usando PRAGMA table_info."""
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(f"PRAGMA table_info({table_name})")
            rows = cur.fetchall()
        except Exception as e:
            self._log(f"Error leyendo estructura de {table_name}: {e}")
            self._show_structure([], [])
            return
        finally:
            try:
                conn.close()
            except Exception:
                pass

        # PRAGMA table_info devuelve:
        # cid, name, type, notnull, dflt_value, pk
        headers = ["cid", "name", "type", "notnull", "default", "pk"]
        data_rows = [
            (cid, name, col_type, notnull, dflt_value, pk)
            for (cid, name, col_type, notnull, dflt_value, pk) in rows
        ]

        self._show_structure(headers, data_rows)

    # ------------------------------------------------------------------
    # Ejecución de SQL
    # ------------------------------------------------------------------
    def run_sql(self):
        sql = self.txt_sql.toPlainText().strip()
        if not sql:
            return

        # Limpia la tabla de resultados antes de ejecutar
        self._show_rows([], [])

        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(sql)

            if sql.lstrip().upper().startswith("SELECT"):
                rows = cur.fetchall()
                headers = [d[0] for d in cur.description] if cur.description else []
                self._show_rows(headers, rows)
                self._log(f"{len(rows)} filas devueltas.")
            else:
                conn.commit()
                self._log(f"Consulta ejecutada OK. Filas afectadas: {cur.rowcount}.")
        except sqlite3.Error as e:
            self._log(f"ERROR SQLite: {e}")
        except Exception as e:
            self._log(f"ERROR: {e}")

    def _show_rows(self, headers, rows):
        """Muestra filas en el QTableView de resultados."""
        model = QtGui.QStandardItemModel(self)

        if headers:
            model.setHorizontalHeaderLabels(headers)

        for r in rows:
            items = [QtGui.QStandardItem("" if v is None else str(v)) for v in r]
            model.appendRow(items)

        self.table_view.setModel(model)
        self.table_view.resizeColumnsToContents()

    def _show_structure(self, headers, rows):
        """Muestra estructura de tabla en el QTableView de estructura."""
        model = QtGui.QStandardItemModel(self)

        if headers:
            model.setHorizontalHeaderLabels(headers)

        for r in rows:
            items = [QtGui.QStandardItem("" if v is None else str(v)) for v in r]
            model.appendRow(items)

        self.table_structure.setModel(model)
        self.table_structure.resizeColumnsToContents()

    def _log(self, msg: str):
        """Agrega un mensaje al panel de mensajes."""
        self.txt_log.append(msg)
