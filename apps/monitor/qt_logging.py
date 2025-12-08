# apps/monitor/qt_logging.py
import logging

from PySide6.QtCore import QObject, Signal


class QtLogEmitter(QObject):
    log = Signal(str)


class QtLogHandler(logging.Handler):
    def __init__(self, emitter: QtLogEmitter):
        super().__init__()
        self.emitter = emitter

    def emit(self, record: logging.LogRecord):
        msg = self.format(record)
        self.emitter.log.emit(msg)
