# -*- coding: utf-8 -*-
"""
Flujo unificado para enviar resultados (GUI y automático usan lo mismo).

Cómo usar:
    from lab_core.result_flow import (
        ResultSender,
        DefaultMappingRepo,
        DefaultExamRepo,
        DefaultXmlBuilder,
        DefaultApiClient,
    )

    sender = ResultSender(
        mapping_repo=DefaultMappingRepo(mapping_path="config/mapping.json"),
        exam_repo=DefaultExamRepo(db_path="data/labintegrador.db"),
        xml_builder=DefaultXmlBuilder(),
        api_client=DefaultApiClient(
            base_url="https://tu-api/sofia",
            token="TOKEN",
            timeout=30,
        ),
        logger=logger,  # opcional
    )

    outcome = sender.process_obx(obx_record)

Requisitos mínimos de `obx_record`:
    {
      "analyzer": "FS-114",
      "text": "PLCC",                 # o "value_text"
      "tubo_muestra": "412503-14",    # o "barcode"
      "value": "123.4",               # numérico o texto (según tu builder)
      "unit": "mg/dL",
      "timestamp": "2025-11-11 10:15:00",  # opcional
      "ultimo_del_examen": False,     # opcional
      "paciente_id": "CC123",         # opcional
    }
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol, List, Tuple
from enum import Enum
import json
import os
import sqlite3
import re
import html
import logging
import requests
from datetime import datetime
from lab_core.file_tracer import FileTraceWriter  # <-- importar


# =========================
# Tipos y contratos (APIs)
# =========================


class ErrorCode(str, Enum):
    MAPPING_NOT_FOUND = "MAPPING_NOT_FOUND"
    EXAM_NOT_FOUND = "EXAM_NOT_FOUND"
    ORDER_DATE_MISSING = "ORDER_DATE_MISSING"
    XML_BUILD_ERROR = "XML_BUILD_ERROR"
    API_SEND_ERROR = "API_SEND_ERROR"
    UNKNOWN = "UNKNOWN"


@dataclass
class Context:
    analyzer: str
    obx_text: str
    tubo_muestra: str
    client_code: str
    id_examen: int
    order_date: str  # "YYYY-MM-DD"
    paciente_id: Optional[str] = None


@dataclass
class SendResultOutcome:
    ok: bool
    id_examen: Optional[int]
    client_code: Optional[str]
    order_date: Optional[str]
    sent_count: int
    errors: List[Tuple[ErrorCode, str]]
    logs: List[str]


class MappingRepo(Protocol):
    def resolve_client_code(self, analyzer: str, obx_text: str) -> Optional[str]: ...


class ExamRepo(Protocol):
    def get_exam_by_barcode(self, tubo_muestra: str) -> Optional[Dict[str, Any]]: ...


class XmlBuilder(Protocol):
    def build_result_xml(self, context: Context, obx_record: Dict[str, Any]) -> str: ...


class ApiClient(Protocol):
    def send_result(
        self, id_examen: int, client_code: str, xml_payload: str
    ) -> Dict[str, Any]: ...
    def close_exam(self, id_examen: int, order_date: str) -> Dict[str, Any]: ...


class LoggerLike(Protocol):
    def info(self, msg: str, *args, **kwargs) -> None: ...
    def warning(self, msg: str, *args, **kwargs) -> None: ...
    def error(self, msg: str, *args, **kwargs) -> None: ...
    def debug(self, msg: str, *args, **kwargs) -> None: ...


# =========================
# Excepciones específicas
# =========================


class FlowError(Exception):
    def __init__(self, code: ErrorCode, message: str):
        super().__init__(message)
        self.code = code


# =========================
# Implementación del flujo
# =========================


class ResultSender:
    """
    Orquesta el flujo estándar:
      1) analyzer + obx_text → client_code (mapping)
      2) tubo_muestra → exams → id_examen + order_date
      3) Construir XML y enviar por analito
      4) (Opcional) Cerrar el examen si obx_record['ultimo_del_examen'] == True
    """

    def __init__(
        self,
        mapping_repo: MappingRepo,
        exam_repo: ExamRepo,
        xml_builder: XmlBuilder,
        api_client: ApiClient,
        logger: Optional[LoggerLike] = None,
        trace_writer: Optional[FileTraceWriter] = None,  # <-- nuevo
    ) -> None:
        self.mapping_repo = mapping_repo
        self.exam_repo = exam_repo
        self.xml_builder = xml_builder
        self.api_client = api_client
        self.log = logger or logging.getLogger("result_flow")
        self.tracer = trace_writer

    # ---------- API principal (llamar igual desde GUI y automático) ----------

    def process_obx(self, obx_record: Dict[str, Any]) -> SendResultOutcome:
        logs: List[str] = []
        errors: List[Tuple[ErrorCode, str]] = []
        sent_count = 0
        ctx: Optional[Context] = None

        def _log_info(msg: str) -> None:
            logs.append(msg)
            self.log.info(msg)

        def _log_err(code: ErrorCode, msg: str) -> None:
            logs.append(f"[{code}] {msg}")
            errors.append((code, msg))
            self.log.error(f"{code}: {msg}")

        try:
            ctx = self._resolve_context(obx_record)
            _log_info(
                f"Contexto → id_examen={ctx.id_examen}, client_code={ctx.client_code}, fecha={ctx.order_date}"
            )

            xml_payload = self._build_xml(ctx, obx_record)
            _log_info("XML construido.")

            self._send_one(ctx, xml_payload, obx_record)
            sent_count += 1
            _log_info("Resultado enviado (analito).")

            if obx_record.get("ultimo_del_examen", False):
                self._close_exam(ctx)
                _log_info("Examen cerrado (actualizar_examenlab_fecha).")

            return SendResultOutcome(
                ok=True,
                id_examen=ctx.id_examen,
                client_code=ctx.client_code,
                order_date=ctx.order_date,
                sent_count=sent_count,
                errors=errors,
                logs=logs,
            )

        except FlowError as fe:
            _log_err(fe.code, str(fe))
        except Exception as ex:
            _log_err(ErrorCode.UNKNOWN, f"Error no controlado: {ex}")

        return SendResultOutcome(
            ok=False,
            id_examen=(ctx.id_examen if ctx else None),
            client_code=(ctx.client_code if ctx else None),
            order_date=(ctx.order_date if ctx else None),
            sent_count=sent_count,
            errors=errors,
            logs=logs,
        )

    # ---------- Pasos internos del flujo ----------

    def _resolve_context(self, obx: Dict[str, Any]) -> Context:
        analyzer = str(obx.get("analyzer") or "").strip()
        obx_text = str(obx.get("text") or obx.get("value_text") or "").strip()
        tubo = str(obx.get("tubo_muestra") or obx.get("barcode") or "").strip()

        # Lo que venga crudo del OBX (puede ser doc, tubo, etc.)
        obx_pid = (
            str(obx.get("paciente_id")).strip() if obx.get("paciente_id") else None
        )

        if not analyzer:
            raise FlowError(ErrorCode.MAPPING_NOT_FOUND, "Falta 'analyzer' en OBX.")
        if not obx_text:
            raise FlowError(
                ErrorCode.MAPPING_NOT_FOUND, "OBX.text vacío; no se puede mapear."
            )
        if not tubo:
            raise FlowError(
                ErrorCode.EXAM_NOT_FOUND,
                "Falta 'tubo_muestra' (código de barras) en el OBX.",
            )

        # 1) Mapping analyzer + texto → client_code
        client_code = self.mapping_repo.resolve_client_code(
            analyzer=analyzer, obx_text=obx_text
        )
        if not client_code:
            raise FlowError(
                ErrorCode.MAPPING_NOT_FOUND,
                f"No hay mapping para analyzer='{analyzer}' y obx_text='{obx_text}'.",
            )

        # 2) Buscar examen por tubo
        exam = self.exam_repo.get_exam_by_barcode(tubo)
        if not exam:
            raise FlowError(
                ErrorCode.EXAM_NOT_FOUND,
                f"No se encontró examen en 'exams' para tubo_muestra='{tubo}'.",
            )

        id_examen = exam.get("id_examen")
        order_date = exam.get("order_date")
        exam_pid = exam.get("paciente_id")  # ← viene de exams.paciente_doc

        if id_examen is None:
            raise FlowError(
                ErrorCode.EXAM_NOT_FOUND, "El examen encontrado no tiene 'id_examen'."
            )
        if not order_date:
            raise FlowError(
                ErrorCode.ORDER_DATE_MISSING,
                "El examen encontrado no tiene 'order_date'.",
            )

        # 👇 SIEMPRE preferimos el documento de la tabla exams
        if exam_pid:
            paciente_id = str(exam_pid)
        else:
            paciente_id = obx_pid  # fallback si algún día `exams` no tiene doc

        return Context(
            analyzer=analyzer,
            obx_text=obx_text,
            tubo_muestra=tubo,
            client_code=str(client_code),
            id_examen=int(id_examen),
            order_date=str(order_date),
            paciente_id=paciente_id,
        )

    def _build_xml(self, ctx: Context, obx: Dict[str, Any]) -> str:
        try:
            # return self.xml_builder.build_result_xml(ctx, obx)
            xml = self.xml_builder.build_result_xml(ctx, obx)
            if getattr(self, "tracer", None):
                self.tracer.save_xml(ctx.id_examen, ctx.client_code, ctx.obx_text, xml)
            return xml
        except Exception as ex:
            raise FlowError(ErrorCode.XML_BUILD_ERROR, f"Error construyendo XML: {ex}")

    def _send_one(
        self, ctx: Context, xml_payload: str, obx_record: Dict[str, Any]
    ) -> None:
        try:
            resp = self.api_client.send_result(
                id_examen=ctx.id_examen,
                client_code=ctx.client_code,
                xml_payload=xml_payload,
                paciente=ctx.paciente_id or "",  # si viene del OBX o de exams
                fecha=ctx.order_date,  # YYYY-MM-DD (el ApiClient quita los guiones)
                texto=ctx.obx_text,  # texto OBX (p.ej. "PLCC")
                valor=str(obx_record.get("value") or ""),
                unidad=str(obx_record.get("unit") or ""),
                ref_range=str(obx_record.get("ref_range") or ""),
            )
            print(f'_send_one.resp : {resp}')
            if getattr(self, "tracer", None):
                self.tracer.save_http(
                    kind="send",
                    id_examen=ctx.id_examen,
                    client_code=ctx.client_code,
                    obx_text=ctx.obx_text,
                    url=resp.get("url", ""),
                    resp_text=resp.get("raw", ""),
                )
        except Exception as ex:
            raise FlowError(ErrorCode.API_SEND_ERROR, f"Error enviando resultado: {ex}")

    def _close_exam(self, ctx: Context) -> None:
        try:
            resp = self.api_client.close_exam(
                id_examen=ctx.id_examen,
                order_date=ctx.order_date,
            )
            print(f'_close_exam.resp : {resp}')
            if getattr(self, "tracer", None):
                self.tracer.save_http(
                    kind="close",
                    id_examen=ctx.id_examen,
                    client_code=ctx.client_code,
                    obx_text=None,
                    url=resp.get("url", ""),
                    resp_text=resp.get("raw", ""),
                )
        except Exception as ex:
            raise FlowError(ErrorCode.API_SEND_ERROR, f"Error cerrando examen: {ex}")


# =========================
# Adaptadores por defecto
# =========================


class DefaultMappingRepo(MappingRepo):
    """
    Compatible con el formato extendido del cliente:
    {
      "version": 1,
      "analyzers": {
        "FINECARE_FS114": {
          "aliases": ["FINECARE", "FS114", "Finecare FS114"],
          "map": {
            "PLCC": { "client_code": "412503-14", "client_title": "..." }
          }
        }
      }
    }
    """

    def __init__(self, mapping_path: str) -> None:
        self.mapping_path = mapping_path
        self._data = self._load()

    def _load(self) -> Any:
        if not os.path.exists(self.mapping_path):
            raise FileNotFoundError(
                f"mapping.json no encontrado en {self.mapping_path}"
            )
        with open(self.mapping_path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _norm(s: str) -> str:
        return re.sub(r"\s+", " ", s.strip().lower())

    def resolve_client_code(self, analyzer: str, obx_text: str) -> Optional[str]:
        if not analyzer or not obx_text:
            return None

        data = self._data
        analyzers = data.get("analyzers", {}) if isinstance(data, dict) else data
        az_norm = self._norm(analyzer)
        tx_norm = self._norm(obx_text)

        # --- Buscar analizador por nombre o alias ---
        selected = None
        for key, block in analyzers.items():
            key_norm = self._norm(str(key))
            aliases = [self._norm(a) for a in block.get("aliases", [])]
            if az_norm == key_norm or az_norm in aliases or key_norm in az_norm:
                selected = block
                break

        if not selected:
            # Buscar coincidencias parciales (ej: FS114 vs FINECARE_FS114)
            for key, block in analyzers.items():
                key_norm = self._norm(str(key))
                aliases = [self._norm(a) for a in block.get("aliases", [])]
                if az_norm in key_norm or any(
                    az_norm in a or a in az_norm for a in aliases
                ):
                    selected = block
                    break

        if not selected:
            return None

        # --- Buscar analito dentro del map ---
        '''
        mapping_map = selected.get("map", {})
        for text, info in mapping_map.items():
            t_norm = self._norm(text)
            if tx_norm == t_norm or tx_norm in t_norm or t_norm in tx_norm:
                return str(info.get("client_code"))
        '''
        mapping_map = selected.get("map", {})
        for text, info in mapping_map.items():
            t_norm = self._norm(text)
            if tx_norm == t_norm:
                return str(info.get("client_code"))
        return None


class DefaultExamRepo(ExamRepo):
    """
    Consulta SQLite `exams` buscando por `tubo_muestra`.
    Debe existir una fila con: id_examen (int), order_date (texto ISO o YYYY-MM-DD).
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def get_exam_by_barcode(self, tubo_muestra: str) -> Optional[Dict[str, Any]]:
        q = """
            SELECT id as id_examen, fecha as order_date, paciente_doc as paciente_id
            FROM exams
            WHERE tubo_muestra = ?
            ORDER BY fecha DESC
            LIMIT 1
        """
        with self._conn() as cx:
            cur = cx.execute(q, (tubo_muestra,))
            row = cur.fetchone()
        if not row:
            return None
        id_examen, order_date, paciente_id = row
        # Normaliza fecha a YYYY-MM-DD si viene con hora
        if order_date and len(order_date) > 10:
            try:
                dt = datetime.fromisoformat(order_date.replace("Z", ""))
                order_date = dt.date().isoformat()
            except Exception:
                order_date = str(order_date)[:10]
        return {
            "id_examen": id_examen,
            "order_date": order_date,
            "paciente_id": paciente_id,
        }


class DefaultXmlBuilder(XmlBuilder):
    """
    Builder de XML simple y robusto.
    - Escapa caracteres.
    - Normaliza ASCII en unidades si se requiere.
    Ajusta las etiquetas a lo que tu API espera.
    """

    def __init__(self, normalize_ascii: bool = True) -> None:
        self.normalize_ascii = normalize_ascii

    @staticmethod
    def _escape(s: Any) -> str:
        if s is None:
            return ""
        return html.escape(str(s), quote=True)

    @staticmethod
    def _strip_non_ascii(s: str) -> str:
        return s.encode("ascii", errors="ignore").decode("ascii")

    def build_result_xml(self, context: Context, obx_record: Dict[str, Any]) -> str:
        value = obx_record.get("value")
        unit = obx_record.get("unit") or ""
        ts = obx_record.get("timestamp") or ""

        # Normalizaciones
        if isinstance(value, float):
            value_str = f"{value}".replace(",", ".")
        else:
            value_str = str(value)

        unit_str = str(unit)
        if self.normalize_ascii:
            unit_str = self._strip_non_ascii(unit_str)

        # XML mínimo (ajústalo a tu esquema)
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Resultado>
  <IdExamen>{self._escape(context.id_examen)}</IdExamen>
  <ClientCode>{self._escape(context.client_code)}</ClientCode>
  <Analizador>{self._escape(context.analyzer)}</Analizador>
  <OBXText>{self._escape(context.obx_text)}</OBXText>
  <Valor>{self._escape(value_str)}</Valor>
  <Unidad>{self._escape(unit_str)}</Unidad>
  <FechaOrden>{self._escape(context.order_date)}</FechaOrden>
  <FechaResultado>{self._escape(ts)}</FechaResultado>
</Resultado>
"""
        return xml


class DefaultApiClient(ApiClient):
    """
    Cliente HTTP (estilo Postman RedNacional).
    Envío de analitos:  accion=agregar_item_examenlab
    Cierre de examen:  accion=actualizar_examenlab_fecha
    Parámetros via query string; POST sin body.
    """

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        timeout: int = 30,
        # Defaults tomados del settings (para cierre):
        default_resultado_global: str = "Normal",
        default_responsable: Optional[str] = None,
        default_notas: Optional[str] = None,
    ) -> None:
        self.base_url = base_url.rstrip("?")
        self.api_key = api_key
        self.api_secret = api_secret
        self.timeout = timeout
        self.default_resultado_global = default_resultado_global
        self.default_responsable = default_responsable
        self.default_notas = default_notas

    # ------------------- ENVÍO ANALITO (OBX) -------------------
    def send_result(
        self,
        id_examen: int,
        client_code: str,
        xml_payload: str = "",
        paciente: Optional[str] = None,
        fecha: Optional[str] = None,
        texto: Optional[str] = None,
        valor: Optional[str] = None,
        unidad: Optional[str] = None,
        ref_range: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        POST <base_url>?API_Key=...&API_Secret=...&accion=agregar_item_examenlab&...
        """
        from urllib.parse import urlencode

        params = {
            "API_Key": self.api_key,
            "API_Secret": self.api_secret,
            "accion": "agregar_item_examenlab",
            "idexamen": str(id_examen),
            "paciente": paciente or "",
            "fecha": (fecha or "").replace("-", ""),  # ej: 20251011
            "texto": texto or "",
            "valor_cualitativo": "" if valor is None else str(valor),
            "valor_referencia": ref_range or "",
            "valor_adicional": f"{unidad or ''}",
        }

        print(f'send_result.params: {params}')

        url = f"{self.base_url}?{urlencode(params)}"
        resp = requests.post(url, timeout=self.timeout)
        if resp.status_code >= 300:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
        return {"status": "ok", "raw": resp.text.strip(), "url": url}

    # ------------------- CIERRE EXAMEN -------------------
    def close_exam(
        self,
        id_examen: int,
        order_date: str,
        paciente: Optional[str] = None,
        resultado_global: Optional[str] = None,
        responsable: Optional[str] = None,
        notas: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        POST <base_url>?API_Key=...&API_Secret=...&accion=actualizar_examenlab_fecha&...
        (según tu Postman)
        """
        from urllib.parse import urlencode

        params = {
            "API_Key": self.api_key,
            "API_Secret": self.api_secret,
            "accion": "actualizar_examenlab_fecha",
            "idexamen": str(id_examen),
            "paciente": paciente or "",
            "fecha": (order_date or "").replace("-", ""),  # ej: 20251011
            "resultado_global": resultado_global or self.default_resultado_global,
            "responsable": (responsable or self.default_responsable)
            or "PENDIENTEVALIDAR",
            "notas": (notas or self.default_notas) or "Enviado desde integracion",
        }

        url = f"{self.base_url}?{urlencode(params)}"
        resp = requests.post(url, timeout=self.timeout)
        if resp.status_code >= 300:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:500]}")
        return {"status": "ok", "raw": resp.text.strip(), "url": url}
