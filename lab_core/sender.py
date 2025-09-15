# lab_core/sender.py
from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

import requests

log = logging.getLogger("lab.integrator.sender")


class SNTClient:
    def __init__(
        self, *, base_url: str, api_key: str, api_secret: str, timeout: int = 20
    ):
        self.base_url, self.api_key, self.api_secret, self.timeout = (
            base_url,
            api_key,
            api_secret,
            timeout,
        )

    def agregar_item_examenlab(
        self,
        *,
        idexamen: str | int,
        paciente: str | int,
        fecha: str,
        texto: str,
        valor_cualitativo: str | float | int | None = None,
        valor_referencia: str | None = None,
        valor_adicional: str | None = None,
        extra_params: Mapping[str, Any] | None = None,
    ) -> requests.Response:
        params = {
            "API_Key": self.api_key,
            "API_Secret": self.api_secret,
            "accion": "agregar_item_examenlab",
            "idexamen": str(idexamen),
            "paciente": str(paciente),
            "fecha": str(fecha),
            "texto": str(texto),
        }
        if valor_cualitativo is not None:
            params["valor_cualitativo"] = str(valor_cualitativo)
        if valor_referencia:
            params["valor_referencia"] = valor_referencia
        if valor_adicional:
            params["valor_adicional"] = valor_adicional
        if extra_params:
            params.update(extra_params)

        log.info(
            "Enviando a SNT idexamen=%s paciente=%s fecha=%s", idexamen, paciente, fecha
        )
        resp = requests.post(self.base_url, params=params, timeout=self.timeout)
        log.info(
            "Resp SNT HTTP %s - %s",
            resp.status_code,
            (resp.text[:300] if resp.text else ""),
        )
        resp.raise_for_status()
        return resp

    def actualizar_examenlab_fecha(
        self,
        *,
        idexamen: str | int,
        paciente: str | int,
        fecha: str,
        resultado_global: str = "Normal",
        responsable: str = "PENDIENTEVALIDAR",
        notas: str = "Enviado desde integracion",
        extra_params: Mapping[str, Any] | None = None,
    ) -> requests.Response:
        params = {
            "API_Key": self.api_key,
            "API_Secret": self.api_secret,
            "accion": "actualizar_examenlab_fecha",
            "idexamen": str(idexamen),
            "paciente": str(paciente),
            "fecha": str(fecha),
            "resultado_global": str(resultado_global),
            "responsable": str(responsable),
            "notas": str(notas),
        }
        if extra_params:
            params.update(extra_params)

        log.info(
            "Notificando examen completo | idexamen=%s paciente=%s fecha=%s resultado_global=%s",
            idexamen,
            paciente,
            fecha,
            resultado_global,
        )
        resp = requests.post(self.base_url, params=params, timeout=self.timeout)
        log.info(
            "Resp SNT HTTP %s - %s",
            resp.status_code,
            (resp.text[:300] if resp.text else ""),
        )
        resp.raise_for_status()
        return resp
