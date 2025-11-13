# lab_core/file_tracer.py
from __future__ import annotations
import os, re, datetime
from typing import Optional


class FileTraceWriter:
    def __init__(self, enabled: bool, base_dir: str) -> None:
        self.enabled = enabled
        self.base_dir = base_dir

    def _ensure(self, subdir: str) -> str:
        d = os.path.join(self.base_dir, subdir)
        os.makedirs(d, exist_ok=True)
        return d

    @staticmethod
    def _now() -> str:
        # yyyyMMdd_HHmmss
        return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    @staticmethod
    def _safe(name: str) -> str:
        return re.sub(r"[^a-zA-Z0-9._-]+", "_", name).strip("_")

    def save_xml(
        self, id_examen: int, client_code: str, obx_text: str, xml: str
    ) -> None:
        if not self.enabled:
            return
        subdir = self._ensure("xml")
        fname = f"{self._now()}__{id_examen}__{self._safe(client_code)}__{self._safe(obx_text)}.xml"
        path = os.path.join(subdir, fname)
        with open(path, "w", encoding="utf-8") as f:
            f.write(xml)

    def save_http(
        self,
        kind: str,
        id_examen: int,
        client_code: str,
        obx_text: Optional[str],
        url: str,
        resp_text: Optional[str],
    ) -> None:
        """
        kind: "send" (analito) | "close" (cierre)
        """
        if not self.enabled:
            return

        # Ofuscar credenciales en el trazo
        url_masked = re.sub(r"(API_Key=)[^&]+", r"\1***", url)
        url_masked = re.sub(r"(API_Secret=)[^&]+", r"\1***", url_masked)

        subdir = self._ensure(kind)
        base = f"{self._now()}__{id_examen}__{self._safe(client_code)}"
        if obx_text:
            base += f"__{self._safe(obx_text)}"

        req_path = os.path.join(subdir, base + ".req.txt")
        with open(req_path, "w", encoding="utf-8") as f:
            f.write(url_masked)

        if resp_text is not None:
            resp_path = os.path.join(subdir, base + ".resp.txt")
            with open(resp_path, "w", encoding="utf-8") as f:
                f.write(resp_text)
