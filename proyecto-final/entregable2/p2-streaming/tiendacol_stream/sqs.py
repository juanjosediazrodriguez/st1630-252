"""Publicación de decisiones en SQS (DISENO §4.3).

Las decisiones por micro-batch son pocas (alertas y carritos abandonados), así que se recogen en el driver
y se envían con `send_message_batch` (hasta 10 por llamada). Garantía *at-least-once*: si el envío falla
tras los reintentos, la excepción tumba el micro-batch y Spark lo reintenta; el consumidor (Lambda de P4)
descarta repetidos por `alert_id` / `message_id` (idempotencia en el consumidor).

Modos: `aws` (boto3), `file` (JSONL local con `received_at`, para las pruebas) y `off`.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from .config import Config

BATCH = 10  # límite de send_message_batch


class SqsError(RuntimeError):
    pass


def _now_iso() -> str:
    t = datetime.now(timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"


class SqsPublisher:
    """`kind` es `alerts` o `carts`; el destino sale de la configuración."""

    def __init__(self, cfg: Config, client=None, sleep=time.sleep, max_attempts: int = 3):
        self.cfg, self.client, self.sleep, self.max_attempts = cfg, client, sleep, max_attempts
        self.sent = {"alerts": 0, "carts": 0}

    # -- destino -------------------------------------------------------------
    def _url(self, kind: str) -> str:
        url = {"alerts": self.cfg.alerts_queue_url, "carts": self.cfg.carts_queue_url}[kind]
        if not url:
            raise SqsError(f"Falta la URL de la cola de {kind} (--{kind}_queue_url)")
        return url

    def _client(self):
        if self.client is None:
            import boto3  # diferido: en modo file/off y en pruebas no se necesita
            self.client = boto3.client("sqs", region_name=self.cfg.region)
        return self.client

    # -- API -----------------------------------------------------------------
    def publish(self, kind: str, messages: list[dict]) -> int:
        if not messages or self.cfg.sqs_mode == "off":
            return 0
        if self.cfg.sqs_mode == "file":
            self._publish_file(kind, messages)
        else:
            self._publish_aws(kind, messages)
        self.sent[kind] += len(messages)
        return len(messages)

    def _publish_file(self, kind: str, messages: list[dict]) -> None:
        if not self.cfg.decisions_dir:
            raise SqsError("--sqs_mode file requiere --decisions_dir")
        d = Path(self.cfg.decisions_dir)
        d.mkdir(parents=True, exist_ok=True)
        with (d / f"{kind}.jsonl").open("a", encoding="utf-8") as fh:
            for m in messages:
                fh.write(json.dumps({**m, "received_at": _now_iso()}, ensure_ascii=False) + "\n")

    def _publish_aws(self, kind: str, messages: list[dict]) -> None:
        url, client = self._url(kind), self._client()
        for i in range(0, len(messages), BATCH):
            pending = [{"Id": str(k), "MessageBody": json.dumps(m, ensure_ascii=False)}
                       for k, m in enumerate(messages[i:i + BATCH])]
            for attempt in range(self.max_attempts):
                resp = client.send_message_batch(QueueUrl=url, Entries=pending)
                failed = {f["Id"] for f in resp.get("Failed", [])}
                if not failed:
                    break
                pending = [e for e in pending if e["Id"] in failed]       # solo se reintentan los que fallaron
                if attempt < self.max_attempts - 1:
                    self.sleep(min(0.2 * 2 ** attempt, 2.0))              # espera exponencial corta
            else:
                raise SqsError(f"SQS rechazó {len(pending)} mensajes de {kind} tras {self.max_attempts} intentos: "
                               f"{[e['MessageBody'][:80] for e in pending[:2]]}")


def make_publisher(cfg: Config, client=None) -> SqsPublisher:
    return SqsPublisher(cfg, client=client)
