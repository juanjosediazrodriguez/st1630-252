"""Consumidores SQS de decisiones de P2. Respuestas parciales evitan reprocesar el lote entero."""
from __future__ import annotations

import json
import os
import time
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError


def _decode(record: dict, kind: str) -> dict:
    msg = json.loads(record["body"])
    if msg.get("schema_version") != "1.0" or msg.get("decision_type") != kind:
        raise ValueError(f"Contrato SQS inesperado: {msg.get('decision_type')}")
    return msg


def _batch(event: dict, fn) -> dict:
    failed = []
    for record in event.get("Records", []):
        try:
            fn(record)
        except Exception:
            import logging
            logging.exception("Fallo al procesar mensaje %s", record.get("messageId"))
            failed.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failed}


def alerts_handler(event, _context):
    table = boto3.resource("dynamodb").Table(os.environ["DEDUPE_TABLE"])
    sns = boto3.client("sns")
    topic = os.environ["ALERTS_TOPIC_ARN"]

    def process(record):
        msg = _decode(record, "stock_depletion_alert")
        alert_id = msg["alert_id"]
        if not alert_id:
            raise ValueError("Falta alert_id")
        state = table.get_item(Key={"alert_id": alert_id}, ConsistentRead=True).get("Item", {})
        if state.get("status") == "SENT":
            return
        # El marcador se escribe después de SNS: si SNS falla, SQS reintenta.
        # Una caída entre SNS y el marcador puede repetir el correo; no se promete exactly-once.
        sns.publish(
            TopicArn=topic,
            Subject=f"TiendaCol: riesgo de agotamiento {msg.get('product_id', '')}"[:100],
            Message=json.dumps(msg, ensure_ascii=False, indent=2),
        )
        table.put_item(Item={
            "alert_id": alert_id,
            "status": "SENT",
            "sent_at": int(time.time()),
            "expires_at": int(time.time()) + 7 * 86400,
        })

    return _batch(event, process)


def carts_handler(event, _context):
    table = boto3.resource("dynamodb").Table(os.environ["CARTS_TABLE"])

    def process(record):
        msg = _decode(record, "abandoned_cart")
        if not msg.get("user_id") or not msg.get("message_id") or not msg.get("abandoned_at"):
            raise ValueError("Faltan claves del carrito")
        item = {k: Decimal(str(v)) if isinstance(v, float) else v for k, v in msg.items()}
        try:
            table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(user_id) OR abandoned_at <= :new_time",
                ExpressionAttributeValues={":new_time": msg["abandoned_at"]},
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            # Ya existe un carrito más reciente para ese usuario.

    return _batch(event, process)
