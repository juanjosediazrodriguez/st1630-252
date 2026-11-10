"""Prueba de integración SQS -> Lambda -> DynamoDB/SNS con mensajes aislados."""
from __future__ import annotations

import argparse
import json
import time
import uuid
from datetime import datetime, timezone

from deploy import config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--carts-only", action="store_true", help="Evita publicar alerta de prueba en SNS")
    args = parser.parse_args()
    cfg, session = config()
    stack = session.client("cloudformation").describe_stacks(StackName=f"{cfg['PROJECT_PREFIX']}-e2")["Stacks"][0]
    out = {x["OutputKey"]: x["OutputValue"] for x in stack["Outputs"]}
    token = uuid.uuid4().hex[:12]
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    cart_user = f"SMOKE-{token}"
    alert_id = f"SMOKE-{token}|{now}"
    cart = {
        "schema_version": "1.0", "decision_type": "abandoned_cart", "message_id": f"{cart_user}|{now}",
        "user_id": cart_user, "cart_value": 650000.0, "currency": "COP", "session_start": now,
        "last_event_ts": now, "abandoned_at": now, "abandoned_date": now[:10],
        "events_count": 2, "product_ids": ["SMOKE-PRODUCT"], "detected_at": now,
    }
    alert = {
        "schema_version": "1.0", "decision_type": "stock_depletion_alert", "alert_id": alert_id,
        "product_id": "SMOKE-PRODUCT", "stock": 1, "units_15m": 10,
        "velocity_per_min": 0.667, "coverage_min": 1.5,
        "window_start": now, "window_end": now, "last_sale_ts": now, "detected_at": now,
    }
    sqs = session.client("sqs")
    ddb = session.resource("dynamodb")
    cart_table = ddb.Table(out["CartsTable"])
    alert_table = ddb.Table(f"{cfg['PROJECT_PREFIX']}-alert-dedupe")
    try:
        sqs.send_message(QueueUrl=out["CartsQueueUrl"], MessageBody=json.dumps(cart))
        if not args.carts_only:
            sqs.send_message(QueueUrl=out["AlertsQueueUrl"], MessageBody=json.dumps(alert))
        for _ in range(18):
            time.sleep(5)
            c = cart_table.get_item(Key={"user_id": cart_user}, ConsistentRead=True).get("Item")
            a = alert_table.get_item(Key={"alert_id": alert_id}, ConsistentRead=True).get("Item")
            if c and (args.carts_only or (a and a.get("status") == "SENT")):
                print("SQS -> Lambda -> DynamoDB: OK para carrito" + ("" if args.carts_only else " y alerta; SNS publish: OK"))
                return
        raise TimeoutError("No llegaron los mensajes de prueba a DynamoDB en 90 segundos")
    finally:
        cart_table.delete_item(Key={"user_id": cart_user})
        alert_table.delete_item(Key={"alert_id": alert_id})


if __name__ == "__main__":
    main()
