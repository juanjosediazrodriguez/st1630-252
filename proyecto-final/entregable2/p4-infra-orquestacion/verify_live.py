"""Verifica escenarios RF-07/08/09 contra servicios AWS y guarda evidencia JSON."""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from deploy import E2, config, outputs

sys.path.insert(0, str(E2 / "p1-generador"))
from tiendacol_gen.verify import parse_ts, verify_rf07, verify_rf08, verify_rf09  # noqa: E402

EVIDENCE = Path(__file__).parent / "evidence" / "aws-scenarios"


def athena_rows(session, out, sql):
    athena = session.client("athena")
    query = athena.start_query_execution(
        QueryString=sql, WorkGroup=out["AthenaWorkGroup"],
        ResultConfiguration={"OutputLocation": f"s3://{out['OpsBucket']}/athena-results/"},
    )["QueryExecutionId"]
    for _ in range(90):
        status = athena.get_query_execution(QueryExecutionId=query)["QueryExecution"]["Status"]
        if status["State"] == "SUCCEEDED":
            pages = athena.get_paginator("get_query_results").paginate(QueryExecutionId=query)
            result = [row for page in pages for row in page["ResultSet"]["Rows"]]
            return [[x.get("VarCharValue") for x in row["Data"]] for row in result[1:]]
        if status["State"] in ("FAILED", "CANCELLED"):
            raise RuntimeError(status.get("StateChangeReason", status["State"]))
        time.sleep(1)
    raise TimeoutError(f"Athena tardó demasiado: {query}")


def save(name, result, extra):
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    data = {"checked_at": datetime.now(timezone.utc).isoformat(), "ok": result.ok,
            "result": str(result), **extra}
    (EVIDENCE / f"observed_{name}.json").write_text(
        json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(json.dumps(data, indent=2, ensure_ascii=False, default=str), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=("rf07", "rf08", "rf09"))
    args = parser.parse_args()
    expected = json.loads((EVIDENCE / f"expected_{args.scenario}.json").read_text(encoding="utf-8"))["expected"]
    cfg, session = config()
    out = outputs(session.client("cloudformation"), f"{cfg['PROJECT_PREFIX']}-e2")

    if args.scenario == "rf08":
        users = [*expected["present"], *expected["absent"]]
        first = {user: None for user in users}
        table = session.resource("dynamodb").Table(out["CartsTable"])
        until = max(parse_ts(x["deadline"]) for x in expected["present"].values()) + timedelta(minutes=5)
        while datetime.now(timezone.utc) < until:
            for user in users:
                if first[user] is None and table.get_item(Key={"user_id": user}, ConsistentRead=True).get("Item"):
                    first[user] = datetime.now(timezone.utc)
                    print(f"Carrito observado: {user} {first[user].isoformat()}", flush=True)
            snapshot = {k: v.isoformat() if v else None for k, v in first.items()}
            (EVIDENCE / "rf08_first_seen.json").write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
            time.sleep(10)
        result = verify_rf08(expected, first)
        carts = {}
        for user in users:
            item = table.get_item(Key={"user_id": user}, ConsistentRead=True).get("Item")
            if item:
                carts[user] = {"cart_value": float(item["cart_value"]),
                               "abandoned_at": item.get("abandoned_at"),
                               "product_ids": item.get("product_ids", [])}
        for user, info in expected["present"].items():
            if user in carts and abs(carts[user]["cart_value"] - info["value"]) > 0.5:
                result.ok = False
                result.details.setdefault("problemas", {})[user] = "valor de carrito incorrecto"
        save("rf08", result, {"first_seen": snapshot, "carts": carts})
    elif args.scenario == "rf09":
        minute = expected["minute_start"][:19].replace("T", " ")
        deadline = parse_ts(expected["deadline"])
        first_seen = None
        rows = []
        while datetime.now(timezone.utc) <= deadline:
            raw = athena_rows(session, out, f"SELECT CAST(minute_start AS varchar), CAST(category_id AS varchar), "
                              f"orders, sales_cop FROM tiendacol_gold.agg_sales_minute "
                              f"WHERE minute_start = TIMESTAMP '{minute}'")
            rows = [{"minute_start": r[0].replace(" ", "T", 1), "category_id": r[1],
                     "orders": r[2], "value": r[3]} for r in raw]
            observed = pd.DataFrame(rows, columns=["minute_start", "category_id", "orders", "value"])
            if verify_rf09(expected, observed).ok:
                first_seen = datetime.now(timezone.utc)
                break
            time.sleep(5)
        result = verify_rf09(expected, pd.DataFrame(rows, columns=["minute_start", "category_id", "orders", "value"]), first_seen)
        save("rf09", result, {"rows": rows, "first_seen": first_seen})
    else:
        deadline = parse_ts(expected["trigger_ts"]) + timedelta(seconds=expected["max_latency_s"] + 60)
        alerts = []
        while True:
            raw = athena_rows(session, out, "SELECT alert_id, product_id "
                              "FROM tiendacol_lake.alert_log WHERE product_id = 'TEST-RF07'")
            dedupe = session.resource("dynamodb").Table(f"{cfg['PROJECT_PREFIX']}-alert-dedupe")
            alerts = []
            for alert_id, product_id in raw:
                item = dedupe.get_item(Key={"alert_id": alert_id}, ConsistentRead=True).get("Item")
                if item and item.get("status") == "SENT":
                    alerts.append({"product_id": product_id, "alert_id": alert_id,
                                   "received_at": datetime.fromtimestamp(int(item["sent_at"]), timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")})
            if alerts or datetime.now(timezone.utc) > deadline:
                break
            time.sleep(5)
        result = verify_rf07(expected, alerts)
        save("rf07", result, {"alerts": alerts})


if __name__ == "__main__":
    main()
