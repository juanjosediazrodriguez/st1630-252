"""Verifica RF-10/11 y trazabilidad RNF-06 con consultas Athena reales."""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from deploy import HERE, config, outputs


QUERIES = {
    "rf10": """SELECT stage, COUNT(DISTINCT session_id) AS sessions
               FROM tiendacol_gold.fact_funnel_event
               GROUP BY stage ORDER BY stage""",
    "rf11": """SELECT COUNT(DISTINCT order_id) AS orders,
                      ROUND(SUM(net_value), 2) AS sales_cop,
                      ROUND(SUM(net_value) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS avg_ticket_cop
               FROM tiendacol_gold.fact_sales""",
    "rnf06": """SELECT COUNT(*) AS total_gold_rows,
                       COUNT(b.event_id) AS linked_to_bronze,
                       COUNT(*) - COUNT(b.event_id) AS unlinked_events
                FROM (SELECT event_id FROM tiendacol_gold.fact_sales
                      UNION ALL
                      SELECT event_id FROM tiendacol_gold.fact_funnel_event) g
                LEFT JOIN (SELECT DISTINCT event_id FROM tiendacol_lake.bronze_events) b
                  ON b.event_id = g.event_id""",
}


def query(athena, sql: str, workgroup: str, output: str) -> list[list[str]]:
    execution = athena.start_query_execution(
        QueryString=sql, WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output},
    )["QueryExecutionId"]
    for _ in range(90):
        status = athena.get_query_execution(QueryExecutionId=execution)["QueryExecution"]["Status"]
        if status["State"] == "SUCCEEDED":
            result = athena.get_query_results(QueryExecutionId=execution)
            return [[cell.get("VarCharValue", "") for cell in row["Data"]]
                    for row in result["ResultSet"]["Rows"]]
        if status["State"] in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"{execution}: {status.get('StateChangeReason', status['State'])}")
        time.sleep(2)
    raise TimeoutError(f"Athena no terminó: {execution}")


def main() -> None:
    cfg, session = config()
    out = outputs(session.client("cloudformation"), f"{cfg['PROJECT_PREFIX']}-e2")
    athena = session.client("athena")
    result = {"checked_at": datetime.now(timezone.utc).isoformat(), "queries": {}}
    for name, sql in QUERIES.items():
        result["queries"][name] = query(
            athena, sql, out["AthenaWorkGroup"], f"s3://{out['OpsBucket']}/athena-results/",
        )
    stages = {row[0] for row in result["queries"]["rf10"][1:]}
    sales = result["queries"]["rf11"][1]
    trace = result["queries"]["rnf06"][1]
    result["ok"] = ({"page_view", "add_to_cart", "purchase"} <= stages
                    and int(sales[0]) > 0 and float(sales[1]) > 0
                    and int(trace[0]) > 0 and int(trace[2]) == 0)
    path = HERE / "evidence" / "aws-scenarios" / "observed_gold.json"
    path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    if not result["ok"]:
        raise AssertionError("RF-10, RF-11 o RNF-06 no pasó")


if __name__ == "__main__":
    main()
