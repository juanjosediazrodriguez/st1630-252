"""Consulta de negocio de demostración: órdenes, ventas y ticket promedio en Gold."""
from __future__ import annotations

import time

from deploy import config


def main() -> None:
    cfg, session = config()
    out = {x["OutputKey"]: x["OutputValue"] for x in session.client("cloudformation").describe_stacks(
        StackName=f"{cfg['PROJECT_PREFIX']}-e2")["Stacks"][0]["Outputs"]}
    sql = """SELECT COUNT(DISTINCT order_id) AS orders,
       ROUND(SUM(net_value), 2) AS sales_cop,
       ROUND(SUM(net_value) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS avg_ticket_cop
FROM tiendacol_gold.fact_sales"""
    athena = session.client("athena")
    execution = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=out["AthenaWorkGroup"],
        ResultConfiguration={"OutputLocation": f"s3://{out['OpsBucket']}/athena-results/"},
    )["QueryExecutionId"]
    for _ in range(60):
        time.sleep(2)
        state = athena.get_query_execution(QueryExecutionId=execution)["QueryExecution"]["Status"]
        if state["State"] == "SUCCEEDED":
            rows = athena.get_query_results(QueryExecutionId=execution)["ResultSet"]["Rows"]
            for row in rows:
                print(" | ".join(c.get("VarCharValue", "") for c in row["Data"]))
            return
        if state["State"] in ("FAILED", "CANCELLED"):
            raise RuntimeError(state.get("StateChangeReason", state["State"]))
    raise TimeoutError("La consulta Athena no terminó")


if __name__ == "__main__":
    main()
