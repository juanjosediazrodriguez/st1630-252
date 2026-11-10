"""Guarda evidencia de alarmas, regla de fallos, presupuesto y SNS confirmado."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from deploy import config, outputs


def main() -> None:
    cfg, session = config()
    stack = f"{cfg['PROJECT_PREFIX']}-e2"
    cfn = session.client("cloudformation")
    out = outputs(cfn, stack)
    resources = {r["LogicalResourceId"]: r["PhysicalResourceId"]
                 for page in cfn.get_paginator("list_stack_resources").paginate(StackName=stack)
                 for r in page["StackResourceSummaries"]}
    cloudwatch = session.client("cloudwatch")
    alarm_ids = ("AlertsDlqAlarm", "CartsDlqAlarm", "IteratorAgeAlarm")
    alarms = cloudwatch.describe_alarms(AlarmNames=[resources[x] for x in alarm_ids])["MetricAlarms"]
    alarm_data = {a["AlarmName"]: {"metric": a["MetricName"], "state": a["StateValue"],
                                  "threshold": a["Threshold"], "actions": len(a.get("AlarmActions", []))}
                  for a in alarms}
    rule = session.client("events").describe_rule(Name=resources["GlueFailureRule"])
    subs = session.client("sns").list_subscriptions_by_topic(TopicArn=out["AlertsTopicArn"])["Subscriptions"]
    budget = session.client("budgets").describe_budget(
        AccountId=session.client("sts").get_caller_identity()["Account"],
        BudgetName=f"{cfg['PROJECT_PREFIX']}-monthly")["Budget"]
    data = {"checked_at": datetime.now(timezone.utc).isoformat(),
            "stack_status": cfn.describe_stacks(StackName=stack)["Stacks"][0]["StackStatus"],
            "alarms": alarm_data, "glue_failure_rule": rule["State"],
            "sns_confirmed": any(s["SubscriptionArn"].startswith("arn:aws:sns:") for s in subs),
            "monthly_budget_usd": budget["BudgetLimit"]["Amount"]}
    data["ok"] = (data["stack_status"] in ("CREATE_COMPLETE", "UPDATE_COMPLETE")
                  and len(alarms) == 3 and all(a["actions"] for a in alarm_data.values())
                  and rule["State"] == "ENABLED" and data["sns_confirmed"])
    path = Path(__file__).parent / "evidence" / "aws-scenarios" / "observed_monitoring.json"
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(json.dumps(data, indent=2))
    if not data["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
