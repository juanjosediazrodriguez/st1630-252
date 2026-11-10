"""Comprueba login, dashboard y datos de los cuatro gráficos en Superset AWS."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from deploy import config, outputs
from setup_superset import run_ssm


SCRIPT = r"""docker exec -i tiendacol-superset python - <<'PY'
import json, os, requests
base = 'http://127.0.0.1:8088'
login = requests.post(base + '/api/v1/security/login', json={
    'username': 'admin', 'password': os.environ['SUPERSET_ADMIN_PASSWORD'],
    'provider': 'db', 'refresh': True}, timeout=30)
login.raise_for_status()
headers = {'Authorization': 'Bearer ' + login.json()['access_token']}
dashboard = requests.get(base + '/api/v1/dashboard/1', headers=headers, timeout=30)
dashboard.raise_for_status()
charts = {}
for chart_id in (1, 2, 3, 4):
    chart = requests.get(base + f'/api/v1/chart/{chart_id}', headers=headers, timeout=30)
    chart.raise_for_status()
    detail = chart.json()['result']
    data = requests.post(base + '/api/v1/chart/data', headers=headers,
                         json=json.loads(detail['query_context']), timeout=120)
    data.raise_for_status()
    rows = data.json()['result'][0]['data']
    charts[detail['slice_name']] = {'id': chart_id, 'rows': len(rows)}
print('EVIDENCE=' + json.dumps({'dashboard_id': 1, 'charts': charts}))
PY"""


def main() -> None:
    cfg, session = config()
    out = outputs(session.client("cloudformation"), f"{cfg['PROJECT_PREFIX']}-e2")
    response = run_ssm(session, out["SupersetInstanceId"], [SCRIPT])
    line = next(line for line in response.splitlines() if line.startswith("EVIDENCE="))
    data = json.loads(line.removeprefix("EVIDENCE="))
    if len(data["charts"]) != 4 or any(c["rows"] < 1 for c in data["charts"].values()):
        raise AssertionError(data)
    data.update({"ok": True, "checked_at": datetime.now(timezone.utc).isoformat()})
    path = Path(__file__).parent / "evidence" / "aws-scenarios" / "observed_superset.json"
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(data, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
