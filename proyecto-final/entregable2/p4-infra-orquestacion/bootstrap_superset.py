"""Crea conexión, datasets, gráficos y dashboard de TiendaCol en Superset 3.1.3.

Se ejecuta dentro del contenedor con ATHENA_URI definido por el rol IAM de EC2.
No usa claves AWS estáticas y es idempotente por nombre.
"""
from __future__ import annotations

import json
import os

from superset.app import create_app


DATASETS = {
    "TiendaCol ventas y ticket": """
        SELECT COUNT(DISTINCT order_id) AS orders,
               ROUND(SUM(net_value), 2) AS sales_cop,
               ROUND(SUM(net_value) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS avg_ticket_cop
        FROM fact_sales
    """,
    "TiendaCol ventas por minuto": """
        SELECT minute_start, orders, sales_cop
        FROM agg_sales_minute WHERE category_id IS NULL
    """,
    "TiendaCol embudo": """
        SELECT stage, COUNT(DISTINCT session_id) AS sessions
        FROM fact_funnel_event GROUP BY stage
    """,
    "TiendaCol ventas por categoría": """
        SELECT p.category, COUNT(DISTINCT f.order_id) AS orders,
               ROUND(SUM(f.net_value), 2) AS sales_cop
        FROM fact_sales f JOIN dim_product p ON p.product_key = f.product_key
        GROUP BY p.category
    """,
}

CHARTS = {
    "Ventas y ticket promedio": ("TiendaCol ventas y ticket", ["orders", "sales_cop", "avg_ticket_cop"]),
    "Ventas por minuto": ("TiendaCol ventas por minuto", ["minute_start", "orders", "sales_cop"]),
    "Embudo de conversión": ("TiendaCol embudo", ["stage", "sessions"]),
    "Ventas por categoría": ("TiendaCol ventas por categoría", ["category", "orders", "sales_cop"]),
}


def main() -> None:
    uri = os.environ["ATHENA_URI"]
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        database = db.session.query(Database).filter_by(database_name="TiendaCol Athena").one_or_none()
        if database is None:
            database = Database(database_name="TiendaCol Athena", sqlalchemy_uri=uri,
                                allow_dml=False, allow_ctas=False, expose_in_sqllab=True)
            db.session.add(database)
            db.session.commit()
        elif database.sqlalchemy_uri != uri:
            database.sqlalchemy_uri = uri
            db.session.commit()

        datasets = {}
        for name, sql in DATASETS.items():
            table = db.session.query(SqlaTable).filter_by(database_id=database.id, table_name=name).one_or_none()
            if table is None:
                table = SqlaTable(database_id=database.id, table_name=name, schema="tiendacol_gold", sql=sql)
                db.session.add(table)
                db.session.commit()
            elif table.sql != sql:
                table.sql = sql
                db.session.commit()
            table.fetch_metadata()
            datasets[name] = table

        charts = []
        for title, (dataset_name, columns) in CHARTS.items():
            table = datasets[dataset_name]
            params = {"datasource": f"{table.id}__table", "viz_type": "table", "query_mode": "raw",
                      "all_columns": columns, "row_limit": 1000, "order_desc": True,
                      "show_cell_bars": False, "server_page_length": 25}
            query_context = {"datasource": {"id": table.id, "type": "table"},
                             "queries": [{"columns": columns, "metrics": [], "filters": [],
                                          "extras": {}, "row_limit": 1000, "order_desc": True}],
                             "form_data": params, "result_format": "json", "result_type": "full"}
            chart = db.session.query(Slice).filter_by(slice_name=title).one_or_none()
            if chart is None:
                chart = Slice(slice_name=title, datasource_id=table.id, datasource_type="table",
                              viz_type="table", params=json.dumps(params), cache_timeout=0)
                db.session.add(chart)
            else:
                chart.datasource_id = table.id
                chart.datasource_type = "table"
                chart.viz_type = "table"
                chart.params = json.dumps(params)
            chart.query_context = json.dumps(query_context)
            db.session.commit()
            charts.append(chart)

        dashboard = db.session.query(Dashboard).filter_by(dashboard_title="TiendaCol · Operación y ventas").one_or_none()
        if dashboard is None:
            dashboard = Dashboard(dashboard_title="TiendaCol · Operación y ventas", published=True)
            db.session.add(dashboard)
        dashboard.slices = charts
        db.session.commit()
        layout = {"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
                  "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": []}}
        for index, chart in enumerate(charts):
            row, component = f"ROW-{index}", f"CHART-{chart.id}"
            layout["GRID_ID"]["children"].append(row)
            layout[row] = {"type": "ROW", "id": row, "children": [component],
                           "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            layout[component] = {"type": "CHART", "id": component, "children": [],
                                 "meta": {"chartId": chart.id, "width": 12, "height": 50}}
        dashboard.position_json = json.dumps(layout)
        db.session.commit()
        print(json.dumps({"database": database.id, "datasets": {k: v.id for k, v in datasets.items()},
                          "charts": {c.slice_name: c.id for c in charts}, "dashboard": dashboard.id}))


if __name__ == "__main__":
    main()
