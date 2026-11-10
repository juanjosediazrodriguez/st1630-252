# Bitácora de IA — P3 (E2)

**Responsable:** Juan José Díaz Rodríguez (P3)  
**Herramienta:** Gemini (Antigravity). P4 consolida la bitácora general del E2; estas son las filas correspondientes al frente P3 (Capa Gold, SCD Tipo 2 y consultas en Athena).

| Tarea | ¿Delegado a agente? | Justificación / cómo se verificó |
| :--- | :---: | :--- |
| Diseño del modelo dimensional en estrella (`dim_product` SCD2, `dim_seller`, `dim_customer`, `dim_location`, `dim_device`, `dim_datetime`, `fact_sales`, `fact_funnel_event`, `agg_product_affinity`) | No | Decisión arquitectónica central basada en el ADR-3 del E1 y alineada con los contratos de `silver/events` (P2) y `datasets.sql` (P1). |
| Implementación del motor de transformación Gold (`tiendacol_gold/`) y del job de AWS Glue (`glue/tiendacol_gold_batch.py`) | Parcial | Borrador generado con asistencia del agente; revisado y verificado contra el esquema `SILVER_SCHEMA` de P2 y el uso de `MERGE INTO delta.\`ruta\`` en Glue 4.0. |
| Redacción de DDLs Delta nativos para Athena v3 (`sql/ddl_gold.sql`) y consultas analíticas (`sql/consultas_athena.sql`) | Parcial | Generadas sobre el contrato del esquema en estrella; verificadas contra las reglas de negocio (`avg_ticket = SUM(net_value) / COUNT(DISTINCT order_id)` y conteo de sesiones distintas por etapa del embudo). |
| Pruebas automatizadas (`tests/test_gold_p3.py`) contra los oráculos de P1 (`funnel_oracle`, `sales_oracle`, `verify_rf13`, `verify_rnf06`) | Sí | Pruebas ejecutadas localmente con `pytest`, comprobando 0 discrepancias entre las consultas del modelo estrella y los oráculos de referencia. |
