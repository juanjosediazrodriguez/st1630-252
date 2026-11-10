import pytest

from tiendacol_stream.config import Config, parse_config


def test_ignora_argumentos_de_glue_y_aplica_defaults():
    cfg = parse_config(["--JOB_ID", "j-1", "--JOB_RUN_ID", "jr-1", "--JOB_NAME", "x",
                        "--lake_root", "s3://lago/", "--checkpoint_root", "s3://ops/ck/"])
    assert cfg.lake_root == "s3://lago" and cfg.checkpoint_root == "s3://ops/ck"
    assert cfg.masters_root == "s3://lago/landing/masters"   # por defecto bajo el lago
    assert cfg.source == "kinesis" and cfg.stream_name == "tiendacol-events" and cfg.region == "us-east-1"
    assert cfg.ingest_trigger_s == 30 and cfg.downstream_trigger_s == 10 and cfg.shuffle_partitions == 8
    assert (cfg.alert_horizon_min, cfg.alert_window_min, cfg.cart_min_value, cfg.cart_gap_min) == (60.0, 15, 500000.0, 30)
    assert cfg.alert_watermark == "1 minute" and cfg.cart_watermark == "3 minutes" and cfg.alert_cooldown_min == 60.0
    assert cfg.run_once is False and cfg.kinesis_endpoint is None and cfg.delta_max_files_per_trigger is None


def test_run_once_acepta_bandera_y_valor():
    base = ["--lake_root", "/l", "--checkpoint_root", "/c"]
    assert parse_config(base + ["--run_once"]).run_once is True
    assert parse_config(base + ["--run_once", "true"]).run_once is True
    assert parse_config(base + ["--run_once", "false"]).run_once is False


def test_parametros_vacios_de_glue_son_ausentes():
    cfg = parse_config(["--lake_root", "/l", "--checkpoint_root", "/c", "--kinesis_endpoint", "",
                        "--delta_max_files_per_trigger", "", "--queries", "ingesta, stock"])
    assert cfg.kinesis_endpoint is None and cfg.delta_max_files_per_trigger is None
    assert cfg.queries == ("ingesta", "stock")


def test_consulta_desconocida_falla():
    with pytest.raises(SystemExit):
        parse_config(["--lake_root", "/l", "--checkpoint_root", "/c", "--queries", "ingesta,nada"])


def test_rutas():
    c = Config(lake_root="/lago", checkpoint_root="/ck")
    assert c.silver == "/lago/silver/events" and c.bronze == "/lago/bronze/events"
    assert c.checkpoint("ingesta") == "/ck/ingesta" and c.agg_sales_minute == "/lago/gold/agg_sales_minute"
