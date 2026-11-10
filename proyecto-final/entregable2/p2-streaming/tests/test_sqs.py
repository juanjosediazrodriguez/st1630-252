"""sqs.py: modo file, modo aws con un cliente boto3 falso (sin red) y modo off."""
import json

import pytest

from tiendacol_stream.config import Config
from tiendacol_stream.sqs import SqsError, SqsPublisher


def cfg(tmp_path, mode, **kw):
    return Config(lake_root=str(tmp_path), checkpoint_root=str(tmp_path / "ck"), sqs_mode=mode,
                  decisions_dir=str(tmp_path / "dec"), alerts_queue_url="https://sqs/alertas",
                  carts_queue_url="https://sqs/carritos", **kw)


def msgs(n):
    return [{"schema_version": "1.0", "decision_type": "x", "alert_id": f"id-{i}"} for i in range(n)]


class FakeSqs:
    """Cliente boto3 falso: registra las llamadas y puede rechazar entradas."""

    def __init__(self, fail_ids_per_call=None):
        self.calls, self.fail = [], list(fail_ids_per_call or [])

    def send_message_batch(self, QueueUrl, Entries):
        self.calls.append((QueueUrl, [dict(e) for e in Entries]))
        bad = self.fail.pop(0) if self.fail else set()
        return {"Successful": [{"Id": e["Id"]} for e in Entries if e["Id"] not in bad],
                "Failed": [{"Id": i, "SenderFault": False, "Code": "InternalError"} for i in bad]}


def test_modo_file_escribe_jsonl_con_received_at(tmp_path):
    pub = SqsPublisher(cfg(tmp_path, "file"))
    assert pub.publish("alerts", msgs(2)) == 2
    assert pub.publish("carts", msgs(1)) == 1
    lines = (tmp_path / "dec" / "alerts.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(l)["alert_id"] for l in lines] == ["id-0", "id-1"]
    r = json.loads(lines[0])["received_at"]
    assert r.endswith("Z") and "T" in r and len(r) == 24          # ISO-8601 UTC con milisegundos
    assert (tmp_path / "dec" / "carts.jsonl").exists()


def test_modo_file_acumula_entre_lotes(tmp_path):
    pub = SqsPublisher(cfg(tmp_path, "file"))
    pub.publish("alerts", msgs(1))
    pub.publish("alerts", msgs(1))
    assert len((tmp_path / "dec" / "alerts.jsonl").read_text().splitlines()) == 2


def test_modo_off_y_lista_vacia_no_hacen_nada(tmp_path):
    fake = FakeSqs()
    assert SqsPublisher(cfg(tmp_path, "off"), client=fake).publish("alerts", msgs(3)) == 0
    assert SqsPublisher(cfg(tmp_path, "aws"), client=fake).publish("alerts", []) == 0
    assert fake.calls == [] and not (tmp_path / "dec").exists()


def test_aws_envia_en_lotes_de_10_a_la_cola_correcta(tmp_path):
    fake = FakeSqs()
    pub = SqsPublisher(cfg(tmp_path, "aws"), client=fake)
    assert pub.publish("alerts", msgs(25)) == 25
    assert [len(c[1]) for c in fake.calls] == [10, 10, 5]
    assert {c[0] for c in fake.calls} == {"https://sqs/alertas"}
    first = fake.calls[0][1][0]
    assert json.loads(first["MessageBody"])["alert_id"] == "id-0" and first["Id"] == "0"
    SqsPublisher(cfg(tmp_path, "aws"), client=fake).publish("carts", msgs(1))
    assert fake.calls[-1][0] == "https://sqs/carritos"


def test_aws_reintenta_solo_los_mensajes_fallidos(tmp_path):
    fake = FakeSqs(fail_ids_per_call=[{"1", "3"}, {"3"}])          # el 3 falla dos veces
    esperas = []
    pub = SqsPublisher(cfg(tmp_path, "aws"), client=fake, sleep=esperas.append)
    pub.publish("alerts", msgs(5))
    assert [sorted(e["Id"] for e in c[1]) for c in fake.calls] == [["0", "1", "2", "3", "4"], ["1", "3"], ["3"]]
    assert len(esperas) == 2 and esperas[1] > esperas[0]            # espera exponencial


def test_aws_si_sigue_fallando_levanta_error_para_que_spark_reintente_el_lote(tmp_path):
    fake = FakeSqs(fail_ids_per_call=[{"0"}, {"0"}, {"0"}])
    pub = SqsPublisher(cfg(tmp_path, "aws"), client=fake, sleep=lambda s: None, max_attempts=3)
    with pytest.raises(SqsError, match="rechazó 1 mensajes"):
        pub.publish("alerts", msgs(1))
    assert len(fake.calls) == 3


def test_errores_de_configuracion(tmp_path):
    c = Config(lake_root=str(tmp_path), checkpoint_root=str(tmp_path), sqs_mode="aws")
    with pytest.raises(SqsError, match="alerts_queue_url"):
        SqsPublisher(c, client=FakeSqs()).publish("alerts", msgs(1))
    c2 = Config(lake_root=str(tmp_path), checkpoint_root=str(tmp_path), sqs_mode="file")
    with pytest.raises(SqsError, match="decisions_dir"):
        SqsPublisher(c2).publish("alerts", msgs(1))
