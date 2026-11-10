import json
from collections import Counter
from pathlib import Path

import jsonschema
import pytest

from tiendacol_gen.events import CLICKSTREAM, ORDERS, STOCK, EventFactory
from tiendacol_gen.masters import build_masters, upload_to_s3, write_masters
from tiendacol_gen.sinks import KinesisSink, chunk_records

CONTRACTS = Path(__file__).parent.parent / "contratos"
SCHEMAS = {s: json.loads((CONTRACTS / f"{s}.schema.json").read_text(encoding="utf-8")) for s in (CLICKSTREAM, ORDERS, STOCK)}
VALIDATORS = {s: jsonschema.Draft202012Validator(sch) for s, sch in SCHEMAS.items()}  # compilados: validate() es lento


def validate(stream, event):
    VALIDATORS[stream].validate(event)


@pytest.fixture(scope="module")
def masters():
    return build_masters(seed=1, n_products=300, n_customers=500, n_sellers=20)


def _gen(masters, n=20_000, **kw):
    f = EventFactory(masters, seed=7, **kw)
    return f, f.next_batch(n)


def test_every_valid_event_matches_its_contract(masters):
    f, recs = _gen(masters, 20_000, dup_rate=0, late_rate=0, malformed_rate=0)
    assert Counter(r.stream_type for r in recs).keys() == {CLICKSTREAM, ORDERS, STOCK}
    for r in recs:
        validate(r.stream_type, r.event)


def test_event_ids_unique_without_mutations(masters):
    _, recs = _gen(masters, dup_rate=0, late_rate=0, malformed_rate=0)
    ids = [r.event_id for r in recs]
    assert len(ids) == len(set(ids))


def test_deterministic_for_same_seed(masters):
    from datetime import datetime, timezone
    now = datetime(2026, 11, 8, tzinfo=timezone.utc)
    a = EventFactory(masters, seed=5).next_batch(500, now=now)
    b = EventFactory(masters, seed=5).next_batch(500, now=now)
    assert [r.payload() for r in a] == [r.payload() for r in b]


def test_duplicates_are_byte_identical_copies(masters):
    f = EventFactory(masters, seed=3, dup_rate=0.2, late_rate=0, malformed_rate=0, dup_max_delay=0)
    recs = f.next_batch(2000) + f.next_batch(500, now_s=1)
    by_id = {}
    dups = 0
    for r in recs:
        if "duplicate" in r.flags:
            dups += 1
            assert r.payload() == by_id[r.event_id].payload()
        else:
            by_id[r.event_id] = r
    assert dups > 0
    assert f.stats.duplicates >= dups


def test_late_events_are_shifted_back_beyond_watermark(masters):
    from datetime import datetime, timezone
    from tiendacol_gen.verify import parse_ts
    f = EventFactory(masters, seed=3, dup_rate=0, late_rate=0.3, malformed_rate=0, late_seconds=120)
    now = datetime.now(timezone.utc)
    recs = f.next_batch(1000, now=now)
    late = [r for r in recs if "late" in r.flags]
    assert late
    assert all((now - parse_ts(r.event["event_ts"])).total_seconds() >= 119 for r in late)


def test_malformed_never_validate_and_never_duplicate(masters):
    f = EventFactory(masters, seed=9, dup_rate=0.5, late_rate=0, malformed_rate=0.2)
    recs = f.next_batch(3000)
    bad = [r for r in recs if any(x.startswith("malformed") for x in r.flags)]
    assert bad and len(bad) == f.stats.malformed
    for r in bad:
        assert "duplicate" not in r.flags
        if r.event is None:
            with pytest.raises(json.JSONDecodeError):
                json.loads(r.payload())
        else:
            with pytest.raises(jsonschema.ValidationError):
                validate(r.stream_type, r.event)


def test_partition_keys_follow_design(masters):
    _, recs = _gen(masters, 5000, dup_rate=0, late_rate=0, malformed_rate=0)
    for r in recs:
        if r.stream_type == CLICKSTREAM:
            assert r.key == (r.event["user_id"] or r.event["session_id"])  # RF-08: orden por usuario
        elif r.stream_type == ORDERS:
            assert r.key == r.event["order_id"]
        else:
            assert r.key == r.event["product_id"]


def test_purchase_links_click_order_and_stock(masters):
    _, recs = _gen(masters, 30_000, dup_rate=0, late_rate=0, malformed_rate=0)
    purchases = {r.event["order_id"]: r.event for r in recs if r.stream_type == CLICKSTREAM and r.event["event_type"] == "purchase"}
    orders = {r.event["order_id"]: r.event for r in recs if r.event["event_type"] == "order_created"}
    assert purchases and set(purchases) == set(orders)
    for oid, o in orders.items():
        assert o["total"] == purchases[oid]["cart_value"]
    sold = Counter()
    for o in orders.values():
        for l in o["lines"]:
            sold[l["product_id"]] += l["quantity"]
    drained = Counter()
    for r in recs:
        if r.stream_type == STOCK and r.event["reason"] == "sale":
            drained[r.event["product_id"]] -= r.event["delta"]
    assert sold == drained


def test_funnel_abandonment_is_near_70_percent(masters):
    _, recs = _gen(masters, 150_000, dup_rate=0, late_rate=0, malformed_rate=0, active_sessions=500)
    carts = {r.event["session_id"] for r in recs if r.stream_type == CLICKSTREAM and r.event["event_type"] == "add_to_cart"}
    bought = {r.event["session_id"] for r in recs if r.stream_type == CLICKSTREAM and r.event["event_type"] == "purchase"}
    rate = 1 - len(bought & carts) / len(carts)
    assert 0.55 < rate < 0.85, rate


# ------------------------------------------------------------------ maestros
def test_masters_deterministic_and_drift_creates_scd2_changes():
    a = build_masters(seed=2, n_products=200, n_customers=300, n_sellers=10)
    b = build_masters(seed=2, n_products=200, n_customers=300, n_sellers=10)
    assert a.products == b.products
    d3 = build_masters(seed=2, day_index=3, n_products=200, n_customers=300, n_sellers=10)
    changed_price = [p for p in a.products if p in d3.products and a.products[p]["price"] != d3.products[p]["price"]]
    changed_cat = [p for p in a.products if p in d3.products and a.products[p]["category_id"] != d3.products[p]["category_id"]]
    assert changed_price and changed_cat and len(d3.products) > len(a.products)


def test_masters_counts_match_files_rf04(tmp_path, masters):
    manifest = write_masters(masters, tmp_path, "2026-11-08")
    for entity, n in manifest["counts"].items():
        lines = (tmp_path / manifest["files"][entity]).read_text(encoding="utf-8").splitlines()
        assert len(lines) - 1 == n, entity


def test_masters_json_products(tmp_path, masters):
    manifest = write_masters(masters, tmp_path, "2026-11-08", products_format="json")
    data = json.loads((tmp_path / manifest["files"]["products"]).read_text(encoding="utf-8"))
    assert len(data) == manifest["counts"]["products"] and isinstance(data[0]["attributes"], dict)


def test_upload_sends_manifest_last(tmp_path, masters):
    write_masters(masters, tmp_path, "2026-11-08")
    calls = []

    class S3:
        def upload_file(self, path, bucket, key):
            calls.append(key)
    n = upload_to_s3(tmp_path, "bkt", client=S3())
    assert n == len(calls) and calls[-1].startswith("landing/masters/_manifest")


def test_customers_have_no_pii(masters):
    assert set(next(iter(masters.customers.values()))) == {"customer_id", "city", "department", "created_at"}


# ------------------------------------------------------------------ Kinesis
class FakeKinesis:
    def __init__(self, fail_first=0):
        self.calls, self.fail_first, self.sent = 0, fail_first, []

    def put_records(self, StreamName, Records):
        self.calls += 1
        res, failed = [], 0
        for i, r in enumerate(Records):
            if self.fail_first and i % 2 == 0:
                res.append({"ErrorCode": "ProvisionedThroughputExceededException"})
                failed += 1
            else:
                res.append({"SequenceNumber": "1"})
                self.sent.append((StreamName, r["PartitionKey"], r["Data"]))
        self.fail_first = max(0, self.fail_first - 1)
        return {"FailedRecordCount": failed, "Records": res}


def test_chunking_respects_putrecords_limits(masters):
    _, recs = _gen(masters, 1234, dup_rate=0, late_rate=0, malformed_rate=0)
    chunks = list(chunk_records(recs))
    assert all(len(c) <= 500 for c in chunks) and sum(map(len, chunks)) == 1234


def test_kinesis_sink_retries_throttled_records_without_loss(masters):
    _, recs = _gen(masters, 1000, dup_rate=0, late_rate=0, malformed_rate=0)
    fake = FakeKinesis(fail_first=2)
    sink = KinesisSink({t: "tiendacol-events" for t in (CLICKSTREAM, ORDERS, STOCK)}, client=fake, workers=2)
    sink.send(recs)
    stats = sink.close()
    assert len(fake.sent) == 1000 and stats["ok"] == 1000 and stats.get("lost", 0) == 0
    assert stats["throttled_retries"] > 0
    assert {d for _, _, d in fake.sent} == {r.payload() for r in recs}


def test_kinesis_sink_routes_by_stream_map(masters):
    _, recs = _gen(masters, 800, dup_rate=0, late_rate=0, malformed_rate=0)
    fake = FakeKinesis()
    sink = KinesisSink({CLICKSTREAM: "c", ORDERS: "o", STOCK: "s"}, client=fake, workers=2)
    sink.send(recs)
    sink.close()
    by = {r.payload(): r.stream_type for r in recs}
    assert all({"c": CLICKSTREAM, "o": ORDERS, "s": STOCK}[name] == by[data] for name, _, data in fake.sent)
