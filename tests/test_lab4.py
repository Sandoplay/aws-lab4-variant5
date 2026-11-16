"""Запуск:  python -m unittest discover -s tests -v   (з кореня lab4-variant5)"""
import copy
import json
import os
import sys
import unittest
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "layer", "python"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import importlib.util  # noqa: E402


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, rel))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


json_fn = _load("json_fn", "functions/json_ingest/lambda_function.py")
csv_fn = _load("csv_fn", "functions/csv_ingest/lambda_function.py")

from fakes import FakeDynamo, FakeS3  # noqa: E402
from lab4_common.events import parse_s3_event  # noqa: E402
from lab4_common.pipeline import SimulatedFailure, process_event  # noqa: E402
from lab4_common.records import split_valid  # noqa: E402
from lab4_common.storage import STAT_PK, Repository  # noqa: E402

with open(os.path.join(ROOT, "events", "sample_s3_event.json"), encoding="utf-8") as f:
    SAMPLE_EVENT = json.load(f)

CLOCK = lambda: datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)  # noqa: E731
PEOPLE_JSON = json.dumps([{"id": "1", "name": "Alice", "age": 22}, {"id": "2", "name": "Bob", "age": 31}])
PEOPLE_CSV = "id,name,age\n1,Alice,22\n2,Bob,31\n"


def event_for(key, etag="e1", bucket="amogusadmin-lab4-ingest", n=1):
    ev = copy.deepcopy(SAMPLE_EVENT)
    rec = ev["Records"][0]
    rec["s3"]["bucket"]["name"] = bucket
    rec["s3"]["object"]["key"] = key
    rec["s3"]["object"]["eTag"] = etag
    ev["Records"] = [copy.deepcopy(rec) for _ in range(n)]
    for i, r in enumerate(ev["Records"][1:], 1):
        r["s3"]["object"]["key"] = f"{key}.{i}"
        r["s3"]["object"]["eTag"] = f"{etag}-{i}"
    return ev


def run(fmt, event, objects, ddb, **kw):
    parse = json_fn.parse_json if fmt == "json" else csv_fn.parse_csv
    return process_event(event, fmt=fmt, parse=parse, s3=FakeS3(objects),
                         repo=Repository(ddb, "lab4-ingest", sleep=lambda s: None), clock=CLOCK, **kw)


class EventParsing(unittest.TestCase):
    def test_sample_event_fields(self):
        ref, = parse_s3_event(SAMPLE_EVENT)
        self.assertEqual(ref.bucket, "amogusadmin-lab4-ingest")
        self.assertEqual(ref.key, "incoming/people 1.json")          # '+' -> пробіл
        self.assertEqual(ref.idempotency_key, "amogusadmin-lab4-ingest/incoming/people 1.json#0123456789abcdef0123456789abcdef")

    def test_version_id_preferred_over_etag(self):
        ev = event_for("incoming/a.json")
        ev["Records"][0]["s3"]["object"]["versionId"] = "v42"
        self.assertTrue(parse_s3_event(ev)[0].idempotency_key.endswith("#v42"))

    def test_all_records_are_parsed(self):
        self.assertEqual(len(parse_s3_event(event_for("incoming/a.json", n=3))), 3)

    def test_not_s3_event(self):
        with self.assertRaises(ValueError):
            parse_s3_event({"foo": "bar"})


class Records(unittest.TestCase):
    def test_json_and_csv_give_same_people(self):
        self.assertEqual(split_valid(json_fn.parse_json(PEOPLE_JSON)), split_valid(csv_fn.parse_csv(PEOPLE_CSV)))

    def test_rejections(self):
        ok, bad = split_valid([{"id": "1", "name": "A", "age": "x"}, {"id": "2", "name": "B"},
                               {"id": "3", "name": "C", "age": True}, {"id": "4", "name": "D", "age": 5}])
        self.assertEqual([p.id for p in ok], ["4"])
        self.assertEqual(len(bad), 3)

    def test_csv_wrong_header(self):
        with self.assertRaisesRegex(ValueError, r"missing=\['age'\], extra=\['email'\]"):
            csv_fn.parse_csv("id,name,email\n1,A,a@x\n")


class Pipeline(unittest.TestCase):
    def test_json_file_written_and_counted(self):
        ddb = FakeDynamo()
        res = run("json", event_for("incoming/p.json"), {"incoming/p.json": PEOPLE_JSON}, ddb)
        self.assertEqual(res[0]["status"], "OK")
        self.assertEqual(ddb.items["REC#1"]["age"], 22)
        self.assertIsInstance(ddb.items["REC#1"]["age"], int)
        self.assertEqual(ddb.items[STAT_PK]["files"], 1)
        self.assertEqual(ddb.items[STAT_PK]["json"], 1)

    def test_same_item_format_for_both_functions(self):
        a, b = FakeDynamo(), FakeDynamo()
        run("json", event_for("incoming/p.json"), {"incoming/p.json": PEOPLE_JSON}, a)
        run("csv", event_for("incoming/p.csv"), {"incoming/p.csv": PEOPLE_CSV}, b)
        strip = lambda it: {k: v for k, v in it.items() if k not in ("sourceFormat", "sourceKey")}  # noqa: E731
        self.assertEqual(strip(a.items["REC#2"]), strip(b.items["REC#2"]))

    def test_duplicate_event_counted_once(self):
        ddb = FakeDynamo()
        ev = event_for("incoming/p.json")
        run("json", ev, {"incoming/p.json": PEOPLE_JSON}, ddb)
        res = run("json", ev, {"incoming/p.json": PEOPLE_JSON}, ddb)
        self.assertEqual(res[0]["status"], "DUPLICATE")
        self.assertEqual(ddb.items[STAT_PK]["files"], 1)
        self.assertEqual(ddb.items[STAT_PK]["records"], 2)

    def test_overwritten_object_is_new_event(self):
        ddb = FakeDynamo()
        run("json", event_for("incoming/p.json", etag="e1"), {"incoming/p.json": PEOPLE_JSON}, ddb)
        run("json", event_for("incoming/p.json", etag="e2"), {"incoming/p.json": PEOPLE_JSON}, ddb)
        self.assertEqual(ddb.items[STAT_PK]["files"], 2)

    def test_multiple_records_in_one_event(self):
        ddb = FakeDynamo()
        objs = {"incoming/p.json": PEOPLE_JSON, "incoming/p.json.1": PEOPLE_JSON, "incoming/p.json.2": PEOPLE_JSON}
        res = run("json", event_for("incoming/p.json", n=3), objs, ddb)
        self.assertEqual(len(res), 3)
        self.assertEqual(ddb.items[STAT_PK]["files"], 3)

    def test_invalid_file_completes_successfully(self):
        ddb = FakeDynamo()
        res = run("csv", event_for("incoming/bad.csv"), {"incoming/bad.csv": "a,b\n1,2\n"}, ddb)
        self.assertEqual(res[0]["status"], "INVALID_FILE")
        self.assertEqual(ddb.items[STAT_PK]["invalidFiles"], 1)

    def test_batches_of_25_and_unprocessed_retry(self):
        ddb = FakeDynamo(unprocessed_first_calls=2)
        rows = "id,name,age\n" + "".join(f"{i},N{i},{20 + i % 30}\n" for i in range(60))
        run("csv", event_for("incoming/big.csv"), {"incoming/big.csv": rows}, ddb)
        self.assertEqual(len([k for k in ddb.items if k.startswith("REC#")]), 60)
        self.assertTrue(all(n <= 25 for n in ddb.batch_calls))
        self.assertGreater(len(ddb.batch_calls), 3)        # були повтори UnprocessedItems

    def test_simulated_failure_raises(self):
        with self.assertRaises(SimulatedFailure):
            run("json", event_for("incoming/p.json"), {"incoming/p.json": PEOPLE_JSON}, FakeDynamo(),
                simulate_failure=True)


class Counters(unittest.TestCase):
    """Детермінована імітація двох паралельних викликів."""

    def _race(self, mode):
        ddb = FakeDynamo()
        other = Repository(ddb, "t")

        def sleep_hook(_):
            # поки перший виклик «спить» між читанням і записом, виконується другий
            other.commit_file(idem_key="k2", fmt="json", accepted=1, rejected=0, status="OK",
                              now="t", epoch=0, mode=mode)

        first = Repository(ddb, "t", sleep=sleep_hook, naive_delay_s=0.5)
        if mode == "atomic":      # в атомарному режимі вікна гонки немає взагалі
            sleep_hook(None)
        first.commit_file(idem_key="k1", fmt="json", accepted=1, rejected=0, status="OK",
                          now="t", epoch=0, mode=mode)
        return ddb.items[STAT_PK]["files"]

    def test_naive_read_modify_write_loses_update(self):
        self.assertEqual(self._race("naive"), 1)   # два файли, а лічильник = 1

    def test_atomic_add_counts_both(self):
        self.assertEqual(self._race("atomic"), 2)


if __name__ == "__main__":
    unittest.main()
