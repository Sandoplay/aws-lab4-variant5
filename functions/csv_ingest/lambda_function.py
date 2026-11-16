"""lab4-ingest-csv: викликається лише правилом сповіщення incoming/ + .csv.

У коді немає перевірки розширення файлу – розрізнення форматів робить фільтр S3.
Уся спільна логіка – у шарі lab4-common.
"""
import csv
import io

from lab4_common.handler import make_handler
from lab4_common.records import FIELDS


def parse_csv(text: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text))
    headers = [h.strip() for h in (reader.fieldnames or [])]
    missing = sorted(set(FIELDS) - set(headers))
    extra = sorted(set(headers) - set(FIELDS))
    if missing or extra:
        raise ValueError(f"wrong CSV header: missing={missing}, extra={extra}")
    reader.fieldnames = headers
    return [dict(row) for row in reader]


lambda_handler = make_handler("csv", parse_csv)
