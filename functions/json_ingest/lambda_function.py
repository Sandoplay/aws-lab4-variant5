"""lab4-ingest-json: викликається лише правилом сповіщення incoming/ + .json.

У коді немає перевірки розширення файлу – розрізнення форматів робить фільтр S3.
Уся спільна логіка – у шарі lab4-common.
"""
import json

from lab4_common.handler import make_handler


def parse_json(text: str) -> list[dict]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:          # підклас ValueError
        raise ValueError(f"not valid JSON: {e}") from e
    if not isinstance(data, list):
        raise ValueError("top-level JSON value must be an array of objects")
    return data


lambda_handler = make_handler("json", parse_json)
