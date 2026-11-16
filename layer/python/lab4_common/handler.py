"""Фабрика lambda_handler. Кожна функція передає лише назву формату й свій парсер."""
import logging
import os

from .pipeline import process_event
from .storage import Repository

log = logging.getLogger("lab4")
log.setLevel(logging.INFO)

_clients = {}


def _get_clients():
    # створюються один раз на середовище виконання (під час cold start) і перевикористовуються
    if not _clients:
        import boto3
        _clients["s3"] = boto3.client("s3")
        _clients["ddb"] = boto3.client("dynamodb")
    return _clients["s3"], _clients["ddb"]


def make_handler(fmt: str, parse):
    def lambda_handler(event, context):
        # Частина B.1: повна подія в журналі (формат журналу функції – JSON)
        log.info("event received", extra={"format": fmt, "event": event})
        s3, ddb = _get_clients()
        repo = Repository(ddb, os.environ["TABLE_NAME"],
                          naive_delay_s=int(os.environ.get("NAIVE_DELAY_MS", "0")) / 1000)
        results = process_event(
            event, fmt=fmt, parse=parse, s3=s3, repo=repo,
            mode=os.environ.get("COUNTER_MODE", "atomic"),
            simulate_failure=os.environ.get("SIMULATE_FAILURE", "false").lower() == "true",
        )
        return {"format": fmt, "results": results}
    return lambda_handler
