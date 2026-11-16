"""Обробка однієї події: для кожного Records[i] – прочитати об'єкт, розібрати, записати."""
import logging
from datetime import datetime, timezone

from .events import parse_s3_event, S3ObjectRef
from .records import split_valid

log = logging.getLogger("lab4")


class SimulatedFailure(RuntimeError):
    pass


def process_event(event, *, fmt, parse, s3, repo, mode="atomic", simulate_failure=False, clock=None):
    clock = clock or (lambda: datetime.now(timezone.utc))
    refs = parse_s3_event(event)
    return [process_object(r, fmt=fmt, parse=parse, s3=s3, repo=repo, mode=mode,
                           simulate_failure=simulate_failure, clock=clock) for r in refs]


def process_object(ref: S3ObjectRef, *, fmt, parse, s3, repo, mode, simulate_failure, clock):
    ctx = {"format": fmt, "bucket": ref.bucket, "key": ref.key, "size": ref.size,
           "eventTime": ref.event_time, "idempotencyKey": ref.idempotency_key}
    log.info("object received", extra=ctx)

    if simulate_failure:
        # для частини E: виклик падає -> 2 повтори -> подія потрапляє в чергу збоїв
        raise SimulatedFailure(f"simulated failure for {ref.key} (SIMULATE_FAILURE=true)")

    get_args = {"Bucket": ref.bucket, "Key": ref.key}
    if ref.version_id:
        get_args["VersionId"] = ref.version_id
    body = s3.get_object(**get_args)["Body"].read().decode("utf-8-sig")

    now_dt = clock()
    now, epoch = now_dt.isoformat(timespec="milliseconds"), int(now_dt.timestamp())

    try:
        raws = parse(body)
    except ValueError as e:
        # некоректні вхідні дані – не збій обробника: повтори не допоможуть,
        # тому фіксуємо помилку в журналі та в таблиці й завершуємо успішно
        log.error("invalid file, skipped", extra={**ctx, "reason": str(e)})
        first = repo.commit_file(idem_key=ref.idempotency_key, fmt=fmt, accepted=0, rejected=0,
                                 status="INVALID_FILE", now=now, epoch=epoch, mode=mode)
        return {"key": ref.key, "status": "INVALID_FILE" if first else "DUPLICATE"}

    people, rejected = split_valid(raws)
    for r in rejected:
        log.warning("record rejected", extra={**ctx, **r})

    repo.put_people(people, fmt=fmt, source_key=ref.key, now=now)
    first = repo.commit_file(idem_key=ref.idempotency_key, fmt=fmt, accepted=len(people),
                             rejected=len(rejected), status="OK", now=now, epoch=epoch, mode=mode)
    if not first:
        log.info("duplicate, skipped", extra=ctx)
        return {"key": ref.key, "status": "DUPLICATE"}

    log.info("file processed", extra={**ctx, "accepted": len(people), "rejected": len(rejected), "counterMode": mode})
    return {"key": ref.key, "status": "OK", "accepted": len(people), "rejected": len(rejected)}
