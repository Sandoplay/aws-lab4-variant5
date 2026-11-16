"""Розбір події S3 -> список S3ObjectRef. Формат події визначає джерело, тому він ізольований тут."""
from dataclasses import dataclass
from urllib.parse import unquote_plus


@dataclass(frozen=True)
class S3ObjectRef:
    bucket: str            # Records[i].s3.bucket.name
    key: str               # Records[i].s3.object.key (URL-декодований)
    version_id: str | None  # Records[i].s3.object.versionId (лише якщо увімкнено версіонування)
    etag: str | None       # Records[i].s3.object.eTag
    size: int              # Records[i].s3.object.size
    event_time: str        # Records[i].eventTime
    event_name: str        # Records[i].eventName, напр. ObjectCreated:Put

    @property
    def idempotency_key(self) -> str:
        """bucket/key#versionId, або bucket/key#eTag, якщо версіонування вимкнено."""
        marker = self.version_id or self.etag
        if not marker:
            raise ValueError(f"no versionId/eTag for s3://{self.bucket}/{self.key}")
        return f"{self.bucket}/{self.key}#{marker}"


def parse_s3_event(event: dict) -> list[S3ObjectRef]:
    """Повертає по одному S3ObjectRef на КОЖЕН елемент Records (а не лише Records[0])."""
    records = event.get("Records")
    if not isinstance(records, list) or not records:
        raise ValueError("event has no Records array – not an S3 notification")

    refs = []
    for rec in records:
        if rec.get("eventSource") != "aws:s3":
            raise ValueError(f"unexpected eventSource: {rec.get('eventSource')!r}")
        s3 = rec["s3"]
        obj = s3["object"]
        refs.append(S3ObjectRef(
            bucket=s3["bucket"]["name"],
            # у події ключ закодований як у URL: пробіл -> '+', кирилиця -> %D0%...
            key=unquote_plus(obj["key"]),
            version_id=obj.get("versionId") or None,
            etag=obj.get("eTag") or None,
            size=int(obj.get("size", 0)),
            event_time=rec.get("eventTime", ""),
            event_name=rec.get("eventName", ""),
        ))
    return refs
