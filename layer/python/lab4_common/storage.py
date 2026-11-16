"""Запис у DynamoDB через низькорівневий клієнт boto3.

Формат елементів однаковий для обох функцій:
  REC#<id>        name, age (Number), sourceFormat, sourceKey, ingestedAt
  IDEMP#<ключ>    processedAt, ttl, format, status, accepted, rejected
  STAT#ingest     files, records, rejected, invalidFiles, json, csv, updatedAt
"""
import time

BATCH_SIZE = 25          # межа BatchWriteItem
MAX_BATCH_ATTEMPTS = 6
IDEMP_TTL_SECONDS = 7 * 24 * 3600
STAT_PK = "STAT#ingest"


def to_av(value):
    """Python -> AttributeValue (потрібні лише S, N, BOOL)."""
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, (int, float)):
        return {"N": str(value)}
    if isinstance(value, str):
        return {"S": value}
    raise TypeError(f"unsupported type {type(value).__name__}")


def from_av(av: dict):
    (t, v), = av.items()
    if t == "N":
        return int(v) if v.lstrip("-").isdigit() else float(v)
    return v


def item_to_av(item: dict) -> dict:
    return {k: to_av(v) for k, v in item.items()}


def _error_code(exc) -> str | None:
    return getattr(exc, "response", {}).get("Error", {}).get("Code")


class Repository:
    def __init__(self, client, table_name: str, sleep=time.sleep, naive_delay_s: float = 0.0):
        self.client = client
        self.table = table_name
        self._sleep = sleep
        self.naive_delay_s = naive_delay_s

    # ---------- записи даних ----------
    def put_people(self, people, *, fmt: str, source_key: str, now: str) -> int:
        """BatchWriteItem по 25 елементів з повтором UnprocessedItems.

        PutItem з тим самим PK перезаписує елемент тими самими даними, тож повторна
        обробка тієї ж події не створює дублікатів – цей крок ідемпотентний сам по собі.
        """
        written = 0
        for start in range(0, len(people), BATCH_SIZE):
            chunk = people[start:start + BATCH_SIZE]
            requests = [{"PutRequest": {"Item": item_to_av({
                "PK": f"REC#{p.id}", "name": p.name, "age": p.age,
                "sourceFormat": fmt, "sourceKey": source_key, "ingestedAt": now,
            })}} for p in chunk]
            pending = {self.table: requests}
            for attempt in range(MAX_BATCH_ATTEMPTS):
                resp = self.client.batch_write_item(RequestItems=pending)
                pending = resp.get("UnprocessedItems") or {}
                if not pending:
                    break
                # тротлінг або перевищення місткості: чекаємо з експоненційною затримкою
                self._sleep(0.05 * (2 ** attempt))
            else:
                left = len(pending.get(self.table, []))
                raise RuntimeError(f"{left} items still unprocessed after {MAX_BATCH_ATTEMPTS} attempts")
            written += len(chunk)
        return written

    # ---------- фіксація файлу: ідемпотентність + лічильники ----------
    def commit_file(self, *, idem_key: str, fmt: str, accepted: int, rejected: int,
                    status: str, now: str, epoch: int, mode: str = "atomic") -> bool:
        """True – файл зараховано вперше; False – дублікат, пропущено."""
        idem_item = item_to_av({
            "PK": f"IDEMP#{idem_key}", "processedAt": now, "ttl": epoch + IDEMP_TTL_SECONDS,
            "format": fmt, "status": status, "accepted": accepted, "rejected": rejected,
        })
        invalid = 1 if status != "OK" else 0
        if mode == "naive":
            return self._commit_naive(idem_item, fmt, accepted, rejected, invalid, now)

        # Одна транзакція: умовний запис ключа ідемпотентності + атомарний ADD.
        # Або виконуються обидві операції, або жодна – лічильник не може зрости двічі
        # за одну подію і не може «загубити» подію, позначену як оброблена.
        try:
            self.client.transact_write_items(TransactItems=[
                {"Put": {"TableName": self.table, "Item": idem_item,
                         "ConditionExpression": "attribute_not_exists(PK)"}},
                {"Update": {"TableName": self.table, "Key": {"PK": {"S": STAT_PK}},
                            "UpdateExpression": "ADD #files :one, #records :acc, #rejected :rej, "
                                                "#invalid :inv, #fmt :one SET #updatedAt = :now",
                            "ExpressionAttributeNames": {
                                "#files": "files", "#records": "records", "#rejected": "rejected",
                                "#invalid": "invalidFiles", "#fmt": fmt, "#updatedAt": "updatedAt"},
                            "ExpressionAttributeValues": {
                                ":one": to_av(1), ":acc": to_av(accepted), ":rej": to_av(rejected),
                                ":inv": to_av(invalid), ":now": to_av(now)}}},
            ])
            return True
        except Exception as e:
            if _error_code(e) != "TransactionCanceledException":
                raise
            reasons = [r.get("Code") for r in getattr(e, "response", {}).get("CancellationReasons", [])]
            if reasons and reasons[0] == "ConditionalCheckFailed":
                return False
            raise

    def _commit_naive(self, idem_item, fmt, accepted, rejected, invalid, now) -> bool:
        """НАВМИСНО НЕПРАВИЛЬНО: read-modify-write. Лише для експерименту частини D."""
        try:
            self.client.put_item(TableName=self.table, Item=idem_item,
                                 ConditionExpression="attribute_not_exists(PK)")
        except Exception as e:
            if _error_code(e) == "ConditionalCheckFailedException":
                return False
            raise
        resp = self.client.get_item(TableName=self.table, Key={"PK": {"S": STAT_PK}}, ConsistentRead=True)
        cur = {k: from_av(v) for k, v in resp.get("Item", {}).items()}
        # «вікно гонки»: імітація повільної обробки між читанням і записом
        if self.naive_delay_s:
            self._sleep(self.naive_delay_s)
        cur.update({
            "PK": STAT_PK,
            "files": cur.get("files", 0) + 1,
            "records": cur.get("records", 0) + accepted,
            "rejected": cur.get("rejected", 0) + rejected,
            "invalidFiles": cur.get("invalidFiles", 0) + invalid,
            fmt: cur.get(fmt, 0) + 1,
            "updatedAt": now,
        })
        self.client.put_item(TableName=self.table, Item=item_to_av(cur))
        return True
