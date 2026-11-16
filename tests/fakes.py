"""Мінімальні підробки клієнтів S3 і DynamoDB – тести запускаються без хмари й без boto3."""
import io
import re

from lab4_common.storage import from_av


class FakeClientError(Exception):
    def __init__(self, code, **extra):
        super().__init__(code)
        self.response = {"Error": {"Code": code}, **extra}


class FakeS3:
    def __init__(self, objects: dict[str, str]):
        self.objects = objects
        self.calls = []

    def get_object(self, Bucket, Key, VersionId=None):
        self.calls.append((Bucket, Key, VersionId))
        return {"Body": io.BytesIO(self.objects[Key].encode("utf-8"))}


class FakeDynamo:
    def __init__(self, unprocessed_first_calls: int = 0):
        self.items: dict[str, dict] = {}
        self.batch_calls = []
        self._unprocessed_left = unprocessed_first_calls

    def _plain(self, item):
        return {k: from_av(v) for k, v in item.items()}

    def batch_write_item(self, RequestItems):
        (table, reqs), = RequestItems.items()
        assert len(reqs) <= 25, "BatchWriteItem accepts max 25 items"
        self.batch_calls.append(len(reqs))
        if self._unprocessed_left:            # імітуємо тротлінг: половина не записана
            self._unprocessed_left -= 1
            keep, back = reqs[: len(reqs) // 2], reqs[len(reqs) // 2:]
        else:
            keep, back = reqs, []
        for r in keep:
            item = self._plain(r["PutRequest"]["Item"])
            self.items[item["PK"]] = item
        return {"UnprocessedItems": {table: back} if back else {}}

    def put_item(self, TableName, Item, ConditionExpression=None):
        item = self._plain(Item)
        if ConditionExpression == "attribute_not_exists(PK)" and item["PK"] in self.items:
            raise FakeClientError("ConditionalCheckFailedException")
        self.items[item["PK"]] = item

    def get_item(self, TableName, Key, ConsistentRead=False):
        pk = Key["PK"]["S"]
        if pk not in self.items:
            return {}
        from lab4_common.storage import item_to_av
        return {"Item": item_to_av(self.items[pk])}

    def _apply_update(self, pk, expr, names, values):
        item = self.items.setdefault(pk, {"PK": pk})
        m = re.fullmatch(r"ADD (.+) SET (.+)", expr)
        for part in m.group(1).split(","):
            n, v = part.split()
            attr = names[n]
            item[attr] = item.get(attr, 0) + from_av(values[v])
        for part in m.group(2).split(","):
            n, v = [s.strip() for s in part.split("=")]
            item[names[n]] = from_av(values[v])

    def transact_write_items(self, TransactItems):
        put, upd = TransactItems[0]["Put"], TransactItems[1]["Update"]
        pk = from_av(put["Item"]["PK"])
        if pk in self.items:
            raise FakeClientError("TransactionCanceledException",
                                  CancellationReasons=[{"Code": "ConditionalCheckFailed"}, {"Code": "None"}])
        self.items[pk] = self._plain(put["Item"])
        self._apply_update(upd["Key"]["PK"]["S"], upd["UpdateExpression"],
                           upd["ExpressionAttributeNames"], upd["ExpressionAttributeValues"])
