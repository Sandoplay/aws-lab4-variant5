"""Виявлення файлів, на які не спрацювало жодне правило сповіщення (напр. .txt).

  python scripts/find_unmatched.py amogusadmin-lab4-ingest

Звірка: усе в incoming/, суфікс якого не .json і не .csv, ніким не оброблялось.
Запускається з вашими обліковими даними CLI, а не від імені ролі функції.
"""
import sys

import boto3

HANDLED = (".json", ".csv")
bucket = sys.argv[1]
s3 = boto3.client("s3", region_name="eu-north-1")
unmatched = []
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="incoming/"):
    for obj in page.get("Contents", []):
        if not obj["Key"].endswith("/") and not obj["Key"].lower().endswith(HANDLED):
            unmatched.append((obj["Key"], obj["Size"], obj["LastModified"].isoformat()))

for key, size, ts in unmatched:
    print(f"UNMATCHED  {ts}  {size:>8}  {key}")
print(f"total unmatched: {len(unmatched)}")
sys.exit(1 if unmatched else 0)
