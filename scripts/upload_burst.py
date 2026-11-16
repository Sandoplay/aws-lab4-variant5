"""Частина D: п'ять одночасних завантажень (потрібні boto3 і профіль AWS CLI).

  pip install boto3
  python scripts/upload_burst.py amogusadmin-lab4-ingest json      # burst-1..5.json
  python scripts/upload_burst.py amogusadmin-lab4-ingest csv       # burst-1..5.csv

Альтернатива без CLI: у консолі S3 виділити всі п'ять файлів і завантажити одним Upload.
Скрипт додає до ключа мітку часу, щоб кожен запуск був новою подією (новий ключ).
"""
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3

bucket, ext = sys.argv[1], sys.argv[2]
files = Path(__file__).resolve().parent.parent / "test-files"
stamp = time.strftime("%H%M%S")
s3 = boto3.client("s3", region_name="eu-north-1")


def up(n):
    key = f"incoming/burst-{stamp}-{n}.{ext}"
    s3.upload_file(str(files / f"burst-{n}.{ext}"), bucket, key)
    return key


with ThreadPoolExecutor(max_workers=5) as pool:
    for key in pool.map(up, range(1, 6)):
        print("uploaded", key)
