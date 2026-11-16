"""Збирає архіви для завантаження в консоль:  python scripts/build_zips.py

dist/lab4-common-layer.zip  – шар (структура python/lab4_common/..., як вимагає Lambda для Python)
dist/lab4-ingest-json.zip   – код функції JSON (lambda_function.py у корені архіву)
dist/lab4-ingest-csv.zip    – код функції CSV
"""
import os
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIST = os.path.join(ROOT, "dist")
os.makedirs(DIST, exist_ok=True)


def zip_dir(src_dir, zip_path, arc_root=""):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for base, dirs, files in os.walk(src_dir):
            dirs[:] = [d for d in dirs if d != "__pycache__"]
            for f in files:
                full = os.path.join(base, f)
                arc = os.path.join(arc_root, os.path.relpath(full, src_dir)).replace(os.sep, "/")
                z.write(full, arc)
    print("built", os.path.relpath(zip_path, ROOT))


zip_dir(os.path.join(ROOT, "layer", "python"), os.path.join(DIST, "lab4-common-layer.zip"), "python")
zip_dir(os.path.join(ROOT, "functions", "json_ingest"), os.path.join(DIST, "lab4-ingest-json.zip"))
zip_dir(os.path.join(ROOT, "functions", "csv_ingest"), os.path.join(DIST, "lab4-ingest-csv.zip"))
