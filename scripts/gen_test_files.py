"""Генерує тестові файли в test-files/.  Запуск: python scripts/gen_test_files.py"""
import json
import os

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "test-files")
os.makedirs(OUT, exist_ok=True)

PEOPLE = [
    {"id": "1", "name": "Alice", "age": 22},
    {"id": "2", "name": "Bob", "age": 31},
    {"id": "3", "name": "Olena", "age": 27},
]


def write(name, text):
    with open(os.path.join(OUT, name), "w", encoding="utf-8", newline="") as f:
        f.write(text)
    print("created", os.path.join("test-files", name))


def to_csv(rows, header="id,name,age"):
    return header + "\n" + "".join(f"{r['id']},{r['name']},{r['age']}\n" for r in rows)


# A: перший файл кожного формату – однакові дані, різний формат
write("people.json", json.dumps(PEOPLE, ensure_ascii=False, indent=2))
write("people.csv", to_csv(PEOPLE))

# варіант 5, додаткова вимога: файл, на який не має відреагувати жодна функція
write("people.txt", to_csv(PEOPLE))

# некоректні вхідні дані (функція має завершитися успішно й записати помилку в журнал)
write("bad-header.csv", "id,name,email\n10,Ivan,ivan@example.com\n")
write("bad-records.json", json.dumps([{"id": "20", "name": "NoAge"},
                                      {"id": "21", "name": "Str", "age": "twenty"},
                                      {"id": "22", "name": "Good", "age": 40}], indent=2))
write("broken.json", '[{"id": "30", "name": "Unclosed"')

# понад 25 рядків: BatchWriteItem буде викликано кілька разів
write("big.csv", to_csv([{"id": str(100 + i), "name": f"Person{i}", "age": 18 + i % 50} for i in range(60)]))

# D: п'ять файлів для одночасного завантаження (по 2 записи в кожному)
for n in range(1, 6):
    write(f"burst-{n}.json", json.dumps([{"id": f"b{n}-{j}", "name": f"Burst{n}_{j}", "age": 20 + j}
                                         for j in range(2)], indent=2))
    write(f"burst-{n}.csv", to_csv([{"id": f"c{n}-{j}", "name": f"Burst{n}_{j}", "age": 20 + j}
                                    for j in range(2)]))

# E: файл для експерименту зі збоєм
write("fail-me.json", json.dumps([{"id": "500", "name": "Fail", "age": 50}], indent=2))
