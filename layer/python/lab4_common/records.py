"""Однакова семантика даних для JSON і CSV: запис людини {id, name, age}.

Парсери форматів (у самих функціях) повертають список dict; тут ці dict
перевіряються й перетворюються в Person. Модуль не знає, звідки прийшли дані.
"""
from dataclasses import dataclass

FIELDS = ("id", "name", "age")


@dataclass(frozen=True)
class Person:
    id: str
    name: str
    age: int


def normalize(raw: dict) -> Person:
    """Перевіряє й нормалізує один запис. Кидає ValueError з причиною відхилення."""
    if not isinstance(raw, dict):
        raise ValueError("record is not an object")
    missing = [f for f in FIELDS if f not in raw or raw[f] in (None, "")]
    if missing:
        raise ValueError(f"missing fields: {', '.join(missing)}")

    rid = str(raw["id"]).strip()
    name = raw["name"]
    if not isinstance(name, str) or not name.strip():
        raise ValueError("name must be a non-empty string")

    age = raw["age"]
    if isinstance(age, bool):          # True/False – теж int у Python, але це не вік
        raise ValueError("age must be an integer")
    if isinstance(age, str):           # з CSV усе приходить рядками
        age = age.strip()
        if not age.lstrip("-").isdigit():
            raise ValueError(f"age is not an integer: {raw['age']!r}")
        age = int(age)
    if not isinstance(age, int):
        raise ValueError(f"age must be an integer, got {type(age).__name__}")
    if not 0 <= age <= 150:
        raise ValueError(f"age out of range: {age}")

    return Person(id=rid, name=name.strip(), age=age)


def split_valid(raws: list[dict]) -> tuple[list[Person], list[dict]]:
    """Повертає (коректні записи, відхилені з причинами).

    Якщо id повторюється у файлі, виграє останній запис: BatchWriteItem не приймає
    двох елементів з однаковим ключем в одному запиті.
    """
    by_id: dict[str, tuple[int, Person]] = {}
    rejected: list[dict] = []
    for i, raw in enumerate(raws):
        try:
            person = normalize(raw)
        except ValueError as e:
            rejected.append({"index": i, "reason": str(e)})
            continue
        if person.id in by_id:
            rejected.append({"index": by_id[person.id][0], "reason": f"duplicate id {person.id} in file, later record wins"})
        by_id[person.id] = (i, person)
    accepted = [p for _, p in sorted(by_id.values(), key=lambda t: t[0])]
    return accepted, rejected
