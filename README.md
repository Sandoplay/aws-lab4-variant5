# Лаб. 4, варіант 5 — «Два формати – дві функції»

S3 (`incoming/*.json`, `incoming/*.csv`) → два правила сповіщення → дві функції Lambda зі спільним шаром → одна таблиця DynamoDB. Події, які не вдалося обробити, потрапляють у чергу SQS.

```
           incoming/ + .json ──► lab4-ingest-json ─┐
 S3 bucket                                         ├─(шар lab4-common)─► DynamoDB lab4-ingest
           incoming/ + .csv  ──► lab4-ingest-csv  ─┘
           incoming/ + .txt  ──► (нічого)          on failure ──► SQS lab4-ingest-failures
```

## Структура

| Шлях | Що там |
|---|---|
| `layer/python/lab4_common/` | шар: `events.py` (розбір події S3), `records.py` (валідація), `storage.py` (DynamoDB), `pipeline.py`, `handler.py` |
| `functions/json_ingest/`, `functions/csv_ingest/` | тільки парсер свого формату + `make_handler(...)` |
| `tests/` | `python -m unittest discover -s tests -v` – без хмари й без boto3 |
| `scripts/gen_test_files.py` | тестові файли в `test-files/` |
| `scripts/build_zips.py` | архіви для консолі в `dist/` |
| `scripts/upload_burst.py` | п'ять одночасних завантажень (частина D) |
| `scripts/find_unmatched.py` | пошук файлів, які не оброблювала жодна функція |
| `policy/Lab4IngestPolicy.json` | політика IAM (замінити `<ACCOUNT_ID>`) |
| `events/sample_s3_event.json` | приклад події S3 |
| `cleanup.sh` | видалення всіх ресурсів через AWS CLI |

Формат елементів однаковий для обох функцій:

| PK | Атрибути |
|---|---|
| `REC#<id>` | `name` (S), `age` (N), `sourceFormat`, `sourceKey`, `ingestedAt` |
| `IDEMP#<bucket>/<key>#<eTag>` | `processedAt`, `ttl` (+7 діб), `format`, `status`, `accepted`, `rejected` |
| `STAT#ingest` | атомарні лічильники `files`, `records`, `rejected`, `invalidFiles`, `json`, `csv`, а також `updatedAt` |

Змінні середовища: `TABLE_NAME` (обов'язкова), `COUNTER_MODE=atomic|naive`, `NAIVE_DELAY_MS`, `SIMULATE_FAILURE=true|false`.

## Ключ ідемпотентності

`<bucket>/<object key>#<versionId>`, а коли версіонування вимкнене (як тут), то `#<eTag>`. Приклад: `amogusadmin-lab4-ingest/incoming/people.json#5d41402abc4b2a76b9719d911017c592`.

- **bucket** — той самий ключ об'єкта може існувати в іншому бакеті, і це буде інший файл.
- **object key** — визначає, який саме файл оброблено (URL-декодований, як у `events.py`).
- **versionId / eTag** — змінюються разом із вмістом. Перезапис файлу новими даними дає нову подію, і її треба обробити. Повтор тієї самої події має той самий eTag, тому його буде пропущено. Час події для ключа не підходить: при повторі він може відрізнятися.
- Наслідок вибору eTag: якщо файл перезалити з **побайтово тим самим вмістом**, eTag не зміниться і файл вважатиметься вже обробленим. Для цих даних це правильна поведінка (результат був би той самий). Якщо ж кожне завантаження треба рахувати окремо, слід увімкнути версіонування: тоді в ключ піде versionId.

Як це реалізовано (`storage.Repository.commit_file`): спершу записуються рядки `REC#…`. PutItem з тим самим PK дає той самий результат, тож цей крок ідемпотентний сам по собі. Потім в одній транзакції `TransactWriteItems` виконуються умовний `Put IDEMP#… attribute_not_exists(PK)` та атомарний `ADD` лічильників. Тому:
- якщо функція впала до транзакції, повтор перезапише ті самі рядки й зарахує файл;
- якщо функція впала після транзакції, повтор отримає `ConditionalCheckFailed` і залогує `duplicate, skipped`, а лічильник не зросте вдруге;
- якби ідемпотентність позначалась окремим записом **до** обробки (як у прикладі методички), збій посередині лишив би подію позначеною, але необробленою, і повтор її пропустив би.

## Вартість обробки 1 млн файлів (eu-north-1, оцінка)

Припущення: файл ~1 КБ із 3 записами; arm64 / 512 МБ; середня оплачувана тривалість **150 мс** (виміряні теплі виклики – 53–180 мс, холодні – 0,9–1,5 с); у журнал ~2,5 КБ на виклик (повна подія + 3–4 рядки).

| Складова | Обсяг | Ціна | Без free tier | З free tier |
|---|---|---|---|---|
| Lambda, запити | 1 млн | $0,20 / млн | $0,20 | $0 (1 млн безкоштовно) |
| Lambda, обчислення | 1 млн × 0,5 ГБ × 0,15 с = 75 000 ГБ-с | $0,0000133334 / ГБ-с (arm64) | $1,00 | $0 (400 000 ГБ-с) |
| S3 PUT (завантаження) | 1 млн | $0,005 / 1000 | $5,00 | $5,00 |
| S3 GET (читання функцією) | 1 млн | $0,0004 / 1000 | $0,40 | $0,40 |
| DynamoDB, on-demand | 3 WRU (BatchWrite) + 4 WRU (транзакція з 2 елементів, ×2) = 7 млн WRU | $0,625 / млн | $4,38 | $4,38 |
| CloudWatch Logs | ~2,5 ГБ | ~$0,50 / ГБ | $1,25 | $0 (5 ГБ) |
| SQS, TTL, шар, IAM | — | — | ≈ $0 | $0 |
| **Разом** | | | **≈ $12,2** | **≈ $9,8** |

На provisioned 5 WCU (як у лабораторній) 1 млн файлів пройде за 7 млн / 5 WCU ≈ 16 діб. Щоб обробити їх за добу, потрібно ~81 WCU, а це більше за безкоштовні 25. Сховище 1 млн × 1 КБ ≈ 1 ГБ коштує ≈ $0,02/міс у S3 і безкоштовне в DynamoDB (до 25 ГБ). Ціни треба звірити з AWS Pricing Calculator для eu-north-1. Найдорожчі складові — S3 PUT і записи DynamoDB, а не Lambda.

Сама лабораторна (≈300 викликів, provisioned 5/5) коштує $0 за всім, крім S3: це частки цента.

## Створені ресурси

| Ресурс | Ім'я |
|---|---|
| S3 bucket | `amogusadmin-lab4-ingest` (префікс `incoming/`) |
| Правила сповіщення | `lab4-json` (incoming/ + .json), `lab4-csv` (incoming/ + .csv) |
| DynamoDB | `lab4-ingest`, PK (String), provisioned 5 RCU / 5 WCU, TTL `ttl` |
| Lambda | `lab4-ingest-json`, `lab4-ingest-csv` — Python 3.13, arm64, 512 МБ, 30 с, журнал JSON |
| Шар | `lab4-common` (Python 3.13, arm64) |
| Роль виконання | `lab4-ingest-json-role-xfz1sy8e` (створена консоллю, спільна для обох функцій) |
| Керована політика | `Lab4IngestPolicy` (3 блоки: S3 read, DynamoDB write, SQS send; тимчасовий GetItem видалено після частини D) + дві згенеровані `AWSLambdaBasicExecutionRole-…` (журнали для кожної функції) |
| SQS | `lab4-ingest-failures` (адресат при збої) |
| Аларми | `lab4-dlq-not-empty`, `lab4-lambda-errors` |
| Групи журналів | `/aws/lambda/lab4-ingest-json`, `/aws/lambda/lab4-ingest-csv` |

## Прибирання

`bash cleanup.sh` (потрібен AWS CLI) або вручну в консолі в тому самому порядку: бакет → таблиця → черга → аларми → функції й шар → роль і політики → групи журналів.

## Журнал експериментів

Знімки екрана, фрагменти журналів і експорти таблиці наведено у звіті до лабораторної, який здається окремо.

| Експеримент | Результат |
|---|---|
| C: два повтори вже обробленої події | обидва `DUPLICATE`, `STAT#ingest` не змінився, DynamoDB «Conditional check failed» = 2 |
| D1: 5 одночасних JSON, атомарний ADD | concurrency 3, `files` +5 з 5 |
| D2: 5 одночасних CSV, read-modify-write | `files` +2 з 5, `records` +4 з 10 (втрачене оновлення) |
| E: збій, 2 повтори, адресат SQS | 3 спроби (+58 с, +110 с), повідомлення `RetriesExhausted`, обидва аларми In alarm |
| Варіант 5: `.txt` | жодного виклику, таблиця побайтово незмінна; Logs Insights – 0 чужих форматів |
