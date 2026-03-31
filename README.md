# 1C Order Matching

Минимальный репозиторий с алгоритмом сопоставления клиентских заявок с остатками 1С.

Состав:
- `scripts/` — пайплайн нормализации, матчинга и оценки качества
- `data/` — словари, правила замены и golden set
- `inputs/stock/` — актуальный файл остатков
- `inputs/orders/` — клиентский файл заказа для воспроизводимого прогона
- `tests/` — регрессионные тесты

Быстрый старт:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-order-service.txt
python3 -m unittest tests.test_nomenclature_classifier -v
```
