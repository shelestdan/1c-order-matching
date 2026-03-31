# 1C Request Pipeline

Основные скрипты:

- [normalize_client_requests.py](/Users/kristinakarpova/statistic/scripts/normalize_client_requests.py)
  Первый алгоритм. Приводит клиентские файлы и свободный текст к единому формату заявки.
- [document_text_extractor.py](/Users/kristinakarpova/statistic/scripts/document_text_extractor.py)
  Linux-first extractor для `pdf/scan/image`: сначала берёт native PDF text через Poppler, затем OCR через `ocrmypdf` или `pdftoppm + tesseract`.
- [process_1c_orders.py](/Users/kristinakarpova/statistic/scripts/process_1c_orders.py)
  Второй алгоритм. Матчит нормализованную заявку против остатков 1С.
- [build_reviewed_analog_decisions.py](/Users/kristinakarpova/statistic/scripts/build_reviewed_analog_decisions.py)
  Превращает размеченный менеджером Excel в `reviewed_analog_decisions.json` и `matching_golden_set.json`.
- [evaluate_matching_golden_set.py](/Users/kristinakarpova/statistic/scripts/evaluate_matching_golden_set.py)
  Считает качество матчинга по golden set: попадание approved в top1/top3/topK и попадание rejected в shortlist.
- [nomenclature_classifier/](/Users/kristinakarpova/statistic/scripts/nomenclature_classifier)
  Новый explainable-классификатор шумных товарных строк: нормализация, синонимы, полный `RapidFuzz`-scoring, confidence, fallback-статусы и аккуратный semantic reranker.
- [nomenclature_classifier_config.json](/Users/kristinakarpova/statistic/data/nomenclature_classifier_config.json)
  Конфиг категорий, алиасов, весов токенов, blocker-токенов и маршрутов обработки.
- [classify_nomenclature_examples.py](/Users/kristinakarpova/statistic/scripts/classify_nomenclature_examples.py)
  Быстрый демонстрационный прогон классификатора на шумных примерах.
- [run_order_pipeline.py](/Users/kristinakarpova/statistic/scripts/run_order_pipeline.py)
  Сквозной запуск: сначала нормализация, потом поиск по остаткам.
- [verify_structured_orders.py](/Users/kristinakarpova/statistic/scripts/verify_structured_orders.py)
  Регрессионная проверка: подтверждает, что хорошие входные таблицы не теряют строки на этапах парсинга, нормализации и матчинга.
- [verify_full_stock_matching.py](/Users/kristinakarpova/statistic/scripts/verify_full_stock_matching.py)
  Полная проверка качества матчинга: сравнивает рабочий режим с полным перебором всех товаров из `csv` по каждой строке заявки.
- [cached_1c_service.py](/Users/kristinakarpova/statistic/scripts/cached_1c_service.py)
  Сервисный слой с кэшем утреннего остатка в памяти.
- [run_1c_matching_server.py](/Users/kristinakarpova/statistic/scripts/run_1c_matching_server.py)
  HTTP-сервер для менеджеров и будущей веб-оболочки.
- [vgs2000_matching_dictionary.json](/Users/kristinakarpova/statistic/data/vgs2000_matching_dictionary.json)
  Предметный словарь VGS2000: группы каталога, семейства товаров, политика материалов и правила ручного подбора.
- [substitution_policy.json](/Users/kristinakarpova/statistic/data/substitution_policy.json)
  Формальные правила допустимости аналогов по ключевым семействам: трубы, фланцы, отводы, тройники, переходы, затворы, воздухоотводчики.
- [matching_golden_set.json](/Users/kristinakarpova/statistic/data/matching_golden_set.json)
  Сгруппированный golden set из ручных решений менеджера.
- [setup_linux_1c_matching.sh](/Users/kristinakarpova/statistic/scripts/setup_linux_1c_matching.sh)
  Быстрый bootstrap для Debian/Ubuntu Linux: системные OCR/PDF-пакеты, virtualenv и Python-зависимости.
- [requirements-order-service.txt](/Users/kristinakarpova/statistic/requirements-order-service.txt)
  Рекомендуемый набор библиотек для боевой версии сервиса.

## Что делает

1. Нормализует клиентский вход:
   - `xlsx/xlsm`,
   - `csv/tsv`,
   - `txt/md`,
   - `docx`,
   - `pdf`,
   - `png/jpg/webp/tif`.
2. Приводит его к единой заявке с колонками:
   `Позиция | Наименование | Тип/марка | Код | Производитель | Единица измер. | Количество`.
3. Для сырого текста нормализует:
   - смешанную кириллицу/латиницу,
   - размеры `DN`, `PN`, `1/2"` и похожие обозначения,
   - разные тире, кавычки, переносы строк.
4. После этого ищет по остаткам:
   - точные/высокоуверенные совпадения,
   - аналоги для ручной проверки,
   - полностью не найденные позиции.
5. Перед матчингом прогоняет строку через гибридный классификатор:
   - нормализация и выделение атрибутов,
   - синонимы и алиасы,
   - fuzzy scoring по категориям через `RapidFuzz`,
   - semantic reranker через `sentence-transformers` только для спорных top-кандидатов,
   - fallback-статусы `classified / needs_review / ambiguous / unclassified`.
6. Последовательно резервирует остаток, чтобы одна и та же позиция не уходила в заявку несколько раз.
7. Поддерживает Linux-first OCR-ветку для сканов:
   - `pdftotext` для text-based PDF,
   - `ocrmypdf` для PDF-сканов,
   - `pdftoppm + tesseract` как запасной OCR-контур.
8. Учитывает формальную `substitution_policy.json`:
   - какие семейства можно считать exact,
   - какие аналоги безопасны,
   - что всегда уходит в согласование.
9. Подтягивает ручные approve/reject решения и отдельный golden set, чтобы не терять накопленную экспертизу менеджеров.

## Linux Setup

Для основной Linux-машины:

```bash
bash scripts/setup_linux_1c_matching.sh
```

Если нужна только проверка OCR/PDF-стека:

```bash
python3 scripts/document_text_extractor.py --show-tools --pretty
```

На Debian/Ubuntu минимальный набор системных пакетов такой:

```bash
sudo apt-get update
sudo apt-get install -y poppler-utils tesseract-ocr tesseract-ocr-rus ocrmypdf python3-venv python3-pip
```

## Запуск по этапам

### 1. Нормализация клиентской заявки

```bash
python3 scripts/normalize_client_requests.py \
  --inputs "/путь/к/клиентскому_файлу.txt" "/путь/к/клиентской_таблице.xlsx" "/путь/к/скану.pdf" \
  --out-dir "/путь/к/normalized"
```

### 2. Матчинг против остатков

```bash
python3 scripts/process_1c_orders.py \
  --stock "/путь/к/остатки.csv" \
  --orders "/путь/к/normalized/заявка__normalized.xlsx" \
  --out-dir "/путь/к/папке_результата"
```

### 3. Сквозной пайплайн

```bash
python3 scripts/run_order_pipeline.py \
  --stock "/путь/к/остатки.csv" \
  --inputs "/путь/к/клиентскому_файлу.txt" "/путь/к/клиентской_таблице.xlsx" \
  --out-dir "/путь/к/pipeline_result"
```

### 4. Проверка, что хорошие таблицы не пропускают строки

```bash
python3 scripts/verify_structured_orders.py \
  --stock "/путь/к/остатки.csv" \
  --orders "/путь/к/хорошая_таблица_1.xlsx" "/путь/к/хорошая_таблица_2.xlsx"
```

### 5. Проверка, что матчер не упускает лучший результат из полного CSV

```bash
python3 scripts/verify_full_stock_matching.py \
  --stock "/путь/к/остатки.csv" \
  --orders "/путь/к/хорошая_таблица_1.xlsx" "/путь/к/хорошая_таблица_2.xlsx" \
  --out-dir "/путь/к/full_stock_verification"
```

### 6. Серверный режим с кэшем утреннего остатка

```bash
python3 scripts/run_1c_matching_server.py \
  --host 0.0.0.0 \
  --port 8010 \
  --stock "/путь/к/остатки.csv" \
  --out-dir "/путь/к/service_jobs"
```

После запуска доступны endpoints:

- `GET /health`
  Проверка, что сервер жив и видит текущий кэш остатка.
- `GET /stock/status`
  Информация о текущем загруженном `csv`.
- `POST /stock/load`
  Загрузка нового утреннего остатка в память:

```json
{
  "stock_path": "/abs/path/to/остатки.csv"
}
```

### 7. Быстрая проверка классификатора на шумных строках

```bash
python3 scripts/classify_nomenclature_examples.py
```

### 8. Регрессионные тесты классификатора

```bash
python3 -m unittest discover -s tests -v
```

### 9. Сбор reviewed decisions и golden set из размеченного Excel

```bash
python3 scripts/build_reviewed_analog_decisions.py \
  --input "/путь/к/размеченным_аналогам.xlsx" \
  --output "/путь/к/data/reviewed_analog_decisions.json" \
  --golden-output "/путь/к/data/matching_golden_set.json"
```

### 10. Оценка качества по golden set

```bash
python3 scripts/evaluate_matching_golden_set.py \
  --stock "/путь/к/остатки.csv" \
  --golden-set "/путь/к/data/matching_golden_set.json" \
  --reviewed-decisions "/путь/к/data/reviewed_analog_decisions.json" \
  --out "/путь/к/golden_eval.json"
```

- `POST /match/orders`
  Запуск матчинга для уже подготовленных `xlsx`:

```json
{
  "order_paths": [
    "/abs/path/to/ГК00-006858_сантехника.xlsx"
  ],
  "out_dir": "/optional/output/dir"
}
```

- `POST /pipeline/run`
  Полный цикл: нормализация клиентского файла и матчинг:

```json
{
  "input_paths": [
    "/abs/path/to/client_request.txt"
  ],
  "out_dir": "/optional/output/dir"
}
```

## Что создаётся

- `normalized/*__normalized.xlsx`
  Единая чистая заявка после первого алгоритма.
- `__результат.xlsx`
  Общий разбор по всем строкам, со статусом, уверенностью, категорией классификатора, маршрутом и аналогами.
- `__для_1с.xlsx`
  Только найденные позиции в шаблоне:
  `Штрихкод | Код | Артикул | Номенклатура | Количество | Цена`.
  Первая колонка `Штрихкод` остаётся пустой специально.
- `__аналоги.xlsx`
  То, что похоже, но требует ручной проверки менеджером.
- `__не_найдено.xlsx`
  Позиции, которых нет в остатке или которые не удалось надёжно сопоставить.
  Для абстрактных категорий вроде сифонов без размеров комментарий может специально помечаться как `требует ручного подбора`.

## Логика точности

- Файл `остатки.csv` является единственным источником истины.
- Свободный текст сначала превращается в стандартную таблицу, и только потом попадает в матчер.
- Точное попадание даётся только при строгих условиях.
- Всё сомнительное уходит в аналоги, а не в файл для 1С.
- Материалы матчятся строго: `сталь != чугун != латунь != полипропилен != PVC`.
- Если в запросе указан специфичный размерной профиль вроде `25x20` у перехода, кандидат без такого же профиля не считается аналогом.
- Для абстрактных запросов из словаря `manual_review_rules` без точного совпадения и без безопасного аналога выдаётся комментарий `требует ручного подбора`.
- `substitution_policy.json` формализует, какие атрибуты обязательны для `exact`, какие нужны для `безопасного аналога`, а какие семьи всегда требуют согласования.
- Для шумных строк вроде `затв. диск.`, `пож рукав`, `фл dn100`, `кран шар 1/2` используется отдельный гибридный классификатор с explainable scoring.
- `RapidFuzz` используется как основной слой: alias retrieval, token correction, `WRatio`, `token_set_ratio`, `token_sort_ratio`, `partial_ratio`.
- `sentence-transformers` используется осторожно: только после жёстких правил и только для небольшого shortlist-а категорий, чтобы не просаживать скорость всего пайплайна.
- Semantic reranker можно аварийно отключить переменной среды `NOMENCLATURE_DISABLE_SEMANTIC=1`.
- Классификатор не пытается насильно классифицировать всё подряд: при низкой уверенности возвращает `ambiguous` или `unclassified`.
- Если остаток уже распределён на предыдущую строку, позиция не будет выдана повторно.
- Для спорных строк рабочий матчер теперь добирает результаты полным просмотром всего `csv`, но уже в ускоренном exact-режиме: формулы совпадения остаются теми же, что у эталонного полного перебора.
- Скрипт [verify_full_stock_matching.py](/Users/kristinakarpova/statistic/scripts/verify_full_stock_matching.py) сравнивает ускоренный режим со старым reference-перебором и подтверждает их эквивалентность на хороших таблицах.
- Для серверного режима базовый matcher по дневному остатку строится один раз утром, а каждый запрос получает быстрый `fork` без пересборки индексов.
- `matching_artifact.json` теперь фиксирует не только хэши входов, но и версии policy/golden set/reviewed decisions.

## Текущий статус

- Первый алгоритм уже умеет разбирать неряшливые таблицы и базовый свободный текст.
- Второй алгоритм уже оптимизирован лучше, чем в первом прототипе:
  на тесте около `2.9 сек` для `10k` остатков и около `12.5 сек` для `100k` остатков на текущей машине, включая построение индекса и матчинг `413` строк заявки.
- При этом полная проверка `fast vs reference full scan` проходит без расхождений на ваших двух хороших файлах.
- Для повышения качества дальше полезно добавить словари брендов, типовых артикулов и ручных соответствий.

## Следующий шаг для продакшена

Чтобы довести это до рабочего ежедневного конвейера, лучше добавить:

1. словарь ручных соответствий для ваших типовых артикулов и брендов;
2. шаблоны разбора для типовых писем/файлов ваших клиентов;
3. папку `rules/` с синонимами и исключениями по вашим номенклатурам.
4. веб-оболочку или API для менеджеров.

Сейчас прототип уже пригоден для тренировочных прогонов и калибровки правил на реальных заявках.
