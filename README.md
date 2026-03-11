# Drift Compare

Сервис сравнения данных дефектоскопии между средами (test / prod) и обучающей выборкой.

## Режимы сравнения

| Режим | Описание |
|---|---|
| **Тестовая БД** | Статистика по дефектам из тестовой среды за выбранный период |
| **Продуктовая БД** | Статистика по дефектам из продуктовой среды за выбранный период |
| **Сравнение БД с CSV** | Попарное сравнение классов дефектов из БД с обучающей CSV-выборкой |
| **Сравнение двух БД** | Сравнение test vs prod по общим рулонам (coilid) |

## Визуализации

- Попарное сравнение количества дефектов по классам (grouped bar + дельта-таблица)
- Гистограмма распределения confidence (0-1), overlay при сравнении двух источников
- Процент переклассификации (raw -> final) по рулонам
- Топ-15 переклассификаций (raw -> final)
- Фильтр классов дефектов (все выбраны по умолчанию, можно убирать)

## Конфигурация

```yaml
databases:
  - label: "test"
    host: "localhost"
    port: 5432
    dbname: "defects_test"
    user: "reader"
    password: "reader"
    table: "public.defect_results"
    timestamp_column: "created_at"

  - label: "prod"
    host: "localhost"
    port: 5432
    dbname: "defects_prod"
    user: "reader"
    password: "reader"
    table: "public.defect_results"
    timestamp_column: "created_at"

storage:
  dir: "storage"
```

## CSV обучающей выборки

CSV должен содержать:
- `instance_label` — класс дефекта (маппится в `defectclass`)
- `bbox_xtl`, `bbox_ytl`, `bbox_xbr`, `bbox_ybr` — координаты боксов
- Confidence в CSV **нет** — по нему не сравниваем

## Быстрый старт

```bash
pip install -r requirements.txt
python seed_compare.py    # генерация демо-данных (test + prod + CSV)
streamlit run compare_app.py
```

## Структура кода

```
drift/
  config.py       # YAML -> dataclasses
  db.py           # PostgreSQL запросы (8 столбцов)
  stats.py        # Вычисление статистик
  storage.py      # Parquet I/O (per-db layout)
compare_app.py    # Streamlit UI (4 режима сравнения)
seed_compare.py   # Генерация синтетических данных
config.yaml       # Креды test/prod БД
```

## Хранилище

```
storage/
  test/
    coil_stats/     # Сводка по рулонам
    confidence/     # describe() по confidence
    conf_buckets/   # Гистограмма confidence
    class_change_top/  # Топ переклассификаций
    ...
  prod/
    ...
```
