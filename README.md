# Drift Monitor

Сервис мониторинга табличного дрифта данных 
Поддерживает два режима работы и подключение к нескольким БД параллельно.

## Два режима работы

### Live (`live: true`)

```
CoilWatcher (фоновый поток)
  │
  ├─ DB "main"     ──► WHERE coilid > watermark ──► Stats ──► Parquet
  ├─ DB "secondary" ──► WHERE coilid > watermark ──► Stats ──► Parquet
  │
  └─ sleep(poll_interval_sec) ──► повтор
```

- Фоновый поток опрашивает **все** настроенные БД каждые N секунд.
- Watermark (последний обработанный coilid) хранится в памяти — один параметр в SQL.
- Кнопка «Run check now» запускает внеочередную проверку.

### Manual (`live: false`)

```
Пользователь в Streamlit UI
  │
  ├─ Выбирает даты (по умолчанию: последняя неделя)
  ├─ Выбирает БД (чекбоксы)
  ├─ Жмёт "Run query"
  │
  └─► SQL: WHERE created_at >= %s AND created_at < %s
       └─► Stats ──► Parquet
```

- Никакого фонового потока — запросы только по кнопке.
- Фильтрация по `timestamp_column` из конфига (например `created_at`).
- Если даты не указаны — берётся последняя неделя.
- Если `timestamp_column` не задан — загружаются все рулоны (без фильтра по дате).

## Конфигурация

```yaml
# Режим: true = auto-polling, false = manual
live: true

# Одна или несколько БД
databases:
  - label: "main"
    host: "localhost"
    port: 5432
    dbname: "defects"
    user: "reader"
    password: "reader"
    table: "public.defect_results"
    timestamp_column: "created_at"    # для manual mode

  - label: "secondary"
    host: "10.0.0.2"
    port: 5432
    dbname: "defects_v2"
    user: "reader"
    password: "reader"
    table: "public.defect_results"
    timestamp_column: "created_at"

watcher:
  poll_interval_sec: 600

storage:
  dir: "storage"
```

| Поле | Описание |
|---|---|
| `live` | `true` — авто-поллинг, `false` — только по кнопке |
| `databases[].label` | Уникальное имя БД (используется как имя поддиректории в storage) |
| `databases[].table` | Полное `schema.table` имя таблицы |
| `databases[].timestamp_column` | Столбец с датой создания строки (для фильтрации в manual mode). Пустая строка = не фильтровать |
| `watcher.poll_interval_sec` | Интервал между проверками в live mode |

## Метрики

| Метрика | Описание |
|---|---|
| `defect_counts` | Количество дефектов каждого класса |
| `confidence_stats` | `describe()` по confidence для каждого класса |
| `class_change_summary` | Сколько дефектов изменили класс (`rawdefectclass != defectclass`) |
| `class_change_matrix` | Матрица переходов raw → final |
| `class_change_top` | Самые частые переходы между классами |
| `bbox_stats` | `describe()` по ширине, высоте, площади и aspect ratio bbox |
| `spatial_stats` | Статистика центров bbox (mean/std cx, cy) |
| `confidence_buckets` | Гистограмма confidence по бакетам |

Каждая метрика содержит `fetched_at` (UTC) и `db_label` для фильтрации.

## Хранилище

```
storage/
├── main/                     # label первой БД
│   ├── coil_stats/
│   │   ├── COIL_001.parquet
│   │   └── COIL_002.parquet
│   ├── confidence/
│   ├── class_changes/
│   ├── class_change_top/
│   ├── bbox/
│   ├── spatial/
│   ├── conf_buckets/
│   └── processed_coils.txt
├── secondary/                # label второй БД
│   ├── coil_stats/
│   └── ...
```

Каждая БД хранит данные в своей поддиректории — coilid из разных БД не пересекаются.

## Быстрый старт

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Streamlit UI

**Live mode:**
- «Run check now» — внеочередная проверка всех БД
- «Start/Stop background watcher» — управление фоновым потоком
- Фильтр результатов по дате и по БД

**Manual mode:**
- Date range (From / To) — по умолчанию последняя неделя
- Мультиселект БД — какие базы запрашивать
- «Run query» — запуск
- Фильтр результатов по дате и по БД

## Нагрузка на БД

| Режим | Запрос | Стоимость |
|---|---|---|
| Live | `SELECT DISTINCT coilid WHERE coilid > %s` | Index seek, 1 параметр |
| Manual | `SELECT DISTINCT coilid WHERE ts >= %s AND ts < %s` | Range scan по timestamp |
| Оба | `SELECT <8 cols> WHERE coilid = %s` | Index seek на каждый рулон |

- Короткоживущие соединения, `statement_timeout=30s`.
- `threading.Lock` исключает дублирование работы.

## Структура кода

```
drift/
├── config.py       # YAML → dataclasses (live, databases[], timestamp_column)
├── db.py           # fetch_new_coils, fetch_coils_in_range, fetch_coil_data
├── stats.py        # Вычисление статистик + fetched_at
├── storage.py      # Parquet per-db layout, watermark recovery
├── watcher.py      # Live polling + manual mode, multi-db, thread lock
app.py              # Streamlit UI (два режима, date range, db selector)
config.yaml
requirements.txt
```

## Расширение

**Добавить новую БД:** добавить блок в `databases:` в config.yaml — watcher подхватит автоматически.

**Добавить новую метрику:** функция в `stats.py` → вызов из `compute_coil_stats()` → запись в `storage.py`.

**Графики:** `pd.read_parquet("storage/main/coil_stats/")` → Plotly / `st.line_chart`. Поля `fetched_at` и `db_label` готовы для фильтрации.
