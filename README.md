# Drift Monitor

Сервис для мониторинга табличного дрифта данных о дефектах металлопроката.
По каждому новому рулону (coil) автоматически собирает статистику и сохраняет её в Parquet-файлы для последующего анализа и визуализации.

## Как это работает

```
                         ┌──────────────────────────────────────────────┐
                         │              CoilWatcher                     │
                         │                                              │
                         │  watermark (in-memory)                       │
                         │       │                                      │
PostgreSQL ◄─────────────│  SQL: WHERE coilid > watermark               │
     │                   │       │                                      │
     │  новые coilid     │       ▼                                      │
     └──────────────────►│  fetch_coil_data() ──► Stats engine          │
                         │                            │                 │
                         │                            ▼                 │
                         │                     Parquet storage          │
                         │                       + fetched_at (UTC)     │
                         └──────────────────────────────────────────────┘
                                                      │
                                   Streamlit UI ◄─────┘
                                   (фильтр по дате)
```

### Цикл обнаружения новых рулонов

1. **Watcher** хранит в памяти watermark — ID последнего обработанного рулона.
2. Каждые N секунд (по умолчанию 600 = 10 мин) выполняется один лёгкий запрос:
   ```sql
   SELECT DISTINCT coilid FROM <table> WHERE coilid > :watermark ORDER BY coilid
   ```
3. Для каждого нового рулона загружаются **только нужные 8 столбцов** одним `SELECT ... WHERE coilid = %s`.
4. **Stats engine** вычисляет набор метрик (см. ниже).
5. Результаты + метка времени `fetched_at` (UTC) пишутся в **Parquet-файлы**.
6. Watermark сдвигается вперёд в памяти — следующий цикл спросит только то, что новее.

При перезапуске сервиса watermark восстанавливается из `processed_coils.txt` (последняя строка).

## Какие метрики считаются

| Метрика | Описание |
|---|---|
| `defect_counts` | Количество дефектов каждого класса |
| `confidence_stats` | `describe()` по confidence для каждого класса (min, max, mean, std, квартили) |
| `class_change_summary` | Сколько дефектов изменили класс после постобработки (`rawdefectclass != defectclass`), процент изменений |
| `class_change_matrix` | Матрица переходов raw → final (crosstab) |
| `class_change_top` | Самые частые переходы между классами |
| `bbox_stats` | `describe()` по ширине, высоте, площади и aspect ratio bbox для каждого класса |
| `spatial_stats` | Статистика центров bbox (mean/std координат cx, cy) — позволяет обнаружить смещение дефектов в пространстве |
| `confidence_buckets` | Гистограмма распределения confidence по бакетам для каждого класса |

Каждая метрика содержит поле `fetched_at` (UTC) — момент забора данных из БД. Это позволяет строить временные срезы в Streamlit UI или при ручном анализе.

## Почему Parquet

- Колоночный, сжатый формат — в 5–10x компактнее CSV для числовых данных.
- Нативная поддержка в pandas (`pd.read_parquet` / `to_parquet`).
- Не требует серверного процесса (в отличие от SQLite при многопоточном доступе).
- Легко агрегируется позднее через DuckDB / Polars для дашбордов.
- Один файл на рулон — атомарная запись, нет проблем с блокировками.

## Структура хранилища

```
storage/
├── coil_stats/           # counts + summary + fetched_at per coil
│   ├── COIL_001.parquet
│   └── COIL_002.parquet
├── confidence/           # confidence describe() per class + fetched_at
├── class_changes/        # raw → final transition matrix + fetched_at
├── class_change_top/     # top transitions + fetched_at
├── bbox/                 # bbox size describe() per class + fetched_at
├── spatial/              # centre-point stats + fetched_at
├── conf_buckets/         # confidence histogram + fetched_at
└── processed_coils.txt   # append-only log (watermark recovery after restart)
```

## Быстрый старт

### 1. Установка

```bash
pip install -r requirements.txt
```

### 2. Конфигурация

Отредактируйте `config.yaml`:

```yaml
db:
  host: "your-pg-host"
  port: 5432
  name: "your_db"
  user: "reader"
  password: "secret"
  table: "public.defect_results"   # ← schema.table

watcher:
  poll_interval_sec: 600           # 10 минут между проверками

storage:
  dir: "storage"                   # куда писать parquet-файлы
```

`db.table` — полное имя таблицы вместе со схемой (например `analytics.defects`).

### 3. Запуск

```bash
# Streamlit UI
streamlit run app.py

# Или одноразовая проверка из кода
python -c "
from drift.config import load_config
from drift.watcher import CoilWatcher

cfg = load_config()
watcher = CoilWatcher(cfg)
new = watcher.run_once()
print(f'Processed: {new}')
"
```

### 4. Чтение результатов из Python

```python
import pandas as pd

# Все сводки по всем рулонам (с fetched_at для фильтрации по дате)
df = pd.read_parquet("storage/coil_stats/")

# Фильтр по дате
df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)
recent = df[df["fetched_at"] >= "2025-03-01"]

# Конкретный рулон — confidence
conf = pd.read_parquet("storage/confidence/COIL_001.parquet")

# Матрица переходов
matrix = pd.read_parquet("storage/class_changes/COIL_001.parquet")
```

## Streamlit UI

Интерфейс предоставляет:

- **Sidebar** — путь к `config.yaml`, отображение текущих настроек (таблица БД, интервал, директория хранилища).
- **Run check now** — ручной запуск проверки новых рулонов.
- **Start/Stop background watcher** — запуск фонового потока, который опрашивает БД по таймеру.
- **Date range** — фильтр сводной таблицы по `fetched_at` (выбор диапазона дат).
- **Сводная таблица** — все обработанные рулоны с метриками.

## Нагрузка на БД

Сервис спроектирован для минимальной нагрузки:

| Запрос | Когда | Стоимость |
|---|---|---|
| `SELECT DISTINCT coilid ... WHERE coilid > %s` | раз в 10 мин | Index range scan, 1 параметр |
| `SELECT <8 columns> WHERE coilid = %s` | на каждый новый рулон | Index seek по coilid |

- Watermark в памяти — на каждом цикле передаётся **один параметр**, а не список всех обработанных ID.
- Короткоживущие соединения (без connection pool) — освобождают ресурсы PG сразу после запроса.
- `statement_timeout = 30s` — защита от зависших запросов.
- Параметризованные запросы — защита от SQL-инъекций.
- Глобальный `threading.Lock` — исключает дублирование работы при одновременном вызове из UI и фонового потока.

## Структура кода

```
drift/
├── config.py       # Загрузка config.yaml → dataclass (db.table как параметр)
├── db.py           # Запросы к PostgreSQL (psycopg2, explicit close)
├── stats.py        # Вычисление статистик по DataFrame + fetched_at
├── storage.py      # Чтение / запись Parquet + watermark recovery
├── watcher.py      # Polling с in-memory watermark + thread lock
app.py              # Streamlit UI (фильтр по дате, управление watcher)
config.yaml         # Параметры подключения и поведения
requirements.txt
```

## Расширение

**Добавить новую метрику:**
1. Написать функцию в `stats.py` (принимает DataFrame, возвращает DataFrame / dict).
2. Вызвать её из `compute_coil_stats()`.
3. Добавить запись в `storage.py` → `save_coil_stats()` (не забыть `fetched_at`).
4. Создать соответствующую поддиректорию в `_SUBDIRS`.

**Графики:**
Parquet-файлы готовы к использованию — достаточно читать их через `pd.read_parquet` и строить графики в Streamlit (`st.line_chart`, `st.bar_chart`) или Plotly. Поле `fetched_at` позволяет строить временные ряды.
