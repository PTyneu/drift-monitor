"""Streamlit single-page app for comparing defect data across sources.

Modes:
  1. Тестовая БД       — statistics from the test database only
  2. Продуктовая БД    — statistics from the prod database only
  3. Сравнение БД с CSV — compare a DB with training CSV (no confidence in CSV)
  4. Сравнение двух БД  — compare test vs prod, matching coils by coilid

Run:
    streamlit run compare_app.py
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from drift.config import load_config
from drift.storage import (
    load_all_summaries,
    load_all_confidence_raw,
    load_all_class_change_top,
)

# ── page config ──────────────────────────────────────────────────

st.set_page_config(page_title="Drift Compare", layout="wide")
st.title("Сравнение моделей")

# ── sidebar ──────────────────────────────────────────────────────

with st.sidebar:
    st.header("Конфигурация")
    config_path = st.text_input("Путь к config.yaml", value="config.yaml")
    cfg = load_config(config_path)
    db_labels = [d.label for d in cfg.databases]

    st.markdown("---")
    mode = st.radio(
        "Режим сравнения",
        options=[
            "Тестовая БД",
            "Продуктовая БД",
            "Сравнение БД с CSV",
            "Сравнение двух БД",
        ],
        key="compare_mode",
    )

    st.markdown("---")
    st.subheader("Период")
    d_from = st.date_input("Дата от", value=date.today() - timedelta(days=7), key="cmp_d_from")
    t_from = st.time_input("Время от", value=time(0, 0), key="cmp_t_from")
    d_to = st.date_input("Дата до", value=date.today(), key="cmp_d_to")
    t_to = st.time_input("Время до", value=time(23, 59), key="cmp_t_to")

    dt_from = pd.Timestamp(datetime.combine(d_from, t_from), tz="UTC")
    dt_to = pd.Timestamp(datetime.combine(d_to, t_to), tz="UTC")

    # CSV uploader (visible only in CSV mode)
    csv_file = None
    csv_db_label = None
    csv_path_input = "training_data.csv"
    if mode == "Сравнение БД с CSV":
        st.markdown("---")
        csv_db_label = st.selectbox("БД для сравнения", options=db_labels, key="csv_db_sel")
        csv_file = st.file_uploader("CSV обучающей выборки", type=["csv"], key="csv_upload")
        csv_path_input = st.text_input(
            "Или путь к CSV на диске", value="training_data.csv", key="csv_path",
        )

# ── helpers ──────────────────────────────────────────────────────


def _filter_by_time(df: pd.DataFrame) -> pd.DataFrame:
    """Filter DataFrame by the sidebar date/time range."""
    if df.empty or "fetched_at" not in df.columns:
        return df
    df = df.copy()
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True)
    mask = (df["fetched_at"] >= dt_from) & (df["fetched_at"] <= dt_to)
    return df.loc[mask]


def _load_for_label(label: str) -> dict[str, pd.DataFrame]:
    """Load parquet data for a single db_label, filtered to the sidebar time range."""
    labels = [label]
    summaries = _filter_by_time(load_all_summaries(cfg.storage.dir, labels))
    conf_raw = load_all_confidence_raw(cfg.storage.dir, labels)
    changes = load_all_class_change_top(cfg.storage.dir, labels)
    # filter conf_raw/changes to coils that survived the time filter
    if not summaries.empty and "coil_id" in summaries.columns:
        coils = set(summaries["coil_id"].unique())
        if not conf_raw.empty and "coil_id" in conf_raw.columns:
            conf_raw = conf_raw[conf_raw["coil_id"].isin(coils)]
        if not changes.empty and "coil_id" in changes.columns:
            changes = changes[changes["coil_id"].isin(coils)]
    else:
        conf_raw = pd.DataFrame()
        changes = pd.DataFrame()
    return {"summaries": summaries, "confidence_raw": conf_raw, "class_changes": changes}


def _load_csv(uploaded_file, fallback_path: str) -> pd.DataFrame | None:
    """Load CSV from upload widget or filesystem path.

    Expected CSV columns:
        instance_label, bbox_xtl, bbox_ytl, bbox_xbr, bbox_ybr
    Maps instance_label ->defectclass for comparison.
    """
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
    else:
        p = Path(fallback_path)
        if not p.exists():
            return None
        df = pd.read_csv(p)
    # normalise column names
    if "instance_label" in df.columns:
        df = df.rename(columns={"instance_label": "defectclass"})
    return df


def _class_counts(df: pd.DataFrame, class_col: str = "defectclass") -> pd.Series:
    """Return defect counts per class as a named Series.

    For summaries (which already have a 'count' column), we sum 'count' by class.
    For raw DataFrames (e.g. CSV), we use value_counts.
    """
    if df.empty or class_col not in df.columns:
        return pd.Series(dtype=int)
    if "count" in df.columns:
        return df.groupby(class_col)["count"].sum().sort_index()
    return df[class_col].value_counts().sort_index()


def _get_all_classes(*dataframes: pd.DataFrame) -> list[str]:
    """Collect unique defectclass values across multiple DataFrames."""
    classes: set[str] = set()
    for df in dataframes:
        if not df.empty and "defectclass" in df.columns:
            classes.update(df["defectclass"].dropna().unique())
    return sorted(classes)


# ── rendering helpers ────────────────────────────────────────────


def render_class_counts_bar(counts: pd.Series, title: str = "Количество дефектов по классам"):
    """Render a simple bar chart of class counts."""
    st.markdown(f"#### {title}")
    if counts.empty:
        st.info("Нет данных.")
        return
    st.bar_chart(counts)


def render_class_counts_comparison(counts_a: pd.Series, counts_b: pd.Series,
                                    label_a: str, label_b: str):
    """Side-by-side bar chart comparing class counts from two sources."""
    st.markdown("#### Попарное сравнение количества дефектов по классам")
    all_classes = sorted(set(counts_a.index) | set(counts_b.index))
    if not all_classes:
        st.info("Нет данных для сравнения.")
        return
    a = counts_a.reindex(all_classes, fill_value=0)
    b = counts_b.reindex(all_classes, fill_value=0)

    fig = go.Figure(data=[
        go.Bar(name=label_a, x=all_classes, y=a.values),
        go.Bar(name=label_b, x=all_classes, y=b.values),
    ])
    fig.update_layout(barmode="group", xaxis_title="Класс дефекта", yaxis_title="Количество")
    st.plotly_chart(fig, use_container_width=True)

    # delta table
    delta = pd.DataFrame({"класс": all_classes, label_a: a.values, label_b: b.values})
    delta["разница"] = delta[label_b] - delta[label_a]
    delta["разница, %"] = ((delta["разница"] / delta[label_a].replace(0, np.nan)) * 100).round(1)
    st.dataframe(delta.set_index("класс"), use_container_width=True)


_CONF_BINS = [i / 10 for i in range(11)]  # [0.0, 0.1, ..., 1.0]
_CONF_LABELS = [f"{_CONF_BINS[i]:.1f}-{_CONF_BINS[i+1]:.1f}" for i in range(len(_CONF_BINS) - 1)]


def render_confidence_histogram(conf_raw: pd.DataFrame, selected_classes: list[str],
                                 title: str = "Распределение confidence"):
    """Render confidence histogram from raw confidence values, binned 0.1 step."""
    st.markdown(f"#### {title}")
    if conf_raw.empty or "confidence" not in conf_raw.columns:
        st.info("Нет данных по confidence.")
        return
    df = conf_raw.copy()
    if "defectclass" in df.columns:
        df = df[df["defectclass"].isin(selected_classes)]
    if df.empty:
        st.info("Нет данных по confidence для выбранных классов.")
        return
    df["bin"] = pd.cut(df["confidence"], bins=_CONF_BINS, labels=_CONF_LABELS, include_lowest=True)
    agg = df.groupby(["defectclass", "bin"], observed=False).size().reset_index(name="count")
    fig = go.Figure()
    for cls in sorted(agg["defectclass"].unique()):
        sub = agg[agg["defectclass"] == cls]
        fig.add_trace(go.Bar(name=cls, x=sub["bin"], y=sub["count"]))
    fig.update_layout(
        barmode="group", xaxis_title="Диапазон confidence", yaxis_title="Количество",
    )
    st.plotly_chart(fig, use_container_width=True)


def render_confidence_comparison(conf_raw_a: pd.DataFrame, conf_raw_b: pd.DataFrame,
                                  label_a: str, label_b: str,
                                  selected_classes: list[str]):
    """Overlaid confidence histograms from two sources, built from raw confidence."""
    st.markdown("#### Сравнение распределений confidence")
    if conf_raw_a.empty and conf_raw_b.empty:
        st.info("Нет данных по confidence.")
        return

    cls_options = sorted(
        set(conf_raw_a["defectclass"].unique() if not conf_raw_a.empty and "defectclass" in conf_raw_a.columns else [])
        | set(conf_raw_b["defectclass"].unique() if not conf_raw_b.empty and "defectclass" in conf_raw_b.columns else [])
    )
    cls_options = [c for c in cls_options if c in selected_classes]
    if not cls_options:
        st.info("Нет данных по confidence для выбранных классов.")
        return

    hist_class = st.selectbox("Класс для гистограммы", options=cls_options, key="cmp_hist_cls")

    fig = go.Figure()
    for raw, lbl in [(conf_raw_a, label_a), (conf_raw_b, label_b)]:
        if raw.empty or "defectclass" not in raw.columns:
            continue
        sub = raw[raw["defectclass"] == hist_class]
        if sub.empty:
            continue
        sub = sub.copy()
        sub["bin"] = pd.cut(sub["confidence"], bins=_CONF_BINS, labels=_CONF_LABELS, include_lowest=True)
        agg = sub.groupby("bin", observed=False).size().reset_index(name="count")
        fig.add_trace(go.Bar(name=lbl, x=agg["bin"], y=agg["count"]))
    fig.update_layout(
        barmode="group", xaxis_title="Диапазон confidence", yaxis_title="Количество",
    )
    st.plotly_chart(fig, use_container_width=True)


def render_class_changes(changes: pd.DataFrame, selected_classes: list[str],
                          title: str = "Переклассификация (raw ->final)"):
    """Show top class transitions and overall reclassification stats."""
    st.markdown(f"#### {title}")
    if changes.empty:
        st.info("Нет данных по переклассификациям.")
        return
    ch = changes.copy()
    if "defectclass" in ch.columns:
        ch = ch[ch["defectclass"].isin(selected_classes)]
    if ch.empty:
        st.info("Нет данных по переклассификациям для выбранных классов.")
        return
    top = (
        ch.groupby(["rawdefectclass", "defectclass"])["count"]
        .sum().reset_index()
        .sort_values("count", ascending=False)
        .head(15)
    )
    top["переход"] = top["rawdefectclass"] + " ->" + top["defectclass"]
    st.dataframe(top[["переход", "count"]].rename(columns={"count": "количество"}).set_index("переход"),
                 use_container_width=True)


def render_class_changes_comparison(changes_a: pd.DataFrame, changes_b: pd.DataFrame,
                                     label_a: str, label_b: str,
                                     selected_classes: list[str]):
    """Side-by-side reclassification stats from two sources."""
    st.markdown("#### Сравнение переклассификаций (raw ->final)")
    frames = []
    for ch, lbl in [(changes_a, label_a), (changes_b, label_b)]:
        if ch.empty:
            continue
        sub = ch.copy()
        if "defectclass" in sub.columns:
            sub = sub[sub["defectclass"].isin(selected_classes)]
        if sub.empty:
            continue
        agg = sub.groupby(["rawdefectclass", "defectclass"])["count"].sum().reset_index()
        agg["source"] = lbl
        frames.append(agg)
    if not frames:
        st.info("Нет данных по переклассификациям.")
        return
    merged = pd.concat(frames, ignore_index=True)
    merged["переход"] = merged["rawdefectclass"] + " ->" + merged["defectclass"]
    pivot = merged.pivot_table(index="переход", columns="source", values="count",
                                aggfunc="sum", fill_value=0)
    pivot = pivot.sort_values(pivot.columns[0], ascending=False).head(15)
    st.dataframe(pivot, use_container_width=True)


def render_reclassification_pct(summaries: pd.DataFrame,
                                 title: str = "Процент переклассификации по рулонам"):
    """Bar chart of changed_pct per coil."""
    st.markdown(f"#### {title}")
    if summaries.empty or "changed_pct" not in summaries.columns:
        st.info("Нет данных.")
        return
    coil_level = summaries.drop_duplicates(subset=["coil_id"]).sort_values("fetched_at")
    series = coil_level.set_index("coil_id")["changed_pct"]
    st.bar_chart(series)


def render_reclassification_pct_comparison(sum_a: pd.DataFrame, sum_b: pd.DataFrame,
                                            label_a: str, label_b: str):
    """Grouped bar chart of changed_pct per coil, test and prod side by side."""
    st.markdown("#### Процент переклассификации по рулонам")
    frames = []
    for s, lbl in [(sum_a, label_a), (sum_b, label_b)]:
        if s.empty or "changed_pct" not in s.columns:
            continue
        coil_level = s.drop_duplicates(subset=["coil_id"])[["coil_id", "changed_pct"]].copy()
        coil_level["source"] = lbl
        frames.append(coil_level)
    if not frames:
        st.info("Нет данных.")
        return
    merged = pd.concat(frames, ignore_index=True)
    pivot = merged.pivot_table(index="coil_id", columns="source", values="changed_pct",
                                aggfunc="first", fill_value=0)
    # sort by coil_id
    pivot = pivot.sort_index()
    fig = go.Figure()
    for col in pivot.columns:
        fig.add_trace(go.Bar(name=col, x=pivot.index.tolist(), y=pivot[col].tolist()))
    fig.update_layout(barmode="group", xaxis_title="Рулон (coil_id)",
                      yaxis_title="Переклассификация, %")
    st.plotly_chart(fig, use_container_width=True)


def render_single_db(data: dict[str, pd.DataFrame], label: str, selected_classes: list[str]):
    """Render full statistics dashboard for a single data source."""
    summaries = data["summaries"]
    conf_raw = data["confidence_raw"]
    changes = data["class_changes"]

    if summaries.empty:
        st.warning(f"Нет данных для «{label}» в выбранном периоде.")
        return

    # filter by selected classes
    if "defectclass" in summaries.columns:
        summaries = summaries[summaries["defectclass"].isin(selected_classes)]

    # summary metrics
    n_coils = summaries["coil_id"].nunique() if "coil_id" in summaries.columns else 0
    total = summaries.drop_duplicates(subset=["coil_id"])["total_defects"].sum() if n_coils else 0
    m1, m2 = st.columns(2)
    m1.metric("Рулонов", n_coils)
    m2.metric("Всего дефектов", int(total))

    # class counts
    counts = _class_counts(summaries)
    render_class_counts_bar(counts, title=f"Количество дефектов по классам ({label})")

    # confidence histogram
    render_confidence_histogram(conf_raw, selected_classes,
                                 title=f"Распределение confidence ({label})")

    # reclassification %
    render_reclassification_pct(summaries, title=f"Процент переклассификации ({label})")

    # class changes
    render_class_changes(changes, selected_classes, title=f"Переклассификация ({label})")


# ── main content ─────────────────────────────────────────────────


def _resolve_label(role: str) -> str:
    """Find the config label matching 'test' or 'prod' substring."""
    for lbl in db_labels:
        if role in lbl.lower():
            return lbl
    # fallback: first or last
    return db_labels[0] if role == "test" else db_labels[-1] if db_labels else "demo"


# Determine data to load based on mode
if mode == "Тестовая БД":
    label = _resolve_label("test")
    data = _load_for_label(label)
    all_classes = _get_all_classes(data["summaries"])
    with st.sidebar:
        st.markdown("---")
        selected_classes = st.multiselect("Классы дефектов", options=all_classes,
                                           default=all_classes, key="cls_filter")
    if not selected_classes:
        st.warning("Выберите хотя бы один класс.")
    else:
        render_single_db(data, label, selected_classes)

elif mode == "Продуктовая БД":
    label = _resolve_label("prod")
    data = _load_for_label(label)
    all_classes = _get_all_classes(data["summaries"])
    with st.sidebar:
        st.markdown("---")
        selected_classes = st.multiselect("Классы дефектов", options=all_classes,
                                           default=all_classes, key="cls_filter")
    if not selected_classes:
        st.warning("Выберите хотя бы один класс.")
    else:
        render_single_db(data, label, selected_classes)

elif mode == "Сравнение БД с CSV":
    db_label = csv_db_label or db_labels[0]
    data = _load_for_label(db_label)
    csv_df = _load_csv(csv_file, csv_path_input)

    if csv_df is None:
        st.warning("CSV-файл не найден. Загрузите файл или укажите путь.")
    else:
        all_classes = _get_all_classes(data["summaries"], csv_df)
        with st.sidebar:
            st.markdown("---")
            selected_classes = st.multiselect("Классы дефектов", options=all_classes,
                                               default=all_classes, key="cls_filter")
        if not selected_classes:
            st.warning("Выберите хотя бы один класс.")
        else:
            # filter DB summaries
            db_sum = data["summaries"]
            if not db_sum.empty and "defectclass" in db_sum.columns:
                db_sum = db_sum[db_sum["defectclass"].isin(selected_classes)]
            csv_filtered = csv_df[csv_df["defectclass"].isin(selected_classes)] if "defectclass" in csv_df.columns else csv_df

            # summary metrics
            st.markdown("### Обзор")
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f"**БД ({db_label})**")
                n_coils = db_sum["coil_id"].nunique() if "coil_id" in db_sum.columns and not db_sum.empty else 0
                total_db = db_sum.drop_duplicates(subset=["coil_id"])["total_defects"].sum() if n_coils else 0
                st.metric("Рулонов", n_coils)
                st.metric("Дефектов (БД)", int(total_db))
            with c2:
                st.markdown("**CSV (обучающая выборка)**")
                st.metric("Записей в CSV", len(csv_filtered))

            # class count comparison
            db_counts = _class_counts(db_sum)
            csv_counts = _class_counts(csv_filtered)
            render_class_counts_comparison(csv_counts, db_counts, "CSV", f"БД ({db_label})")

            # confidence — only from DB (CSV has no confidence)
            render_confidence_histogram(data["confidence_raw"], selected_classes,
                                         title=f"Распределение confidence — только БД ({db_label})")

            # class changes — only from DB
            render_class_changes(data["class_changes"], selected_classes,
                                  title=f"Переклассификация — только БД ({db_label})")

elif mode == "Сравнение двух БД":
    label_a = _resolve_label("test")
    label_b = _resolve_label("prod")

    # allow override if more than 2 DBs
    if len(db_labels) > 2:
        with st.sidebar:
            st.markdown("---")
            label_a = st.selectbox("БД A", options=db_labels, index=0, key="cmp_db_a")
            label_b = st.selectbox("БД B", options=db_labels,
                                    index=min(1, len(db_labels) - 1), key="cmp_db_b")

    data_a = _load_for_label(label_a)
    data_b = _load_for_label(label_b)

    # match by coilid — keep only common coils
    coils_a = set(data_a["summaries"]["coil_id"].unique()) if not data_a["summaries"].empty and "coil_id" in data_a["summaries"].columns else set()
    coils_b = set(data_b["summaries"]["coil_id"].unique()) if not data_b["summaries"].empty and "coil_id" in data_b["summaries"].columns else set()
    common_coils = coils_a & coils_b

    if common_coils:
        st.info(f"Общих рулонов (coilid): **{len(common_coils)}** из {len(coils_a)} (A) и {len(coils_b)} (B)")
    else:
        st.warning("Нет общих рулонов по coilid. Показываем все данные обеих БД.")
        common_coils = coils_a | coils_b

    # filter to common coils
    for data in [data_a, data_b]:
        for key in ("summaries", "confidence_raw", "class_changes"):
            df = data[key]
            if not df.empty and "coil_id" in df.columns:
                data[key] = df[df["coil_id"].isin(common_coils)]

    all_classes = _get_all_classes(data_a["summaries"], data_b["summaries"])
    with st.sidebar:
        st.markdown("---")
        selected_classes = st.multiselect("Классы дефектов", options=all_classes,
                                           default=all_classes, key="cls_filter")

    if not selected_classes:
        st.warning("Выберите хотя бы один класс.")
    else:
        # summary metrics side by side
        st.markdown("### Обзор")
        c1, c2 = st.columns(2)
        for col, data_x, lbl in [(c1, data_a, label_a), (c2, data_b, label_b)]:
            with col:
                st.markdown(f"**{lbl}**")
                s = data_x["summaries"]
                if not s.empty and "defectclass" in s.columns:
                    s = s[s["defectclass"].isin(selected_classes)]
                n = s["coil_id"].nunique() if not s.empty and "coil_id" in s.columns else 0
                t = s.drop_duplicates(subset=["coil_id"])["total_defects"].sum() if n else 0
                st.metric("Рулонов", n)
                st.metric("Всего дефектов", int(t))

        # class counts comparison
        sum_a = data_a["summaries"]
        sum_b = data_b["summaries"]
        if "defectclass" in sum_a.columns:
            sum_a = sum_a[sum_a["defectclass"].isin(selected_classes)]
        if not sum_b.empty and "defectclass" in sum_b.columns:
            sum_b = sum_b[sum_b["defectclass"].isin(selected_classes)]
        render_class_counts_comparison(
            _class_counts(sum_a), _class_counts(sum_b), label_a, label_b,
        )

        # confidence comparison
        render_confidence_comparison(
            data_a["confidence_raw"], data_b["confidence_raw"],
            label_a, label_b, selected_classes,
        )

        # reclassification % grouped by coil
        render_reclassification_pct_comparison(
            data_a["summaries"], data_b["summaries"], label_a, label_b,
        )

        # class changes comparison
        render_class_changes_comparison(
            data_a["class_changes"], data_b["class_changes"],
            label_a, label_b, selected_classes,
        )
