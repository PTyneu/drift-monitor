"""Streamlit entry point for Drift Monitor.

Run:
    streamlit run app.py
"""

from datetime import date, datetime, time, timedelta

import pandas as pd
import streamlit as st

from drift.config import load_config
from drift.watcher import CoilWatcher
from drift.storage import (
    load_all_summaries,
    load_all_confidence,
    load_all_conf_buckets,
    load_all_class_change_top,
)
from drift.comparison import compare_periods
from drift.live import compute_drift_from_db

# ── config ──────────────────────────────────────────────────────

st.set_page_config(page_title="Drift Monitor", layout="wide")
st.title("Drift Monitor")

with st.sidebar:
    st.header("Конфигурация")
    config_path = st.text_input("Путь к config.yaml", value="config.yaml")
    cfg = load_config(config_path)

    st.markdown("---")
    mode_label = "Live (авто)" if cfg.live else "Ручной (по запросу)"
    st.markdown(f"**Режим:** `{mode_label}`")
    st.markdown(f"**Базы данных:** {', '.join(d.label for d in cfg.databases)}")
    if cfg.live:
        st.markdown(f"**Интервал опроса:** {cfg.watcher.poll_interval_sec} сек")
    st.markdown(f"**Хранилище:** `{cfg.storage.dir}`")

# ── watcher instance (shared via session_state) ────────────────

if "watcher" not in st.session_state:
    st.session_state["watcher"] = CoilWatcher(cfg)
    st.session_state["watcher_running"] = False

watcher: CoilWatcher = st.session_state["watcher"]

# ── tabs ────────────────────────────────────────────────────────

tab_monitor, tab_drift = st.tabs(["Мониторинг", "Анализ дрифта"])

db_labels = [d.label for d in cfg.databases]

# ================================================================
#  TAB 1 — Мониторинг 
# ================================================================

with tab_monitor:
    st.markdown("---")

    if cfg.live:
        st.subheader("Live-режим")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Проверить сейчас"):
                result = watcher.run_once()
                if result:
                    for db_label, coils in result.items():
                        st.success(f"**{db_label}**: {', '.join(str(c) for c in coils)}")
                else:
                    st.info("Новых рулонов не обнаружено")
        with col2:
            if not st.session_state["watcher_running"]:
                if st.button("Запустить фоновый мониторинг"):
                    watcher.start()
                    st.session_state["watcher_running"] = True
                    st.success("Фоновый мониторинг запущен")
            else:
                if st.button("Остановить фоновый мониторинг"):
                    watcher.stop()
                    st.session_state["watcher_running"] = False
                    st.session_state["watcher"] = CoilWatcher(cfg)
                    st.info("Мониторинг остановлен")
    else:
        st.subheader("Выберите диапазон дат для анализа")
        default_from = date.today() - timedelta(days=7)
        default_to = date.today()
        col_fd, col_ft, col_td, col_tt = st.columns(4)
        with col_fd:
            d_from = st.date_input("Дата от", value=default_from)
        with col_ft:
            t_from = st.time_input("Время от", value=time(0, 0))
        with col_td:
            d_to = st.date_input("Дата до", value=default_to)
        with col_tt:
            t_to = st.time_input("Время до", value=time(23, 59))
        dt_from = datetime.combine(d_from, t_from)
        dt_to = datetime.combine(d_to, t_to)

        all_labels = [d.label for d in cfg.databases]
        if len(all_labels) > 1:
            selected_dbs = st.multiselect(
                "Базы данных для запроса", options=all_labels, default=all_labels,
            )
        else:
            selected_dbs = all_labels

        if st.button("Запустить запрос", type="primary"):
            with st.spinner("Запрос к базам данных..."):
                result = watcher.run_manual(
                    date_from=dt_from, date_to=dt_to, db_labels=selected_dbs,
                )
            if result:
                for db_label, coils in result.items():
                    st.success(f"**{db_label}**: обработано {len(coils)} рулонов")
            else:
                st.info("Рулонов в выбранном диапазоне не найдено")

    st.markdown("---")
    st.subheader("Результаты")

    all_summaries = load_all_summaries(cfg.storage.dir, db_labels)

    if not all_summaries.empty:
        if "fetched_at" in all_summaries.columns:
            all_summaries["fetched_at"] = pd.to_datetime(all_summaries["fetched_at"], utc=True)

        summaries = all_summaries.copy()

        if "fetched_at" in summaries.columns:
            min_dt = summaries["fetched_at"].min()
            max_dt = summaries["fetched_at"].max()
            col_rd, col_rt, col_rd2, col_rt2 = st.columns(4)
            with col_rd:
                rf_date_from = st.date_input(
                    "Дата от", value=min_dt.date(),
                    min_value=min_dt.date(), max_value=max_dt.date(), key="rf_date_from",
                )
            with col_rt:
                rf_time_from = st.time_input("Время от", value=time(0, 0), key="rf_time_from")
            with col_rd2:
                rf_date_to = st.date_input(
                    "Дата до", value=max_dt.date(),
                    min_value=min_dt.date(), max_value=max_dt.date(), key="rf_date_to",
                )
            with col_rt2:
                rf_time_to = st.time_input("Время до", value=time(23, 59), key="rf_time_to")
            filter_from = pd.Timestamp(datetime.combine(rf_date_from, rf_time_from), tz="UTC")
            filter_to = pd.Timestamp(datetime.combine(rf_date_to, rf_time_to), tz="UTC")
            mask = (summaries["fetched_at"] >= filter_from) & (summaries["fetched_at"] <= filter_to)
            summaries = summaries.loc[mask]

        if "db_label" in summaries.columns and summaries["db_label"].nunique() > 1:
            selected = st.multiselect(
                "Фильтр по базе данных",
                options=sorted(summaries["db_label"].unique()),
                default=sorted(summaries["db_label"].unique()),
                key="results_db_filter",
            )
            summaries = summaries[summaries["db_label"].isin(selected)]

        if not summaries.empty:
            st.markdown("#### Сводка")
            n_coils = summaries["coil_id"].nunique() if "coil_id" in summaries.columns else 0
            total_defects = summaries.drop_duplicates(subset=["coil_id"])["total_defects"].sum() if n_coils else 0
            m1, m2 = st.columns(2)
            m1.metric("Рулонов", n_coils)
            m2.metric("Всего дефектов", int(total_defects))

            if "defectclass" in summaries.columns and "count" in summaries.columns:
                class_counts = summaries.groupby("defectclass")["count"].sum().sort_values(ascending=False)
                st.markdown("**Дефекты по классам**")
                st.bar_chart(class_counts)

            st.markdown("#### Статистика по порогам")
            conf_data = load_all_confidence(cfg.storage.dir, db_labels)
            if not conf_data.empty:
                if "coil_id" in conf_data.columns and "coil_id" in summaries.columns:
                    filtered_coils = summaries["coil_id"].unique()
                    conf_data = conf_data[conf_data["coil_id"].isin(filtered_coils)]
                if not conf_data.empty:
                    stat_cols = [c for c in conf_data.columns if c not in ("coil_id", "fetched_at", "db_label")]
                    if stat_cols:
                        conf_agg = conf_data[stat_cols].groupby(level=0).mean()
                        st.dataframe(conf_agg, width="stretch")
                    else:
                        st.info("Нет данных по confidence")
                else:
                    st.info("Нет данных по confidence для выбранных рулонов")
            else:
                st.info("Нет данных по confidence")

            st.markdown("#### Исходные данные")
            st.dataframe(summaries, width="stretch")
        else:
            st.info("Нет данных, соответствующих выбранным фильтрам.")
    else:
        action = "запустите мониторинг" if cfg.live else "нажмите 'Запустить запрос'"
        st.info(f"Статистика ещё не вычислена. {action.capitalize()}, чтобы начать.")

    # ── period comparison ──────────────────────────────────────────
    st.markdown("---")
    st.subheader("Сравнение периодов")

    if all_summaries.empty:
        st.info("Нет данных для сравнения.")
    else:
        st.markdown("**Период A**")
        ca_d, ca_t, ca_d2, ca_t2 = st.columns(4)
        with ca_d:
            pa_date_from = st.date_input("A: дата от", value=date.today() - timedelta(days=3), key="pa_df")
        with ca_t:
            pa_time_from = st.time_input("A: время от", value=time(0, 0), key="pa_tf")
        with ca_d2:
            pa_date_to = st.date_input("A: дата до", value=date.today() - timedelta(days=1), key="pa_dt")
        with ca_t2:
            pa_time_to = st.time_input("A: время до", value=time(23, 59), key="pa_tt")

        st.markdown("**Период B**")
        cb_d, cb_t, cb_d2, cb_t2 = st.columns(4)
        with cb_d:
            pb_date_from = st.date_input("B: дата от", value=date.today() - timedelta(days=1), key="pb_df")
        with cb_t:
            pb_time_from = st.time_input("B: время от", value=time(0, 0), key="pb_tf")
        with cb_d2:
            pb_date_to = st.date_input("B: дата до", value=date.today(), key="pb_dt")
        with cb_t2:
            pb_time_to = st.time_input("B: время до", value=time(23, 59), key="pb_tt")

        if st.button("Сравнить", type="primary", key="compare_btn"):
            pa_from = pd.Timestamp(datetime.combine(pa_date_from, pa_time_from), tz="UTC")
            pa_to = pd.Timestamp(datetime.combine(pa_date_to, pa_time_to), tz="UTC")
            pb_from = pd.Timestamp(datetime.combine(pb_date_from, pb_time_from), tz="UTC")
            pb_to = pd.Timestamp(datetime.combine(pb_date_to, pb_time_to), tz="UTC")

            mask_a = (all_summaries["fetched_at"] >= pa_from) & (all_summaries["fetched_at"] <= pa_to)
            mask_b = (all_summaries["fetched_at"] >= pb_from) & (all_summaries["fetched_at"] <= pb_to)
            period_a = all_summaries.loc[mask_a]
            period_b = all_summaries.loc[mask_b]

            if period_a.empty and period_b.empty:
                st.warning("Нет данных ни в одном из периодов.")
            else:
                cmp_result = compare_periods(period_a, period_b)
                st.markdown("#### Бок о бок")
                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown(f"**Период A** ({pa_from} -- {pa_to})")
                    if not cmp_result["agg_a"].empty:
                        st.dataframe(cmp_result["agg_a"], width="stretch")
                    else:
                        st.info("Нет данных в периоде A")
                with col_b:
                    st.markdown(f"**Период B** ({pb_from} -- {pb_to})")
                    if not cmp_result["agg_b"].empty:
                        st.dataframe(cmp_result["agg_b"], width="stretch")
                    else:
                        st.info("Нет данных в периоде B")
                if not cmp_result["delta"].empty:
                    st.markdown("#### Разница")
                    st.dataframe(cmp_result["delta"], width="stretch")


# ================================================================
#  TAB 2 — Анализ дрифта
# ================================================================

with tab_drift:
    st.subheader("Анализ дрифта модели")

    # ── Data source selector ───────────────────────────────────
    drift_source = st.radio(
        "Источник данных",
        options=["Parquet (локальное хранилище)", "БД (live-запрос)"],
        horizontal=True,
        key="drift_source",
    )

    all_sum = pd.DataFrame()
    all_conf = pd.DataFrame()
    all_buckets = pd.DataFrame()
    all_changes = pd.DataFrame()

    if drift_source == "Parquet (локальное хранилище)":
        all_sum = load_all_summaries(cfg.storage.dir, db_labels)
        all_conf = load_all_confidence(cfg.storage.dir, db_labels)
        all_buckets = load_all_conf_buckets(cfg.storage.dir, db_labels)
        all_changes = load_all_class_change_top(cfg.storage.dir, db_labels)
    else:
        # ── DB date range ──────────────────────────────────────
        st.markdown("**Диапазон дат для запроса к БД**")
        dc1, dc2, dc3, dc4 = st.columns(4)
        with dc1:
            drift_d_from = st.date_input(
                "Дата от", value=date.today() - timedelta(days=7), key="drift_db_df",
            )
        with dc2:
            drift_t_from = st.time_input("Время от", value=time(0, 0), key="drift_db_tf")
        with dc3:
            drift_d_to = st.date_input("Дата до", value=date.today(), key="drift_db_dt")
        with dc4:
            drift_t_to = st.time_input("Время до", value=time(23, 59), key="drift_db_tt")

        drift_db_labels = db_labels
        if len(db_labels) > 1:
            drift_db_labels = st.multiselect(
                "Базы данных", options=db_labels, default=db_labels, key="drift_db_sel",
            )

        if st.button("Загрузить из БД", type="primary", key="drift_db_btn"):
            dt_from = datetime.combine(drift_d_from, drift_t_from)
            dt_to = datetime.combine(drift_d_to, drift_t_to)
            with st.spinner("Запрос к БД и расчёт статистик..."):
                result = compute_drift_from_db(
                    cfg, date_from=dt_from, date_to=dt_to, db_labels=drift_db_labels,
                )
            st.session_state["drift_db_result"] = result

        if "drift_db_result" in st.session_state:
            result = st.session_state["drift_db_result"]
            all_sum = result["summaries"]
            all_conf = result["confidence"]
            all_buckets = result["conf_buckets"]
            all_changes = result["class_change_top"]

    # ── Drift visualisations (same logic for both sources) ─────
    if all_sum.empty:
        if drift_source == "Parquet (локальное хранилище)":
            st.info("Нет данных. Сначала выполните сканирование на вкладке «Мониторинг».")
        else:
            st.info("Нажмите «Загрузить из БД» для получения данных.")
    else:
        if "fetched_at" in all_sum.columns:
            all_sum["fetched_at"] = pd.to_datetime(all_sum["fetched_at"], utc=True)

        # ── Class selector ─────────────────────────────────────────
        available_classes = sorted(all_sum["defectclass"].unique()) if "defectclass" in all_sum.columns else []
        selected_classes = st.multiselect(
            "Классы дефектов для анализа",
            options=available_classes,
            default=available_classes,
            key="drift_classes",
        )

        if not selected_classes:
            st.warning("Выберите хотя бы один класс.")
        else:
            # Filter summaries
            df = all_sum[all_sum["defectclass"].isin(selected_classes)].copy()
            df = df.sort_values("fetched_at")

            # ── 1. Defects per coil over time ──────────────────────
            st.markdown("---")
            st.markdown("#### Количество дефектов на рулон (по классам)")

            pivot_counts = df.pivot_table(
                index="coil_id", columns="defectclass", values="count",
                aggfunc="sum", fill_value=0,
            )
            coil_order = df.drop_duplicates("coil_id").sort_values("fetched_at")["coil_id"]
            pivot_counts = pivot_counts.reindex(coil_order)
            st.bar_chart(pivot_counts)

            # ── 2. Confidence mean per class over coils ────────────
            st.markdown("#### Средний confidence по классам (по рулонам)")

            if not all_conf.empty:
                conf = all_conf.copy()
                if hasattr(conf.index, 'name') and conf.index.name == "defectclass":
                    conf = conf.reset_index()
                if "defectclass" in conf.columns:
                    conf = conf[conf["defectclass"].isin(selected_classes)]
                if "fetched_at" in conf.columns:
                    conf["fetched_at"] = pd.to_datetime(conf["fetched_at"], utc=True)

                if not conf.empty and "coil_id" in conf.columns and "mean" in conf.columns:
                    conf_pivot = conf.pivot_table(
                        index="coil_id", columns="defectclass", values="mean",
                        aggfunc="mean",
                    )
                    coil_order_conf = conf.drop_duplicates("coil_id").sort_values("fetched_at")["coil_id"]
                    conf_pivot = conf_pivot.reindex(coil_order_conf).dropna(how="all")
                    st.line_chart(conf_pivot)
                else:
                    st.info("Недостаточно данных для графика confidence.")
            else:
                st.info("Нет данных по confidence.")

            # ── 3. Confidence distribution (buckets) ───────────────
            st.markdown("#### Распределение confidence (гистограмма)")

            if not all_buckets.empty:
                bk = all_buckets.copy()
                if "defectclass" in bk.columns:
                    bk_classes = sorted(bk["defectclass"].unique())
                    hist_class = st.selectbox(
                        "Класс для гистограммы confidence",
                        options=bk_classes,
                        key="hist_class_select",
                    )
                    bk_filtered = bk[bk["defectclass"] == hist_class]
                    if not bk_filtered.empty and "conf_bucket" in bk_filtered.columns and "count" in bk_filtered.columns:
                        bk_agg = bk_filtered.groupby("conf_bucket")["count"].sum()
                        st.bar_chart(bk_agg)
                    else:
                        st.info(f"Нет данных по бакетам для класса «{hist_class}».")
                else:
                    st.info("Нет данных по бакетам confidence.")
            else:
                st.info("Нет данных по бакетам confidence.")

            # ── 4. Class change % over coils ───────────────────────
            st.markdown("#### Процент переклассификации (raw → final) по рулонам")

            coil_level = df.drop_duplicates(subset=["coil_id"]).sort_values("fetched_at")
            if "changed_pct" in coil_level.columns:
                change_series = coil_level.set_index("coil_id")["changed_pct"]
                st.bar_chart(change_series)
            else:
                st.info("Нет данных по переклассификации.")

            # ── 5. Top class transitions (sankey-like table) ───────
            st.markdown("#### Топ переклассификаций (raw → final)")

            if not all_changes.empty:
                ch = all_changes.copy()
                if "defectclass" in ch.columns:
                    ch = ch[ch["defectclass"].isin(selected_classes)]
                if not ch.empty and "rawdefectclass" in ch.columns:
                    top_trans = (
                        ch.groupby(["rawdefectclass", "defectclass"])["count"]
                        .sum()
                        .reset_index()
                        .sort_values("count", ascending=False)
                        .head(15)
                    )
                    top_trans["transition"] = top_trans["rawdefectclass"] + " → " + top_trans["defectclass"]
                    st.dataframe(
                        top_trans[["transition", "count"]].set_index("transition"),
                        width="stretch",
                    )
                else:
                    st.info("Нет данных по переклассификациям.")
            else:
                st.info("Нет данных по переклассификациям.")

            # ── 6. Total defects per coil (trend) ──────────────────
            st.markdown("#### Общее количество дефектов на рулон (тренд)")

            if "total_defects" in coil_level.columns:
                defects_trend = coil_level.set_index("coil_id")["total_defects"]
                st.line_chart(defects_trend)

                # Simple drift indicator
                if len(defects_trend) >= 3:
                    first_half = defects_trend.iloc[:len(defects_trend)//2].mean()
                    second_half = defects_trend.iloc[len(defects_trend)//2:].mean()
                    delta = second_half - first_half
                    delta_pct = (delta / first_half * 100) if first_half > 0 else 0
                    col_m1, col_m2, col_m3 = st.columns(3)
                    col_m1.metric("Среднее (1-я половина)", f"{first_half:.1f}")
                    col_m2.metric("Среднее (2-я половина)", f"{second_half:.1f}")
                    col_m3.metric("Дельта", f"{delta:+.1f} ({delta_pct:+.1f}%)")
