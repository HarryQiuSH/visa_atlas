from __future__ import annotations

import os
from pathlib import Path

import streamlit as st

from src.charts import (
    build_bar_chart,
    build_salary_histogram,
    build_trend_chart,
)
from src.data_loader import (
    DataSourceError,
    discover_data_source,
    get_filter_bounds,
    get_source_status,
)
from src.filters import FilterState, build_sidebar_filters, reset_filters
from src.queries import (
    get_export_frame,
    get_filtered_record_count,
    get_kpi_summary,
    get_results_table,
    get_salary_distribution,
    get_time_trend,
    get_top_categories,
)
from src.utils import format_currency, format_number

DATA_DIR = Path("data")
DEFAULT_TABLE_LIMIT = 500
DEFAULT_HF_REPO = os.getenv("HF_DATASET_REPO", "HarryQiuSH/LCA2226")
DEFAULT_HF_PATTERN = os.getenv("HF_PARQUET_PATTERN", "*.parquet")
TOP_N = 10


def render_error_state(message: str) -> None:
    st.error(message)
    st.stop()


def render_data_loading_status(source_info) -> None:
    """Render a visible data-loading status block for the active source."""
    if source_info.source_kind == "huggingface":
        st.info(
            "Remote data source active.\n\n"
            f"- Source: {source_info.source_label}\n"
            f"- DuckDB path: `{source_info.parquet_files[0]}`\n"
            "- Queries are reading parquet remotely from Hugging Face."
        )
    else:
        st.caption(f"Using local parquet files from `{DATA_DIR}`.")


def render_kpis(filters: FilterState, total_records: int, source_info) -> None:
    unfiltered_total = get_filtered_record_count(source_info, FilterState())
    summary = get_kpi_summary(source_info, filters)

    delta_value = total_records - unfiltered_total
    delta_label = f"{delta_value:+,} vs all records" if unfiltered_total else None

    top_dimension = "Top location"
    top_value = summary.top_location
    if not top_value:
        top_dimension = "Top job title"
        top_value = summary.top_job_title

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Matching cases", format_number(summary.total_cases), delta=delta_label)
    col2.metric("Distinct employers", format_number(summary.distinct_employers))
    col3.metric("Median offered wage", format_currency(summary.median_wage))
    col4.metric(top_dimension, top_value or "N/A")


def render_analytics(source_info, filters: FilterState) -> None:
    st.subheader("Analytics")
    if st.button("Reset all filters", key="reset_filters_top"):
        reset_filters()
        st.rerun()

    trend_df = get_time_trend(source_info, filters)
    top_employers_df = get_top_categories(source_info, filters, "employer_name", TOP_N)
    top_titles_df = get_top_categories(source_info, filters, "job_title", TOP_N)
    top_locations_df = get_top_categories(source_info, filters, "work_location", TOP_N)
    salary_df = get_salary_distribution(source_info, filters)

    fig = build_trend_chart(trend_df)
    if fig is None:
        st.info("Submit date data is unavailable for the filings trend.")
    else:
        st.plotly_chart(fig, use_container_width=True)

    fig = build_bar_chart(top_employers_df, title="Top employers", x="label", y="case_count")
    if fig is None:
        st.info("Employer data is unavailable for ranking charts.")
    else:
        st.plotly_chart(fig, use_container_width=True)

    fig = build_bar_chart(top_titles_df, title="Top job titles", x="label", y="case_count")
    if fig is None:
        st.info("Job title data is unavailable for ranking charts.")
    else:
        st.plotly_chart(fig, use_container_width=True)

    fig = build_bar_chart(top_locations_df, title="Top locations", x="label", y="case_count")
    if fig is None:
        st.info("Location data is unavailable for ranking charts.")
    else:
        st.plotly_chart(fig, use_container_width=True)

    salary_fig = build_salary_histogram(salary_df)
    if salary_fig is None:
        st.info("Salary distribution is unavailable because no supported wage column was detected.")
    else:
        st.plotly_chart(salary_fig, use_container_width=True)


def render_results(source_info, filters: FilterState, total_records: int) -> None:
    st.subheader("Detailed records")

    if total_records == 0:
        st.info("No records match the current filters.")
        return

    result_limit = st.number_input(
        "Rows to display",
        min_value=100,
        max_value=5000,
        value=DEFAULT_TABLE_LIMIT,
        step=100,
    )

    results_df = get_results_table(source_info, filters, limit=int(result_limit))
    if results_df.empty:
        st.info("No rows are available to display.")
        return

    st.dataframe(results_df, use_container_width=True, hide_index=True)

    export_df = get_export_frame(source_info, filters)
    st.download_button(
        "Download filtered CSV",
        data=export_df.to_csv(index=False).encode("utf-8"),
        file_name="lca_filtered_results.csv",
        mime="text/csv",
    )


def build_data_source_config() -> tuple[str, str, str]:
    """Render source-selection controls and return the chosen configuration."""
    with st.sidebar:
        st.header("Data source")
        source_option = st.radio(
            "Source",
            options=("Local data/", "Hugging Face"),
            key="data_source_kind",
        )

        if source_option == "Hugging Face":
            hf_repo_id = st.text_input(
                "Dataset repo",
                value=DEFAULT_HF_REPO,
                key="hf_dataset_repo",
                help="Example: HarryQiuSH/LCA2226",
            )
            hf_parquet_pattern = st.text_input(
                "Parquet pattern",
                value=DEFAULT_HF_PATTERN,
                key="hf_parquet_pattern",
                help="Example: *.parquet or data/*.parquet",
            )
            return "huggingface", hf_repo_id.strip(), hf_parquet_pattern.strip()

    return "local", "", "*.parquet"


def main() -> None:
    st.set_page_config(page_title="Visa Atlas", page_icon=":material/travel_explore:", layout="wide")

    st.title("Visa Atlas")
    st.caption("Explore U.S. Labor Condition Application filings with SQL-backed search and lightweight analytics.")

    source_kind, hf_repo_id, hf_parquet_pattern = build_data_source_config()

    try:
        source_info = discover_data_source(
            DATA_DIR,
            source_kind=source_kind,
            hf_repo_id=hf_repo_id,
            hf_parquet_pattern=hf_parquet_pattern,
        )
    except DataSourceError as exc:
        render_error_state(str(exc))

    status = get_source_status(source_info)
    status_type = st.success if status.ok else st.warning
    status_type(status.message)
    render_data_loading_status(source_info)

    st.write(
        "Data is read with DuckDB from either local parquet files under `data/` or a Hugging Face dataset repo. "
        "Wage metrics use the first supported wage-style column detected, so totals are useful for exploration "
        "but may mix units if the source data does."
    )

    if source_info.missing_recommended:
        st.warning(
            "Some expected columns were not detected: "
            + ", ".join(source_info.missing_recommended)
            + ". The app will hide unsupported metrics and filters."
        )

    filter_bounds = get_filter_bounds(source_info)
    filters = build_sidebar_filters(source_info, filter_bounds)
    matching_count = get_filtered_record_count(source_info, filters)

    st.caption(f"Matching records after filters: {format_number(matching_count)}")

    render_kpis(filters, matching_count, source_info)
    render_analytics(source_info, filters)
    render_results(source_info, filters, matching_count)

    st.caption(
        "Date parsing uses DuckDB `TRY_CAST`, so malformed date values are ignored instead of crashing the app."
    )


if __name__ == "__main__":
    main()
