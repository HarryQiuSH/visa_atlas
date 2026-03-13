from __future__ import annotations

import os
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

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
    get_approval_denial_rates,
    get_export_frame,
    get_filtered_record_count,
    get_kpi_summary,
    get_results_table,
    get_salary_distribution,
    get_status_by_fiscal_year,
    get_time_trend,
    get_top_categories,
)
from src.utils import format_currency, format_number

DATA_DIR = Path("data")
DEFAULT_TABLE_LIMIT = 500
TOP_N = 10


def render_error_state(message: str) -> None:
    st.error(message)
    st.stop()


def render_data_loading_status(source_info) -> None:
    """Render a visible data-loading status block for the active source."""
    if source_info.source_kind == "cloudflare_r2":
        st.info(
            "Remote data source active.\n\n"
            f"- Source: {source_info.source_label}\n"
            f"- DuckDB path: `{source_info.parquet_files[0]}`\n"
            "- Queries are reading parquet remotely from Cloudflare R2."
        )
        if source_info.skipped_parquet_files:
            skipped_names = ", ".join(Path(path).name for path in source_info.skipped_parquet_files[:5])
            extra_count = len(source_info.skipped_parquet_files) - min(len(source_info.skipped_parquet_files), 5)
            if extra_count > 0:
                skipped_names += f", and {extra_count} more"
            st.warning(f"Skipped missing remote parquet files: {skipped_names}")
    else:
        st.caption(f"Using local parquet files from `{DATA_DIR}`.")


def get_r2_base_url() -> str:
    """Return the configured R2 base URL for display."""
    return os.getenv("R2_BASE_URL", "")


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
        st.plotly_chart(fig, width='stretch')
    
    # Add approval/denial summary table
    status_by_fy_df = get_status_by_fiscal_year(source_info, filters)
    
    if not status_by_fy_df.empty:
        st.markdown("#### LCA Approvals & Denials")
        # Pivot the data for display
        pivot_df = status_by_fy_df.pivot(index='fiscal_year', columns='status', values='count').fillna(0).astype(int)
        pivot_df = pivot_df.sort_index(ascending=False)
        pivot_df.index.name = 'Fiscal Year'
        st.dataframe(pivot_df, width='stretch')

    fig = build_bar_chart(top_employers_df, title="Top employers", x="label", y="case_count")
    if fig is None:
        st.info("Employer data is unavailable for ranking charts.")
    else:
        st.plotly_chart(fig, width='stretch')

    fig = build_bar_chart(top_titles_df, title="Top job titles", x="label", y="case_count")
    if fig is None:
        st.info("Job title data is unavailable for ranking charts.")
    else:
        st.plotly_chart(fig, width='stretch')

    fig = build_bar_chart(top_locations_df, title="Top locations", x="label", y="case_count")
    if fig is None:
        st.info("Location data is unavailable for ranking charts.")
    else:
        st.plotly_chart(fig, width='stretch')

    salary_fig = build_salary_histogram(salary_df)
    if salary_fig is None:
        st.info("Salary distribution is unavailable because no supported wage column was detected.")
    else:
        st.plotly_chart(salary_fig, width='stretch')


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

    st.dataframe(results_df, width='stretch', hide_index=True)

    export_df = get_export_frame(source_info, filters)
    st.download_button(
        "Download filtered CSV",
        data=export_df.to_csv(index=False).encode("utf-8"),
        file_name="lca_filtered_results.csv",
        mime="text/csv",
    )


# def build_data_source_config() -> str:
#     """Render source-selection controls and return the chosen configuration."""
#     with st.sidebar:
#         st.header("Data source")
#         source_option = st.radio(
#             "Source",
#             options=("Local data/", "Cloudflare R2"),
#             key="data_source_kind",
#         )

#         if source_option == "Cloudflare R2":
#             st.caption(f"Using configured R2 base URL: `{get_r2_base_url() or 'missing'}`")
#             st.caption(
#                 "Remote parquet file names are inferred from local `data/*.parquet` when available, "
#                 "or from `R2_PARQUET_FILES` in `.env`."
#             )
#             return "cloudflare_r2"

#     return "local"


def main() -> None:
    load_dotenv(override=True)
    st.set_page_config(page_title="Visa Atlas", page_icon=":material/travel_explore:", layout="wide")

    st.title("Visa Atlas")
    st.caption("Explore U.S. Labor Condition Application filings with SQL-backed search and lightweight analytics.")
    
    # GitHub link
    st.markdown(
        "📊 [GitHub Repository](https://github.com/HarryQiuSH/visa_atlas) • "
        "Comments, issues, and pull requests are welcome!"
    )

    # Default to Cloudflare R2
    source_kind = "cloudflare_r2"

    try:
        source_info = discover_data_source(
            DATA_DIR,
            source_kind=source_kind,
        )
    except DataSourceError as exc:
        render_error_state(str(exc))

    # status = get_source_status(source_info)
    # status_type = st.success if status.ok else st.warning
    # status_type(status.message)
    # render_data_loading_status(source_info)

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

    # Render table first
    render_results(source_info, filters, matching_count)
    
    # Then KPIs and analytics
    render_kpis(filters, matching_count, source_info)
    render_analytics(source_info, filters)

    st.caption(
        "Date parsing uses DuckDB `TRY_CAST`, so malformed date values are ignored instead of crashing the app."
    )


if __name__ == "__main__":
    main()
