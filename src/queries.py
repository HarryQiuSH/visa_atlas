from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd
import streamlit as st

from src.data_loader import (
    DataSourceInfo,
    get_canonical_column,
    get_connection,
    get_date_expr,
    get_location_expr,
    get_numeric_expr,
)
from src.utils import quote_identifier

if TYPE_CHECKING:
    from src.filters import FilterState


DISPLAY_COLUMNS = (
    "case_status",
    "employer_name",
    "job_title",
    "work_location",
    "case_submit_date",
    "employment_start_date",
    "wage",
)


@dataclass(frozen=True)
class KpiSummary:
    total_cases: int
    distinct_employers: int
    median_wage: float | None
    top_location: str | None
    top_job_title: str | None


def _get_field_expr(source_info: DataSourceInfo, canonical_name: str) -> str | None:
    if canonical_name in {"case_submit_date", "employment_start_date"}:
        return get_date_expr(source_info, canonical_name)
    if canonical_name == "work_location":
        return get_location_expr(source_info)
    if canonical_name == "wage":
        return get_numeric_expr(source_info, canonical_name)
    column = get_canonical_column(source_info, canonical_name)
    if not column:
        return None
    return quote_identifier(column)


def build_where_clause(source_info: DataSourceInfo, filters: FilterState) -> tuple[str, list[object]]:
    """Translate UI filters into a SQL WHERE clause with bound parameters."""
    conditions: list[str] = []
    params: list[object] = []

    def add_contains(canonical_name: str, value: str) -> None:
        expr = _get_field_expr(source_info, canonical_name)
        if not expr or not value:
            return
        conditions.append(f"{expr} IS NOT NULL AND contains(lower(CAST({expr} AS VARCHAR)), ?)")
        params.append(value.lower())

    add_contains("employer_name", filters.employer_name)
    add_contains("job_title", filters.job_title)
    add_contains("work_location", filters.work_location)
    if filters.case_status:
        expr = _get_field_expr(source_info, "case_status")
        if expr:
            placeholders = ", ".join("?" for _ in filters.case_status)
            conditions.append(f"{expr} IN ({placeholders})")
            params.extend(filters.case_status)

    if filters.keyword:
        keyword_conditions: list[str] = []
        for name in ("employer_name", "job_title", "work_location"):
            expr = _get_field_expr(source_info, name)
            if expr:
                keyword_conditions.append(f"contains(lower(CAST({expr} AS VARCHAR)), ?)")
                params.append(filters.keyword.lower())
        if keyword_conditions:
            conditions.append("(" + " OR ".join(keyword_conditions) + ")")

    submit_expr = _get_field_expr(source_info, "case_submit_date")
    if submit_expr and filters.submit_date_start:
        conditions.append(f"{submit_expr} >= ?")
        params.append(filters.submit_date_start)
    if submit_expr and filters.submit_date_end:
        conditions.append(f"{submit_expr} <= ?")
        params.append(filters.submit_date_end)

    start_expr = _get_field_expr(source_info, "employment_start_date")
    if start_expr and filters.employment_start_date_start:
        conditions.append(f"{start_expr} >= ?")
        params.append(filters.employment_start_date_start)
    if start_expr and filters.employment_start_date_end:
        conditions.append(f"{start_expr} <= ?")
        params.append(filters.employment_start_date_end)

    wage_expr = _get_field_expr(source_info, "wage")
    if wage_expr and filters.salary_min is not None:
        conditions.append(f"{wage_expr} >= ?")
        params.append(filters.salary_min)
    if wage_expr and filters.salary_max is not None:
        conditions.append(f"{wage_expr} <= ?")
        params.append(filters.salary_max)

    if not conditions:
        return "", params
    return " WHERE " + " AND ".join(conditions), params


def _execute_df(sql: str, params: list[object] | None = None) -> pd.DataFrame:
    conn = get_connection()
    return conn.execute(sql, params or []).df()


@st.cache_data(show_spinner=False)
def get_filtered_record_count(source_info: DataSourceInfo, filters: FilterState) -> int:
    where_sql, params = build_where_clause(source_info, filters)
    sql = f"SELECT COUNT(*) AS record_count FROM ({source_info.table_sql}) AS src{where_sql}"  # noqa: S608
    row = get_connection().execute(sql, params).fetchone()
    return int(row[0]) if row else 0


@st.cache_data(show_spinner=False)
def get_distinct_filter_count(source_info: DataSourceInfo, filters: FilterState, canonical_name: str) -> int:
    expr = _get_field_expr(source_info, canonical_name)
    if not expr:
        return 0
    where_sql, params = build_where_clause(source_info, filters)
    sql = f"SELECT COUNT(DISTINCT {expr}) FROM ({source_info.table_sql}) AS src{where_sql}"  # noqa: S608
    row = get_connection().execute(sql, params).fetchone()
    return int(row[0]) if row else 0


@st.cache_data(show_spinner=False)
def get_distinct_values(source_info: DataSourceInfo, canonical_name: str, limit: int = 100) -> list[str]:
    expr = _get_field_expr(source_info, canonical_name)
    if not expr:
        return []
    sql = (
        f"SELECT DISTINCT CAST({expr} AS VARCHAR) AS value "  # noqa: S608
        f"FROM ({source_info.table_sql}) AS src "  # noqa: S608
        f"WHERE {expr} IS NOT NULL "  # noqa: S608
        "ORDER BY 1 "
        f"LIMIT {int(limit)}"
    )  # noqa: S608
    df = _execute_df(sql)
    return df["value"].tolist()


@st.cache_data(show_spinner=False)
def get_matching_values(
    source_info: DataSourceInfo, canonical_name: str, query: str, limit: int = 8
) -> list[str]:
    expr = _get_field_expr(source_info, canonical_name)
    if not expr or not query.strip():
        return []
    sql = (
        f"SELECT DISTINCT CAST({expr} AS VARCHAR) AS value "  # noqa: S608
        f"FROM ({source_info.table_sql}) AS src "  # noqa: S608
        f"WHERE {expr} IS NOT NULL "  # noqa: S608
        f"AND contains(lower(CAST({expr} AS VARCHAR)), ?) "  # noqa: S608
        "ORDER BY 1 "
        f"LIMIT {int(limit)}"
    )  # noqa: S608
    df = _execute_df(sql, [query.lower()])
    return df["value"].tolist()


@st.cache_data(show_spinner=False)
def get_kpi_summary(source_info: DataSourceInfo, filters: FilterState) -> KpiSummary:
    where_sql, params = build_where_clause(source_info, filters)
    wage_expr = _get_field_expr(source_info, "wage")
    employer_expr = _get_field_expr(source_info, "employer_name")
    location_expr = _get_field_expr(source_info, "work_location")
    title_expr = _get_field_expr(source_info, "job_title")

    summary_parts = ["COUNT(*) AS total_cases"]
    summary_parts.append(f"COUNT(DISTINCT {employer_expr}) AS distinct_employers" if employer_expr else "0 AS distinct_employers")
    summary_parts.append(f"MEDIAN({wage_expr}) AS median_wage" if wage_expr else "NULL AS median_wage")

    summary_sql = f"SELECT {', '.join(summary_parts)} FROM ({source_info.table_sql}) AS src{where_sql}"  # noqa: S608
    summary_row = get_connection().execute(summary_sql, params).fetchone()

    def _top_value(expr: str | None) -> str | None:
        if not expr:
            return None
        if where_sql:
            sql = (  # noqa: S608
                f"SELECT {expr} AS value FROM ({source_info.table_sql}) AS src"  # noqa: S608
                f"{where_sql} AND {expr} IS NOT NULL"  # noqa: S608
            )
        else:
            sql = f"SELECT {expr} AS value FROM ({source_info.table_sql}) AS src WHERE {expr} IS NOT NULL"  # noqa: S608
        sql += " GROUP BY 1 ORDER BY COUNT(*) DESC, value ASC LIMIT 1"  # noqa: S608
        row = get_connection().execute(sql, params).fetchone()  # noqa: S608
        return row[0] if row else None

    return KpiSummary(
        total_cases=int(summary_row[0]) if summary_row else 0,
        distinct_employers=int(summary_row[1]) if summary_row else 0,
        median_wage=float(summary_row[2]) if summary_row and summary_row[2] is not None else None,
        top_location=_top_value(location_expr),
        top_job_title=_top_value(title_expr),
    )


@st.cache_data(show_spinner=False)
def get_time_trend(source_info: DataSourceInfo, filters: FilterState) -> pd.DataFrame:
    date_expr = _get_field_expr(source_info, "case_submit_date")
    if not date_expr:
        return pd.DataFrame()
    where_sql, params = build_where_clause(source_info, filters)
    extra_filter = f"{where_sql} AND {date_expr} IS NOT NULL" if where_sql else f" WHERE {date_expr} IS NOT NULL"
    sql = f"""
        SELECT
            date_trunc('month', {date_expr}) AS filing_month,
            COUNT(*) AS case_count
        FROM ({source_info.table_sql}) AS src
        {extra_filter}
        GROUP BY 1
        ORDER BY 1
    """
    return _execute_df(sql, params)


@st.cache_data(show_spinner=False)
def get_top_categories(source_info: DataSourceInfo, filters: FilterState, canonical_name: str, limit: int) -> pd.DataFrame:
    expr = _get_field_expr(source_info, canonical_name)
    if not expr:
        return pd.DataFrame()
    where_sql, params = build_where_clause(source_info, filters)
    extra_filter = f"{where_sql} AND {expr} IS NOT NULL" if where_sql else f" WHERE {expr} IS NOT NULL"
    sql = f"""
        SELECT
            CAST({expr} AS VARCHAR) AS label,
            COUNT(*) AS case_count
        FROM ({source_info.table_sql}) AS src
        {extra_filter}
        GROUP BY 1
        ORDER BY case_count DESC, label ASC
        LIMIT {int(limit)}
    """
    return _execute_df(sql, params)


@st.cache_data(show_spinner=False)
def get_salary_distribution(source_info: DataSourceInfo, filters: FilterState) -> pd.DataFrame:
    wage_expr = _get_field_expr(source_info, "wage")
    if not wage_expr:
        return pd.DataFrame()
    where_sql, params = build_where_clause(source_info, filters)
    extra_filter = f"{where_sql} AND {wage_expr} IS NOT NULL" if where_sql else f" WHERE {wage_expr} IS NOT NULL"
    sql = (
        f"SELECT {wage_expr} AS wage "  # noqa: S608
        f"FROM ({source_info.table_sql}) AS src "  # noqa: S608
        f"{extra_filter} "  # noqa: S608
        "LIMIT 10000"
    )  # noqa: S608
    return _execute_df(sql, params)


@st.cache_data(show_spinner=False)
def get_results_table(source_info: DataSourceInfo, filters: FilterState, limit: int) -> pd.DataFrame:
    where_sql, params = build_where_clause(source_info, filters)
    select_parts: list[str] = []

    for canonical_name in DISPLAY_COLUMNS:
        expr = _get_field_expr(source_info, canonical_name)
        if expr:
            select_parts.append(f"{expr} AS {quote_identifier(canonical_name)}")  # noqa: S608

    if not select_parts:
        fallback_column = quote_identifier(source_info.normalized_columns[0])
        select_parts = [fallback_column]

    order_expr = _get_field_expr(source_info, "case_submit_date")
    if not order_expr:
        order_expr = _get_field_expr(source_info, "employment_start_date")
    order_sql = f" ORDER BY {order_expr} DESC NULLS LAST" if order_expr else ""

    sql = (  # noqa: S608
        f"SELECT {', '.join(select_parts)} FROM ({source_info.table_sql}) AS src"  # noqa: S608
        f"{where_sql}{order_sql} LIMIT {int(limit)}"  # noqa: S608
    )
    df = _execute_df(sql, params)

    for column in ("case_submit_date", "employment_start_date"):
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce").dt.date
    if "wage" in df.columns:
        df["wage"] = pd.to_numeric(df["wage"], errors="coerce")

    renamed_columns = {
        "case_status": "Case status",
        "employer_name": "Employer",
        "job_title": "Job title",
        "work_location": "Work location",
        "case_submit_date": "Case submit date",
        "employment_start_date": "Employment start date",
        "wage": "Offered wage",
    }
    return df.rename(columns=renamed_columns)


@st.cache_data(show_spinner=False)
def get_export_frame(source_info: DataSourceInfo, filters: FilterState) -> pd.DataFrame:
    """Return the full filtered result set for CSV export."""
    where_sql, params = build_where_clause(source_info, filters)
    select_parts: list[str] = []
    for canonical_name in DISPLAY_COLUMNS:
        expr = _get_field_expr(source_info, canonical_name)
        if expr:
            select_parts.append(f"{expr} AS {quote_identifier(canonical_name)}")
    if not select_parts:
        select_parts = [quote_identifier(column) for column in source_info.normalized_columns[:10]]

    sql = f"SELECT {', '.join(select_parts)} FROM ({source_info.table_sql}) AS src{where_sql}"  # noqa: S608
    return _execute_df(sql, params)
