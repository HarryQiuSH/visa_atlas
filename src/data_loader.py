from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import duckdb
import streamlit as st

from src.utils import make_unique_names, normalize_column_name, quote_identifier, quote_literal

if TYPE_CHECKING:
    from pathlib import Path


CANONICAL_ALIASES: dict[str, tuple[str, ...]] = {
    "employer_name": (
        "employer_name",
        "employer",
        "employer_business_name",
        "petitioner_name",
        "employer_full_name",
    ),
    "job_title": (
        "job_title",
        "soc_title",
        "position_title",
        "job_name",
        "occupation_title",
    ),
    "work_location": (
        "work_location",
        "worksite_city",
        "worksite_state",
        "worksite",
        "worksite_location",
        "job_location",
        "primary_worksite",
        "employment_location",
        "city_state",
    ),
    "case_submit_date": (
        "case_submit_date",
        "submitted_date",
        "submission_date",
        "case_received_date",
        "received_date",
        "lca_case_submit_date",
        "decision_date",
    ),
    "employment_start_date": (
        "employment_start_date",
        "begin_date",
        "start_date",
        "employment_begin_date",
        "lca_case_start_date",
    ),
    "wage": (
        "wage",
        "wage_rate_of_pay_from",
        "wage_rate_of_pay_to",
        "wage_rate",
        "wage_from",
        "offered_wage",
        "offered_wage_from",
        "prevailing_wage",
        "pw_wage_level_1",
        "salary",
        "annual_salary",
        "base_salary",
    ),
    "case_status": (
        "case_status",
        "status",
        "application_status",
        "decision_status",
    ),
}

RECOMMENDED_COLUMNS = (
    "employer_name",
    "job_title",
    "work_location",
    "case_submit_date",
    "employment_start_date",
    "wage",
    "case_status",
)


class DataSourceError(RuntimeError):
    """Raised when the parquet-backed source cannot be used."""


@dataclass(frozen=True)
class DataSourceInfo:
    source_kind: str
    source_label: str
    parquet_files: tuple[str, ...]
    table_sql: str
    normalized_columns: tuple[str, ...]
    canonical_map: dict[str, str]
    missing_recommended: tuple[str, ...]


@dataclass(frozen=True)
class SourceStatus:
    ok: bool
    message: str


@dataclass(frozen=True)
class FilterBounds:
    submit_date_min: object | None
    submit_date_max: object | None
    start_date_min: object | None
    start_date_max: object | None
    wage_min: float | None
    wage_max: float | None


@st.cache_resource(show_spinner=False)
def get_connection() -> duckdb.DuckDBPyConnection:
    """Create a reusable in-memory DuckDB connection."""
    connection = duckdb.connect(database=":memory:")
    connection.execute("PRAGMA threads=4")
    return connection


def discover_parquet_files(data_dir: Path) -> list[Path]:
    """Return all parquet files found under the configured data directory."""
    if not data_dir.exists():
        return []
    return sorted(data_dir.rglob("*.parquet"))


def _build_local_source_sql(parquet_files: tuple[str, ...]) -> str:
    file_literals = ", ".join(quote_literal(file_path) for file_path in parquet_files)
    return f"read_parquet([{file_literals}], union_by_name=true, filename=true)"  # noqa: S608


def build_hf_source_path(repo_id: str, parquet_pattern: str) -> str:
    """Return the DuckDB hf:// path for a dataset repo and parquet glob."""
    repo_path = repo_id.strip().strip("/")
    pattern_path = parquet_pattern.strip().lstrip("/")
    return f"hf://datasets/{repo_path}/{pattern_path}"


def _build_table_sql(source_sql: str) -> str:
    conn = get_connection()
    describe_sql = f"DESCRIBE SELECT * FROM {source_sql}"  # noqa: S608
    raw_columns = conn.execute(describe_sql).df()["column_name"].tolist()

    normalized_names = make_unique_names(normalize_column_name(column) for column in raw_columns)
    select_parts = [
        f"{quote_identifier(original)} AS {quote_identifier(normalized)}"
        for original, normalized in zip(raw_columns, normalized_names, strict=True)
    ]
    return f"SELECT {', '.join(select_parts)} FROM {source_sql}"  # noqa: S608


@st.cache_data(show_spinner=False)
def discover_data_source(
    data_dir: Path,
    source_kind: str = "local",
    hf_repo_id: str = "",
    hf_parquet_pattern: str = "*.parquet",
) -> DataSourceInfo:
    """Inspect parquet files and return the normalized source configuration."""
    if source_kind == "huggingface":
        repo_id = hf_repo_id.strip()
        if not repo_id:
            raise DataSourceError("Enter a Hugging Face dataset repo ID such as `HarryQiuSH/LCA2226`.")
        source_path = build_hf_source_path(repo_id, hf_parquet_pattern)
        parquet_files = (source_path,)
        source_label = f"Hugging Face dataset `{repo_id}` ({hf_parquet_pattern})"
        source_sql = f"read_parquet({quote_literal(source_path)}, union_by_name=true, filename=true)"  # noqa: S608
    else:
        parquet_files = tuple(str(path) for path in discover_parquet_files(data_dir))
        if not parquet_files:
            raise DataSourceError("No parquet files were found under `data/`. Add one or more `.parquet` files and rerun the app.")
        source_label = f"Local data directory `{data_dir}`"
        source_sql = _build_local_source_sql(parquet_files)

    try:
        table_sql = _build_table_sql(source_sql)
    except duckdb.Error as exc:
        if source_kind == "huggingface":
            raise DataSourceError(
                "Unable to read parquet files from Hugging Face. Confirm the repo ID, file pattern, network access, "
                "and that the dataset is public or configured for DuckDB access."
            ) from exc
        raise DataSourceError("Unable to inspect local parquet files. Check that the files are readable and valid.") from exc

    conn = get_connection()
    normalized_columns = tuple(
        conn.execute(f"DESCRIBE SELECT * FROM ({table_sql}) AS src").df()["column_name"].tolist()  # noqa: S608
    )

    canonical_map: dict[str, str] = {}
    for canonical_name, aliases in CANONICAL_ALIASES.items():
        for candidate in aliases:
            if candidate in normalized_columns:
                canonical_map[canonical_name] = candidate
                break

    missing_recommended = tuple(canonical_name for canonical_name in RECOMMENDED_COLUMNS if canonical_name not in canonical_map)

    return DataSourceInfo(
        source_kind=source_kind,
        source_label=source_label,
        parquet_files=parquet_files,
        table_sql=table_sql,
        normalized_columns=normalized_columns,
        canonical_map=canonical_map,
        missing_recommended=missing_recommended,
    )


def get_source_status(source_info: DataSourceInfo) -> SourceStatus:
    """Build a concise status message for the current data source."""
    file_count = len(source_info.parquet_files)
    column_count = len(source_info.normalized_columns)
    message = f"Data source loaded: {source_info.source_label}. {file_count} source path(s), {column_count} normalized column(s)."
    return SourceStatus(ok=file_count > 0, message=message)


def get_canonical_column(source_info: DataSourceInfo, canonical_name: str) -> str | None:
    """Resolve a canonical name to the normalized column present in the source."""
    return source_info.canonical_map.get(canonical_name)


def get_date_expr(source_info: DataSourceInfo, canonical_name: str) -> str | None:
    """Return a tolerant date expression for the requested canonical field."""
    column = get_canonical_column(source_info, canonical_name)
    if not column:
        return None
    return f"TRY_CAST({quote_identifier(column)} AS DATE)"


def get_location_expr(source_info: DataSourceInfo) -> str | None:
    """Return a best-effort location expression, preferring city plus state."""
    city_column = get_canonical_column(source_info, "work_location")
    state_column = get_canonical_column(source_info, "worksite_state")

    if city_column and state_column:
        city_expr = quote_identifier(city_column)
        state_expr = quote_identifier(state_column)
        return (
            "CASE "
            f"WHEN {city_expr} IS NULL AND {state_expr} IS NULL THEN NULL "
            f"WHEN {state_expr} IS NULL OR trim(CAST({state_expr} AS VARCHAR)) = '' THEN CAST({city_expr} AS VARCHAR) "
            f"WHEN {city_expr} IS NULL OR trim(CAST({city_expr} AS VARCHAR)) = '' THEN CAST({state_expr} AS VARCHAR) "
            f"ELSE CAST({city_expr} AS VARCHAR) || ', ' || CAST({state_expr} AS VARCHAR) END"
        )

    if city_column:
        return quote_identifier(city_column)
    if state_column:
        return quote_identifier(state_column)
    return None


def get_numeric_expr(source_info: DataSourceInfo, canonical_name: str) -> str | None:
    """Return a tolerant numeric expression for salary-like fields."""
    column = get_canonical_column(source_info, canonical_name)
    if not column:
        return None
    return "TRY_CAST(" f"regexp_replace(CAST({quote_identifier(column)} AS VARCHAR), '[^0-9.\\-]', '', 'g')" " AS DOUBLE)"


@st.cache_data(show_spinner=False)
def get_filter_bounds(source_info: DataSourceInfo) -> FilterBounds:
    """Calculate filter defaults without scanning the full dataset into pandas."""
    conn = get_connection()
    submit_expr = get_date_expr(source_info, "case_submit_date")
    start_expr = get_date_expr(source_info, "employment_start_date")
    wage_expr = get_numeric_expr(source_info, "wage")

    queries: list[str] = []
    if submit_expr:
        queries.extend(
            [
                f"(SELECT MIN({submit_expr}) FROM ({source_info.table_sql}) AS src) AS submit_date_min",  # noqa: S608
                f"(SELECT MAX({submit_expr}) FROM ({source_info.table_sql}) AS src) AS submit_date_max",  # noqa: S608
            ]
        )
    else:
        queries.extend(["NULL AS submit_date_min", "NULL AS submit_date_max"])

    if start_expr:
        queries.extend(
            [
                f"(SELECT MIN({start_expr}) FROM ({source_info.table_sql}) AS src) AS start_date_min",  # noqa: S608
                f"(SELECT MAX({start_expr}) FROM ({source_info.table_sql}) AS src) AS start_date_max",  # noqa: S608
            ]
        )
    else:
        queries.extend(["NULL AS start_date_min", "NULL AS start_date_max"])

    if wage_expr:
        queries.extend(
            [
                f"(SELECT MIN({wage_expr}) FROM ({source_info.table_sql}) AS src WHERE {wage_expr} IS NOT NULL) AS wage_min",  # noqa: S608
                f"(SELECT MAX({wage_expr}) FROM ({source_info.table_sql}) AS src WHERE {wage_expr} IS NOT NULL) AS wage_max",  # noqa: S608
            ]
        )
    else:
        queries.extend(["NULL AS wage_min", "NULL AS wage_max"])

    row = conn.execute(f"SELECT {', '.join(queries)}").fetchone()  # noqa: S608
    return FilterBounds(
        submit_date_min=row[0],
        submit_date_max=row[1],
        start_date_min=row[2],
        start_date_max=row[3],
        wage_min=row[4],
        wage_max=row[5],
    )
