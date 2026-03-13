from __future__ import annotations

import re
from datetime import date, datetime
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Iterable


def normalize_column_name(name: str) -> str:
    """Convert arbitrary column names to lowercase snake_case."""
    normalized = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "column"


def make_unique_names(names: Iterable[str]) -> list[str]:
    """Preserve normalized names while resolving collisions deterministically."""
    counts: dict[str, int] = {}
    result: list[str] = []
    for name in names:
        count = counts.get(name, 0)
        result_name = name if count == 0 else f"{name}_{count + 1}"
        counts[name] = count + 1
        result.append(result_name)
    return result


def quote_identifier(name: str) -> str:
    """Safely quote a SQL identifier for DuckDB."""
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """Safely quote a SQL string literal for DuckDB."""
    return "'" + value.replace("'", "''") + "'"


def coerce_date(value: object) -> date | None:
    """Convert Streamlit widget values into a Python date when possible."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, pd.Timestamp):
        return value.date()
    return None


def format_number(value: int | float | None) -> str:
    """Format counts and numeric KPIs."""
    if value is None:
        return "N/A"
    return f"{value:,.0f}"


def format_currency(value: float | None) -> str:
    """Format wage-like values for display."""
    if value is None or pd.isna(value):
        return "N/A"
    return f"${value:,.0f}"


def get_config_value(key: str, default: str = "") -> str:
    """Get config from st.secrets (Streamlit Cloud) or os.getenv (local .env)."""
    import os
    import streamlit as st
    
    # Try Streamlit secrets first (cloud deployment)
    if hasattr(st, 'secrets') and key in st.secrets:
        return st.secrets[key]
    # Fallback to environment variables (local .env)
    return os.getenv(key, default)
