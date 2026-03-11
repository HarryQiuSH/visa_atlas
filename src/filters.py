from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import streamlit as st

from src.data_loader import DataSourceInfo, FilterBounds, get_canonical_column
from src.queries import get_distinct_values, get_matching_values
from src.utils import coerce_date

if TYPE_CHECKING:
    from datetime import date


FILTER_KEYS = (
    "filter_keyword",
    "filter_employer",
    "filter_job_title",
    "filter_location",
    "filter_case_status",
    "filter_submit_date_range",
    "filter_start_date_range",
    "filter_salary_min",
    "filter_salary_max",
)


@dataclass(frozen=True)
class FilterState:
    keyword: str = ""
    employer_name: str = ""
    job_title: str = ""
    work_location: str = ""
    case_status: tuple[str, ...] = ()
    submit_date_start: date | None = None
    submit_date_end: date | None = None
    employment_start_date_start: date | None = None
    employment_start_date_end: date | None = None
    salary_min: float | None = None
    salary_max: float | None = None


def reset_filters() -> None:
    for key in FILTER_KEYS:
        if key in st.session_state:
            del st.session_state[key]


def _date_range_widget(
    label: str,
    key: str,
    start: object | None,
    end: object | None,
    disabled: bool,
) -> tuple[date | None, date | None]:
    if disabled or not start or not end:
        st.date_input(label, value=(), key=key, disabled=True)
        return None, None

    selection = st.date_input(
        label,
        value=(coerce_date(start), coerce_date(end)),
        key=key,
    )
    if isinstance(selection, tuple) and len(selection) == 2:
        return coerce_date(selection[0]), coerce_date(selection[1])
    return None, None


def build_sidebar_filters(source_info: DataSourceInfo, bounds: FilterBounds) -> FilterState:
    """Render the sidebar filter controls and return the selected state."""
    with st.sidebar:
        st.header("Search filters")
        if st.button("Reset filters", use_container_width=True):
            reset_filters()
            st.rerun()

        keyword = st.text_input(
            "Keyword search",
            key="filter_keyword",
            help="Search across employer, job title, and location fields when available.",
        )

        employer_name = st.text_input(
            "Employer name",
            key="filter_employer",
            disabled=get_canonical_column(source_info, "employer_name") is None,
        )
        if employer_name.strip():
            employer_matches = get_matching_values(source_info, "employer_name", employer_name)
            if employer_matches:
                st.caption("Employer matches: " + " | ".join(employer_matches))
        job_title = st.text_input(
            "Job title",
            key="filter_job_title",
            disabled=get_canonical_column(source_info, "job_title") is None,
        )
        if job_title.strip():
            title_matches = get_matching_values(source_info, "job_title", job_title)
            if title_matches:
                st.caption("Job title matches: " + " | ".join(title_matches))
        work_location = st.text_input(
            "Work location",
            key="filter_location",
            disabled=get_canonical_column(source_info, "work_location") is None,
        )
        if work_location.strip():
            location_matches = get_matching_values(source_info, "work_location", work_location)
            if location_matches:
                st.caption("Location matches: " + " | ".join(location_matches))
        if get_canonical_column(source_info, "case_status") is None:
            st.text_input("Case status", key="filter_case_status", disabled=True)
            case_status: tuple[str, ...] = ()
        else:
            case_status = tuple(
                st.multiselect(
                    "Case status",
                    options=get_distinct_values(source_info, "case_status", limit=20),
                    key="filter_case_status",
                )
            )

        submit_start, submit_end = _date_range_widget(
            "Case submit date range",
            "filter_submit_date_range",
            bounds.submit_date_min,
            bounds.submit_date_max,
            disabled=get_canonical_column(source_info, "case_submit_date") is None,
        )
        start_start, start_end = _date_range_widget(
            "Employment start date range",
            "filter_start_date_range",
            bounds.start_date_min,
            bounds.start_date_max,
            disabled=get_canonical_column(source_info, "employment_start_date") is None,
        )

        salary_enabled = get_canonical_column(source_info, "wage") is not None
        salary_min = st.number_input(
            "Minimum salary",
            key="filter_salary_min",
            min_value=float(bounds.wage_min) if bounds.wage_min is not None else 0.0,
            value=float(bounds.wage_min) if salary_enabled and bounds.wage_min is not None else 0.0,
            step=1000.0,
            disabled=not salary_enabled,
        )
        salary_max = st.number_input(
            "Maximum salary",
            key="filter_salary_max",
            min_value=float(bounds.wage_min) if bounds.wage_min is not None else 0.0,
            value=float(bounds.wage_max) if salary_enabled and bounds.wage_max is not None else 0.0,
            step=1000.0,
            disabled=not salary_enabled,
        )

        if salary_enabled:
            st.caption(
                f"Detected salary range: ${bounds.wage_min:,.0f} to ${bounds.wage_max:,.0f}"
                if bounds.wage_min is not None and bounds.wage_max is not None
                else "Salary values were detected but bounds could not be inferred."
            )

    return FilterState(
        keyword=keyword.strip(),
        employer_name=employer_name.strip(),
        job_title=job_title.strip(),
        work_location=work_location.strip(),
        case_status=case_status,
        submit_date_start=submit_start,
        submit_date_end=submit_end,
        employment_start_date_start=start_start,
        employment_start_date_end=start_end,
        salary_min=salary_min if salary_enabled and salary_min > float(bounds.wage_min or 0) else None,
        salary_max=salary_max if salary_enabled and bounds.wage_max is not None and salary_max < float(bounds.wage_max) else None,
    )
