from __future__ import annotations

from typing import TYPE_CHECKING

import plotly.express as px
import plotly.graph_objects as go

if TYPE_CHECKING:
    import pandas as pd


def build_trend_chart(df: pd.DataFrame) -> go.Figure | None:
    """Create a monthly filings trend chart."""
    if df.empty:
        return None
    figure = px.line(
        df,
        x="filing_month",
        y="case_count",
        markers=True,
        title="Filings trend over time",
    )
    figure.update_layout(margin={"l": 16, "r": 16, "t": 48, "b": 16}, xaxis_title="", yaxis_title="Cases")
    return figure


def build_bar_chart(df: pd.DataFrame, title: str, x: str, y: str) -> go.Figure | None:
    """Create a horizontal ranking bar chart."""
    if df.empty:
        return None
    chart_df = df.sort_values(y, ascending=True)
    figure = px.bar(chart_df, x=y, y=x, orientation="h", title=title)
    figure.update_layout(margin={"l": 16, "r": 16, "t": 48, "b": 16}, xaxis_title="Cases", yaxis_title="")
    return figure


def build_salary_histogram(df: pd.DataFrame) -> go.Figure | None:
    """Create a wage distribution chart when numeric wage data exists."""
    if df.empty or "wage" not in df.columns:
        return None
    figure = px.histogram(df, x="wage", nbins=30, title="Salary distribution")
    figure.update_layout(margin={"l": 16, "r": 16, "t": 48, "b": 16}, xaxis_title="Wage", yaxis_title="Records")
    return figure
