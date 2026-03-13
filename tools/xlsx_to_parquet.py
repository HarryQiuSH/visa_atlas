from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import pandas as pd

DEFAULT_DATA_DIR = Path("data")
MAX_SUFFIX_LENGTH = 32


def log(message: str) -> None:
    """Print a timestamped progress line."""
    print(f"[xlsx-to-parquet] {message}")


def sanitize_sheet_name(sheet_name: str) -> str:
    """Create a filesystem-friendly suffix for sheet-derived output names."""
    value = re.sub(r"[^0-9a-zA-Z]+", "_", sheet_name.strip().lower())
    return value.strip("_") or "sheet"


def condense_sheet_suffix(workbook_path: Path, sheet_name: str) -> str:
    """Build a short sheet suffix by removing tokens already present in the workbook name."""
    workbook_tokens = [token for token in sanitize_sheet_name(workbook_path.stem).split("_") if token]
    sheet_tokens = [token for token in sanitize_sheet_name(sheet_name).split("_") if token]

    while workbook_tokens and sheet_tokens and workbook_tokens[0] == sheet_tokens[0]:
        workbook_tokens.pop(0)
        sheet_tokens.pop(0)

    workbook_suffix_tokens = [token for token in sanitize_sheet_name(workbook_path.stem).split("_") if token]
    while workbook_suffix_tokens and sheet_tokens and workbook_suffix_tokens[-1] == sheet_tokens[-1]:
        workbook_suffix_tokens.pop()
        sheet_tokens.pop()

    suffix = "_".join(sheet_tokens) or "sheet"
    if len(suffix) > MAX_SUFFIX_LENGTH:
        suffix = suffix[:MAX_SUFFIX_LENGTH].rstrip("_")
    return suffix or "sheet"


def discover_workbooks(data_dir: Path) -> list[Path]:
    """Return Excel workbooks under the configured data directory."""
    workbooks = sorted(data_dir.rglob("*.xlsx"))
    return [path for path in workbooks if not path.name.startswith("~$")]


def build_output_path(workbook_path: Path, sheet_name: str | None, all_sheets: bool) -> Path:
    """Choose the parquet output path for a workbook or workbook sheet."""
    if all_sheets and sheet_name:
        condensed_suffix = condense_sheet_suffix(workbook_path, sheet_name)
        if condensed_suffix in {"", "sheet"}:
            return workbook_path.with_suffix(".parquet")
        return workbook_path.with_name(f"{workbook_path.stem}__{condensed_suffix}.parquet")
    return workbook_path.with_suffix(".parquet")


def expected_output_paths(workbook_path: Path, sheet_names: list[str], all_sheets: bool) -> list[Path]:
    """Return the parquet files expected for a workbook conversion."""
    if all_sheets:
        return [build_output_path(workbook_path, sheet_name, all_sheets=True) for sheet_name in sheet_names]
    return [build_output_path(workbook_path, sheet_names[0], all_sheets=False)]


def write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """Persist a dataframe to parquet, creating parent directories when needed."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)


def find_mixed_object_columns(df: pd.DataFrame) -> list[str]:
    """Return object columns containing multiple concrete Python value types."""
    mixed_columns: list[str] = []
    object_columns = df.select_dtypes(include=["object"]).columns

    for column_name in object_columns:
        non_null_values = df[column_name].dropna()
        if non_null_values.empty:
            continue
        value_types = {type(value).__name__ for value in non_null_values}
        if len(value_types) > 1:
            mixed_columns.append(column_name)

    return mixed_columns


def normalize_for_parquet(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Coerce mixed-type object columns to string before parquet serialization."""
    normalized_df = df.copy()
    mixed_columns = find_mixed_object_columns(normalized_df)

    for column_name in mixed_columns:
        normalized_df[column_name] = normalized_df[column_name].astype("string")

    return normalized_df, mixed_columns


def safe_write_parquet(df: pd.DataFrame, output_path: Path) -> None:
    """Write parquet after normalizing mixed object columns in pandas."""
    normalized_df, coerced_columns = normalize_for_parquet(df)
    if coerced_columns:
        log("Coercing mixed-type object columns to string: " + ", ".join(coerced_columns))

    try:
        write_parquet(normalized_df, output_path)
    except Exception as exc:
        log(f"Parquet write failed after pandas normalization: {exc}")
        raise


def convert_workbook(workbook_path: Path, all_sheets: bool) -> int:
    """Convert one workbook into one or more parquet files."""
    start_time = time.perf_counter()
    log(f"Reading workbook: {workbook_path}")

    excel_file = pd.ExcelFile(workbook_path, engine="openpyxl")
    log(f"Discovered sheets: {', '.join(excel_file.sheet_names)}")

    sheet_names = excel_file.sheet_names if all_sheets else [excel_file.sheet_names[0]]
    if not all_sheets and len(excel_file.sheet_names) > 1:
        log(f"Multiple sheets detected. Converting only the first sheet: {sheet_names[0]}")

    output_paths = expected_output_paths(workbook_path, sheet_names, all_sheets=all_sheets)
    if output_paths and all(path.exists() for path in output_paths):
        log(f"Parquet output already exists for {workbook_path.name}. Removing source workbook.")
        workbook_path.unlink()
        return 0

    written_files = 0
    for sheet_name in sheet_names:
        sheet_start = time.perf_counter()
        log(f"Loading sheet '{sheet_name}'")
        dataframe = pd.read_excel(excel_file, sheet_name=sheet_name, engine="openpyxl")
        log(f"Loaded {len(dataframe):,} rows x {len(dataframe.columns):,} columns from '{sheet_name}'")

        output_path = build_output_path(workbook_path, sheet_name, all_sheets=all_sheets)
        log(f"Writing parquet: {output_path}")
        safe_write_parquet(dataframe, output_path)

        elapsed = time.perf_counter() - sheet_start
        size_mb = output_path.stat().st_size / (1024 * 1024)
        log(f"Finished '{sheet_name}' in {elapsed:.2f}s -> {output_path.name} ({size_mb:.2f} MB)")
        written_files += 1

    total_elapsed = time.perf_counter() - start_time
    log(f"Removing source workbook: {workbook_path}")
    workbook_path.unlink()
    log(f"Workbook complete: {workbook_path.name} in {total_elapsed:.2f}s")
    return written_files


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for workbook discovery and conversion behavior."""
    parser = argparse.ArgumentParser(
        description="Convert .xlsx files under data/ into parquet files with verbose progress output."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory to scan for .xlsx files. Defaults to ./data",
    )
    parser.add_argument(
        "--all-sheets",
        action="store_true",
        help="Convert every sheet in each workbook. Output files are suffixed by sheet name.",
    )
    return parser.parse_args()


def main() -> int:
    """Run workbook discovery and conversion."""
    args = parse_args()
    data_dir = args.data_dir.resolve()

    log(f"Using data directory: {data_dir}")
    if not data_dir.exists():
        log("Data directory does not exist.")
        return 1

    workbooks = discover_workbooks(data_dir)
    if not workbooks:
        log("No .xlsx files found. Add Excel workbooks under data/ and run again.")
        return 1

    log(f"Found {len(workbooks)} workbook(s) to convert.")
    converted_files = 0
    for workbook_path in workbooks:
        converted_files += convert_workbook(workbook_path, all_sheets=args.all_sheets)

    log(f"Done. Wrote {converted_files} parquet file(s) from {len(workbooks)} workbook(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
