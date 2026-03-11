# visa_atlas
Streamlit MVP for exploring U.S. Labor Condition Application data from local parquet files or a Hugging Face dataset repo.

## Inspiration

This project was inspired by [`lca-analysis`](https://github.com/lansetaowa/lca-analysis/tree/main). Thanks to the author for publishing that work openly.

This project may look like reinventing the wheel, but part of the goal is to provide an ads-free, open source, local-first version that is easy to inspect and extend.

## Project structure

```text
app.py
main.py
tools/
  xlsx_to_parquet.py
src/
  data_loader.py
  filters.py
  queries.py
  charts.py
  utils.py
data/
requirements.txt
README.md
```

## Setup

```bash
uv sync
```

Or with `pip`:

```bash
pip install -r requirements.txt
```

## Add data

Place one or more `.parquet` files under [`data/`](/Users/shihaoqiu/Documents/GitHub/visa_atlas/data).

Or point the app at a Hugging Face dataset repo from the sidebar. By default the app uses local files, but you can switch to `Hugging Face` and enter:

- Dataset repo: `HarryQiuSH/LCA2226`
- Parquet pattern: `*.parquet`

You can also preconfigure those defaults with:

```bash
export HF_DATASET_REPO=HarryQiuSH/LCA2226
export HF_PARQUET_PATTERN='*.parquet'
```

If your source files are Excel workbooks, convert them first:

```bash
uv run python tools/xlsx_to_parquet.py
```

To convert every sheet in each workbook:

```bash
uv run python tools/xlsx_to_parquet.py --all-sheets
```

The app inspects parquet schemas, normalizes column names to lowercase snake_case, and maps common LCA field variants such as employer, job title, work location, submit date, start date, wage, and case status.

## Run

```bash
uv run streamlit run app.py
```

Compatibility entrypoint:

```bash
uv run streamlit run main.py
```

## Notes

- DuckDB queries parquet files directly; filtered tables and aggregations are pushed down into SQL where possible.
- Wage metrics use the first supported wage-like column detected, which may mix units across source files.
- Missing expected columns do not crash the app; unsupported filters and charts are disabled with warnings.

## Lint

```bash
uv run ruff check .
```

## Appendix

Stage two is planned to add PERM data tracking for green card applicants, so the app can evolve from LCA exploration into a broader employment-based immigration tracking tool.

What you can usually analyze from public PERM disclosure data:

- Case metadata: case number or public ID style fields, filing or received dates, decision dates, and status
- Employer info: employer name, city, state, ZIP, with FEIN often excluded from public disclosure
- Job info: job title, SOC or occupation code, worksite location, and full-time flag
- Wage info: offered wage or wage range, plus wage unit such as hourly or yearly
- Recruitment and legal process fields: selected recruitment or program-form fields depending on filing year and form version
- Decision outcomes: certified, denied, and withdrawn trends, plus processing timing

Collaborators are welcome if you want to help extend the project toward PERM support, better analytics, or broader employment-based immigration data tooling.
