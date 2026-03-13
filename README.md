# visa_atlas
Streamlit MVP for exploring U.S. Labor Condition Application data from local parquet files or a Cloudflare R2 bucket URL.
[Applink](https://visa-atlas.streamlit.app/)

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

Or point the app at Cloudflare R2 from the sidebar. The app reads the R2 settings from `.env`.

Create a `.env` file like this:

```bash
R2_BASE_URL=https://your-r2-bucket-url.r2.dev/your-path
R2_PARQUET_FILES="LCA_Disclosure_Data_FY2022_Q1.parquet
LCA_Disclosure_Data_FY2022_Q2.parquet"
```

Notes:

- `R2_BASE_URL` is the fixed Cloudflare R2 prefix
- `R2_PARQUET_FILES` is only needed if local parquet files are not present under `data/`
- if local parquet files are present, the app infers the remote file names from those local file names and does not ask for them in the UI

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
