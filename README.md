# Financial Data Generator – PySpark / Databricks

End-to-end mock data generator for the `dev_pbi_perform_cf_poc` financial schema.
Designed to run in a Databricks Repo and generate production-scale data
(e.g. 25 billion rows in `general_ledger_fact`) with realistic, logically
consistent values across all related tables.

---

## Project Structure

```
datagen/
├── configs/
│   ├── data_volumes.yaml        ← Row counts, partitioning, write settings
│   └── distributions.yaml       ← Domain values, distributions, weights
├── src/
│   ├── __init__.py
│   ├── registry.py              ← Table registry & GenerationContext
│   ├── dim_generators.py        ← All dimension table generators
│   └── fact_generators.py       ← All fact table generators
├── utils/
│   ├── __init__.py
│   ├── schema_loader.py         ← YAML config loader (cached)
│   ├── spark_helpers.py         ← Spark tuning, Delta write helpers
│   └── data_helpers.py          ← Column expressions, fiscal calendar builder
├── notebooks/
│   └── run_datagen.py           ← Main Databricks notebook (run this)
├── sql/
│   └── create_tables.sql        ← DDL for all tables
└── schema.yaml                  ← Source of truth schema (place here)
```

---

## Quick Start

### 1. Set up in Databricks

```bash
# Clone into a Databricks Repo
git clone <this-repo> /Workspace/Repos/<user>/datagen
```

Or upload the folder to a Databricks workspace.

### 2. Place `schema.yaml`

Copy your `schema.yaml` into the project root:

```
datagen/schema.yaml
```

### 3. Configure row counts

Edit `configs/data_volumes.yaml`:

```yaml
fact_tables:
  general_ledger_fact:
    row_count: 25_000_000_000     # ← 25 billion rows
```

All other table sizes scale automatically from this value.

### 4. Create tables (optional)

Run the DDL script in a Databricks SQL notebook:

```sql
%run /sql/create_tables.sql
```

Or let `saveAsTable` create them automatically.

### 5. Run the generator

Open `notebooks/run_datagen.py` as a Databricks notebook and click **Run All**.

Set the `DATAGEN_ROOT` variable at the top to your repo path:

```python
DATAGEN_ROOT = "/Workspace/Repos/your-user/datagen"
```

---

## Configuration Reference

### `configs/data_volumes.yaml`

| Key | Description |
|-----|-------------|
| `generation.catalog` | Unity Catalog name |
| `generation.schema` | Schema name |
| `generation.write_mode` | `overwrite` or `append` |
| `generation.spark_shuffle_partitions` | Tune for cluster size |
| `generation.rows_per_partition` | Target rows per Delta file |
| `fact_tables.<name>.row_count` | Absolute row count |
| `fact_tables.<name>.row_count_ratio` | Fraction of GL fact size |
| `fact_tables.<name>.generation_partitions` | Spark parallelism during generation |
| `dimension_tables.<name>.row_count` | Dimension cardinality |

### `configs/distributions.yaml`

Controls all domain pools:

- **currencies** – currency codes and frequency weights
- **companies** – company names and currencies
- **products** – brands, categories, franchises, etc.
- **customers** – channel types and marketplace channels
- **gl_accounts** – account number ranges by category
- **fiscal** – fiscal year start month, periods per year
- **amounts** – log-normal mean/std for monetary amounts
- **versions** – forecast version codes and groups
- **indicators** – flag field values (Y/N, 0/1)

---

## Architecture

### Generation Strategy for Billion-Row Tables

```
spark.range(0, N, numPartitions=P)
  → assigns monotonic IDs across all executors (zero shuffle)
  → FK values resolved via modulo on row ID against pre-collected PK arrays
  → amounts generated with log-normal Box-Muller approximation
  → written to Delta with ZORDER BY for query optimisation
```

### Dependency Chain

```
Lookups            Calendar     Company
    │                  │           │
    └──────────────────┴───────────┴──► Products / Customers / Cost Centers
                                               │
                              GL Accounts / Profit Centers
                                               │
                                    ┌──────────┴──────────────┐
                                    ▼                         ▼
                               CIS_fact          consolidated_balance_sheet_fact
                                    │                         │
                                    └──────────┬──────────────┘
                                               ▼
                                    general_ledger_fact (25 B rows)
```

---

## Notebook Widgets

The main notebook supports four run modes selectable via Databricks widgets:

| Widget | Values | Description |
|--------|--------|-------------|
| `run_mode` | `all` / `dims_only` / `facts_only` / `single_table` | Scope of generation |
| `single_table_name` | table name string | Used when `run_mode=single_table` |
| `write_mode` | `overwrite` / `append` | Delta write mode |

---

## Cluster Recommendations

| Target rows | Cluster type | Nodes | Notes |
|-------------|-------------|-------|-------|
| 1 B | Standard (8 cores) | 4–8 | ~30 min |
| 10 B | Memory-optimised | 8–16 | ~2 hrs |
| 25 B | Memory-optimised | 16–32 | ~4–6 hrs |

- Set `spark_shuffle_partitions` to `200 × num_cores`
- Enable **Photon** accelerator for Delta writes
- Use spot instances for cost savings

---

## Data Quality Notes

- **Referential integrity**: Every FK in `general_ledger_fact` resolves to a valid PK in the corresponding dimension table (modulo assignment is deterministic and bounded).
- **Realistic amounts**: Log-normal distribution mirrors real-world financial data skew; roughly 12% of amounts are negative (credits / adjustments).
- **Fiscal calendar**: 13-period Nike-style fiscal calendar starting in June, covering FY2019–FY2025.
- **Deduplication**: Dimension PKs are strictly sequential integers with no gaps or duplicates.

---

## Extending the Generator

To add a new table:

1. Add its schema to `schema.yaml`
2. Add its row count to `configs/data_volumes.yaml`
3. Write a generator function in `src/dim_generators.py` or `src/fact_generators.py`
4. Call `register(table_name, gen_fn, ...)` at the bottom of the module
5. Add the table name to `DIMENSION_TABLES` or `FACT_TABLES` in `src/registry.py`
