# Databricks notebook source
# ============================================================================
# FINANCIAL DATA GENERATOR  –  Databricks Notebook
# ============================================================================
# How to use:
#   1. Upload this entire project to a Databricks Repo or Workspace folder.
#   2. Set DATAGEN_ROOT (widget or env var) to the repo root path.
#   3. Adjust configs/data_volumes.yaml to set your target row counts.
#   4. Run All Cells.
#
# The pipeline generates tables in dependency order (dims first, then facts).
# Each table is written to the Unity Catalog location configured in
# configs/data_volumes.yaml → generation.catalog / generation.schema.
# ============================================================================

# COMMAND ----------

# ── 0. Bootstrap: add repo root to sys.path ──────────────────────────────────
import sys, os

# Set DATAGEN_ROOT to the root of the cloned repo.
# In Databricks Repos this is typically /Workspace/Repos/<user>/<repo-name>
DATAGEN_ROOT = os.environ.get(
    "DATAGEN_ROOT",
    "/Workspace/Users/balachandar.bhagyaraj@nike.com/datagen_comm"   # ← update before running
)
if DATAGEN_ROOT not in sys.path:
    sys.path.insert(0, DATAGEN_ROOT)

print(f"DATAGEN_ROOT = {DATAGEN_ROOT}")

# COMMAND ----------

# ── 1. Imports ────────────────────────────────────────────────────────────────
import logging
import time
from typing import Any, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger("datagen.notebook")

# Core modules
from utils.schema_loader import (
    get_full_table_name,
    get_generation_config,
    load_distributions,
    load_volumes,
)
from utils.spark_helpers import (
    configure_spark,
    create_schema_if_not_exists,
    write_table,
)
from src.registry import (
    DIMENSION_TABLES,
    FACT_TABLES,
    GENERATION_ORDER,
    GenerationContext,
    TABLE_REGISTRY,
)

# Import generators – this populates TABLE_REGISTRY via register() calls
import src.dim_generators   # noqa: F401
import src.fact_generators  # noqa: F401

print(f"Registered tables: {list(TABLE_REGISTRY.keys())}")

# COMMAND ----------

# ── 2. Spark configuration ────────────────────────────────────────────────────
configure_spark(spark)

gen_cfg = get_generation_config()
print("Generation config:")
for k, v in gen_cfg.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# ── 3. Create Unity Catalog schema if not present ─────────────────────────────
create_schema_if_not_exists(spark)
print(f"Schema ready: {gen_cfg['catalog']}.{gen_cfg['schema']}")

# COMMAND ----------

# ── 4. Notebook widgets (optional overrides) ──────────────────────────────────
try:
    dbutils.widgets.dropdown(
        "run_mode",
        "all",
        ["all", "dims_only", "facts_only", "single_table"],
        "Run Mode",
    )
    dbutils.widgets.text(
        "single_table_name",
        "",
        "Single Table Name (if run_mode=single_table)",
    )
    dbutils.widgets.dropdown(
        "write_mode",
        gen_cfg.get("write_mode", "overwrite"),
        ["overwrite", "append"],
        "Write Mode",
    )
    RUN_MODE   = dbutils.widgets.get("run_mode")
    SINGLE_TBL = dbutils.widgets.get("single_table_name").strip()
    WRITE_MODE = dbutils.widgets.get("write_mode")
except Exception:
    # Widgets not available (e.g. running as a job without widget setup)
    RUN_MODE   = "all"
    SINGLE_TBL = ""
    WRITE_MODE = gen_cfg.get("write_mode", "overwrite")

print(f"run_mode={RUN_MODE}  write_mode={WRITE_MODE}  single_table={SINGLE_TBL!r}")

# COMMAND ----------

# ── 5. Determine which tables to generate ────────────────────────────────────

def resolve_table_list(run_mode: str, single_table: str):
    if run_mode == "dims_only":
        return DIMENSION_TABLES
    if run_mode == "facts_only":
        return FACT_TABLES
    if run_mode == "single_table":
        if not single_table:
            raise ValueError("single_table_name widget must be set when run_mode=single_table")
        if single_table not in TABLE_REGISTRY:
            raise KeyError(f"Table '{single_table}' is not registered. Available: {list(TABLE_REGISTRY.keys())}")
        return [single_table]
    # default: all
    return GENERATION_ORDER


tables_to_run = resolve_table_list(RUN_MODE, SINGLE_TBL)
print(f"\nTables to generate ({len(tables_to_run)}):")
for t in tables_to_run:
    print(f"  {'[FACT]' if t in FACT_TABLES else '[DIM] '} {t}")

# COMMAND ----------

# ── 6. Generation context – stores dim DataFrames for FK resolution ───────────

# Dimension PK columns (used to collect IDs into ctx)
DIM_PK_MAP: Dict[str, str] = {
    "accounting_document_type":             "accounting_document_type_id",
    "atscale_geo_security":                 "region",
    "version_forecast_mapping":             "version_forecast_mapping_id",
    "functional_area":                      "functional_area_id",
    "division_text":                        "division_id",
    "division_text_dim_v":                  "division_nbr",
    "finance_foreign_currency_exchange_rate":"finance_foreign_currency_exchange_rate_id",
    "calendar_fiscal_period_v":             "fiscal_year_period_nbr",
    "company_code":                         "company_id",
    "finance_product_dim_v":                "product_id",
    "finance_customer_dim_v":               "finance_customer_id",
    "copa_attribution_dim":                 "copa_attribution_id",
    "cost_center_dim_v":                    "cost_center_nbr",
    "profit_center":                        "profit_center_id",
    "gl_account_dim":                       "gl_account_nbr",
    "gl_account_zfsm_measures_hierarchy_dim":"zfsm_measure_id",
    "gl_account_hierarchy":                 "gl_account_hierarchy_id",
    "management_gl_account_hierarchy":      "gl_account_hierarchy_id",
    "geo_marketplace_channel_dim":          "geo_marketplace_channel_id",
    "geo_wholesale_value_business_dim":     "geo_wholesale_value_business_id",
}

ctx = GenerationContext()

# COMMAND ----------

# ── 7. Main generation loop ───────────────────────────────────────────────────

pipeline_start = time.time()
results = []

for table_name in tables_to_run:

    if table_name not in TABLE_REGISTRY:
        logger.warning("Table '%s' not registered – skipping", table_name)
        continue

    entry       = TABLE_REGISTRY[table_name]
    full_name   = get_full_table_name(table_name)
    write_opts  = entry["write_opts"]
    is_fact     = table_name in FACT_TABLES

    print(f"\n{'='*70}")
    print(f"  Generating: {table_name}  →  {full_name}")
    print(f"{'='*70}")

    t0 = time.time()
    try:
        # ── Generate DataFrame ────────────────────────────────────────────
        df = entry["generator"](spark, ctx)

        # ── Write to Delta ────────────────────────────────────────────────
        write_table(
            df            = df,
            table_name    = full_name,
            partition_cols= write_opts.get("partition_cols") or [],
            zorder_cols   = write_opts.get("zorder_cols")    or [],
            write_mode    = WRITE_MODE,
        )

        elapsed = round(time.time() - t0, 1)
        print(f"  ✓ Written in {elapsed}s")

        # ── Cache dim ids into context (for FK resolution in fact tables) ─
        if not is_fact and table_name in DIM_PK_MAP:
            pk_col = DIM_PK_MAP[table_name]
            # Only cache tables with < 5 M rows to avoid OOM on driver
            try:
                row_cnt = df.count()
                if row_cnt <= 5_000_000:
                    ctx.register_dim(table_name, df, pk_col)
                    print(f"  ✓ Context cached {row_cnt:,} PK values ({pk_col})")
                else:
                    print(f"  ⚠ Table too large to cache in ctx ({row_cnt:,} rows) – reading back from Delta")
                    small_df = spark.table(full_name).select(pk_col)
                    ctx.register_dim(table_name, small_df, pk_col)
            except Exception as e:
                logger.warning("Could not cache dim '%s': %s", table_name, e)

        results.append({"table": table_name, "status": "OK", "elapsed_s": elapsed})

    except Exception as exc:
        elapsed = round(time.time() - t0, 1)
        logger.error("FAILED %s after %.1fs: %s", table_name, elapsed, exc, exc_info=True)
        results.append({"table": table_name, "status": "FAILED", "elapsed_s": elapsed, "error": str(exc)})


# COMMAND ----------

# ── 8. Summary report ─────────────────────────────────────────────────────────

total_elapsed = round(time.time() - pipeline_start, 1)
ok_count      = sum(1 for r in results if r["status"] == "OK")
fail_count    = sum(1 for r in results if r["status"] != "OK")

print("\n" + "="*70)
print("  GENERATION SUMMARY")
print("="*70)
print(f"  Total tables : {len(results)}")
print(f"  Succeeded    : {ok_count}")
print(f"  Failed       : {fail_count}")
print(f"  Total time   : {total_elapsed}s ({total_elapsed/60:.1f} min)")
print("="*70)
print(f"\n{'Table':<55} {'Status':<10} {'Elapsed (s)'}")
print("-"*70)
for r in results:
    status = r["status"]
    flag   = "✓" if status == "OK" else "✗"
    err    = f"  ← {r.get('error','')}" if status != "OK" else ""
    print(f"  {flag} {r['table']:<52} {status:<10} {r['elapsed_s']}{err}")

if fail_count > 0:
    print("\n⚠  Some tables failed. Check the logs above for details.")
else:
    print("\n✓  All tables generated successfully.")

# COMMAND ----------

# ── 9. (Optional) Quick validation queries ────────────────────────────────────

print("\n── Row count validation ──")
for table_name in tables_to_run:
    full_name = get_full_table_name(table_name)
    try:
        cnt = spark.table(full_name).count()
        print(f"  {table_name:<55} {cnt:>20,} rows")
    except Exception as e:
        print(f"  {table_name:<55} ERROR: {e}")

# COMMAND ----------

# ── 10. (Optional) FK integrity spot-check on general_ledger_fact ─────────────

print("\n── FK integrity check: general_ledger_fact → profit_center ──")
try:
    gl_full  = get_full_table_name("general_ledger_fact")
    pc_full  = get_full_table_name("profit_center")

    gl_df = spark.table(gl_full).select("profit_center_id").distinct()
    pc_df = spark.table(pc_full).select("profit_center_id")

    orphans = gl_df.join(pc_df, "profit_center_id", "left_anti").count()
    print(f"  Orphan profit_center_id values in GL fact: {orphans}")
except Exception as e:
    print(f"  Skipped (table not yet available): {e}")
