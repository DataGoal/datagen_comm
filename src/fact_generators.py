"""
src/fact_generators.py
======================
Generator functions for all fact tables.

Design principles for large-scale (25 B+ row) generation:
──────────────────────────────────────────────────────────
1.  spark.range(0, N) is the single most efficient way to generate a
    large dataset in Spark – it produces monotonically increasing IDs
    in parallel across all executors with zero shuffle.

2.  FK values are resolved by collecting dimension PKs into Python lists
    and using modulo arithmetic in Spark Column expressions.  This means
    we never join or shuffle the large fact DataFrame during generation.

3.  Monetary amounts use log-normal distributions (see data_helpers.py)
    so the generated values have a realistic spread and are always positive
    before sign adjustment.

4.  All columns are populated – no placeholder NULLs unless the schema
    genuinely allows it (active_ind, optional FKs, etc.).
"""
from __future__ import annotations

import datetime
import math
from typing import Any, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
)

from src.registry import GenerationContext, register
from utils.data_helpers import (
    log_normal_amt,
    rand_decimal,
    sign_adjusted,
)
from utils.schema_loader import get_fact_row_count, load_distributions, load_volumes

_D = load_distributions


# ---------------------------------------------------------------------------
# Internal FK helpers
# ---------------------------------------------------------------------------

def _arr(values: List[Any]) -> F.Column:
    """Build a Spark array literal from a Python list."""
    return F.array(*[F.lit(v) for v in values])


def _fk(values: List[Any], index_col: str) -> F.Column:
    """Pick FK value deterministically by modulo on index_col."""
    return _arr(values)[F.col(index_col) % len(values)]


def _weighted_str(values: List[str], weights: List[int], index_col: str) -> F.Column:
    """Pick string with frequency proportional to weights."""
    pool: List[str] = []
    for v, w in zip(values, weights):
        pool.extend([v] * w)
    return _arr(pool)[F.col(index_col) % len(pool)]


# ===========================================================================
#  GENERAL LEDGER FACT  ← central, largest fact table
# ===========================================================================

def gen_general_ledger_fact(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    """
    Generate the general_ledger_fact table.

    Foreign key resolution order:
      All FK id-lists come from the GenerationContext which is populated
      after each dimension is written.  If a dimension is missing from the
      context we fall back to a small synthetic pool so the pipeline
      does not fail.
    """
    d      = _D()
    n      = get_fact_row_count("general_ledger_fact")
    vols   = load_volumes()
    neg_p  = d["amounts"]["negative_probability"]

    # ── FK pools (collected from ctx) ────────────────────────────────────
    def safe_ids(table: str, pk: str, fallback_size: int = 100) -> List[Any]:
        ids = ctx.get_ids(table)
        return ids if ids else list(range(1, fallback_size + 1))

    def safe_str_pool(table: str, col: str, fallback: List[str]) -> List[str]:
        pool = ctx.get_str_pool(table, col)
        return pool if pool else fallback

    adt_ids    = safe_ids("accounting_document_type",    "accounting_document_type_id",  40)
    fy_periods = safe_ids("calendar_fiscal_period_v",    "fiscal_year_period_nbr",       91)
    pc_ids     = safe_ids("profit_center",               "profit_center_id",           2000)
    div_ids    = safe_ids("division_text",               "division_id",                  30)
    vfm_ids    = safe_ids("version_forecast_mapping",    "version_forecast_mapping_id",  25)
    fa_ids     = safe_ids("functional_area",             "functional_area_id",          150)
    prod_ids   = safe_ids("finance_product_dim_v",       "product_id",             500_000)
    cust_ids   = safe_ids("finance_customer_dim_v",      "finance_customer_id",    120_000)
    comp_ids   = safe_ids("company_code",                "company_id",                   80)
    copa_ids   = safe_ids("copa_attribution_dim",        "copa_attribution_id",         800)
    gwvb_ids   = safe_ids("geo_wholesale_value_business_dim", "geo_wholesale_value_business_id", 500)
    gmch_ids   = safe_ids("geo_marketplace_channel_dim", "geo_marketplace_channel_id",  200)
    zfsm_ids   = safe_ids("gl_account_zfsm_measures_hierarchy_dim", "zfsm_measure_id", 3000)

    cc_pool    = safe_str_pool("cost_center_dim_v",  "cost_center_nbr",
                               [f"CC{i:06d}" for i in range(1, 15001)])
    gl_pool    = safe_str_pool("gl_account_dim",     "gl_account_nbr",
                               [str(1000000 + i) for i in range(1, 8001)])

    # ── fiscal year pool (derived from period numbers) ────────────────────
    fy_pool = sorted(set(p // 100 for p in fy_periods))
    if not fy_pool:
        fy_pool = list(range(2019, 2026))

    # ── company currency pool ─────────────────────────────────────────────
    currencies = d["currencies"]["codes"]
    cur_weights = d["currencies"]["weights"]
    cur_pool: List[str] = []
    for c, w in zip(currencies, cur_weights):
        cur_pool.extend([c] * w)

    # ── amount distribution params ────────────────────────────────────────
    rev_cfg  = d["amounts"]["revenue"]
    cogs_cfg = d["amounts"]["cogs"]
    opex_cfg = d["amounts"]["opex"]

    # ── generation partitions ─────────────────────────────────────────────
    gen_parts = vols["fact_tables"]["general_ledger_fact"].get("generation_partitions", 2000)

    df = (
        spark.range(0, n, numPartitions=gen_parts)
        .withColumnRenamed("id", "general_ledger_fact_id")

        # ── Foreign Keys ─────────────────────────────────────────────────
        .withColumn("fiscal_year_period_nbr",
            _fk(fy_periods, "general_ledger_fact_id").cast(IntegerType()))
        .withColumn("profit_center_id",
            _fk(pc_ids, "general_ledger_fact_id").cast(LongType()))
        .withColumn("division_id",
            _fk(div_ids, "general_ledger_fact_id").cast(LongType()))
        .withColumn("version_forecast_mapping_id",
            _fk(vfm_ids, "general_ledger_fact_id").cast(LongType()))
        .withColumn("functional_area_id",
            _fk(fa_ids, "general_ledger_fact_id").cast(LongType()))
        .withColumn("accounting_document_type_id",
            _fk(adt_ids, "general_ledger_fact_id").cast(LongType()))
        .withColumn("product_id",
            _fk(prod_ids, "general_ledger_fact_id").cast(LongType()))
        .withColumn("customer_id",
            _fk(cust_ids, "general_ledger_fact_id").cast(LongType()))
        .withColumn("company_id",
            _fk(comp_ids, "general_ledger_fact_id").cast(LongType()))
        .withColumn("copa_attribution_id",
            _fk(copa_ids, "general_ledger_fact_id").cast(LongType()))
        .withColumn("cost_center_nbr",
            _fk(cc_pool, "general_ledger_fact_id"))
        .withColumn("geo_wholesale_value_business_id",
            _fk(gwvb_ids, "general_ledger_fact_id").cast(LongType()))
        .withColumn("geo_marketplace_channel_id",
            _fk(gmch_ids, "general_ledger_fact_id").cast(LongType()))
        .withColumn("gl_account_nbr",
            _fk(gl_pool, "general_ledger_fact_id"))
        .withColumn("zfsm_measure_id",
            _fk(zfsm_ids, "general_ledger_fact_id").cast(LongType()))

        # ── Amounts ───────────────────────────────────────────────────────
        # Base transaction amount using log-normal distribution
        .withColumn("_base_amt",
            log_normal_amt(rev_cfg["mean"], rev_cfg["std_dev"], scale=5))
        .withColumn("transaction_currency_amt",
            sign_adjusted(F.col("_base_amt"), neg_p))
        .withColumn("company_currency_amt",
            sign_adjusted(
                F.round(F.col("_base_amt") * (F.lit(0.85) + F.rand() * F.lit(0.30)), 5),
                neg_p).cast(DecimalType(28, 5)))
        .withColumn("performance_management_currency_amt",
            sign_adjusted(
                F.round(F.col("_base_amt") * (F.lit(0.88) + F.rand() * F.lit(0.24)), 5),
                neg_p).cast(DecimalType(28, 5)))
        .withColumn("sales_qty",
            F.round(F.lit(d["quantities"]["sales_qty_min"]) +
                    F.rand() * F.lit(d["quantities"]["sales_qty_max"] - d["quantities"]["sales_qty_min"]), 5)
            .cast(DecimalType(28, 5)))
        .withColumn("returns_qty",
            F.round(F.col("sales_qty") * F.lit(d["quantities"]["returns_qty_ratio"]) * F.rand(), 5)
            .cast(DecimalType(28, 5)))
        .drop("_base_amt")

        # ── Currency codes ────────────────────────────────────────────────
        .withColumn("company_currency_cd",
            _fk(cur_pool[:20], "general_ledger_fact_id"))
        .withColumn("transaction_currency_cd",
            _fk(cur_pool[:20], "company_id"))

        # ── FK cross-reference IDs ─────────────────────────────────────
        .withColumn("etm_ind",
            (F.col("general_ledger_fact_id") % 5 == 0).cast(IntegerType()))
        .withColumn("etm_foreign_currency_exchange_rate_id",
            (F.col("general_ledger_fact_id") % 300 + 1).cast(LongType()))
        .withColumn("gaap_foreign_currency_exchange_rate_id",
            (F.col("general_ledger_fact_id") % 300 + 1).cast(LongType()))

        # ── Indicator flags ───────────────────────────────────────────────
        .withColumn("general_ledger_fact_ind",
            F.when(F.col("general_ledger_fact_id") % 10 == 0, F.lit("N")).otherwise(F.lit("Y")))
        .withColumn("cis_delta_ind",
            F.when(F.col("general_ledger_fact_id") % 20 == 0, F.lit("Y")).otherwise(F.lit("N")))
        .withColumn("general_ledger_ocogs_allocation_fact_ind",
            F.when(F.col("general_ledger_fact_id") % 15 == 0, F.lit("Y")).otherwise(F.lit("N")))
        .withColumn("anaplan_corporate_ind",
            F.when(F.col("general_ledger_fact_id") % 25 == 0, F.lit("Y")).otherwise(F.lit("N")))
    )

    return df


# ===========================================================================
#  CIS FACT  (Consolidated Income Statement)
# ===========================================================================

def gen_CIS_fact(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d     = _D()
    n     = get_fact_row_count("CIS_fact")
    vols  = load_volumes()
    neg_p = d["amounts"]["negative_probability"]

    def safe_ids(t, pk, fb=100):
        ids = ctx.get_ids(t)
        return ids if ids else list(range(1, fb + 1))

    def safe_str(t, col, fb):
        pool = ctx.get_str_pool(t, col)
        return pool if pool else fb

    gl_pool  = safe_str("gl_account_dim", "gl_account_nbr", [str(1000000 + i) for i in range(1, 8001)])
    fy_periods = safe_ids("calendar_fiscal_period_v", "fiscal_year_period_nbr", 91)
    pc_ids     = safe_ids("profit_center", "profit_center_id", 2000)

    currencies = d["currencies"]["codes"]
    fy_pool    = sorted(set(p // 100 for p in fy_periods))
    ver_codes  = [v for grp in d["versions"]["groups"].values() for v in grp]

    segments    = d["profit_centers"]["segments"]
    cons_groups = ["NIKE", "CONVERSE", "JORDAN", "TOTAL"]
    coa_codes   = d["gl_accounts"]["chart_of_accounts"]
    ledger_cds  = ["0L", "2L", "3L"]

    gen_parts = vols["fact_tables"]["CIS_fact"].get("generation_partitions", 100)

    df = (
        spark.range(0, n, numPartitions=gen_parts)
        .withColumnRenamed("id", "consolidated_income_statement_fact_id")

        .withColumn("gl_account_id",
            _fk(gl_pool, "consolidated_income_statement_fact_id")
            .cast(LongType()))
        .withColumn("profit_center_id",
            _fk(pc_ids, "consolidated_income_statement_fact_id").cast(LongType()))
        .withColumn("profit_center_nbr",
            F.lpad(F.col("profit_center_id").cast(StringType()), 7, "0"))
        .withColumn("fiscal_year_period_nbr",
            _fk(fy_periods, "consolidated_income_statement_fact_id").cast(IntegerType()))
        .withColumn("fiscal_yr",
            _fk(fy_pool, "consolidated_income_statement_fact_id").cast(IntegerType()))

        # Amounts
        .withColumn("_base",
            log_normal_amt(d["amounts"]["revenue"]["mean"], d["amounts"]["revenue"]["std_dev"], 5))
        .withColumn("transaction_currency_amt",    sign_adjusted(F.col("_base"), neg_p).cast(DecimalType(18, 5)))
        .withColumn("group_currency_amt",          sign_adjusted(F.round(F.col("_base") * F.lit(0.92), 5), neg_p).cast(DecimalType(18, 5)))
        .withColumn("local_currency_amt",          sign_adjusted(F.round(F.col("_base") * F.lit(0.95), 5), neg_p).cast(DecimalType(18, 5)))
        .withColumn("sign_adjusted_group_currency_amt",       F.col("group_currency_amt"))
        .withColumn("sign_adjusted_local_currency_amt",       F.col("local_currency_amt"))
        .withColumn("sign_adjusted_transaction_currency_amt", F.col("transaction_currency_amt"))
        .withColumn("qty",
            F.round(F.rand() * F.lit(50000), 5).cast(DecimalType(18, 5)))
        .withColumn("sign_adjusted_qty", F.col("qty"))
        .drop("_base")

        # Codes / dims
        .withColumn("functional_area_cd",
            F.concat(F.lit("FA"), F.lpad((F.col("consolidated_income_statement_fact_id") % 150 + 1).cast(StringType()), 4, "0")))
        .withColumn("division_nbr",
            F.lpad((F.col("consolidated_income_statement_fact_id") % 30 + 1).cast(StringType()), 2, "0"))
        .withColumn("segment_nbr",
            (F.col("consolidated_income_statement_fact_id") % 100 + 1).cast(IntegerType()))
        .withColumn("partner_segment_nbr",
            (F.col("consolidated_income_statement_fact_id") % 50 + 1).cast(IntegerType()))
        .withColumn("document_type_cd",
            _fk(["SA", "KR", "DR", "RV", "IC", "JV"], "consolidated_income_statement_fact_id"))
        .withColumn("original_company_cd",
            (F.col("consolidated_income_statement_fact_id") % 80 + 1001).cast(IntegerType()))
        .withColumn("financial_statement_item_cd",
            F.concat(F.lit("FSI"), F.lpad((F.col("consolidated_income_statement_fact_id") % 500 + 1).cast(StringType()), 5, "0")))
        .withColumn("local_currency_cd",
            _fk(currencies[:8], "consolidated_income_statement_fact_id"))
        .withColumn("transaction_currency_cd",
            _fk(currencies[:8], "profit_center_id"))
        .withColumn("version_nbr",
            _fk(ver_codes, "consolidated_income_statement_fact_id"))
        .withColumn("partner_profit_center_nbr",
            F.lpad((F.col("consolidated_income_statement_fact_id") % 2000 + 1).cast(StringType()), 7, "0"))
        .withColumn("partner_unit_cd",
            F.concat(F.lit("PU"), F.lpad((F.col("consolidated_income_statement_fact_id") % 50 + 1).cast(StringType()), 3, "0")))
        .withColumn("consolidation_unit_cd",
            (F.col("consolidated_income_statement_fact_id") % 80 + 1001).cast(IntegerType()))
        .withColumn("ledger_cd", _fk(ledger_cds, "consolidated_income_statement_fact_id"))
        .withColumn("dimension_cd",
            F.concat(F.lit("DIM"), F.lpad((F.col("consolidated_income_statement_fact_id") % 20 + 1).cast(StringType()), 3, "0")))
        .withColumn("record_type_cd",
            _fk(["0", "1", "2", "3"], "consolidated_income_statement_fact_id"))
        .withColumn("consolidation_group_cd",
            _fk(cons_groups, "consolidated_income_statement_fact_id"))
        .withColumn("consolidation_of_investment_activity_nbr",
            (F.col("consolidated_income_statement_fact_id") % 5).cast(IntegerType()))
        .withColumn("chart_of_accounts_cd",
            _fk(coa_codes, "consolidated_income_statement_fact_id"))
        .withColumn("trading_partner_nbr",
            (F.col("consolidated_income_statement_fact_id") % 200 + 1).cast(IntegerType()))
        .withColumn("region_summary_product_group_cd",
            _fk(["FW", "AP", "EQ", "ACC"], "consolidated_income_statement_fact_id"))
        .withColumn("version_group_nm",
            _fk(list(d["versions"]["groups"].keys()), "consolidated_income_statement_fact_id"))
        .withColumn("consolidated_segment_nm",
            _fk(segments, "consolidated_income_statement_fact_id"))
        .withColumn("consolidated_channel_nm",
            _fk(["Wholesale", "Direct", "Digital"], "consolidated_income_statement_fact_id"))
        .withColumn("user_nm",             F.lit("ETL_SERVICE"))
        .withColumn("additional_operation_information_nm", F.lit(None).cast(StringType()))
        .withColumn("created_by_user_id",  F.lit("ETL_SERVICE"))
        .withColumn("updated_by_user_id",  F.lit("ETL_SERVICE"))
        .withColumn("physical_source_cd",  F.lit("SAP_ECC"))
        .withColumn("cis_store_cd",        F.lit(None).cast(StringType()))
        .withColumn("posting_level_cd",    _fk(["00", "10", "20", "30"], "consolidated_income_statement_fact_id"))
        .withColumn("base_unit_of_measure_cd",
            _fk(["EA", "PR", "PK", "BX"], "consolidated_income_statement_fact_id"))
        .withColumn("foreign_exchange_type_cd",
            _fk(["M", "P", "B"], "consolidated_income_statement_fact_id"))
    )
    return df


# ===========================================================================
#  CONSOLIDATED BALANCE SHEET FACT
# ===========================================================================

def gen_consolidated_balance_sheet_fact(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d     = _D()
    n     = get_fact_row_count("consolidated_balance_sheet_fact")
    vols  = load_volumes()
    neg_p = d["amounts"]["negative_probability"]

    def safe_ids(t, pk, fb=100):
        ids = ctx.get_ids(t)
        return ids if ids else list(range(1, fb + 1))

    fy_periods = safe_ids("calendar_fiscal_period_v", "fiscal_year_period_nbr", 91)
    fy_pool    = sorted(set(p // 100 for p in fy_periods))
    currencies = d["currencies"]["codes"]
    ver_codes  = [v for grp in d["versions"]["groups"].values() for v in grp]
    coa_codes  = d["gl_accounts"]["chart_of_accounts"]
    segments   = d["profit_centers"]["segments"]
    fsi_pool   = [f"FSI{i:05d}" for i in range(1, 501)]
    cons_groups= ["NIKE", "CONVERSE", "JORDAN", "TOTAL"]

    gen_parts  = vols["fact_tables"]["consolidated_balance_sheet_fact"].get("generation_partitions", 50)
    today      = datetime.date.today()

    df = (
        spark.range(0, n, numPartitions=gen_parts)
        .withColumnRenamed("id", "consolidated_balance_sheet_fact_id")

        .withColumn("financial_statement_item_cd",
            _fk(fsi_pool, "consolidated_balance_sheet_fact_id"))
        .withColumn("profit_center_nbr",
            F.lpad((F.col("consolidated_balance_sheet_fact_id") % 2000 + 1).cast(StringType()), 7, "0"))
        .withColumn("functional_area_cd",
            F.concat(F.lit("FA"), F.lpad((F.col("consolidated_balance_sheet_fact_id") % 150 + 1).cast(StringType()), 4, "0")))
        .withColumn("local_currency_cd",      _fk(currencies[:8], "consolidated_balance_sheet_fact_id"))
        .withColumn("transaction_currency_cd",_fk(currencies[:8], "consolidated_balance_sheet_fact_id"))
        .withColumn("version_nbr",            _fk(ver_codes, "consolidated_balance_sheet_fact_id"))
        .withColumn("division_nbr",
            F.lpad((F.col("consolidated_balance_sheet_fact_id") % 30 + 1).cast(StringType()), 2, "0"))
        .withColumn("fiscal_year_period_nbr",
            _fk(fy_periods, "consolidated_balance_sheet_fact_id").cast(IntegerType()))
        .withColumn("partner_unit_cd",
            F.concat(F.lit("PU"), F.lpad((F.col("consolidated_balance_sheet_fact_id") % 50 + 1).cast(StringType()), 3, "0")))
        .withColumn("posting_level_cd", _fk(["00", "10", "20", "30"], "consolidated_balance_sheet_fact_id"))
        .withColumn("document_type_cd", _fk(["SA", "KR", "DR", "RV", "IC"], "consolidated_balance_sheet_fact_id"))
        .withColumn("consolidation_unit_cd",
            (F.col("consolidated_balance_sheet_fact_id") % 80 + 1001).cast(IntegerType()))
        .withColumn("partner_profit_center_nbr",
            F.lpad((F.col("consolidated_balance_sheet_fact_id") % 2000 + 1).cast(StringType()), 7, "0"))
        .withColumn("trading_partner_nbr",
            (F.col("consolidated_balance_sheet_fact_id") % 200 + 1).cast(IntegerType()))
        .withColumn("region_summary_product_group_cd",
            _fk(["FW", "AP", "EQ", "ACC"], "consolidated_balance_sheet_fact_id"))

        # Amounts
        .withColumn("_base",
            log_normal_amt(d["amounts"]["revenue"]["mean"], d["amounts"]["revenue"]["std_dev"], 5))
        .withColumn("transaction_currency_amt", sign_adjusted(F.col("_base"), neg_p).cast(DecimalType(18, 5)))
        .withColumn("local_currency_amt",       sign_adjusted(F.round(F.col("_base") * F.lit(0.95), 5), neg_p).cast(DecimalType(18, 5)))
        .withColumn("group_currency_amt",       sign_adjusted(F.round(F.col("_base") * F.lit(0.92), 5), neg_p).cast(DecimalType(18, 5)))
        .withColumn("ending_balance_amt",       F.col("group_currency_amt"))
        .withColumn("qty",                      F.round(F.rand() * F.lit(10000), 5).cast(DecimalType(18, 5)))
        .drop("_base")

        # Segment / channel
        .withColumn("partner_segment_nbr",
            (F.col("consolidated_balance_sheet_fact_id") % 50 + 1).cast(IntegerType()))
        .withColumn("segment_nbr",
            F.lpad((F.col("consolidated_balance_sheet_fact_id") % 100 + 1).cast(StringType()), 4, "0"))
        .withColumn("consolidated_segment_nm",  _fk(segments, "consolidated_balance_sheet_fact_id"))
        .withColumn("consolidated_channel_nm",  _fk(["Wholesale", "Direct", "Digital"], "consolidated_balance_sheet_fact_id"))
        .withColumn("fiscal_yr",                _fk(fy_pool, "consolidated_balance_sheet_fact_id").cast(IntegerType()))
        .withColumn("version_group_nm",
            _fk(list(d["versions"]["groups"].keys()), "consolidated_balance_sheet_fact_id"))
        .withColumn("group_currency_cd",        F.lit("USD"))
        .withColumn("foreign_exchange_type_cd", _fk(["M", "P", "B"], "consolidated_balance_sheet_fact_id"))
        .withColumn("user_nm",                  F.lit("ETL_SERVICE"))
        .withColumn("additional_operation_information_nm", F.lit(None).cast(StringType()))
        .withColumn("created_by_user_id",       F.lit("ETL_SERVICE"))
        .withColumn("updated_by_user_id",       F.lit("ETL_SERVICE"))
        .withColumn("physical_source_cd",       F.lit("SAP_ECC"))
        .withColumn("_acdocu_latest_load_timestamp", F.lit(today))
    )
    return df


# ===========================================================================
#  REGISTER FACT GENERATORS
# ===========================================================================

register("CIS_fact",
         gen_CIS_fact,
         zorder_cols=["profit_center_id"])

register("consolidated_balance_sheet_fact",
         gen_consolidated_balance_sheet_fact,
         # partition_cols=["fiscal_year_period_nbr"],
         zorder_cols=["financial_statement_item_cd"])

register("general_ledger_fact",
         gen_general_ledger_fact,
         partition_cols=["fiscal_year_period_nbr"],
         zorder_cols=["profit_center_id", "gl_account_nbr", "product_id"])
