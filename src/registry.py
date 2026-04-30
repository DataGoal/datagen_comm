"""
src/registry.py
===============
Defines the generation order for all tables (dimension tables first,
then fact tables in dependency order) and maps each table name to its
generator function.

The registry is the single source of truth for:
  • Which tables to generate
  • In what order they must be generated
  • How to call each generator

Usage
-----
from src.registry import TABLE_REGISTRY, GENERATION_ORDER

for table_name in GENERATION_ORDER:
    entry = TABLE_REGISTRY[table_name]
    df    = entry["generator"](spark, context)
    write_table(df, get_full_table_name(table_name), **entry["write_opts"])
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List

# Generator module imports (lazy at runtime to avoid circular imports)
# They are injected by the notebook after both modules are loaded.


# ---------------------------------------------------------------------------
# Generation order
# Dimension tables must come before fact tables that reference them.
# ---------------------------------------------------------------------------

DIMENSION_TABLES: List[str] = [
    # Small lookups first
    "accounting_document_type",
    "atscale_geo_security",
    "version_forecast_mapping",
    "functional_area",
    "division_text",
    "division_text_dim_v",
    "finance_foreign_currency_exchange_rate",

    # Calendar
    "calendar_fiscal_period_v",

    # Company
    "company_code",

    # Products & customers
    "finance_product_dim_v",
    "finance_customer_dim_v",
    "copa_attribution_dim",

    # Cost & profit centers
    "cost_center_dim_v",
    "profit_center",

    # GL accounts
    "gl_account_dim",
    "gl_account_zfsm_measures_hierarchy_dim",
    "gl_account_hierarchy",
    "management_gl_account_hierarchy",

    # Channel / geography
    "geo_marketplace_channel_dim",
    "geo_wholesale_value_business_dim",

    # Hierarchy dimensions
    "consolidation_functional_area_hierarchy",
    "consolidation_segment_hierarchy_dim",
    "segment_cost_center_hierarchy_dim_v",
    "segment_profit_center_hierarchy",
    "DisChannel_cost_center_hierarchy_dim_v",
    "DisChannel_profit_center_hierarchy",
    "PartDisChannel_profit_center_hierarchy",

    # Retail
    "retail_global_store_profile_v",
]

FACT_TABLES: List[str] = [
    "CIS_fact",
    "consolidated_balance_sheet_fact",
    "general_ledger_fact",    # largest – generated last
]

GENERATION_ORDER: List[str] = DIMENSION_TABLES + FACT_TABLES


# ---------------------------------------------------------------------------
# Registry entry schema
# {
#   "generator": Callable[[SparkSession, GenerationContext], DataFrame],
#   "write_opts": { "partition_cols": [...], "zorder_cols": [...] }
# }
# ---------------------------------------------------------------------------

# This dict is populated by register() calls at the bottom of the generator
# modules.  At import time it is empty; populate it by calling
# build_registry() from the notebook after importing the generators.

TABLE_REGISTRY: Dict[str, Dict[str, Any]] = {}


def register(
    table_name: str,
    generator_fn: Callable,
    partition_cols: List[str] | None = None,
    zorder_cols: List[str] | None = None,
) -> None:
    """Register a generator function for *table_name*."""
    TABLE_REGISTRY[table_name] = {
        "generator":     generator_fn,
        "write_opts": {
            "partition_cols": partition_cols or [],
            "zorder_cols":    zorder_cols or [],
        },
    }


# ---------------------------------------------------------------------------
# Context object passed to every generator
# ---------------------------------------------------------------------------

class GenerationContext:
    """
    Carries pre-materialised dimension DataFrames and their PK id lists
    so fact generators can reference FK pools without re-reading Delta tables.

    Populated incrementally as dimensions are written.
    """

    def __init__(self) -> None:
        self._dfs:  Dict[str, Any] = {}   # table_name -> DataFrame (small dims)
        self._ids:  Dict[str, List[Any]] = {}  # table_name -> [pk values]

    def register_dim(self, table_name: str, df: Any, pk_col: str) -> None:
        self._dfs[table_name] = df
        self._ids[table_name] = [row[pk_col] for row in df.select(pk_col).collect()]

    def get_df(self, table_name: str) -> Any:
        return self._dfs[table_name]

    def get_ids(self, table_name: str) -> List[Any]:
        return self._ids.get(table_name, [])

    def get_str_pool(self, table_name: str, col: str) -> List[str]:
        df = self._dfs[table_name]
        return [row[col] for row in df.select(col).collect()]
