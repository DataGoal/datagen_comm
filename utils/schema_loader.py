"""
utils/schema_loader.py
======================
Loads and caches the YAML configuration files.  Provides typed access to
data-volume targets, distributions, and schema metadata parsed from
schema.yaml.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


# ---------------------------------------------------------------------------
# Default config paths (override via env vars for Databricks Repos)
# ---------------------------------------------------------------------------
_ROOT = Path(os.environ.get("DATAGEN_ROOT", Path(__file__).parent.parent))
_SCHEMA_PATH       = _ROOT / "schema.yaml"  # user-supplied schema
_VOLUMES_PATH      = _ROOT / "configs" / "data_volumes.yaml"
_DISTRIBUTIONS_PATH = _ROOT / "configs" / "distributions.yaml"


# ---------------------------------------------------------------------------
# Public loader functions
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def load_schema() -> Dict[str, Any]:
    """Parse schema.yaml and index tables by name."""
    with open(_SCHEMA_PATH, "r") as f:
        raw = yaml.safe_load(f)

    index: Dict[str, Any] = {}
    for table in raw.get("tables", []):
        index[table["name"]] = table

    return {
        "version": raw.get("version"),
        "catalog": raw.get("catalog"),
        "schema": raw.get("schema"),
        "tables": index,
    }


@lru_cache(maxsize=1)
def load_volumes() -> Dict[str, Any]:
    with open(_VOLUMES_PATH, "r") as f:
        return yaml.safe_load(f)


@lru_cache(maxsize=1)
def load_distributions() -> Dict[str, Any]:
    with open(_DISTRIBUTIONS_PATH, "r") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def get_table_schema(table_name: str) -> Dict[str, Any]:
    """Return column definitions and relationships for a table."""
    schema = load_schema()
    if table_name not in schema["tables"]:
        raise KeyError(f"Table '{table_name}' not found in schema.yaml")
    return schema["tables"][table_name]


def get_column_names(table_name: str) -> List[str]:
    return [c["name"] for c in get_table_schema(table_name)["columns"]]


def get_relationships(table_name: str) -> List[Dict[str, Any]]:
    """Return FK relationship defs for a table (empty list if none)."""
    tbl = get_table_schema(table_name)
    return tbl.get("relationships", [])


def get_fact_row_count(fact_table: str) -> int:
    """
    Resolve the absolute row count for a fact table.
    If defined as a ratio, it is multiplied by general_ledger_fact.row_count.
    """
    vols = load_volumes()
    facts = vols["fact_tables"]

    if fact_table not in facts:
        raise KeyError(f"Fact table '{fact_table}' not configured in data_volumes.yaml")

    entry = facts[fact_table]
    if "row_count" in entry:
        return int(entry["row_count"])

    if "row_count_ratio" in entry:
        base = int(facts["general_ledger_fact"]["row_count"])
        return max(1, int(base * entry["row_count_ratio"]))

    raise ValueError(f"Fact table '{fact_table}' has neither row_count nor row_count_ratio")


def get_dim_row_count(dim_table: str) -> int:
    vols = load_volumes()
    dims = vols["dimension_tables"]
    if dim_table not in dims:
        raise KeyError(f"Dim table '{dim_table}' not in data_volumes.yaml")
    entry = dims[dim_table]
    if isinstance(entry, dict) and "row_count" in entry:
        return int(entry["row_count"])
    raise ValueError(f"Cannot determine row_count for dim table '{dim_table}'")


def get_fiscal_years() -> List[int]:
    vols = load_volumes()
    return vols["dimension_tables"]["calendar_fiscal_period_v"]["fiscal_years"]


def get_generation_config() -> Dict[str, Any]:
    return load_volumes()["generation"]


def get_full_table_name(table_name: str) -> str:
    cfg = get_generation_config()
    return f"{cfg['catalog']}.{cfg['schema']}.{table_name}"


def clear_cache() -> None:
    """Invalidate all cached YAML data (useful for testing)."""
    load_schema.cache_clear()
    load_volumes.cache_clear()
    load_distributions.cache_clear()
