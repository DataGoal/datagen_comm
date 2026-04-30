"""
utils/spark_helpers.py
======================
PySpark / Databricks utility functions used across the generator modules.
All functions are stateless – they accept a SparkSession and return either
a DataFrame or a configuration dict.
"""
from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from utils.schema_loader import get_generation_config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SparkSession configuration
# ---------------------------------------------------------------------------

def configure_spark(spark: SparkSession) -> SparkSession:
    """Apply performance tuning settings for large-scale generation."""
    cfg = get_generation_config()
    shuffle_partitions = cfg.get("spark_shuffle_partitions", 800)

    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    # spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    # Delta write optimisations
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    logger.info("Spark configured: shuffle_partitions=%s", shuffle_partitions)
    return spark


# ---------------------------------------------------------------------------
# Delta table helpers
# ---------------------------------------------------------------------------

def write_table(
    df: DataFrame,
    table_name: str,
    partition_cols: Optional[List[str]] = None,
    zorder_cols: Optional[List[str]] = None,
    write_mode: str = "overwrite",
) -> None:
    """
    Write a DataFrame to a Unity Catalog Delta table.

    Parameters
    ----------
    df           : The DataFrame to persist.
    table_name   : Fully-qualified table name  (catalog.schema.table).
    partition_cols: Optional list of partition columns.
    zorder_cols  : Columns to ZORDER BY after writing (Databricks Delta).
    write_mode   : 'overwrite' or 'append'.
    """
    cfg = get_generation_config()
    fmt = cfg.get("output_format", "delta")

    writer = df.write.format(fmt).mode(write_mode)

    if write_mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")    

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.saveAsTable(table_name)
    logger.info("Written %s rows to %s", df.count() if logger.isEnabledFor(logging.DEBUG) else "?", table_name)

    if zorder_cols:
        zorder_expr = ", ".join(zorder_cols)
        spark = df.sparkSession
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_expr})")
        logger.info("ZORDER BY %s on %s", zorder_expr, table_name)


def create_schema_if_not_exists(spark: SparkSession) -> None:
    cfg = get_generation_config()
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{cfg['catalog']}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cfg['catalog']}`.`{cfg['schema']}`")


# ---------------------------------------------------------------------------
# Array broadcast helpers (FK resolution for large fact tables)
# ---------------------------------------------------------------------------

def build_id_array(df: DataFrame, id_col: str) -> List[Any]:
    """
    Collect PK values from a small dimension DataFrame into a Python list.
    Used to build broadcast arrays for FK generation.
    """
    return [row[id_col] for row in df.select(id_col).collect()]


def build_str_array(df: DataFrame, col: str) -> List[str]:
    return [row[col] for row in df.select(col).collect()]


def array_literal(values: List[Any], spark: SparkSession) -> DataFrame:
    """
    Turn a Python list into a single-column broadcast DataFrame.
    Useful for joining against large ranges.
    """
    schema = StructType([StructField("value", StringType(), False)])
    return spark.createDataFrame([(str(v),) for v in values], schema)


def pick_from_array(values: List[Any], index_col: str = "id") -> F.Column:
    """
    Return a Spark Column expression that picks a value from *values*
    deterministically using modulo on *index_col*.

    Example
    -------
    df.withColumn("profit_center_id", pick_from_array(pc_ids, "row_id"))
    """
    arr = F.array(*[F.lit(v) for v in values])
    return arr[F.col(index_col).cast(LongType()) % F.lit(len(values))]


def pick_weighted(values: List[Any], weights: List[int], index_col: str = "id") -> F.Column:
    """
    Pick from *values* using *weights* as relative frequency.
    Expands to a weighted pool then uses modulo.
    """
    if len(values) != len(weights):
        raise ValueError("values and weights must have the same length")

    pool: List[Any] = []
    for v, w in zip(values, weights):
        pool.extend([v] * w)

    arr = F.array(*[F.lit(v) for v in pool])
    return arr[F.col(index_col).cast(LongType()) % F.lit(len(pool))]


# ---------------------------------------------------------------------------
# Partitioning helpers
# ---------------------------------------------------------------------------

def calc_partitions(row_count: int, rows_per_partition: int = 50_000_000) -> int:
    """Return a sensible partition count for a target row count."""
    return max(1, math.ceil(row_count / rows_per_partition))


def repartition_for_write(df: DataFrame, n_partitions: int) -> DataFrame:
    """Repartition before writing to control output file sizes."""
    return df.repartition(n_partitions)
