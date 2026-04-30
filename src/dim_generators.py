"""
src/dim_generators.py
=====================
Generator functions for every dimension table in the schema.

Each generator:
  1. Accepts (spark: SparkSession, ctx: GenerationContext) → DataFrame
  2. Produces realistic, logically consistent data
  3. Is registered in TABLE_REGISTRY via register()

Principles:
  • Small dims  (<= 500 K rows)  → built with spark.createDataFrame or spark.range
  • Medium dims (500 K – 5 M)   → spark.range + withColumn expressions
  • All string IDs / codes follow realistic SAP / enterprise naming conventions
"""
from __future__ import annotations

import datetime
import decimal
import math
from typing import Any, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.registry import GenerationContext, register
from utils.data_helpers import (
    audit_timestamps,
    build_fiscal_periods,
    cost_center_number,
    gl_account_number,
    log_normal_amt,
    profit_center_number,
    rand_decimal,
    random_date,
    random_physical_source,
    random_user_id,
)
from utils.schema_loader import (
    get_dim_row_count,
    get_fiscal_years,
    load_distributions,
)

_D = load_distributions


# ===========================================================================
#  SMALL LOOKUP TABLES  (built from Python lists → createDataFrame)
# ===========================================================================

def gen_accounting_document_type(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d = _D()
    # Realistic SAP document type codes
    rows = [
        (1,  "SA",  "G/L Account Document",         "Y", "EN", "SA - G/L Account Document"),
        (2,  "KR",  "Vendor Invoice",                "Y", "EN", "KR - Vendor Invoice"),
        (3,  "KZ",  "Vendor Payment",                "Y", "EN", "KZ - Vendor Payment"),
        (4,  "DR",  "Customer Invoice",              "Y", "EN", "DR - Customer Invoice"),
        (5,  "DZ",  "Customer Payment",              "Y", "EN", "DZ - Customer Payment"),
        (6,  "AB",  "Accounting Document",           "Y", "EN", "AB - Accounting Document"),
        (7,  "WA",  "Goods Issue",                   "Y", "EN", "WA - Goods Issue"),
        (8,  "WE",  "Goods Receipt",                 "Y", "EN", "WE - Goods Receipt"),
        (9,  "RE",  "Invoice – Gross",               "Y", "EN", "RE - Invoice Gross"),
        (10, "RN",  "Invoice – Net",                 "Y", "EN", "RN - Invoice Net"),
        (11, "MI",  "Inventory Document",            "Y", "EN", "MI - Inventory Document"),
        (12, "PR",  "Price Change",                  "Y", "EN", "PR - Price Change"),
        (13, "ZP",  "Payment Posting",               "Y", "EN", "ZP - Payment Posting"),
        (14, "AA",  "Asset Posting",                 "Y", "EN", "AA - Asset Posting"),
        (15, "AF",  "Dep. Posting",                  "Y", "EN", "AF - Dep. Posting"),
        (16, "CO",  "Controlling Document",          "Y", "EN", "CO - Controlling Document"),
        (17, "RA",  "Sub-Ledger Accrual",            "Y", "EN", "RA - Sub-Ledger Accrual"),
        (18, "RV",  "SD Billing Transfer",           "Y", "EN", "RV - SD Billing Transfer"),
        (19, "JV",  "Journal Voucher",               "Y", "EN", "JV - Journal Voucher"),
        (20, "IC",  "Intercompany",                  "Y", "EN", "IC - Intercompany"),
    ]
    # Pad to configured row count
    n = get_dim_row_count("accounting_document_type")
    for i in range(21, n + 1):
        code = f"Z{i:02d}"
        rows.append((i, code, f"Custom Document Type {i}", "Y", "EN", f"{code} - Custom Document Type {i}"))

    schema = StructType([
        StructField("accounting_document_type_id",    LongType(),   False),
        StructField("accounting_document_type_cd",    StringType(), True),
        StructField("accounting_document_type_nm",    StringType(), True),
        StructField("active_ind",                     StringType(), True),
        StructField("language_cd",                    StringType(), True),
        StructField("accounting_document_type_cd_nm", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def gen_atscale_geo_security(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d = _D()
    regions = ["North America", "EMEA", "Greater China", "APLA",
               "Global", "Americas", "Europe", "Asia Pacific",
               "Latin America", "Middle East Africa",
               "Emerging Markets", "Developed Markets",
               "Corporate", "Digital", "Direct"]
    rows = [(r, f"ROLE_{r.upper().replace(' ', '_')}") for r in regions[:get_dim_row_count("atscale_geo_security")]]
    schema = StructType([
        StructField("region", StringType(), False),
        StructField("role",   StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def gen_version_forecast_mapping(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d = _D()
    v = d["versions"]
    rows = []
    pk = 1
    for group, codes in v["groups"].items():
        for code in codes:
            rows.append((pk, code, group, "Y"))
            pk += 1
    # Pad to n
    n = get_dim_row_count("version_forecast_mapping")
    for i in range(pk, n + 1):
        rows.append((i, f"V{i:03d}", "Other", "Y"))

    schema = StructType([
        StructField("version_forecast_mapping_id", LongType(),   False),
        StructField("version_nbr",                 StringType(), True),
        StructField("version_group_nm",            StringType(), True),
        StructField("active_ind",                  StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def gen_functional_area(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d   = _D()
    n   = get_dim_row_count("functional_area")
    names = d["functional_areas"]["names"]
    rows = []
    for i in range(1, n + 1):
        nm = names[(i - 1) % len(names)]
        suffix = "" if i <= len(names) else f" {(i - 1) // len(names) + 1}"
        rows.append((i, "EN", f"FA{i:04d}", nm + suffix))

    schema = StructType([
        StructField("functional_area_id", LongType(),   False),
        StructField("language_cd",        StringType(), True),
        StructField("functional_area_cd", StringType(), True),
        StructField("functional_area_nm", StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


# ===========================================================================
#  CALENDAR DIMENSION
# ===========================================================================

def gen_calendar_fiscal_period_v(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    fiscal_years = get_fiscal_years()
    periods      = build_fiscal_periods(fiscal_years)

    schema = StructType([
        StructField("fiscal_year_period_nbr",          IntegerType(), False),
        StructField("month_long_nm",                   StringType(),  True),
        StructField("month_short_nm",                  StringType(),  True),
        StructField("month_nbr",                       IntegerType(), True),
        StructField("year_mth",                        IntegerType(), True),
        StructField("month_relevance_dt",              DateType(),    True),
        StructField("month_start_dt",                  DateType(),    True),
        StructField("month_end_dt",                    DateType(),    True),
        StructField("month_sort_sequence_nbr",         IntegerType(), True),
        StructField("fiscal_period_nbr",               IntegerType(), True),
        StructField("fiscal_period_cd",                StringType(),  True),
        StructField("fiscal_period_sort_sequence_nbr", IntegerType(), True),
        StructField("fiscal_year_period_cd",           StringType(),  True),
        StructField("fiscal_year_period_nm",           StringType(),  True),
        StructField("season_period_cd",                StringType(),  True),
        StructField("season_alternate_period_cd",      StringType(),  True),
        StructField("season_nm",                       StringType(),  True),
        StructField("season_relevance_dt",             DateType(),    True),
        StructField("season_start_dt",                 DateType(),    True),
        StructField("season_end_dt",                   DateType(),    True),
        StructField("season_sort_sequence_nbr",        IntegerType(), True),
        StructField("quarter_calendar_nbr",            IntegerType(), True),
        StructField("quarter_calendar_sequence_nbr",   IntegerType(), True),
        StructField("quarter_business_nbr",            IntegerType(), True),
        StructField("fiscal_quarter_nbr",              IntegerType(), True),
        StructField("fiscal_quarter_cd",               StringType(),  True),
        StructField("fiscal_quarter_sort_sequence_nbr",IntegerType(), True),
        StructField("fiscal_year_quarter_nbr",         IntegerType(), True),
        StructField("fiscal_year_quarter_cd",          StringType(),  True),
        StructField("fiscal_year_quarter_alternate_cd",StringType(),  True),
        StructField("year_cd",                         StringType(),  True),
        StructField("year_nm",                         StringType(),  True),
        StructField("year_nbr",                        StringType(),  True),
        StructField("year_start_dt",                   DateType(),    True),
        StructField("year_end_dt",                     DateType(),    True),
        StructField("business_year_nbr",               IntegerType(), True),
        StructField("fiscal_year_nbr",                 IntegerType(), True),
        StructField("fiscal_year_cd",                  StringType(),  True),
        StructField("fiscal_period_sort",              IntegerType(), True),
    ])

    rows = [tuple(p[f.name] for f in schema.fields) for p in periods]
    return spark.createDataFrame(rows, schema)


# ===========================================================================
#  COMPANY CODE
# ===========================================================================

def gen_company_code(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d     = _D()
    n     = get_dim_row_count("company_code")
    names = d["companies"]["names"]
    curs  = d["companies"]["currencies"]
    rows  = []
    for i in range(1, n + 1):
        idx  = (i - 1) % len(names)
        code = f"{1000 + i}"
        nm   = names[idx] + (f" {i}" if i > len(names) else "")
        cur  = curs[idx] if idx < len(curs) else "USD"
        rows.append((i, code, nm, cur, "ETL_SERVICE", "ETL_SERVICE", "SAP_ECC"))

    schema = StructType([
        StructField("company_id",           LongType(),   False),
        StructField("company_cd",           StringType(), True),
        StructField("company_nm",           StringType(), True),
        StructField("currency_cd",          StringType(), True),
        StructField("created_by_user_id",   StringType(), True),
        StructField("updated_by_user_id",   StringType(), True),
        StructField("physical_source_cd",   StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


# ===========================================================================
#  PROFIT CENTER
# ===========================================================================

def gen_profit_center(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d        = _D()
    n        = get_dim_row_count("profit_center")
    segments = d["profit_centers"]["segments"]
    channels = d["profit_centers"]["channels"]
    geos     = d["geographies"]["regions"]

    df = (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "profit_center_id")
        .withColumn("profit_center_nbr",
            F.lpad(F.col("profit_center_id").cast(StringType()), 7, "0"))
        .withColumn("profit_center_nm",
            F.concat(
                F.array(*[F.lit(s) for s in segments])[F.col("profit_center_id") % len(segments)],
                F.lit(" "),
                F.array(*[F.lit(c) for c in channels])[F.col("profit_center_id") % len(channels)],
                F.lit(" PC-"),
                F.col("profit_center_id").cast(StringType()),
            ))
        .withColumn("segment_id", F.concat(F.lit("SEG"), F.lpad((F.col("profit_center_id") % 50 + 1).cast(StringType()), 3, "0")))
        .withColumn("geography_nm", F.array(*[F.lit(g) for g in geos])[F.col("profit_center_id") % len(geos)])
        .withColumn("profit_center_channel_nm", F.array(*[F.lit(c) for c in channels])[F.col("profit_center_id") % len(channels)])
        .withColumn("territory_nm", F.concat(F.col("geography_nm"), F.lit(" Territory")))
        .withColumn("sub_territory_nm", F.concat(F.col("geography_nm"), F.lit(" Sub-Territory-"), (F.col("profit_center_id") % 10 + 1).cast(StringType())))
        .withColumn("begin_effective_dt", F.lit(datetime.date(2015, 1, 1)))
        .withColumn("end_effective_dt",   F.lit(datetime.date(2099, 12, 31)))
        .withColumn("active_ind", F.when(F.col("profit_center_id") % 20 == 0, F.lit("N")).otherwise(F.lit("Y")))
        .withColumn("geography_sort", (F.col("profit_center_id") % 100 + 1).cast(IntegerType()))
        .withColumn("operating_segment_nm",
            F.array(*[F.lit(s) for s in d["profit_centers"]["operating_segments"]])[F.col("profit_center_id") % len(d["profit_centers"]["operating_segments"])])
    )
    return df


# ===========================================================================
#  DIVISION
# ===========================================================================

def gen_division_text(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    groups  = ["Footwear", "Apparel", "Equipment"]
    n       = get_dim_row_count("division_text")
    d       = _D()
    now     = datetime.date.today()

    rows = []
    for i in range(1, n + 1):
        grp  = groups[(i - 1) % len(groups)]
        code = f"{i:02d}"
        rows.append((
            i, code, f"{grp} Division {code}", grp, "Y",
            "DIVISION_V", "SAP_ECC",
            now, now, now,
            "ETL_SERVICE", "ETL_SERVICE", "SAP_ECC", "EN", "Y",
        ))

    schema = StructType([
        StructField("division_id",                LongType(),   False),
        StructField("division_nbr",               StringType(), True),
        StructField("division_nm",                StringType(), True),
        StructField("division_group",             StringType(), True),
        StructField("last_row_ind",               StringType(), True),
        StructField("common_data_service_view_nm",StringType(), True),
        StructField("source_system_nm",           StringType(), True),
        StructField("raw_tmst",                   DateType(),   True),
        StructField("record_created_tmst_utc",    DateType(),   True),
        StructField("record_update_tmst_utc",     DateType(),   True),
        StructField("created_by_user_id",         StringType(), True),
        StructField("updated_by_user_id",         StringType(), True),
        StructField("physical_source_cd",         StringType(), True),
        StructField("language_cd",                StringType(), True),
        StructField("active_ind",                 StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def gen_division_text_dim_v(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    """Derived view-like dim – mirrors division_text."""
    groups = ["Footwear", "Apparel", "Equipment"]
    n      = get_dim_row_count("division_text_dim_v")
    rows   = []
    for i in range(1, n + 1):
        grp  = groups[(i - 1) % len(groups)]
        code = f"{i:02d}"
        rows.append((code, f"{grp} Division {code}", i, grp))

    schema = StructType([
        StructField("division_nbr",    StringType(), False),
        StructField("division_nm",     StringType(), True),
        StructField("division_id",     LongType(),   True),
        StructField("division_group",  StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


# ===========================================================================
#  FINANCE PRODUCT DIM  (medium – spark.range)
# ===========================================================================

def gen_finance_product_dim_v(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d          = _D()
    n          = get_dim_row_count("finance_product_dim_v")
    brands     = d["products"]["brands"]
    bweights   = d["products"]["brand_weights"]
    cats       = d["products"]["categories"]
    cwts       = d["products"]["category_weights"]
    genders    = d["products"]["genders"]
    platforms  = d["products"]["platforms"]
    silhouettes= d["products"]["silhouettes"]
    franchises = d["products"]["franchises"]

    def weighted_arr(vals, wts):
        pool = []
        for v, w in zip(vals, wts):
            pool.extend([v] * w)
        return F.array(*[F.lit(x) for x in pool])

    brand_pool   = weighted_arr(brands,  bweights)
    cat_pool     = weighted_arr(cats,    cwts)
    gender_pool  = weighted_arr(genders, d["products"]["gender_weights"])
    platform_arr = F.array(*[F.lit(p) for p in platforms])
    sil_arr      = F.array(*[F.lit(s) for s in silhouettes])
    fran_arr     = F.array(*[F.lit(f) for f in franchises])

    brand_pool_size  = sum(bweights)
    cat_pool_size    = sum(cwts)
    gender_pool_size = sum(d["products"]["gender_weights"])

    df = (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "product_id")
        .withColumn("brand_nm",         brand_pool[F.col("product_id") % brand_pool_size])
        .withColumn("sub_category_desc",cat_pool[F.col("product_id") % cat_pool_size])
        .withColumn("gender_desc",      gender_pool[F.col("product_id") % gender_pool_size])
        .withColumn("primary_platform_desc", platform_arr[F.col("product_id") % len(platforms)])
        .withColumn("silhouette_desc",  sil_arr[F.col("product_id") % len(silhouettes)])
        .withColumn("franchise_nm",     fran_arr[F.col("product_id") % len(franchises)])
        .withColumn("style_nbr",        F.concat(F.lit("STY"),
            F.lpad((F.col("product_id") % 900000 + 100000).cast(StringType()), 6, "0")))
        .withColumn("style_nm",         F.concat(F.col("franchise_nm"), F.lit(" "), F.col("style_nbr")))
        .withColumn("product_cd",       F.concat(F.col("brand_nm").substr(1, 2), F.lit("-"),
            F.lpad(F.col("product_id").cast(StringType()), 8, "0")))
        .withColumn("product_company_nm", F.col("brand_nm"))
        .withColumn("age_desc",         F.array(*[F.lit(a) for a in ["Adult", "Kids", "Youth", "Infant"]])[F.col("product_id") % 4])
        .withColumn("league_desc",      F.array(*[F.lit(l) for l in ["NBA", "NFL", "FIFA", "MLB", "None", "None", "None"]])[F.col("product_id") % 7])
        .withColumn("team_nm",          F.array(*[F.lit(t) for t in ["Lakers", "Bulls", "Heat", "Celtics", "None", "None"]])[F.col("product_id") % 6])
        .withColumn("athlete_full_nm",  F.array(*[F.lit(a) for a in ["LeBron James", "Cristiano Ronaldo", "Serena Williams", "Tiger Woods", ""]])[F.col("product_id") % 5])
        .withColumn("consumer_construct_segment_nm",
            F.array(*[F.lit(s) for s in ["Performance", "Lifestyle", "Jordan", "Converse"]])[F.col("product_id") % 4])
        .withColumn("consumer_construct_dimension_nm",
            F.array(*[F.lit(s) for s in ["Footwear", "Apparel", "Equipment"]])[F.col("product_id") % 3])
        .withColumn("consumer_construct_global_consumer_offense_nm",
            F.array(*[F.lit(s) for s in ["Running", "Training", "Basketball", "Football", "Lifestyle"]])[F.col("product_id") % 5])
        .withColumn("fields_of_play_nm", F.col("primary_platform_desc"))
        .withColumn("global_category_core_focus_desc",
            F.array(*[F.lit(g) for g in ["Category Focus", "Core", "Scale", "Amplify"]])[F.col("product_id") % 4])
        .withColumn("merchandising_classification_desc",
            F.array(*[F.lit(m) for m in ["Core", "Fashion", "Performance", "Training"]])[F.col("product_id") % 4])
        .withColumn("global_sport_focus_desc",        F.col("primary_platform_desc"))
        .withColumn("global_sport_focus_derived_desc", F.col("primary_platform_desc"))
        .withColumn("global_sport_sub_focus_desc",
            F.concat(F.col("primary_platform_desc"), F.lit(" - Sub")))
        .withColumn("silhouette_type_desc",
            F.array(*[F.lit(s) for s in ["Low Cut", "Mid Cut", "High Cut", "Slip On"]])[F.col("product_id") % 4])
        .withColumn("blank_usage_ind", F.lit("N"))
        .withColumn("sub_brand_cd",   F.col("brand_nm").substr(1, 2))
        .withColumn("sub_brand_desc", F.col("brand_nm"))
        .withColumn("active_ind",
            F.when(F.col("product_id") % 50 == 0, F.lit("N")).otherwise(F.lit("Y")))
        .withColumn("created_by_user_id",  F.lit("ETL_SERVICE"))
        .withColumn("updated_by_user_id",  F.lit("ETL_SERVICE"))
        .withColumn("physical_source_cd",  F.lit("SAP_ECC"))
    )
    return df


# ===========================================================================
#  FINANCE CUSTOMER DIM
# ===========================================================================

def gen_finance_customer_dim_v(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d         = _D()
    n         = get_dim_row_count("finance_customer_dim_v")
    channels  = d["customers"]["channel_descs"]
    cweights  = d["customers"]["channel_weights"]
    mp_chans  = d["customers"]["marketplace_channels"]
    geos      = d["geographies"]["regions"]
    biz_types = d["customers"]["business_types"]

    def weighted_pool(vals, wts):
        pool = []
        for v, w in zip(vals, wts):
            pool.extend([v] * w)
        return pool

    ch_pool   = weighted_pool(channels, cweights)
    pool_size = len(ch_pool)

    ch_arr  = F.array(*[F.lit(c) for c in ch_pool])
    mp_arr  = F.array(*[F.lit(m) for m in mp_chans])
    geo_arr = F.array(*[F.lit(g) for g in geos])
    biz_arr = F.array(*[F.lit(b) for b in biz_types])

    df = (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "finance_customer_id")
        .withColumn("customer_nbr", F.concat(F.lit("CUST"),
            F.lpad(F.col("finance_customer_id").cast(StringType()), 8, "0")))
        .withColumn("channel_desc",       ch_arr[F.col("finance_customer_id") % pool_size])
        .withColumn("marketplace_channel_nm", mp_arr[F.col("finance_customer_id") % len(mp_chans)])
        .withColumn("geo_marketplace_unit_nm", geo_arr[F.col("finance_customer_id") % len(geos)])
        .withColumn("customer_nm", F.concat(
            ch_arr[F.col("finance_customer_id") % pool_size], F.lit(" Customer "),
            F.col("finance_customer_id").cast(StringType())))
        .withColumn("customer_owner_group_nm", geo_arr[F.col("finance_customer_id") % len(geos)])
        .withColumn("customer_business_type_nm", biz_arr[F.col("finance_customer_id") % len(biz_types)])
        .withColumn("customer_subtype_nm",
            F.array(*[F.lit(s) for s in ["Premium", "Standard", "Value", "Key Account"]])[F.col("finance_customer_id") % 4])
        .withColumn("sub_territory_nm",
            F.concat(geo_arr[F.col("finance_customer_id") % len(geos)], F.lit(" Sub-"),
                     (F.col("finance_customer_id") % 10 + 1).cast(StringType())))
        .withColumn("integrated_business_planning_level_1_desc", geo_arr[F.col("finance_customer_id") % len(geos)])
        .withColumn("integrated_business_planning_level_2_desc", ch_arr[F.col("finance_customer_id") % pool_size])
        .withColumn("integrated_business_planning_level_3_desc",
            F.concat(F.col("integrated_business_planning_level_2_desc"), F.lit(" Detail")))
        .withColumn("integrated_business_planning_mpu_desc", F.col("geo_marketplace_unit_nm"))
        .withColumn("partner_channel",     F.col("channel_desc"))
        .withColumn("partner_sub_channel", F.col("marketplace_channel_nm"))
        .withColumn("partner_account_classification",
            F.array(*[F.lit(p) for p in ["Tier 1", "Tier 2", "Tier 3"]])[F.col("finance_customer_id") % 3])
    )
    return df


# ===========================================================================
#  COPA ATTRIBUTION DIM
# ===========================================================================

def gen_copa_attribution_dim(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d   = _D()
    n   = get_dim_row_count("copa_attribution_dim")
    c   = d["copa"]

    rbm_arr = F.array(*[F.lit(v) for v in c["responsive_business_models"]])
    ds_arr  = F.array(*[F.lit(v) for v in c["demand_streams"]])
    bt_arr  = F.array(*[F.lit(v) for v in c["business_types"]])
    mt_arr  = F.array(*[F.lit(v) for v in c["marketing_types"]])
    ga_arr  = F.array(*[F.lit(v) for v in c["gender_ages"]])
    pl_arr  = F.array(*[F.lit(v) for v in c["product_lifecycles"]])
    dm_arr  = F.array(*[F.lit(v) for v in c["distribution_methods"]])

    rsp_prods = ["Inline", "Carryover", "Closeout", "Special Make-Up", "NikeID"]
    qt_arr    = F.array(*[F.lit(v) for v in ["A+", "A", "B", "C"]])
    so_types  = ["ZOR", "ZOQ", "ZOB", "ZORE", "ZURB"]
    soi_cats  = ["TAN", "TAF", "TAS", "TAX"]

    df = (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "copa_attribution_id")
        .withColumn("responsive_business_model_cd",   rbm_arr[F.col("copa_attribution_id") % len(c["responsive_business_models"])])
        .withColumn("responsive_business_model_desc", F.col("responsive_business_model_cd"))
        .withColumn("demand_stream_cd",   ds_arr[F.col("copa_attribution_id") % len(c["demand_streams"])])
        .withColumn("demand_stream_desc", F.col("demand_stream_cd"))
        .withColumn("business_type_cd",   bt_arr[F.col("copa_attribution_id") % len(c["business_types"])])
        .withColumn("business_type_desc", F.col("business_type_cd"))
        .withColumn("marketing_type_cd",  mt_arr[F.col("copa_attribution_id") % len(c["marketing_types"])])
        .withColumn("marketing_type_desc",F.col("marketing_type_cd"))
        .withColumn("gender_age_cd",      ga_arr[F.col("copa_attribution_id") % len(c["gender_ages"])])
        .withColumn("gender_age_desc",    F.col("gender_age_cd"))
        .withColumn("direct_business_model_cd",   rbm_arr[F.col("copa_attribution_id") % len(c["responsive_business_models"])])
        .withColumn("direct_business_model_desc", F.col("direct_business_model_cd"))
        .withColumn("product_lifecycle_cd",   pl_arr[F.col("copa_attribution_id") % len(c["product_lifecycles"])])
        .withColumn("product_lifecycle_desc", F.col("product_lifecycle_cd"))
        .withColumn("quality_cd",   qt_arr[F.col("copa_attribution_id") % 4])
        .withColumn("quality_desc", F.col("quality_cd"))
        .withColumn("region_summary_product_group_cd",
            F.array(*[F.lit(v) for v in ["FW", "AP", "EQ", "ACC"]])[F.col("copa_attribution_id") % 4])
        .withColumn("region_summary_product_group_desc",
            F.array(*[F.lit(v) for v in ["Footwear", "Apparel", "Equipment", "Accessories"]])[F.col("copa_attribution_id") % 4])
        .withColumn("sales_order_type_cd",
            F.array(*[F.lit(v) for v in so_types])[F.col("copa_attribution_id") % len(so_types)])
        .withColumn("sales_order_type_desc", F.concat(F.col("sales_order_type_cd"), F.lit(" Order")))
        .withColumn("sales_order_type_group_desc", F.col("business_type_desc"))
        .withColumn("sales_order_item_category_cd",
            F.array(*[F.lit(v) for v in soi_cats])[F.col("copa_attribution_id") % len(soi_cats)])
        .withColumn("sales_order_item_category_desc", F.concat(F.col("sales_order_item_category_cd"), F.lit(" - Standard")))
        .withColumn("distribution_method_cd",   dm_arr[F.col("copa_attribution_id") % len(c["distribution_methods"])])
        .withColumn("distribution_method_desc", F.col("distribution_method_cd"))
        .withColumn("sales_order_reason_cd",
            F.lpad((F.col("copa_attribution_id") % 20 + 1).cast(StringType()), 2, "0"))
        .withColumn("sales_order_reason_desc", F.concat(F.lit("Reason "), F.col("sales_order_reason_cd")))
    )
    return df


# ===========================================================================
#  COST CENTER DIM
# ===========================================================================

def gen_cost_center_dim_v(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d       = _D()
    n       = get_dim_row_count("cost_center_dim_v")
    geos    = d["geographies"]["regions"]
    cntries = d["geographies"]["countries"]
    geo_arr = F.array(*[F.lit(g) for g in geos])
    cty_arr = F.array(*[F.lit(c) for c in cntries])

    df = (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "cost_center_id")
        .withColumn("cost_center_nbr",
            F.concat(F.lit("CC"), F.lpad(F.col("cost_center_id").cast(StringType()), 6, "0")))
        .withColumn("controlling_area_cd", F.lit("1000"))
        .withColumn("valid_to_dt",   F.lit(datetime.date(2099, 12, 31)))
        .withColumn("valid_from_dt", F.lit(datetime.date(2015, 1, 1)))
        .withColumn("iso_language_cd", F.lit("EN"))
        .withColumn("cost_center_nm",
            F.concat(geo_arr[F.col("cost_center_id") % len(geos)], F.lit(" Cost Center "),
                     F.col("cost_center_id").cast(StringType())))
        .withColumn("cost_center_desc",
            F.concat(F.lit("Description for "), F.col("cost_center_nm")))
        .withColumn("cost_center_category_hierarchy_1_cd",
            F.array(*[F.lit(v) for v in ["A", "B", "C", "D"]])[F.col("cost_center_id") % 4])
        .withColumn("cost_center_category_hierarchy_2_cd",
            F.array(*[F.lit(v) for v in ["1", "2", "3", "4"]])[F.col("cost_center_id") % 4])
        .withColumn("company_cd",
            F.lpad((F.col("cost_center_id") % 80 + 1001).cast(StringType()), 4, "0"))
        .withColumn("source_system", F.lit("SAP_ECC"))
        .withColumn("cost_center_type_cd",
            F.array(*[F.lit(v) for v in ["E", "F", "L", "M", "P"]])[F.col("cost_center_id") % 5])
        .withColumn("cost_center_category_short_desc",
            F.array(*[F.lit(v) for v in ["Expenses", "Finance", "Logistics", "Marketing", "Production"]])[F.col("cost_center_id") % 5])
        .withColumn("business_area_cd",
            F.concat(F.lit("BA"), F.lpad((F.col("cost_center_id") % 10 + 1).cast(StringType()), 2, "0")))
        .withColumn("tax_jurisdiction_cd",
            F.concat(cty_arr[F.col("cost_center_id") % len(cntries)], F.lit("0000000")))
        .withColumn("functional_area_cd",
            F.concat(F.lit("FA"), F.lpad((F.col("cost_center_id") % 150 + 1).cast(StringType()), 4, "0")))
        .withColumn("currency_cd",
            F.array(*[F.lit(c) for c in ["USD", "EUR", "GBP", "JPY", "CNY"]])[F.col("cost_center_id") % 5])
        .withColumn("posting_allowed_ind",                  F.lit("X"))
        .withColumn("planning_allowed_ind",                 F.lit("X"))
        .withColumn("secondary_costs_posting_allowed_ind",  F.lit("X"))
        .withColumn("revenue_posting_allowed_ind",
            F.when(F.col("cost_center_id") % 3 == 0, F.lit("X")).otherwise(F.lit("")))
        .withColumn("commitment_update_allowed_ind",        F.lit("X"))
        .withColumn("secondary_costs_planning_allowed_ind", F.lit("X"))
        .withColumn("revenue_planning_allowed_ind",
            F.when(F.col("cost_center_id") % 3 == 0, F.lit("X")).otherwise(F.lit("")))
        .withColumn("quantity_required_ind",
            F.when(F.col("cost_center_id") % 5 == 0, F.lit("X")).otherwise(F.lit("")))
        .withColumn("department_nm", F.concat(F.lit("Dept-"), (F.col("cost_center_id") % 20 + 1).cast(StringType())))
        .withColumn("profit_center_nbr",
            F.lpad((F.col("cost_center_id") % 2000 + 1).cast(StringType()), 7, "0"))
        .withColumn("country_cd", cty_arr[F.col("cost_center_id") % len(cntries)])
        .withColumn("region_cd", geo_arr[F.col("cost_center_id") % len(geos)])
        .withColumn("city_nm",
            F.array(*[F.lit(c) for c in ["New York", "London", "Tokyo", "Shanghai", "Paris", "Sydney"]])[F.col("cost_center_id") % 6])
        .withColumn("postal_cd",
            F.lpad((F.col("cost_center_id") % 90000 + 10000).cast(StringType()), 5, "0"))
        .withColumn("begin_effective_dt", F.lit(datetime.date(2015, 1, 1)))
        .withColumn("end_effective_dt",   F.lit(datetime.date(2099, 12, 31)))
        .withColumn("_cost_center_cleansed_latest_load_timestamp", F.lit(datetime.date.today()))
        # Nullable / rarely used fields set to None
        .withColumn("cost_center_report_printer_destination_cd", F.lit(None).cast(StringType()))
        .withColumn("company_legal_entity_id",   F.lit(None).cast(StringType()))
        .withColumn("responsible_user_nm",        F.lit("SYSTEM"))
        .withColumn("responsible_user_id",        F.lit("SYSTEM"))
        .withColumn("responsible_user_title",     F.lit(None).cast(StringType()))
        .withColumn("line_1_nm", F.col("cost_center_nm"))
        .withColumn("line_2_nm", F.lit(None).cast(StringType()))
        .withColumn("line_3_nm", F.lit(None).cast(StringType()))
        .withColumn("line_4_nm", F.lit(None).cast(StringType()))
        .withColumn("district_nm",            F.lit(None).cast(StringType()))
        .withColumn("street_address_txt",     F.concat(F.lit("100 Commerce Way, "), F.col("city_nm")))
        .withColumn("po_box_postal_cd",       F.lit(None).cast(StringType()))
        .withColumn("po_box_nbr",             F.lit(None).cast(StringType()))
        .withColumn("correspondence_language_cd", F.lit("EN"))
        .withColumn("first_telephone_nbr",    F.lit(None).cast(StringType()))
        .withColumn("second_telephone_nbr",   F.lit(None).cast(StringType()))
        .withColumn("telebox_nbr",            F.lit(None).cast(StringType()))
        .withColumn("fax_nbr",                F.lit(None).cast(StringType()))
        .withColumn("teletex_nbr",            F.lit(None).cast(StringType()))
        .withColumn("telex_nbr",              F.lit(None).cast(StringType()))
        .withColumn("data_communication_line_nbr", F.lit(None).cast(StringType()))
        .withColumn("msg_header_tmst",        F.lit(datetime.date.today()))
    )
    return df


# ===========================================================================
#  GL ACCOUNT DIM
# ===========================================================================

def gen_gl_account_dim(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d = _D()
    n = get_dim_row_count("gl_account_dim")

    account_types = ["Revenue", "COGS", "Gross Profit", "Selling", "Admin",
                     "R&D", "Other Income", "Other Expense", "Tax"]
    at_arr = F.array(*[F.lit(a) for a in account_types])

    df = (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "gl_accnt_id")
        .withColumn("_acct_type", at_arr[F.col("gl_accnt_id") % len(account_types)])
        .withColumn("gl_account_nbr",
            # Deterministic mapping to realistic account number ranges
            F.when(F.col("gl_accnt_id") % 9 == 0, F.concat(F.lit("7"), F.lpad((F.col("gl_accnt_id") % 100000).cast(StringType()), 6, "0")))
            .when(F.col("gl_accnt_id") % 9 == 1, F.concat(F.lit("1"), F.lpad((F.col("gl_accnt_id") % 999999).cast(StringType()), 6, "0")))
            .when(F.col("gl_accnt_id") % 9 == 2, F.concat(F.lit("2"), F.lpad((F.col("gl_accnt_id") % 999999).cast(StringType()), 6, "0")))
            .when(F.col("gl_accnt_id") % 9 == 3, F.concat(F.lit("3"), F.lpad((F.col("gl_accnt_id") % 100000).cast(StringType()), 6, "0")))
            .when(F.col("gl_accnt_id") % 9 == 4, F.concat(F.lit("44"), F.lpad((F.col("gl_accnt_id") % 100000).cast(StringType()), 5, "0")))
            .when(F.col("gl_accnt_id") % 9 == 5, F.concat(F.lit("45"), F.lpad((F.col("gl_accnt_id") % 100000).cast(StringType()), 5, "0")))
            .when(F.col("gl_accnt_id") % 9 == 6, F.concat(F.lit("5"), F.lpad((F.col("gl_accnt_id") % 100000).cast(StringType()), 6, "0")))
            .when(F.col("gl_accnt_id") % 9 == 7, F.concat(F.lit("6"), F.lpad((F.col("gl_accnt_id") % 100000).cast(StringType()), 6, "0")))
            .otherwise(                           F.concat(F.lit("8"), F.lpad((F.col("gl_accnt_id") % 100000).cast(StringType()), 6, "0")))
        )
        .withColumn("gl_account_short_desc", F.concat(F.col("_acct_type"), F.lit(" Acct "), F.col("gl_account_nbr")))
        .withColumn("gl_account_long_desc",  F.concat(F.lit("General Ledger Account - "), F.col("gl_account_short_desc")))
        .withColumn("begin_effective_dt",    F.lit(datetime.date(2015, 1, 1)))
        .withColumn("end_effective_dt",      F.lit(datetime.date(2099, 12, 31)))
        .withColumn("active_ind", F.when(F.col("gl_accnt_id") % 100 == 0, F.lit("N")).otherwise(F.lit("Y")))
        .withColumn("cost_component_calc",   F.lit(None).cast(StringType()))
        .drop("_acct_type")
    )
    return df


# ===========================================================================
#  GEO DIMS
# ===========================================================================

def gen_geo_marketplace_channel_dim(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d = _D()
    n = get_dim_row_count("geo_marketplace_channel_dim")
    channels = d["customers"]["marketplace_channels"]
    geos     = d["geographies"]["regions"]

    rows = []
    for i in range(1, n + 1):
        ch = channels[(i - 1) % len(channels)]
        geo= geos[(i - 1) % len(geos)]
        rows.append((i, f"{geo} - {ch}", "ETL_SERVICE", "ETL_SERVICE", "SAP_ECC", "Y"))

    schema = StructType([
        StructField("geo_marketplace_channel_id", LongType(),   False),
        StructField("geo_marketplace_channel_nm", StringType(), True),
        StructField("created_by_user_id",         StringType(), True),
        StructField("updated_by_user_id",         StringType(), True),
        StructField("physical_source_cd",         StringType(), True),
        StructField("active_ind",                 StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


def gen_geo_wholesale_value_business_dim(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d    = _D()
    n    = get_dim_row_count("geo_wholesale_value_business_dim")
    geos = d["geographies"]["regions"]
    tiers= ["Tier 1", "Tier 2", "Tier 3", "Key Account", "Standard"]

    rows = []
    for i in range(1, n + 1):
        geo  = geos[(i - 1) % len(geos)]
        tier = tiers[(i - 1) % len(tiers)]
        rows.append((i, f"{geo} Wholesale {tier}", "ETL_SERVICE", "ETL_SERVICE", "SAP_ECC"))

    schema = StructType([
        StructField("geo_wholesale_value_business_id",   LongType(),   False),
        StructField("geo_wholesale_value_business_desc", StringType(), True),
        StructField("created_by_user_id",                StringType(), True),
        StructField("updated_by_user_id",                StringType(), True),
        StructField("physical_source_cd",                StringType(), True),
    ])
    return spark.createDataFrame(rows, schema)


# ===========================================================================
#  ZFSM MEASURES HIERARCHY
# ===========================================================================

def gen_gl_account_zfsm_measures_hierarchy_dim(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d        = _D()
    n        = get_dim_row_count("gl_account_zfsm_measures_hierarchy_dim")
    measures = [
        "Net Revenue", "Gross Profit", "EBIT", "EBITDA",
        "Operating Income", "Net Income", "COGS", "Gross Margin",
        "Operating Expense", "Capital Expenditure",
    ]
    today = datetime.date.today()

    df = (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "zfsm_measure_id")
        .withColumn("zfsm_measure_cd",
            F.concat(F.lit("ZFSM"), F.lpad(F.col("zfsm_measure_id").cast(StringType()), 6, "0")))
        .withColumn("zfsm_measure_desc",
            F.concat(
                F.array(*[F.lit(m) for m in measures])[F.col("zfsm_measure_id") % len(measures)],
                F.lit(" - "), F.col("zfsm_measure_cd")))
        .withColumn("active_ind", F.when(F.col("zfsm_measure_id") % 50 == 0, F.lit("N")).otherwise(F.lit("Y")))
        .withColumn("created_by_user_id", F.lit("ETL_SERVICE"))
        .withColumn("updated_by_user_id", F.lit("ETL_SERVICE"))
        .withColumn("physical_source_cd", F.lit("SAP_ECC"))
        .withColumn("gl_account_level_1_cd", F.concat(F.lit("L1_"), (F.col("zfsm_measure_id") % 10 + 1).cast(StringType())))
        .withColumn("gl_account_level_1_nm", F.concat(F.lit("Level 1 - "), F.col("gl_account_level_1_cd")))
        .withColumn("gl_account_level_2_cd", F.concat(F.lit("L2_"), (F.col("zfsm_measure_id") % 20 + 1).cast(StringType())))
        .withColumn("gl_account_level_2_nm", F.concat(F.lit("Level 2 - "), F.col("gl_account_level_2_cd")))
        .withColumn("gl_account_level_3_cd", F.concat(F.lit("L3_"), (F.col("zfsm_measure_id") % 30 + 1).cast(StringType())))
        .withColumn("gl_account_level_3_nm", F.concat(F.lit("Level 3 - "), F.col("gl_account_level_3_cd")))
        # Levels 4-13 follow the same pattern
        *[col for level in range(4, 14) for col in [
            F.lit(None).cast(StringType()).alias(f"gl_account_level_{level}_cd"),
            F.lit(None).cast(StringType()).alias(f"gl_account_level_{level}_nm"),
        ]]
        .withColumn("record_created_tmst_utc", F.lit(today))
        .withColumn("record_update_tmst_utc",  F.lit(today))
    )
    return df


# ===========================================================================
#  HIERARCHY TABLES (generated via spark.range + pattern)
# ===========================================================================

def _gen_cost_center_hierarchy(spark, table_name, n) -> DataFrame:
    """Shared generator for cost-center hierarchy tables (30 levels)."""
    d       = _D()
    geos    = d["geographies"]["regions"]
    today   = datetime.date.today()

    df = spark.range(1, n + 1).withColumnRenamed("id", "cost_center_hierarchy_hist_id")
    df = (df
        .withColumn("cost_center_nbr",
            F.concat(F.lit("CC"), F.lpad(F.col("cost_center_hierarchy_hist_id").cast(StringType()), 6, "0")))
        .withColumn("cost_center_hierarchy_nm",
            F.array(*[F.lit(g) for g in geos])[F.col("cost_center_hierarchy_hist_id") % len(geos)])
        .withColumn("controlling_area_cd", F.lit("1000"))
    )
    # Generate 30 levels of hierarchy
    for level in range(1, 31):
        max_nodes_at_level = max(1, n // (level * 10))
        df = df.withColumn(f"cost_center_level_{level}_cd",
            F.concat(F.lit(f"L{level:02d}_"),
                     (F.col("cost_center_hierarchy_hist_id") % max_nodes_at_level + 1).cast(StringType())))
        df = df.withColumn(f"cost_center_level_{level}_nm",
            F.concat(F.lit(f"Level {level} Node "),
                     (F.col("cost_center_hierarchy_hist_id") % max_nodes_at_level + 1).cast(StringType())))
    return df


def gen_segment_cost_center_hierarchy_dim_v(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    return _gen_cost_center_hierarchy(spark, "segment_cost_center_hierarchy_dim_v",
                                      get_dim_row_count("segment_cost_center_hierarchy_dim_v"))


def gen_DisChannel_cost_center_hierarchy_dim_v(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    return _gen_cost_center_hierarchy(spark, "DisChannel_cost_center_hierarchy_dim_v",
                                      get_dim_row_count("DisChannel_cost_center_hierarchy_dim_v"))


def _gen_profit_center_hierarchy(spark, pk_col, extra_cols, n) -> DataFrame:
    """Shared generator for profit-center hierarchy tables (9 levels)."""
    d    = _D()
    geos = d["geographies"]["regions"]

    df = spark.range(1, n + 1).withColumnRenamed("id", pk_col)
    df = df.withColumn("profit_center_hierarchy_nm",
        F.array(*[F.lit(g) for g in geos])[F.col(pk_col) % len(geos)])
    for col_name, col_expr in extra_cols.items():
        df = df.withColumn(col_name, col_expr(F.col(pk_col)))
    for level in range(1, 10):
        max_nodes = max(1, n // (level * 5))
        df = df.withColumn(f"profit_center_level_{level}_cd",
            F.concat(F.lit(f"PCL{level}_"), (F.col(pk_col) % max_nodes + 1).cast(StringType())))
        df = df.withColumn(f"profit_center_level_{level}_nm",
            F.concat(F.lit(f"PC Level {level} - "), (F.col(pk_col) % max_nodes + 1).cast(StringType())))
    return df


def gen_segment_profit_center_hierarchy(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    n = get_dim_row_count("segment_profit_center_hierarchy")
    return _gen_profit_center_hierarchy(
        spark, "segment_profit_center_nbr",
        {"profit_center_hierarchy_nm": lambda c: F.concat(F.lit("Segment PC Hierarchy "), c.cast(StringType()))},
        n
    )


def gen_DisChannel_profit_center_hierarchy(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    n  = get_dim_row_count("DisChannel_profit_center_hierarchy")
    df = _gen_profit_center_hierarchy(
        spark, "profit_center_hierarchy_id",
        {
            "distrchnl_profit_center_nbr": lambda c: F.lpad(c.cast(StringType()), 7, "0"),
            "controlling_area_cd":         lambda c: F.lit("1000"),
        },
        n
    )
    return df


def gen_PartDisChannel_profit_center_hierarchy(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    n  = get_dim_row_count("PartDisChannel_profit_center_hierarchy")
    df = _gen_profit_center_hierarchy(
        spark, "profit_center_hierarchy_id",
        {
            "controlling_area_cd":         lambda c: F.lit("1000"),
            "prtrdistrchnl_profit_center_nbr": lambda c: F.lpad(c.cast(StringType()), 7, "0"),
        },
        n
    )
    return df


# ===========================================================================
#  CONSOLIDATION HIERARCHY DIMS
# ===========================================================================

def gen_consolidation_functional_area_hierarchy(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d     = _D()
    n     = get_dim_row_count("consolidation_functional_area_hierarchy")
    today = datetime.date.today()
    fa_names = d["functional_areas"]["names"]

    df = spark.range(1, n + 1).withColumnRenamed("id", "consolidation_functional_area_hierarchy_id")
    df = (df
        .withColumn("active_ind", F.when(F.col("consolidation_functional_area_hierarchy_id") % 20 == 0, F.lit("N")).otherwise(F.lit("Y")))
        .withColumn("functional_area_cd", F.concat(F.lit("FA"), F.lpad(F.col("consolidation_functional_area_hierarchy_id").cast(StringType()), 4, "0")))
        .withColumn("consolidation_functional_area_hierarchy_parent_cd",
            F.when(F.col("consolidation_functional_area_hierarchy_id") <= 10, F.lit(None).cast(StringType()))
            .otherwise(F.concat(F.lit("FA"), F.lpad((F.col("consolidation_functional_area_hierarchy_id") % 10 + 1).cast(StringType()), 4, "0"))))
        .withColumn("functional_area_level_nbr",
            (F.col("consolidation_functional_area_hierarchy_id") % 7 + 1).cast(IntegerType()))
        .withColumn("functional_area_type_nm",
            F.array(*[F.lit(nm) for nm in fa_names])[F.col("consolidation_functional_area_hierarchy_id") % len(fa_names)])
        .withColumn("consolidation_functional_area_hierarchy_cd", F.col("functional_area_cd"))
        .withColumn("consolidation_functional_area_hierarchy_desc",
            F.concat(F.lit("CFA Hierarchy "), F.col("functional_area_cd")))
        .withColumn("created_by_user_id", F.lit("ETL_SERVICE"))
        .withColumn("updated_by_user_id", F.lit("ETL_SERVICE"))
        .withColumn("physical_source_cd", F.lit("SAP_ECC"))
        .withColumn("record_created_tmst_utc", F.lit(today))
        .withColumn("record_update_tmst_utc",  F.lit(today))
        .withColumn("_consolidation_functional_area_hierarchy_raw_latest_load_tmst", F.lit(today))
    )
    # Add 7 levels of hierarchy
    for lvl in range(1, 8):
        df = df.withColumn(f"consolidation_functional_area_{lvl}_cd",
            F.concat(F.lit(f"CFA{lvl}_"), (F.col("consolidation_functional_area_hierarchy_id") % (n // (lvl * 5) + 1) + 1).cast(StringType())))
        df = df.withColumn(f"consolidation_functional_area_{lvl}_nm",
            F.concat(F.lit(f"CFA Level {lvl} "), (F.col("consolidation_functional_area_hierarchy_id") % (n // (lvl * 5) + 1) + 1).cast(StringType())))
    return df


def gen_consolidation_segment_hierarchy_dim(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d     = _D()
    n     = get_dim_row_count("consolidation_segment_hierarchy_dim")
    segs  = d["profit_centers"]["segments"]
    today = datetime.date.today()

    df = spark.range(1, n + 1).withColumnRenamed("id", "consolidation_segment_hierarchy_id")
    df = (df
        .withColumn("segment_nbr", (F.col("consolidation_segment_hierarchy_id") % 100 + 1).cast(IntegerType()))
        .withColumn("segment_desc",
            F.array(*[F.lit(s) for s in segs])[F.col("consolidation_segment_hierarchy_id") % len(segs)])
        .withColumn("consolidation_segment_hierarchy_parent_cd",
            F.when(F.col("consolidation_segment_hierarchy_id") <= 10, F.lit(None).cast(StringType()))
            .otherwise(F.concat(F.lit("SEG"), F.lpad((F.col("consolidation_segment_hierarchy_id") % 10 + 1).cast(StringType()), 3, "0"))))
        .withColumn("segment_type_nm",
            F.array(*[F.lit(s) for s in ["Operating", "Reporting", "Management"]])[F.col("consolidation_segment_hierarchy_id") % 3])
        .withColumn("segment_level_nbr",
            (F.col("consolidation_segment_hierarchy_id") % 10 + 1).cast(IntegerType()))
        .withColumn("consolidation_segment_hierarchy_cd",
            F.concat(F.lit("CSH"), F.lpad(F.col("consolidation_segment_hierarchy_id").cast(StringType()), 5, "0")))
        .withColumn("consolidation_segment_hierarchy_desc",
            F.concat(F.lit("Cons Segment Hierarchy "), F.col("consolidation_segment_hierarchy_cd")))
        .withColumn("consolidation_segment_hierarchy_cd_desc",
            F.concat(F.col("consolidation_segment_hierarchy_cd"), F.lit(" - "), F.col("consolidation_segment_hierarchy_desc")))
        .withColumn("created_by_user_id", F.lit("ETL_SERVICE"))
        .withColumn("updated_by_user_id", F.lit("ETL_SERVICE"))
        .withColumn("physical_source_cd", F.lit("SAP_ECC"))
        .withColumn("active_ind", F.lit("Y"))
    )
    # 10 levels × 3 columns each
    for lvl in range(1, 11):
        max_nodes = max(1, n // (lvl * 4))
        cd_val = F.concat(F.lit(f"SEG{lvl}_"), (F.col("consolidation_segment_hierarchy_id") % max_nodes + 1).cast(StringType()))
        nm_val = F.concat(F.lit(f"Segment Level {lvl} - "), (F.col("consolidation_segment_hierarchy_id") % max_nodes + 1).cast(StringType()))
        df = (df
            .withColumn(f"consolidation_segment_{lvl}_cd",     cd_val)
            .withColumn(f"consolidation_segment_{lvl}_nm",     nm_val)
            .withColumn(f"consolidation_segment_{lvl}_cd_nm",  F.concat(cd_val, F.lit(" - "), nm_val))
        )
    return df


# ===========================================================================
#  GL ACCOUNT HIERARCHY  (shared logic for gl_account_hierarchy & management)
# ===========================================================================

def _gen_gl_account_hierarchy(spark, pk_col, n, extra_cols=None) -> DataFrame:
    d         = _D()
    today     = datetime.date.today()
    coa_codes = d["gl_accounts"]["chart_of_accounts"]

    df = spark.range(1, n + 1).withColumnRenamed("id", pk_col)
    df = (df
        .withColumn("gl_account_nbr",
            F.lpad((F.col(pk_col) % 8000 + 1000000).cast(StringType()), 7, "0"))
        .withColumn("hierarchy_chart_of_accounts_cd",
            F.array(*[F.lit(c) for c in coa_codes])[F.col(pk_col) % len(coa_codes)])
        .withColumn("hierarchy_nm",
            F.array(*[F.lit(h) for h in ["P&L", "Balance Sheet", "Management", "CONSO"]])[F.col(pk_col) % 4])
        .withColumn("hierarchy_cd",    F.concat(F.lit("H"), F.lpad((F.col(pk_col) % 20 + 1).cast(StringType()), 3, "0")))
        .withColumn("parent_cd",       F.concat(F.lit("P"), F.lpad((F.col(pk_col) % 50 + 1).cast(StringType()), 4, "0")))
        .withColumn("node_nm",         F.concat(F.lit("Node "), F.col(pk_col).cast(StringType())))
        .withColumn("node_cd",         F.concat(F.lit("N"), F.lpad(F.col(pk_col).cast(StringType()), 6, "0")))
        .withColumn("node_gl_account_nbr",    (F.col(pk_col) % 7000 + 1000000).cast(IntegerType()))
        .withColumn("node_gl_account_to_nbr", (F.col(pk_col) % 7000 + 1000100).cast(IntegerType()))
        .withColumn("depth_of_leaf_nbr", (F.col(pk_col) % 8 + 1).cast(IntegerType()))
        .withColumn("depth_of_tree_nbr", F.lit(30).cast(IntegerType()))
        .withColumn("functional_area_assignment_allowed_ind", F.lit("X"))
        .withColumn("consolidation_chart_of_accounts_used_ind", F.lit("X"))
        .withColumn("created_by_user_id", F.lit("ETL_SERVICE"))
        .withColumn("updated_by_user_id", F.lit("ETL_SERVICE"))
        .withColumn("physical_source_cd", F.lit("SAP_ECC"))
        .withColumn("record_created_tmst_utc", F.lit(today))
        .withColumn("record_update_tmst_utc",  F.lit(today))
        # Indicator flags
        .withColumn("hierarchy_liabilty_cd",        F.lit(None).cast(StringType()))
        .withColumn("hierarchy_net_loss_cd",         F.lit(None).cast(StringType()))
        .withColumn("hierarchy_net_profit_cd",       F.lit(None).cast(StringType()))
        .withColumn("hierarchy_profit_loss_cd",      F.lit(None).cast(StringType()))
        .withColumn("hierarchy_not_assignable_asset_cd", F.lit(None).cast(StringType()))
        .withColumn("hierarchy_notes_cd",            F.lit(None).cast(StringType()))
        .withColumn("hierarchy_language_cd",         F.lit("EN"))
        .withColumn("hierarchy_action_cd",           F.lit("U"))
        .withColumn("hierarchy_category_cd",         F.lit("GL"))
        .withColumn("sign_reversed_ind",             F.when(F.col(pk_col) % 5 == 0, F.lit("X")).otherwise(F.lit("")))
        .withColumn("credit_balance_ind",            F.when(F.col(pk_col) % 3 == 0, F.lit("X")).otherwise(F.lit("")))
        .withColumn("debit_balance_ind",             F.when(F.col(pk_col) % 3 != 0, F.lit("X")).otherwise(F.lit("")))
        .withColumn("totals_visible_ind",            F.lit("X"))
        .withColumn("item_list_complete_transmission_ind",         F.lit("X"))
        .withColumn("hierarchy_name_list_complete_transmission_ind", F.lit("X"))
        .withColumn("node_name_list_complete_transmission_ind",    F.lit("X"))
        .withColumn("functional_area_list_complete_transmission_ind", F.lit("X"))
        .withColumn("gl_account_list_complete_transmission_ind",   F.lit("X"))
        .withColumn("relationship_list_complete_transmission_ind", F.lit("X"))
        .withColumn("top_level_assets_financial_reporting_structure_item_cd", F.lit(None).cast(StringType()))
        .withColumn("recipient_business_system_id",  F.lit("ECC_PROD"))
        .withColumn("sender_business_system_id",     F.lit("ECC_PROD"))
        .withColumn("gl_account_hierarchy_hist_id",  F.col(pk_col))
        .withColumn("_gl_account_hierarchy_cleansed_latest_load_timestamp", F.lit(today))
    )
    # 30 levels
    for lvl in range(1, 31):
        max_nodes = max(1, n // (lvl * 3))
        df = df.withColumn(f"gl_account_level_{lvl}_cd",
            F.concat(F.lit(f"GL{lvl}_"), (F.col(pk_col) % max_nodes + 1).cast(StringType())))
        df = df.withColumn(f"gl_account_level_{lvl}_nm",
            F.concat(F.lit(f"GL Level {lvl} - "), (F.col(pk_col) % max_nodes + 1).cast(StringType())))
    # Extra columns for specific tables
    if extra_cols:
        for col_name, col_expr in extra_cols.items():
            df = df.withColumn(col_name, col_expr)
    return df


def gen_gl_account_hierarchy(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    n = get_dim_row_count("gl_account_hierarchy")
    return _gen_gl_account_hierarchy(spark, "gl_account_hierarchy_id", n)


def gen_management_gl_account_hierarchy(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    n  = get_dim_row_count("management_gl_account_hierarchy")
    df = _gen_gl_account_hierarchy(spark, "gl_account_hierarchy_id", n,
        extra_cols={"hierarchy_cd_nm": F.concat(F.lit("H"), F.lpad(F.col("gl_account_hierarchy_id").cast(StringType()), 3, "0"))})
    # management hierarchy doesn't have some audit columns
    for drop_col in ["gl_account_hierarchy_hist_id", "record_created_tmst_utc",
                     "record_update_tmst_utc", "_gl_account_hierarchy_cleansed_latest_load_timestamp"]:
        if drop_col in df.columns:
            df = df.drop(drop_col)
    return df


# ===========================================================================
#  FOREIGN CURRENCY EXCHANGE RATES
# ===========================================================================

def gen_finance_foreign_currency_exchange_rate(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d     = _D()
    rates = d["exchange_rates"]
    curs  = d["currencies"]["codes"]
    rate_types = [("M", "Average Rate"), ("P", "Month-End Rate"), ("B", "Budget Rate")]

    rows = []
    pk   = 1
    for from_cur in curs:
        for rt_cd, rt_nm in rate_types:
            for to_cur in ["USD", "EUR"]:
                if from_cur == to_cur:
                    rate = decimal.Decimal("1.0")
                else:
                    from_usd = rates.get(from_cur, 1.0)
                    to_usd   = rates.get(to_cur,   1.0)
                    rate     = decimal.Decimal(str(round(to_usd / from_usd, 8)))
                rows.append((pk, from_cur, rt_cd, rt_nm, rate, from_cur, to_cur, to_cur, "Y"))
                pk += 1
                if pk > get_dim_row_count("finance_foreign_currency_exchange_rate"):
                    break
            if pk > get_dim_row_count("finance_foreign_currency_exchange_rate"):
                break
        if pk > get_dim_row_count("finance_foreign_currency_exchange_rate"):
            break

    schema = StructType([
        StructField("finance_foreign_currency_exchange_rate_id", LongType(),               False),
        StructField("from_currency_cd",                          StringType(),              True),
        StructField("exchange_rate_cd",                          StringType(),              True),
        StructField("exchange_rate_nm",                          StringType(),              True),
        StructField("exchange_rate",                             DecimalType(38, 18),       True),
        StructField("from_currency_nm",                          StringType(),              True),
        StructField("to_currency_cd",                            StringType(),              True),
        StructField("to_currency_nm",                            StringType(),              True),
        StructField("active_ind",                                StringType(),              True),
    ])
    return spark.createDataFrame(rows, schema)


# ===========================================================================
#  RETAIL GLOBAL STORE PROFILE
# ===========================================================================

def gen_retail_global_store_profile_v(spark: SparkSession, ctx: GenerationContext) -> DataFrame:
    d       = _D()
    n       = get_dim_row_count("retail_global_store_profile_v")
    geos    = d["geographies"]["regions"]
    cntries = d["geographies"]["countries"]

    df = (
        spark.range(1, n + 1)
        .withColumnRenamed("id", "_store_row_id")
        .withColumn("store_uuid",   F.concat(F.lit("STR-"), F.lpad(F.col("_store_row_id").cast(StringType()), 8, "0")))
        .withColumn("pos_id",       F.col("_store_row_id").cast(IntegerType()))
        .withColumn("store_id_kona",F.col("_store_row_id").cast(IntegerType()))
        .withColumn("brand_cd",
            F.array(*[F.lit(b) for b in ["NK", "JD", "CV"]])[F.col("_store_row_id") % 3])
        .withColumn("brand_desc",
            F.array(*[F.lit(b) for b in ["Nike", "Jordan", "Converse"]])[F.col("_store_row_id") % 3])
        .withColumn("country_cd",
            F.array(*[F.lit(c) for c in cntries])[F.col("_store_row_id") % len(cntries)])
        .withColumn("country_name",
            F.array(*[F.lit(c) for c in cntries])[F.col("_store_row_id") % len(cntries)])
        .withColumn("geography_region_name",
            F.array(*[F.lit(g) for g in geos])[F.col("_store_row_id") % len(geos)])
        .withColumn("store_name_english",
            F.concat(F.col("brand_desc"), F.lit(" Store "), F.col("_store_row_id").cast(StringType())))
        .withColumn("store_cd",
            F.concat(F.col("brand_cd"), F.lpad(F.col("_store_row_id").cast(StringType()), 6, "0")))
        .withColumn("store_status_cd",
            F.array(*[F.lit(s) for s in ["OPEN", "CLOSED", "TEMP_CLOSED"]])[F.col("_store_row_id") % 10 > 1].cast(StringType()))
        .withColumn("store_status_cd",
            F.when(F.col("_store_row_id") % 15 == 0, F.lit("CLOSED"))
            .when(F.col("_store_row_id") % 20 == 0, F.lit("TEMP_CLOSED"))
            .otherwise(F.lit("OPEN")))
        .withColumn("store_status_desc",
            F.when(F.col("store_status_cd") == "OPEN", F.lit("Open"))
            .when(F.col("store_status_cd") == "CLOSED", F.lit("Permanently Closed"))
            .otherwise(F.lit("Temporarily Closed")))
        .withColumn("retail_concept_cd",
            F.array(*[F.lit(c) for c in ["NSO", "NFS", "NCS", "EMPLOYEE"]])[F.col("_store_row_id") % 4])
        .withColumn("retail_concept_desc",
            F.array(*[F.lit(c) for c in ["Nike Store", "Nike Factory Store", "Nike Community Store", "Employee Store"]])[F.col("_store_row_id") % 4])
        .withColumn("store_open_dt",          F.lit("2015-01-01"))
        .withColumn("store_level",             (F.col("_store_row_id") % 5 + 1).cast(IntegerType()))
        .withColumn("store_tier_id",           (F.col("_store_row_id") % 4 + 1).cast(IntegerType()))
        .withColumn("store_tier_desc",         F.concat(F.lit("Tier "), F.col("store_tier_id").cast(StringType())))
        .withColumn("iso_country_cd",          F.col("country_cd"))
        .withColumn("default_transaction_iso_currency_cd",
            F.array(*[F.lit(c) for c in ["USD", "EUR", "GBP", "JPY", "CNY"]])[F.col("_store_row_id") % 5])
        .withColumn("territory_name",          F.col("geography_region_name"))
        .withColumn("city_name",
            F.array(*[F.lit(c) for c in ["New York", "London", "Tokyo", "Shanghai", "Paris"]])[F.col("_store_row_id") % 5])
        .withColumn("comparable_status",
            F.when(F.col("_store_row_id") % 3 == 0, F.lit("Non-Comparable")).otherwise(F.lit("Comparable")))
        .withColumn("nike_selling_space_size_quantity",
            (F.rand() * F.lit(10000) + F.lit(500)).cast(FloatType()))
        .withColumn("store_selling_space_size_quantity",
            F.col("nike_selling_space_size_quantity"))
        .withColumn("store_total_space_size",
            (F.col("nike_selling_space_size_quantity") * F.lit(1.3)).cast(FloatType()))
        .withColumn("space_uom",    F.lit("SQF"))
        .withColumn("space_uom_cd", F.lit("SQF"))
        .withColumn("region_cd",    F.col("geography_region_name"))
        # Null-fill remaining string columns
        *[F.lit(None).cast(StringType()).alias(c) for c in [
            "address_match_type_name", "application_reason_desc",
            "china_store_sub_channel_cd", "china_store_sub_channel_desc",
            "connect_global_store_key_cd", "customer_city_local_name",
            "customer_id", "customer_local_province_state_name",
            "customer_nbr", "customer_ship_to_name",
            "dma_cd", "dma_desc", "fixture_type_name", "fixture_program_name",
            "greater_china_sub_territory_name", "id",
            "global_key_city_name", "landlord",
        ]]
        .withColumn("change_dt",          F.lit(None).cast(StringType()))
        .withColumn("change_timestamp",   F.lit(datetime.date.today()))
        .withColumn("create_dt",          F.lit("2015-01-01"))
        .withColumn("stream_change_timestamp", F.lit(None).cast(StringType()))
        .drop("_store_row_id")
    )
    return df


# ===========================================================================
#  REGISTER ALL DIMENSION GENERATORS
# ===========================================================================

register("accounting_document_type",             gen_accounting_document_type)
register("atscale_geo_security",                 gen_atscale_geo_security)
register("version_forecast_mapping",             gen_version_forecast_mapping)
register("functional_area",                      gen_functional_area)
register("division_text",                        gen_division_text)
register("division_text_dim_v",                  gen_division_text_dim_v)
register("finance_foreign_currency_exchange_rate", gen_finance_foreign_currency_exchange_rate)
register("calendar_fiscal_period_v",             gen_calendar_fiscal_period_v)
register("company_code",                         gen_company_code)
register("finance_product_dim_v",                gen_finance_product_dim_v)
register("finance_customer_dim_v",               gen_finance_customer_dim_v)
register("copa_attribution_dim",                 gen_copa_attribution_dim)
register("cost_center_dim_v",                    gen_cost_center_dim_v)
register("profit_center",                        gen_profit_center)
register("gl_account_dim",                       gen_gl_account_dim)
register("gl_account_zfsm_measures_hierarchy_dim", gen_gl_account_zfsm_measures_hierarchy_dim)
register("gl_account_hierarchy",                 gen_gl_account_hierarchy,
         zorder_cols=["gl_account_nbr"])
register("management_gl_account_hierarchy",      gen_management_gl_account_hierarchy,
         zorder_cols=["gl_account_nbr"])
register("geo_marketplace_channel_dim",          gen_geo_marketplace_channel_dim)
register("geo_wholesale_value_business_dim",     gen_geo_wholesale_value_business_dim)
register("consolidation_functional_area_hierarchy", gen_consolidation_functional_area_hierarchy)
register("consolidation_segment_hierarchy_dim",  gen_consolidation_segment_hierarchy_dim)
register("segment_cost_center_hierarchy_dim_v",  gen_segment_cost_center_hierarchy_dim_v)
register("segment_profit_center_hierarchy",      gen_segment_profit_center_hierarchy)
register("DisChannel_cost_center_hierarchy_dim_v", gen_DisChannel_cost_center_hierarchy_dim_v)
register("DisChannel_profit_center_hierarchy",   gen_DisChannel_profit_center_hierarchy)
register("PartDisChannel_profit_center_hierarchy", gen_PartDisChannel_profit_center_hierarchy)
register("retail_global_store_profile_v",        gen_retail_global_store_profile_v)
