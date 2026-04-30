"""
utils/data_helpers.py
=====================
Pure-Python and PySpark helper functions for generating realistic,
domain-consistent field values.  These are called by the generator modules
and must not depend on the generated tables (no circular imports).
"""
from __future__ import annotations

import datetime
import math
import random
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType

from utils.schema_loader import load_distributions


# ---------------------------------------------------------------------------
# Lazy distribution config access
# ---------------------------------------------------------------------------

_DIST: Optional[Dict[str, Any]] = None

def _d() -> Dict[str, Any]:
    global _DIST
    if _DIST is None:
        _DIST = load_distributions()
    return _DIST


# ---------------------------------------------------------------------------
# Fiscal calendar helpers
# ---------------------------------------------------------------------------

def build_fiscal_periods(fiscal_years: List[int], periods_per_year: int = 13) -> List[Dict]:
    """
    Build a list of fiscal period dicts.

    Nike's fiscal year runs June – May.
    Period 1  = June
    Period 13 = May (or a supplemental period)
    fiscal_year_period_nbr = YYYY * 100 + PP  e.g. 202401 = FY2024 Period 1
    """
    dist = _d()
    fy_start_month = dist["fiscal"].get("fiscal_year_start_month", 6)
    seasons = ["Spring/Summer", "Fall/Winter"]

    records = []
    for fy in fiscal_years:
        for period in range(1, periods_per_year + 1):
            # Map period to calendar month
            cal_month = ((fy_start_month - 1 + (period - 1)) % 12) + 1
            cal_year  = fy if (fy_start_month + period - 2) < 12 else fy + 1

            month_start = datetime.date(cal_year, cal_month, 1)
            # last day of cal_month
            if cal_month == 12:
                month_end = datetime.date(cal_year, 12, 31)
            else:
                month_end = datetime.date(cal_year, cal_month + 1, 1) - datetime.timedelta(days=1)

            fy_period_nbr = fy * 100 + period
            quarter_nbr   = math.ceil(period / (periods_per_year / 4))
            season_cd     = "SS" if period <= periods_per_year // 2 else "FW"

            records.append({
                "fiscal_year_period_nbr":          fy_period_nbr,
                "month_long_nm":                   month_start.strftime("%B"),
                "month_short_nm":                  month_start.strftime("%b"),
                "month_nbr":                       cal_month,
                "year_mth":                        int(f"{cal_year}{cal_month:02d}"),
                "month_relevance_dt":              month_start,
                "month_start_dt":                  month_start,
                "month_end_dt":                    month_end,
                "month_sort_sequence_nbr":         (fy - fiscal_years[0]) * periods_per_year + period,
                "fiscal_period_nbr":               period,
                "fiscal_period_cd":                f"P{period:02d}",
                "fiscal_period_sort_sequence_nbr": period,
                "fiscal_year_period_cd":           f"FY{fy}P{period:02d}",
                "fiscal_year_period_nm":           f"FY{fy} Period {period:02d}",
                "season_period_cd":                season_cd,
                "season_alternate_period_cd":      season_cd + str(fy),
                "season_nm":                       seasons[0] if season_cd == "SS" else seasons[1],
                "season_relevance_dt":             month_start,
                "season_start_dt":                 month_start,
                "season_end_dt":                   month_end,
                "season_sort_sequence_nbr":        (fy - fiscal_years[0]) * 2 + (1 if season_cd == "SS" else 2),
                "quarter_calendar_nbr":            (cal_month - 1) // 3 + 1,
                "quarter_calendar_sequence_nbr":   (cal_year - fiscal_years[0]) * 4 + quarter_nbr,
                "quarter_business_nbr":            quarter_nbr,
                "fiscal_quarter_nbr":              quarter_nbr,
                "fiscal_quarter_cd":               f"Q{quarter_nbr}",
                "fiscal_quarter_sort_sequence_nbr": quarter_nbr,
                "fiscal_year_quarter_nbr":         fy * 10 + quarter_nbr,
                "fiscal_year_quarter_cd":          f"FY{fy}Q{quarter_nbr}",
                "fiscal_year_quarter_alternate_cd": f"{fy}Q{quarter_nbr}",
                "year_cd":                         str(fy),
                "year_nm":                         f"FY{fy}",
                "year_nbr":                        str(fy),
                "year_start_dt":                   datetime.date(cal_year, fy_start_month, 1),
                "year_end_dt":                     datetime.date(cal_year + 1, fy_start_month, 1) - datetime.timedelta(days=1),
                "business_year_nbr":               fy,
                "fiscal_year_nbr":                 fy,
                "fiscal_year_cd":                  f"FY{fy}",
                "fiscal_period_sort":              (fy - fiscal_years[0]) * periods_per_year + period,
            })
    return records


# ---------------------------------------------------------------------------
# GL Account helpers
# ---------------------------------------------------------------------------

def gl_account_number(account_id: int, n_accounts: int) -> str:
    """
    Map sequential IDs to realistic GL account numbers distributed
    across account categories defined in distributions.yaml.
    """
    dist   = _d()
    ranges = dist["gl_accounts"]["ranges"]
    cats   = list(ranges.keys())
    weights = dist["gl_accounts"]["category_weights"]

    # Expand categories into a weighted pool
    total_weight = sum(weights)
    thresholds   = []
    cumulative   = 0
    for w in weights:
        cumulative += w
        thresholds.append(cumulative / total_weight)

    # Pick category deterministically
    ratio = (account_id % n_accounts) / max(n_accounts, 1)
    cat   = cats[-1]
    for i, t in enumerate(thresholds):
        if ratio < t:
            cat = cats[i]
            break

    rng   = ranges[cat]
    span  = rng["end"] - rng["start"]
    nbr   = rng["start"] + (account_id % max(span, 1))
    return str(nbr)


# ---------------------------------------------------------------------------
# Profit center helpers
# ---------------------------------------------------------------------------

def profit_center_number(pc_id: int) -> str:
    """Generate a SAP-style profit center number."""
    return f"{pc_id:07d}"


# ---------------------------------------------------------------------------
# Cost center helpers
# ---------------------------------------------------------------------------

def cost_center_number(cc_id: int) -> str:
    return f"CC{cc_id:06d}"


# ---------------------------------------------------------------------------
# Spark Column expressions (reusable across generators)
# ---------------------------------------------------------------------------

def rand_decimal(min_val: float, max_val: float, scale: int = 5) -> F.Column:
    """Random decimal in [min_val, max_val) with *scale* decimal places."""
    return F.round(
        F.lit(min_val) + F.rand() * F.lit(max_val - min_val),
        scale
    ).cast(DecimalType(28, scale))


def log_normal_amt(mean: float, std_dev: float, scale: int = 5) -> F.Column:
    """
    Log-normal distribution approximated in Spark using Box-Muller.
    Returns a positive decimal suitable for financial amounts.
    """
    # Box-Muller: Z = sqrt(-2 * ln(U1)) * cos(2π * U2)
    u1 = F.rand() + F.lit(1e-10)   # avoid ln(0)
    u2 = F.rand()
    z  = F.sqrt(F.lit(-2.0) * F.log(u1)) * F.cos(F.lit(2 * math.pi) * u2)
    # Convert z-score to log-normal
    ln_mean  = math.log(mean ** 2 / math.sqrt(mean ** 2 + std_dev ** 2))
    ln_sigma = math.sqrt(math.log(1 + (std_dev / mean) ** 2))
    result   = F.exp(F.lit(ln_mean) + F.lit(ln_sigma) * z)
    return F.round(result, scale).cast(DecimalType(28, scale))


def sign_adjusted(amt_col: F.Column, negative_prob: float = 0.12) -> F.Column:
    """Flip sign with *negative_prob* probability to simulate credits/adjustments."""
    return F.when(F.rand() < F.lit(negative_prob), -amt_col).otherwise(amt_col)


def random_date(start: datetime.date, end: datetime.date) -> F.Column:
    """Random date between *start* and *end* as a Spark Column."""
    days   = (end - start).days
    offset = (F.rand() * F.lit(days)).cast(IntegerType())
    return F.date_add(F.lit(start), offset)


def audit_timestamps() -> Tuple[F.Column, F.Column]:
    """Return (created_tmst, updated_tmst) column expressions."""
    start_dt = datetime.date(2018, 1, 1)
    end_dt   = datetime.date(2024, 12, 31)
    created  = random_date(start_dt, end_dt)
    updated  = F.when(F.rand() < 0.3, random_date(start_dt, end_dt)).otherwise(created)
    return created, updated


def random_user_id(index_col: str = "id") -> F.Column:
    dist  = _d()
    users = dist["audit"]["created_by_users"]
    arr   = F.array(*[F.lit(u) for u in users])
    return arr[F.col(index_col).cast("long") % F.lit(len(users))]


def random_physical_source(index_col: str = "id") -> F.Column:
    dist    = _d()
    sources = dist["audit"]["physical_sources"]
    weights = dist["audit"]["physical_source_weights"]
    pool: List[str] = []
    for s, w in zip(sources, weights):
        pool.extend([s] * w)
    arr = F.array(*[F.lit(s) for s in pool])
    return arr[F.col(index_col).cast("long") % F.lit(len(pool))]
