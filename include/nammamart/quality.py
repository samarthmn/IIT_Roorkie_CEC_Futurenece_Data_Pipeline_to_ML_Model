"""
include/nammamart/quality.py
----------------------------
pandas data-quality rules for the ETL (Part D). The ELT applies the same rules in SQL
(include/sql/2*_stg_*.sql) with the same check names and reason codes.

Severity:
  Critical -> row is quarantined (failure_reason)
  Warning  -> row loads, reason kept in warning_reason and counted in dq_results

Each validate_* function returns:
  checked      canonical rows (one per key) with dq_status, failure_reason, warning_reason
  quarantined  rows to set aside: canonical rows that failed + extra duplicate copies
  results      one dict per check, ready for dq_results
"""

import numpy as np
import pandas as pd

from include.nammamart import config
from include.nammamart.sources import increment_date

CRITICAL, WARNING = "Critical", "Warning"
TOLERANCE = config.MONEY_TOLERANCE + 1e-9      # float-safe "more than one paisa"


# ── Parsing and standardisation (decisions D5, D8) ────────────────────────────

def is_blank(s: pd.Series) -> pd.Series:
    return s.str.strip().eq("")


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.str.strip(), errors="coerce")


def to_ts(s: pd.Series, fmt: str = config.TS_FORMAT) -> pd.Series:
    return pd.to_datetime(s.str.strip(), format=fmt, errors="coerce")


def is_int(n: pd.Series) -> pd.Series:
    return n.notna() & n.eq(n.round())


def std_order_status(s: pd.Series) -> pd.Series:
    return s.str.strip().str.upper()


def std_payment_mode(s: pd.Series) -> pd.Series:
    """Alias map on trimmed, lower-cased value; unknown values (e.g. 'Cashh') become NaN."""
    return s.str.strip().str.lower().map(config.PAYMENT_MODE_ALIASES)


def std_loyalty_tier(s: pd.Series) -> pd.Series:
    return s.str.strip().str.capitalize()


def matches(s: pd.Series, regex: str) -> pd.Series:
    return s.str.strip().str.match(regex)


# ── Rule engine ───────────────────────────────────────────────────────────────

class Checks:
    def __init__(self, df: pd.DataFrame):
        self.index = df.index
        self.results: list[dict] = []
        self._fail: list[pd.Series] = []
        self._warn: list[pd.Series] = []

    def add(self, name, dimension, severity, reasons: dict, rows_checked=None):
        """reasons: {reason_code: boolean mask}. A row fails the check if any mask is True."""
        failed = pd.Series(False, index=self.index)
        for code, mask in reasons.items():
            mask = mask.reindex(self.index).fillna(False).astype(bool)
            failed |= mask
            target = self._fail if severity == CRITICAL else self._warn
            target.append(pd.Series(np.where(mask, code, None), index=self.index, dtype=object))
        self.record(name, dimension, severity, int(failed.sum()),
                    len(self.index) if rows_checked is None else rows_checked)

    def record(self, name, dimension, severity, rows_failed, rows_checked):
        status = "PASS" if rows_failed == 0 else ("FAIL" if severity == CRITICAL else "WARN")
        self.results.append({
            "check_name": name, "dimension": dimension, "severity": severity,
            "rows_checked": int(rows_checked), "rows_failed": int(rows_failed), "status": status,
        })

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out["failure_reason"] = _join(self._fail, self.index)
        out["warning_reason"] = _join(self._warn, self.index)
        out["dq_status"] = np.where(out["failure_reason"].notna(), "FAIL",
                                    np.where(out["warning_reason"].notna(), "WARN", "PASS"))
        return out


def _join(parts: list[pd.Series], index) -> pd.Series:
    if not parts:
        return pd.Series(None, index=index, dtype=object)
    joined = pd.concat(parts, axis=1).apply(lambda r: ";".join(v for v in r if isinstance(v, str)), axis=1)
    return joined.where(joined.ne(""), None)


def split_duplicates(df: pd.DataFrame, key: str, prefer: pd.Series | None = None):
    """Decision D4: keep one canonical row per key (preferred rows first, then file order);
    the extra copies are returned separately with failure_reason duplicate_<key>."""
    rank = df.assign(_prefer=prefer.astype(int) if prefer is not None else 0)
    rank = rank.sort_values(["_prefer", "_source_file", "_source_row"], ascending=[False, True, True])
    keep = rank.drop_duplicates(key).index
    canonical = df.loc[keep].sort_values(["_source_file", "_source_row"])
    extras = df.drop(keep).assign(failure_reason=f"duplicate_{key}", warning_reason=None, dq_status="FAIL")
    return canonical, extras


def _finish(canonical, extras, checks):
    checked = checks.apply(canonical)
    quarantined = pd.concat([checked[checked["dq_status"] == "FAIL"], extras], ignore_index=True)
    return checked, quarantined, checks.results


# ── Master files ──────────────────────────────────────────────────────────────

def validate_stores(raw: pd.DataFrame):
    canon, extras = split_duplicates(raw, "store_id")
    c = Checks(canon)
    c.record("stores_store_id_unique", "Uniqueness", CRITICAL, len(extras), len(raw))
    c.add("stores_zone_present", "Completeness", WARNING, {"missing_zone": is_blank(canon["zone"])})
    c.add("stores_pincode_6_digits", "Validity", WARNING,
          {"invalid_pincode": ~matches(canon["pincode"], config.PINCODE_REGEX)})
    return _finish(canon, extras, c)


def validate_products(raw: pd.DataFrame):
    canon, extras = split_duplicates(raw, "product_id")
    mrp, cost = to_num(canon["mrp"]), to_num(canon["cost_price"])
    c = Checks(canon)
    c.record("products_product_id_unique", "Uniqueness", CRITICAL, len(extras), len(raw))
    c.add("products_category_present", "Completeness", CRITICAL, {"missing_category": is_blank(canon["category"])})
    c.add("products_mrp_positive", "Accuracy", CRITICAL, {"nonpositive_mrp": mrp.isna() | (mrp <= 0)})
    c.add("products_cost_positive", "Accuracy", CRITICAL, {"nonpositive_cost_price": cost.isna() | (cost <= 0)})
    c.add("products_cost_le_mrp", "Accuracy", CRITICAL, {"cost_above_mrp": cost > mrp})
    return _finish(canon, extras, c)


def validate_customers(raw: pd.DataFrame, run: dict):
    email_ok = matches(raw["email"].str.lower(), config.EMAIL_REGEX)
    phone_ok = matches(raw["phone"], config.PHONE_REGEX)
    canon, extras = split_duplicates(raw, "customer_id", prefer=email_ok & phone_ok)
    signup = to_ts(canon["signup_date"], config.DATE_FORMAT)
    tier = canon["loyalty_tier"]
    c = Checks(canon)
    c.record("customers_customer_id_unique", "Uniqueness", CRITICAL, len(extras), len(raw))
    c.add("customers_email_format", "Validity", WARNING,
          {"invalid_email": ~matches(canon["email"].str.lower(), config.EMAIL_REGEX)})
    c.add("customers_phone_10_digits", "Validity", WARNING,
          {"invalid_phone": ~matches(canon["phone"], config.PHONE_REGEX)})
    c.add("customers_pincode_6_digits", "Validity", WARNING,
          {"invalid_pincode": ~matches(canon["pincode"], config.PINCODE_REGEX)})
    c.add("customers_loyalty_tier_casing", "Consistency", WARNING,
          {"loyalty_tier_standardised": std_loyalty_tier(tier).ne(tier)})
    c.add("customers_signup_not_future", "Timeliness", CRITICAL,
          {"future_signup_date": signup > pd.Timestamp(run["run_ts"])})
    return _finish(canon, extras, c)


# ── Orders (base file + increments, decision D11) ─────────────────────────────

def merge_orders(base: pd.DataFrame, increments: list[pd.DataFrame]):
    """Dedupe the base file, then upsert each increment by order_id in file-date order.
    Returns (merged canonical rows, duplicate extras, per-increment merge stats)."""
    start, end = config.EXPECTED_PERIOD["orders.csv"]
    merged, extras = split_duplicates(base, "order_id")
    merged = merged.assign(_period_start=start, _period_end=end)
    all_extras, stats = [extras], []
    for inc in increments:
        file_name = inc["_source_file"].iloc[0] if len(inc) else "increment"
        canon_inc, inc_extras = split_duplicates(inc, "order_id")
        canon_inc = canon_inc.assign(_period_start=None,
                                     _period_end=f"{increment_date(file_name)} 23:59:59")
        existing = canon_inc["order_id"].isin(merged["order_id"])
        stats.append({"file": file_name, "rows": len(inc), "inserted": int((~existing).sum()),
                      "updated": int(existing.sum()), "duplicates": len(inc_extras)})
        merged = pd.concat([merged[~merged["order_id"].isin(canon_inc["order_id"])], canon_inc],
                           ignore_index=True)
        all_extras.append(inc_extras)
    return merged.reset_index(drop=True), pd.concat(all_extras, ignore_index=True), stats


def validate_orders(base, increments, master_keys: dict, run: dict):
    raw_rows = len(base) + sum(len(i) for i in increments)
    o, extras, merge_stats = merge_orders(base, increments)

    status, mode = std_order_status(o["order_status"]), std_payment_mode(o["payment_mode"])
    ts = to_ts(o["order_ts"])
    q, price, disc, amount = (to_num(o[col]) for col in ["quantity", "unit_price", "discount_pct", "order_amount"])
    minutes, minutes_blank = to_num(o["delivery_minutes"]), is_blank(o["delivery_minutes"])
    expected = (q * price * (1 - disc / 100)).round(2)
    start, end = pd.to_datetime(o["_period_start"]), pd.to_datetime(o["_period_end"])

    c = Checks(o)
    c.add("orders_required_fields", "Completeness", CRITICAL,
          {f"missing_{col}": is_blank(o[col])
           for col in ["order_id", "customer_id", "store_id", "product_id", "order_ts"]})
    c.add("orders_delivered_has_minutes", "Completeness", CRITICAL,
          {"delivered_missing_delivery_minutes": status.eq("DELIVERED") & minutes_blank})
    c.record("orders_order_id_unique", "Uniqueness", CRITICAL, len(extras), raw_rows)
    c.add("orders_status_valid", "Validity", CRITICAL,
          {"invalid_order_status": ~status.isin(config.ALLOWED_ORDER_STATUS)})
    c.add("orders_status_standardised", "Validity", WARNING,
          {"order_status_standardised": status.isin(config.ALLOWED_ORDER_STATUS) & status.ne(o["order_status"])})
    c.add("orders_payment_mode_valid", "Validity", CRITICAL, {"invalid_payment_mode": mode.isna()})
    c.add("orders_payment_mode_standardised", "Validity", WARNING,
          {"payment_mode_standardised": mode.notna() & mode.ne(o["payment_mode"])})
    c.add("orders_ts_parses", "Validity", CRITICAL, {"invalid_order_ts": ~is_blank(o["order_ts"]) & ts.isna()})
    c.add("orders_in_expected_period", "Validity", CRITICAL,
          {"outside_expected_period": ts.notna() & ((start.notna() & (ts < start)) | (ts > end))})
    c.add("orders_discount_range", "Validity", CRITICAL,
          {"invalid_discount_pct": disc.isna() | (disc < 0) | (disc > 100)})
    c.add("orders_quantity_positive", "Accuracy", CRITICAL,
          {"invalid_quantity": ~is_int(q), "nonpositive_quantity": q <= 0})
    c.add("orders_unit_price_positive", "Accuracy", CRITICAL,
          {"nonpositive_unit_price": price.isna() | (price <= 0)})
    c.add("orders_amount_formula", "Accuracy", CRITICAL,
          {"order_amount_mismatch": amount.isna() | expected.isna() | ((expected - amount).abs() > TOLERANCE)})
    c.add("orders_delivery_max", "Accuracy", CRITICAL,
          {"invalid_delivery_minutes": ~minutes_blank & ~is_int(minutes),
           "negative_delivery_minutes": minutes < 0,
           "delivery_minutes_over_120": minutes > config.DELIVERY_MAX_MINUTES})
    lo, hi = config.DELIVERY_NORMAL_RANGE
    c.add("orders_delivery_normal_range", "Accuracy", WARNING,
          {"delivery_minutes_outside_8_35":
               (minutes >= 0) & (minutes <= config.DELIVERY_MAX_MINUTES) & ((minutes < lo) | (minutes > hi))})
    for col, master in [("customer_id", "customers"), ("product_id", "products"), ("store_id", "stores")]:
        key = o[col].str.strip()
        c.add(f"orders_fk_{col.removesuffix('_id')}", "Consistency", CRITICAL,
              {f"unknown_{col}": key.ne("") & ~key.isin(master_keys[master])})
    c.add("orders_not_future", "Timeliness", CRITICAL, {"future_order_ts": ts > pd.Timestamp(run["run_ts"])})

    checked = c.apply(o)

    # Freshness (decision D10): dataset-level Warning on the latest valid order_ts.
    latest = ts[checked["dq_status"] != "FAIL"].max()
    lag_hours = None if pd.isna(latest) else round((pd.Timestamp(run["as_of"]) - latest).total_seconds() / 3600, 1)
    stale = lag_hours is None or lag_hours > config.FRESHNESS_MAX_LAG_HOURS
    c.record("orders_freshness", "Timeliness", WARNING, int(stale), 1)
    freshness = {"latest_order_ts": None if pd.isna(latest) else str(latest), "as_of": run["as_of"],
                 "lag_hours": lag_hours, "stale": stale}

    quarantined = pd.concat([checked[checked["dq_status"] == "FAIL"], extras], ignore_index=True)
    return checked, quarantined, c.results, merge_stats, freshness


# ── Payments ──────────────────────────────────────────────────────────────────

def validate_payments(raw: pd.DataFrame, source_order_ids: set, orders_checked: pd.DataFrame, run: dict):
    canon, extras = split_duplicates(raw, "payment_id")
    status, mode = canon["payment_status"].str.strip().str.upper(), std_payment_mode(canon["payment_mode"])
    ts, paid = to_ts(canon["payment_ts"]), to_num(canon["amount_paid"])
    order_amount = canon["order_id"].str.strip().map(
        orders_checked.assign(a=to_num(orders_checked["order_amount"])).set_index("order_id")["a"])

    c = Checks(canon)
    c.record("payments_payment_id_unique", "Uniqueness", CRITICAL, len(extras), len(raw))
    c.add("payments_payment_mode_valid", "Validity", CRITICAL, {"invalid_payment_mode": mode.isna()})
    c.add("payments_payment_mode_standardised", "Validity", WARNING,
          {"payment_mode_standardised": mode.notna() & mode.ne(canon["payment_mode"])})
    c.add("payments_ts_parses", "Validity", CRITICAL, {"invalid_payment_ts": ts.isna()})
    c.add("payments_amount_positive", "Accuracy", CRITICAL,
          {"nonpositive_amount_paid": status.eq("SUCCESS") & (paid.isna() | (paid <= 0))})
    c.add("payments_fk_order", "Consistency", CRITICAL,
          {"unknown_order_id": ~canon["order_id"].str.strip().isin(source_order_ids)})
    c.add("payments_amount_matches_order", "Consistency", WARNING,
          {"amount_differs_from_order":
               status.eq("SUCCESS") & order_amount.notna() & ((paid - order_amount).abs() > TOLERANCE)})
    c.add("payments_not_future", "Timeliness", CRITICAL, {"future_payment_ts": ts > pd.Timestamp(run["run_ts"])})
    return _finish(canon, extras, c)


def source_files_check(files_checked: int, files_failed: int) -> dict:
    """Recorded on every run by audit_run, even when the run fails before validation:
    did every source file arrive with its expected columns? rows_* count files, not rows."""
    return {"check_name": "source_files_schema", "dimension": "Validity", "severity": CRITICAL,
            "rows_checked": files_checked, "rows_failed": files_failed, "status": "FAIL" if files_failed else "PASS"}


def volume_check(current_rows: int, previous_rows) -> dict:
    """Experiment 7: Warning when orders drop by more than 90% vs the previous run (none on the first run)."""
    dropped = previous_rows is not None and previous_rows > 0 and \
        current_rows < previous_rows * (1 - config.VOLUME_DROP_WARN_PCT / 100)
    return {"check_name": "orders_volume_vs_previous", "dimension": "Completeness", "severity": WARNING,
            "rows_checked": 1, "rows_failed": int(dropped), "status": "WARN" if dropped else "PASS"}
