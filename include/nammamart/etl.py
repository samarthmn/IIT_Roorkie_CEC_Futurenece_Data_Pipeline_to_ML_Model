"""
include/nammamart/etl.py
------------------------
ETL transforms (Part A): dimensions, fact and payment reconciliation, all in pandas.

Dimensions keep one row per key, including master rows that failed their own checks
(flagged with dq_status / failure_reason and also written to quarantine). Decision D7: an order
whose key exists in the source master is kept, so its dimension row must exist for the join.
"""

import numpy as np
import pandas as pd

from include.nammamart import config
from include.nammamart.masking import mask_email, mask_phone
from include.nammamart.quality import (TOLERANCE, std_loyalty_tier, std_order_status, std_payment_mode,
                                       to_num, to_ts)

DQ_COLUMNS = ["dq_status", "failure_reason", "warning_reason"]


def _text(s: pd.Series) -> pd.Series:
    s = s.str.strip()
    return s.where(s.ne(""), None)


def _blank_failed(values: pd.Series, failure_reason: pd.Series, codes) -> pd.Series:
    """Decision D7: a master row that failed stays in its dimension (its orders must still join),
    but the value that failed is not published: it becomes NULL. Raw and quarantine keep the original."""
    pattern = r"(?:^|;)(?:" + "|".join(codes) + r")(?:;|$)"
    failed = failure_reason.fillna("").str.contains(pattern)
    return values.mask(failed)


def build_dim_store(stores: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "store_id": stores["store_id"].str.strip(),
        "store_name": _text(stores["store_name"]),
        "zone": _text(stores["zone"]).fillna("Unknown"),
        "area": _text(stores["area"]),
        "pincode": _text(stores["pincode"]),
        "open_date": to_ts(stores["open_date"], config.DATE_FORMAT).dt.date,
        "capacity_orders_per_day": to_num(stores["capacity_orders_per_day"]).astype("Int64"),
        **{c: stores[c] for c in DQ_COLUMNS},
    })


def build_dim_product(products: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "product_id": products["product_id"].str.strip(),
        "product_name": _text(products["product_name"]),
        "category": _text(products["category"]).fillna("Unknown"),
        "sub_category": _text(products["sub_category"]),
        "brand": _text(products["brand"]),
        "mrp": _blank_failed(to_num(products["mrp"]), products["failure_reason"], ["nonpositive_mrp"]),
        "cost_price": _blank_failed(to_num(products["cost_price"]), products["failure_reason"],
                                    config.UNTRUSTED_COST_REASONS),
        "unit": _text(products["unit"]),
        "is_perishable": products["is_perishable"].str.strip().str.lower().eq("true"),
        "launch_date": to_ts(products["launch_date"], config.DATE_FORMAT).dt.date,
        **{c: products[c] for c in DQ_COLUMNS},
    })


def build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    """Decision D13: no full_name, no clear email or phone."""
    return pd.DataFrame({
        "customer_id": customers["customer_id"].str.strip(),
        "email_masked": customers["email"].map(mask_email),
        "phone_masked": customers["phone"].map(mask_phone),
        "city": _text(customers["city"]),
        "area": _text(customers["area"]),
        "loyalty_tier": std_loyalty_tier(customers["loyalty_tier"]),
        "signup_date": _blank_failed(to_ts(customers["signup_date"], config.DATE_FORMAT), customers["failure_reason"],
                                     ["future_signup_date"]).dt.date,
        "is_active": customers["is_active"].str.strip().str.upper().eq("Y"),
        **{c: customers[c] for c in DQ_COLUMNS},
    })


def build_fact(orders: pd.DataFrame, dim_product: pd.DataFrame, run: dict) -> pd.DataFrame:
    """orders = merged canonical orders with no Critical failure. Decisions D1–D3.
    gross_margin is NULL for a DELIVERED order whose product failed a cost check
    (config.UNTRUSTED_COST_REASONS): the revenue is real, the cost is not."""
    f = pd.DataFrame({
        "order_id": orders["order_id"].str.strip(),
        "order_ts": to_ts(orders["order_ts"]),
        "customer_id": orders["customer_id"].str.strip(),
        "store_id": orders["store_id"].str.strip(),
        "product_id": orders["product_id"].str.strip(),
        "quantity": to_num(orders["quantity"]).astype("int64"),
        "unit_price": to_num(orders["unit_price"]),
        "discount_pct": to_num(orders["discount_pct"]),
        "order_amount": to_num(orders["order_amount"]).round(2),
        "payment_mode": std_payment_mode(orders["payment_mode"]),
        "order_status": std_order_status(orders["order_status"]),
        "channel": _text(orders["channel"]),
        "delivery_minutes": to_num(orders["delivery_minutes"]).astype("Int64"),
        "source_file": orders["_source_file"],
        "source_row": orders["_source_row"].astype("int64"),
        "run_id": run["run_id"],
    })
    f.insert(2, "order_date", f["order_ts"].dt.date)       # IST calendar date (decision D8)

    # Left join + assertion: an inner join would silently drop orders.
    products = dim_product.set_index("product_id")
    if not f["product_id"].isin(products.index).all():
        raise ValueError(f"Orders without a product row: {f.loc[~f['product_id'].isin(products.index), 'order_id'].tolist()[:10]}")
    cost = f["product_id"].map(products["cost_price"])
    reasons = f["product_id"].map(products["failure_reason"]).fillna("")
    cost_trusted = ~reasons.str.contains("|".join(config.UNTRUSTED_COST_REASONS)) & cost.notna()

    delivered = f["order_status"].eq("DELIVERED")
    f["product_dq_status"] = f["product_id"].map(products["dq_status"])
    f["net_revenue"] = np.where(delivered, f["order_amount"], 0.0).round(2)
    margin = (f["order_amount"] - f["quantity"] * cost).round(2)
    f["gross_margin"] = pd.Series(np.where(delivered, margin, 0.0), index=f.index).astype("Float64")
    f.loc[delivered & ~cost_trusted, "gross_margin"] = pd.NA
    f["within_15_min"] = delivered & f["delivery_minutes"].le(config.SLA_MINUTES).fillna(False).astype(bool)
    return f


def reconcile(fact: pd.DataFrame, orders_checked: pd.DataFrame, payments_checked: pd.DataFrame,
              payment_extras: pd.DataFrame, source_order_ids: set) -> pd.DataFrame:
    """One row per order/payment mismatch, one primary reason each (decision D14).
    difference = SUCCESS amount paid − amount owed (order_amount if DELIVERED, else 0)."""
    def prep(p):
        p = p.assign(order_id=p["order_id"].str.strip(),
                     status=p["payment_status"].str.strip().str.upper(),
                     paid=to_num(p["amount_paid"]),
                     ts=to_ts(p["payment_ts"]))
        return p.assign(success_paid=np.where(p["status"].eq("SUCCESS"), p["paid"].fillna(0), 0.0))

    pay, dups = prep(payments_checked), prep(payment_extras)
    orders = orders_checked.assign(order_id=orders_checked["order_id"].str.strip(),
                                   std_status=std_order_status(orders_checked["order_status"]),
                                   amount=to_num(orders_checked["order_amount"]).round(2)
                                   ).set_index("order_id")
    fact_ids = set(fact["order_id"])

    def payment_rows(p, reason):
        return pd.DataFrame({
            "order_id": p["order_id"], "payment_id": p["payment_id"].str.strip(),
            "order_status": p["order_id"].map(orders["std_status"]),
            "order_amount": p["order_id"].map(orders["amount"]),
            "amount_paid": p["paid"], "difference": p["success_paid"], "reason": reason,
        })

    in_source = pay["order_id"].isin(source_order_ids)
    parts = [
        payment_rows(pay[~in_source], "ORDER_NOT_FOUND"),
        payment_rows(pay[in_source & ~pay["order_id"].isin(fact_ids)], "ORDER_REJECTED"),
        payment_rows(dups, "DUPLICATE_PAYMENT_ID"),
    ]

    # Order-level reasons for every order in the fact table.
    pf = pay[pay["order_id"].isin(fact_ids)]
    agg = pf.groupby("order_id").agg(
        payment_id=("payment_id", lambda s: ";".join(sorted(s.str.strip()))),
        n_payments=("payment_id", "size"),
        paid=("success_paid", "sum"),
        has_success=("status", lambda s: s.eq("SUCCESS").any()),
        has_refund=("status", lambda s: s.eq("REFUNDED").any()),
    )
    feed_end = pf["ts"].max().date()      # last day covered by the payment feed for valid orders
    f = fact.merge(agg, on="order_id", how="left")
    f["n_payments"] = f["n_payments"].fillna(0).astype(int)
    f["paid"] = f["paid"].fillna(0.0)
    f["has_success"] = f["has_success"].astype("boolean").fillna(False).astype(bool)
    f["has_refund"] = f["has_refund"].astype("boolean").fillna(False).astype(bool)

    delivered, returned, cancelled = (f["order_status"].eq(s) for s in ["DELIVERED", "RETURNED", "CANCELLED"])
    no_payment = f["n_payments"].eq(0)
    f["reason"] = np.select(
        [delivered & no_payment & (pd.to_datetime(f["order_date"]).dt.date > feed_end),
         delivered & no_payment,
         delivered & f["has_success"] & ((f["paid"] - f["order_amount"]).abs() > TOLERANCE),
         delivered & ~no_payment & ~f["has_success"],
         returned & f["has_success"] & ~f["has_refund"],
         cancelled & f["has_success"]],
        ["PAYMENT_NOT_YET_RECEIVED", "MISSING_PAYMENT", "AMOUNT_MISMATCH", "PAYMENT_FAILED",
         "RETURNED_NOT_REFUNDED", "CANCELLED_WITH_PAYMENT"],
        default="",
    )
    owed = np.where(delivered, f["order_amount"], 0.0)
    mismatched = f[f["reason"].ne("")]
    parts.append(pd.DataFrame({
        "order_id": mismatched["order_id"], "payment_id": mismatched["payment_id"],
        "order_status": mismatched["order_status"], "order_amount": mismatched["order_amount"],
        "amount_paid": mismatched["paid"],
        "difference": (mismatched["paid"] - owed[f["reason"].ne("").to_numpy()]),
        "reason": mismatched["reason"],
    }))

    out = pd.concat(parts, ignore_index=True)
    out["difference"] = out["difference"].round(2)
    return out.sort_values(["reason", "order_id", "payment_id"], na_position="last").reset_index(drop=True)
