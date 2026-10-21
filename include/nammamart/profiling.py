"""
include/nammamart/profiling.py
------------------------------
Part 0 profiler for the NammaMart dataset. Prints a Markdown report to stdout.

Usage (inside the Airflow container):
  python /usr/local/airflow/include/nammamart/profiling.py [data_dir] [extra_file ...]

Reads every value as text (keep_default_na=False) so blanks and literal 'N/A' stay visible.
"""

import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from include.nammamart import config  # noqa: E402

CATEGORICAL = {"order_status", "payment_status", "payment_mode", "loyalty_tier", "zone", "category",
               "sub_category", "channel", "is_active", "is_perishable", "unit", "city"}
MAX_LIST = 25


def read_raw(path) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])


def show(values) -> str:
    values = list(values)
    more = f" … (+{len(values) - MAX_LIST} more)" if len(values) > MAX_LIST else ""
    return ", ".join(map(str, values[:MAX_LIST])) + more


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.str.strip(), errors="coerce")


def to_ts(s: pd.Series, fmt: str) -> pd.Series:
    return pd.to_datetime(s, format=fmt, errors="coerce")


def profile_file(name: str, df: pd.DataFrame, key: str) -> None:
    print(f"\n## {name}: {len(df)} rows × {df.shape[1]} columns\n")
    expected = config.EXPECTED_COLUMNS.get(name, config.ORDER_COLUMNS)
    print(f"- Header matches expected: **{list(df.columns) == expected}**")

    blank = df.apply(lambda s: s.str.strip().eq("").sum())
    na_literal = df.apply(lambda s: s.str.strip().str.upper().isin(["N/A", "NA", "NULL", "NONE"]).sum())
    padded = df.apply(lambda s: (s != s.str.strip()).sum())
    print(f"- Blank values per column: {blank[blank > 0].to_dict() or 'none'}")
    print(f"- Literal N/A/NULL per column: {na_literal[na_literal > 0].to_dict() or 'none'}")
    print(f"- Leading/trailing spaces per column: {padded[padded > 0].to_dict() or 'none'}")

    dup_mask = df[key].duplicated(keep=False)
    print(f"- Duplicate `{key}`: {int(df[key].duplicated().sum())} extra copies "
          f"| exact duplicate rows: {int(df.duplicated().sum())}")
    for k, grp in df[dup_mask].groupby(key):
        differing = [c for c in df.columns if grp[c].nunique() > 1]
        print(f"  - {k}: {len(grp)} copies, differing columns: {differing or 'none (exact)'}")

    print("\n| column | profile |\n|---|---|")
    for col in df.columns:
        s = df[col]
        if col in CATEGORICAL:
            vc = s.value_counts(dropna=False)
            print(f"| {col} | {', '.join(f'`{v!r}`:{n}' for v, n in vc.items())} |")
        elif col.endswith("_ts"):
            t = to_ts(s, config.TS_FORMAT)
            print(f"| {col} | min={t.min()} max={t.max()} unparsable={show(s[t.isna()].map(repr))} |")
        elif col.endswith("_date"):
            t = to_ts(s, config.DATE_FORMAT)
            print(f"| {col} | min={t.min()} max={t.max()} unparsable={show(s[t.isna()].map(repr))} |")
        else:
            n = to_num(s)
            if n.notna().mean() > 0.8 and col != key:
                bad = s[n.isna() & s.str.strip().ne("")]
                print(f"| {col} | min={n.min()} max={n.max()} non-numeric={show(bad.map(repr))} |")
            else:
                print(f"| {col} | distinct={s.nunique()} sample={show(s.head(3).map(repr))} |")


def anti_join(left: pd.DataFrame, lcol: str, right: pd.DataFrame, rcol: str) -> pd.DataFrame:
    keys = set(right[rcol])
    present = left[lcol].str.strip().ne("")
    return left[present & ~left[lcol].isin(keys)]


def profile_orders_rules(name: str, o: pd.DataFrame, period_end: str | None) -> None:
    print(f"\n### Business rules: {name}\n")
    q, up, disc, amt = (to_num(o[c]) for c in ["quantity", "unit_price", "discount_pct", "order_amount"])
    dm = to_num(o["delivery_minutes"])
    ts = to_ts(o["order_ts"], config.TS_FORMAT)
    status = o["order_status"].str.strip().str.upper()
    mode = o["payment_mode"].str.strip().str.lower().map(config.PAYMENT_MODE_ALIASES)

    expected = (q * up * (1 - disc / 100)).round(2)
    bad_formula = (expected - amt).abs() > config.MONEY_TOLERANCE
    checks = {
        "blank customer_id": o["customer_id"].str.strip().eq(""),
        "quantity <= 0": q <= 0,
        "discount_pct outside 0–100": (disc < 0) | (disc > 100),
        "order_amount ≠ formula (±0.01)": bad_formula & q.notna() & amt.notna(),
        "order_status raw not in allowed (before standardising)": ~o["order_status"].isin(config.ALLOWED_ORDER_STATUS),
        "order_status invalid after UPPER(TRIM)": ~status.isin(config.ALLOWED_ORDER_STATUS),
        "payment_mode raw not in allowed": ~o["payment_mode"].isin(config.ALLOWED_PAYMENT_MODE),
        "payment_mode invalid after aliasing": mode.isna(),
        "order_ts unparsable (strict)": ts.isna(),
        "DELIVERED without delivery_minutes": status.eq("DELIVERED") & o["delivery_minutes"].str.strip().eq(""),
        f"delivery_minutes > {config.DELIVERY_MAX_MINUTES}": dm > config.DELIVERY_MAX_MINUTES,
        "delivery_minutes outside 8–35 (non-blank)": dm.notna() & ((dm < 8) | (dm > 35)),
        "CANCELLED with blank delivery_minutes (expected)": status.eq("CANCELLED") & dm.isna(),
    }
    if name == "orders.csv":
        lo, hi = config.EXPECTED_PERIOD[name]
        checks["order_ts outside 1–30 Sept"] = ts.notna() & ((ts < lo) | (ts > hi))
    elif period_end:
        checks[f"order_ts after {period_end}"] = ts.notna() & (ts > pd.Timestamp(period_end))
    checks["order_ts after 2026-10-21 (future at profiling date)"] = ts > pd.Timestamp("2026-10-21 23:59:59")

    for label, mask in checks.items():
        mask = mask.fillna(False)
        ids = o.loc[mask, "order_id"]
        print(f"- {label}: **{int(mask.sum())}** {('— ' + show(ids)) if 0 < mask.sum() <= 40 else ''}")
    raw_vals = {
        "order_status": o.loc[~o["order_status"].isin(config.ALLOWED_ORDER_STATUS), "order_status"],
        "payment_mode": o.loc[~o["payment_mode"].isin(config.ALLOWED_PAYMENT_MODE), "payment_mode"],
        "order_ts": o.loc[ts.isna(), "order_ts"],
    }
    for col, vals in raw_vals.items():
        print(f"- non-standard `{col}` values: {vals.map(repr).value_counts().to_dict()}")
    ratio = (amt / expected).where(bad_formula & expected.ne(0))
    print(f"- order_amount / formula ratio on mismatches: {ratio.dropna().round(2).value_counts().to_dict()}")


def main() -> None:
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(config.AIRFLOW_HOME) / config.DEFAULT_DATA_DIR
    extra = [Path(p) for p in sys.argv[2:]]
    files = {f: data_dir / f for f in config.BASE_FILES}
    for p in sorted(data_dir.glob(config.INCREMENT_GLOB)) + extra:
        files[p.name] = p

    dfs = {name: read_raw(path) for name, path in files.items()}
    print("# NammaMart data profile\n")
    print("| file | rows | columns |\n|---|---:|---:|")
    for name, df in dfs.items():
        print(f"| {name} | {len(df)} | {df.shape[1]} |")
    print(f"| **total** | **{sum(len(d) for d in dfs.values())}** | |")

    for name, df in dfs.items():
        profile_file(name, df, config.PRIMARY_KEYS.get(name, "order_id"))

    o, p, c, pr, s = (dfs[f] for f in config.BASE_FILES)
    print("\n# Cross-file checks\n")
    print("## Referential integrity (anti-joins)\n")
    for name in [n for n in dfs if n.startswith("orders")]:
        od = dfs[name]
        for col, master, mname in [("customer_id", c, "customers"), ("product_id", pr, "products"),
                                   ("store_id", s, "stores")]:
            miss = anti_join(od, col, master, col)
            print(f"- {name}.{col} not in {mname}: **{len(miss)}** — "
                  f"{show(miss[col].value_counts().to_dict().items())}")
    orphan = anti_join(p, "order_id", o, "order_id")
    print(f"- payments.order_id not in orders: **{len(orphan)}** — {show(orphan['order_id'])}")
    status = o["order_status"].str.strip().str.upper()
    no_pay = o[~o["order_id"].isin(set(p["order_id"]))]
    print(f"- orders with no payment, by status: {no_pay['order_status'].str.strip().str.upper().value_counts().to_dict()}")

    print("\n## Business rules\n")
    profile_orders_rules("orders.csv", o, None)
    for name in [n for n in dfs if n.startswith("orders_")]:
        m = re.search(r"orders_(\d{4}-\d{2}-\d{2})_increment", name)
        profile_orders_rules(name, dfs[name], f"{m.group(1)} 23:59:59" if m else None)

    print("\n### payments vs orders\n")
    pd_ = p.drop_duplicates("payment_id")
    paid = to_num(pd_["amount_paid"])
    print(f"- SUCCESS with amount_paid <= 0: **{int(((pd_['payment_status'] == 'SUCCESS') & (paid <= 0)).sum())}** "
          f"— {show(pd_.loc[(pd_['payment_status'] == 'SUCCESS') & (paid <= 0), 'order_id'])}")
    j = pd_.merge(o.drop_duplicates("order_id"), on="order_id", how="inner", suffixes=("_pay", "_ord"))
    diff = (to_num(j["amount_paid"]) - to_num(j["order_amount"])).abs()
    mism = j[(j["payment_status"] == "SUCCESS") & (diff > config.MONEY_TOLERANCE)]
    print(f"- SUCCESS amount ≠ order_amount: **{len(mism)}** — {show(mism['order_id'])}")
    pm = (j["payment_mode_pay"].str.strip().str.lower().map(config.PAYMENT_MODE_ALIASES)
          != j["payment_mode_ord"].str.strip().str.lower().map(config.PAYMENT_MODE_ALIASES))
    print(f"- payment_mode differs between payment and order (after aliasing): **{int(pm.sum())}**")
    mtx = pd.crosstab(j["order_status"].str.strip().str.upper(), j["payment_status"])
    print(f"\nOrder status × payment status (deduplicated payments):\n\n```\n{mtx.to_string()}\n```")
    per_order = pd_.groupby("order_id")["payment_status"].agg(set)
    delivered = o[status.eq("DELIVERED")].drop_duplicates("order_id")
    only_failed = delivered["order_id"].map(per_order).map(lambda v: isinstance(v, set) and v == {"FAILED"})
    no_payment = ~delivered["order_id"].isin(per_order.index)
    print(f"\n- DELIVERED orders whose payments are all FAILED: **{int(only_failed.sum())}**")
    print(f"- DELIVERED orders with no payment: **{int(no_payment.sum())}**")
    pts = to_ts(pd_["payment_ts"], config.TS_FORMAT)
    print(f"- payment_ts after 2026-10-21: **{int((pts > pd.Timestamp('2026-10-21 23:59:59')).sum())}**")
    gw_blank = pd_["gateway_ref"].str.strip().eq("")
    print(f"- blank gateway_ref by payment_mode: {pd_.loc[gw_blank, 'payment_mode'].value_counts().to_dict()}")

    print("\n### products\n")
    mrp, cost = to_num(pr["mrp"]), to_num(pr["cost_price"])
    print(f"- mrp <= 0: {show(pr.loc[mrp <= 0, 'product_id'])}")
    print(f"- cost_price > mrp: {show(pr.loc[cost > mrp, 'product_id'])}")
    print(f"- blank category: {show(pr.loc[pr['category'].str.strip().eq(''), 'product_id'])}")
    first_mrp = pr.drop_duplicates("product_id").set_index("product_id")["mrp"].pipe(pd.to_numeric, errors="coerce")
    up = to_num(o["unit_price"])
    diff_mrp = (up - o["product_id"].map(first_mrp)).abs() > config.MONEY_TOLERANCE
    print(f"- orders where unit_price ≠ product mrp (first copy): **{int(diff_mrp.sum())}**")
    for pid in pr.loc[pr["product_id"].duplicated(keep=False), "product_id"].unique():
        prices = o.loc[o["product_id"] == pid, "unit_price"].value_counts().to_dict()
        print(f"  - {pid}: copies' mrp={pr.loc[pr['product_id'] == pid, 'mrp'].tolist()}, order unit_price={prices}")
    bad_products = set(pr.loc[(mrp <= 0) | (cost > mrp) | pr["category"].str.strip().eq(""), "product_id"])
    print(f"- September orders on a product that fails its own checks: **{int(o['product_id'].isin(bad_products).sum())}**")

    print("\n### customers\n")
    em = c["email"].str.strip().str.lower()
    print(f"- invalid email: {show(c.loc[~em.str.match(config.EMAIL_REGEX), 'email'].map(repr))}")
    print(f"- phone not 10 digits: {show(c.loc[~c['phone'].str.strip().str.match(config.PHONE_REGEX), 'phone'].map(repr))}")
    print(f"- pincode not 6 digits: {show(c.loc[~c['pincode'].str.strip().str.match(config.PINCODE_REGEX), 'pincode'].map(repr))}")
    tier = c["loyalty_tier"]
    print(f"- loyalty_tier needing case fix: {tier[tier != tier.str.strip().str.title()].value_counts().to_dict()}")
    sd = to_ts(c["signup_date"], config.DATE_FORMAT)
    fut = c.loc[sd > pd.Timestamp("2026-10-21"), ["customer_id", "signup_date"]]
    print(f"- signup_date after 2026-10-21: {fut.to_dict('records')}")
    print(f"- September orders by those customers: **{int(o['customer_id'].isin(set(fut['customer_id'])).sum())}**")
    first_order = o.assign(ts=to_ts(o["order_ts"], config.TS_FORMAT)).groupby("customer_id")["ts"].min()
    cs = c.drop_duplicates("customer_id").set_index("customer_id")
    before = (first_order < to_ts(cs["signup_date"], config.DATE_FORMAT).reindex(first_order.index)).sum()
    print(f"- customers whose first order precedes signup_date: **{int(before)}**")

    print("\n### stores\n")
    print(f"- blank zone: {show(s.loc[s['zone'].str.strip().eq(''), 'store_id'])} "
          f"(orders on them: {int(o['store_id'].isin(set(s.loc[s['zone'].str.strip().eq(''), 'store_id'])).sum())})")
    cap = to_num(s["capacity_orders_per_day"])
    print(f"- capacity_orders_per_day <= 0: {show(s.loc[cap <= 0, 'store_id'])}")

    for name in [n for n in dfs if n.startswith("orders_")]:
        inc = dfs[name]
        existing = inc["order_id"].isin(set(o["order_id"]))
        print(f"\n### {name} vs orders.csv\n")
        print(f"- new order_ids: **{int((~existing).sum())}** "
              f"({inc.loc[~existing, 'order_id'].min()}–{inc.loc[~existing, 'order_id'].max()})")
        print(f"- existing order_ids (corrections): **{int(existing.sum())}**")
        base = o.drop_duplicates("order_id").set_index("order_id")
        for _, row in inc[existing].iterrows():
            old = base.loc[row["order_id"]]
            changed = {col: f"{old[col]!r}→{row[col]!r}" for col in config.ORDER_COLUMNS[1:] if old[col] != row[col]}
            print(f"  - {row['order_id']}: {changed}")
        print(f"- increment rows on products failing own checks: {show(inc.loc[inc['product_id'].isin(bad_products), 'order_id'])}")


if __name__ == "__main__":
    main()
