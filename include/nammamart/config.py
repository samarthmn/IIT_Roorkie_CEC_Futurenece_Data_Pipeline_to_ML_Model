"""
include/nammamart/config.py
---------------------------
Single source of truth for the NammaMart ETL and ELT pipelines.
Every rule constant lives here; the SQL files in include/sql mirror these values exactly.
"""

import os

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
INCLUDE_DIR = os.path.join(AIRFLOW_HOME, "include")

DEFAULT_DATA_DIR = "include/data/nammamart"           # relative to AIRFLOW_HOME; overridable via DAG param
WAREHOUSE_DB = os.path.join(INCLUDE_DIR, "warehouse_samarth.duckdb")
QUARANTINE_DIR = os.path.join(INCLUDE_DIR, "quarantine", "samarth")
TMP_DIR = os.path.join(INCLUDE_DIR, "tmp", "samarth")
SQL_DIR = os.path.join(INCLUDE_DIR, "sql")

POOL = "duckdb_samarth"
TIMEZONE = "Asia/Kolkata"

# ── Source files ──────────────────────────────────────────────────────────────

BASE_FILES = ["orders.csv", "payments.csv", "customers.csv", "products.csv", "stores.csv"]
INCREMENT_GLOB = "orders_*_increment.csv"

ORDER_COLUMNS = [
    "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
    "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes",
]
EXPECTED_COLUMNS = {
    "orders.csv": ORDER_COLUMNS,
    "payments.csv": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
                     "payment_status", "gateway_ref"],
    "customers.csv": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode",
                      "signup_date", "loyalty_tier", "is_active"],
    "products.csv": ["product_id", "product_name", "category", "sub_category", "brand", "mrp",
                     "cost_price", "unit", "is_perishable", "launch_date"],
    "stores.csv": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
                   "capacity_orders_per_day"],
}
PRIMARY_KEYS = {
    "orders.csv": "order_id",
    "payments.csv": "payment_id",
    "customers.csv": "customer_id",
    "products.csv": "product_id",
    "stores.csv": "store_id",
}

# ── Allowed values ────────────────────────────────────────────────────────────

ALLOWED_ORDER_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
ALLOWED_PAYMENT_MODE = {"UPI", "Card", "Cash on Delivery", "Wallet"}
ALLOWED_PAYMENT_STATUS = {"SUCCESS", "FAILED", "REFUNDED"}
ALLOWED_LOYALTY_TIER = {"Bronze", "Silver", "Gold", "Platinum"}
ALLOWED_ZONE = {"North", "South", "East", "West"}

# Standardisation (decision D5). Keys are trimmed + lower-cased raw values.
# 'cashh' is deliberately NOT aliased (decision D6).
PAYMENT_MODE_ALIASES = {
    "upi": "UPI",
    "card": "Card",
    "cod": "Cash on Delivery",
    "cash on delivery": "Cash on Delivery",
    "wallet": "Wallet",
}

# ── Formats and thresholds ────────────────────────────────────────────────────

TS_FORMAT = "%Y-%m-%d %H:%M:%S"          # strict (decision D8)
DATE_FORMAT = "%Y-%m-%d"
EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
PHONE_REGEX = r"^\d{10}$"
PINCODE_REGEX = r"^\d{6}$"

MONEY_TOLERANCE = 0.01
DELIVERY_MAX_MINUTES = 120               # accuracy limit (Critical)
DELIVERY_NORMAL_RANGE = (8, 35)          # outside = Warning
SLA_MINUTES = 15
VOLUME_DROP_WARN_PCT = 90
FRESHNESS_MAX_LAG_HOURS = 24

# Base orders must fall inside September 2026 (decision D9).
# Increment rows must be on or before the end of the date in the file name.
EXPECTED_PERIOD = {"orders.csv": ("2026-09-01 00:00:00", "2026-09-30 23:59:59")}
