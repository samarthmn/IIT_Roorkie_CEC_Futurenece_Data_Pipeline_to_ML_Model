"""
include/nammamart/masking.py
----------------------------
PII masking used by the ETL. include/sql/22_stg_customers.sql mirrors these rules exactly,
so both pipelines produce identical masked strings.
"""

import re

from include.nammamart import config

EMAIL_RE = re.compile(config.EMAIL_REGEX)


def mask_email(value):
    """priya.hegde199@example.com -> pr***@example.com. Invalid emails are never echoed back."""
    s = (value or "").strip().lower()
    if not EMAIL_RE.match(s):
        return None
    local, domain = s.split("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(value):
    """9774349021 -> 97******21. Fewer than 4 digits -> None."""
    digits = re.sub(r"\D", "", value or "")
    if len(digits) < 4:
        return None
    return digits[:2] + "*" * (len(digits) - 4) + digits[-2:]
