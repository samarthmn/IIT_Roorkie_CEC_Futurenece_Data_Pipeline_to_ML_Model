-- STAGING: customers. One row per customer_id: prefer the copy whose email and phone are valid,
-- then the first copy in file order (decision D4).
-- PII: no full_name, email or phone in clear text. Masking mirrors include/nammamart/masking.py:
--   email -> first 2 chars of the local part + '***@' + domain (NULL when the email is invalid)
--   phone -> first 2 digits + '*' x (n - 4) + last 2 digits (NULL when fewer than 4 digits)
-- Warning rules: email format, phone 10 digits, pincode 6 digits, loyalty_tier casing.
-- Critical rule: customers_signup_not_future (the customer row only; their orders are kept, D7).

INSERT INTO stg_rejected_duplicates
SELECT _source_file, customer_id, _source_row, 'duplicate_customer_id', _run_id
FROM raw_customers
WHERE _run_id = (SELECT run_id FROM run_ctx)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY (regexp_matches(LOWER(TRIM(email)), '^[^@\s]+@[^@\s]+\.[^@\s]+$')
              AND regexp_matches(TRIM(phone), '^\d{10}$')) DESC, _source_file, _source_row) > 1;

CREATE OR REPLACE TABLE stg_customers AS
WITH src AS (
    SELECT *,
        LOWER(TRIM(email))                                              AS email_clean,
        regexp_replace(phone, '\D', '', 'g')                            AS phone_digits,
        UPPER(LEFT(TRIM(loyalty_tier), 1)) || LOWER(SUBSTR(TRIM(loyalty_tier), 2)) AS tier_std,
        TRY_STRPTIME(TRIM(signup_date), '%Y-%m-%d')                     AS signup_ts
    FROM raw_customers
    WHERE _run_id = (SELECT run_id FROM run_ctx)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY (regexp_matches(LOWER(TRIM(email)), '^[^@\s]+@[^@\s]+\.[^@\s]+$')
                  AND regexp_matches(TRIM(phone), '^\d{10}$')) DESC, _source_file, _source_row) = 1
),
checked AS (
    SELECT *,
        NULLIF(concat_ws(';',
            CASE WHEN signup_ts > (SELECT run_ts FROM run_ctx) THEN 'future_signup_date' END
        ), '') AS fr,
        NULLIF(concat_ws(';',
            CASE WHEN NOT regexp_matches(email_clean, '^[^@\s]+@[^@\s]+\.[^@\s]+$') THEN 'invalid_email' END,
            CASE WHEN NOT regexp_matches(TRIM(phone), '^\d{10}$') THEN 'invalid_phone' END,
            CASE WHEN NOT regexp_matches(TRIM(pincode), '^\d{6}$') THEN 'invalid_pincode' END,
            CASE WHEN tier_std <> loyalty_tier THEN 'loyalty_tier_standardised' END
        ), '') AS wr
    FROM src
)
SELECT
    TRIM(customer_id)                                           AS customer_id,
    CASE WHEN regexp_matches(email_clean, '^[^@\s]+@[^@\s]+\.[^@\s]+$')
         THEN LEFT(email_clean, 2) || '***@' || split_part(email_clean, '@', 2) END AS email_masked,
    CASE WHEN length(phone_digits) >= 4
         THEN LEFT(phone_digits, 2) || repeat('*', length(phone_digits) - 4) || RIGHT(phone_digits, 2)
    END                                                         AS phone_masked,
    NULLIF(TRIM(city), '')                                      AS city,
    NULLIF(TRIM(area), '')                                      AS area,
    tier_std                                                    AS loyalty_tier,
    CASE WHEN fr IS NULL THEN CAST(signup_ts AS DATE) END       AS signup_date,   -- a future date is not published
    UPPER(TRIM(is_active)) = 'Y'                                AS is_active,
    CASE WHEN fr IS NOT NULL THEN 'FAIL' WHEN wr IS NOT NULL THEN 'WARN' ELSE 'PASS' END AS dq_status,
    fr                                                          AS failure_reason,
    wr                                                          AS warning_reason,
    _source_file, _source_row, _run_id
FROM checked
ORDER BY _source_row;
