# NammaMart ETL/ELT Assignment — Samarth

## 1. Executive summary

_To be completed after both pipelines and the experiments have run._

## 2. How to run

```bash
astro dev start
# one-slot pool: only one task at a time may write to warehouse_samarth.duckdb (across both DAGs)
astro dev run pools set duckdb_samarth 1 "Serialize writes to warehouse_samarth.duckdb"
# profiler (Part 0): open a shell in the scheduler container, then run it there
astro dev bash --scheduler
python include/nammamart/profiling.py include/data/nammamart > include/reports/profile_report.md
exit
```

Trigger a DAG (UI: **Trigger → with config**, or CLI):

```bash
astro dev run dags unpause nammamart_etl_samarth
astro dev run dags trigger nammamart_etl_samarth --conf '{"as_of_date": "2026-10-01"}'
```

| Param | Default | Meaning |
|---|---|---|
| `data_dir` | `include/data/nammamart` | Input folder relative to `AIRFLOW_HOME`; must be under `include/data` (experiments point it at a generated fixture folder) |
| `as_of_date` | none (logical date) | Freshness reference, `YYYY-MM-DD`, taken as 09:00 IST that day |

- Input folder: `include/data/nammamart/` (the five base files). The 1 October increment is added in Experiment 3 by copying `assignment/nammamart_dataset/orders_2026-10-01_increment.csv` into that folder.
- Warehouse: `include/warehouse_samarth.duckdb` (gitignored). All warehouse timestamps are IST.
- Quarantine files: `include/quarantine/samarth/` (gitignored: they hold raw rows, including customer PII).
- Query the warehouse while no DAG is running (DuckDB allows one writer; a read-only client still blocks writers):

```bash
astro dev bash --scheduler
python -c "import duckdb; c=duckdb.connect('include/warehouse_samarth.duckdb', read_only=True); print(c.sql('SELECT * FROM pipeline_runs'))"
```

## 3. Design decisions

These decisions apply identically in the ETL (pandas) and ELT (SQL) pipelines so that both produce the same numbers.

| ID | Decision | Choice |
|---|---|---|
| D1 | Fact grain | One row per `order_id`; every order has exactly one product line. |
| D2 | Revenue | `net_revenue = order_amount` when the current status is `DELIVERED` (`order_amount` already includes the discount), else 0. Only rows that passed every Critical check count. |
| D3 | Gross margin | `net_revenue − quantity × cost_price` for DELIVERED rows, else 0. |
| D4 | Duplicate keys | Keep one canonical row (first occurrence in file order) and quarantine only the extra copies as `duplicate_<key>`. For customers, prefer the copy whose email and phone are valid, then the first occurrence. Profiling confirms the first product copy is the right one: orders for P0030 and P0140 are priced at the first copy's `mrp`. |
| D5 | Fixable values | Trim, case-fold and documented aliases are standardisation, not rejection: `delivered` → `DELIVERED`; `upi`, `UPI ` → `UPI`; `CARD` → `Card`; `cod` → `Cash on Delivery`; `BRONZE`/`bronze`/`SILVER` → `Bronze`/`Silver`. Each mapping is logged as a Warning check. |
| D6 | Ambiguous values | `Pending??` (status) and `Cashh` (payment mode) are not guessed. They are quarantined as Critical validity failures and raised as client questions. |
| D7 | Dirty master rows | An order is consistent if its `customer_id`, `product_id` and `store_id` exist in the source master file. A master row that fails its own checks is quarantined from its dimension, but its orders still load, using that source row's attributes (category, cost, zone, tier). S07's blank zone is a Warning: the store is kept with zone `Unknown`. |
| D8 | Timestamps | Strict format `YYYY-MM-DD HH:MM:SS` in both pandas and DuckDB; dates `YYYY-MM-DD`. Source timestamps are naive and treated as IST. `order_date` is the IST calendar date. |
| D9 | Future / out-of-period | Two checks. `future_*` (Timeliness, Critical): later than the run's wall-clock time in IST. `outside_expected_period` (Validity, Critical): `orders.csv` rows outside 1–30 Sept 2026, or increment rows after the date in the file name. The second check keeps results identical whatever day the DAG runs. |
| D10 | Freshness reference | Stale (Warning) = latest valid `order_ts` more than 24 h before the reference time. Reference = DAG param `as_of_date` when given, else the run's logical date in IST. |
| D11 | What a run processes | The files currently in `data_dir`: the five base files plus every `orders_*_increment.csv`, applied in file-date order. ETL rebuilds its outputs from these inputs. ELT appends a raw batch under the run's `_run_id` and builds staging only from that batch. |
| D12 | Money | Compare with ±0.01 tolerance; store rounded to 2 decimals. |
| D13 | Personal data | Reporting tables carry `email_masked`, `phone_masked` and `loyalty_tier`; no raw email or phone and no `full_name`. Only `raw_customers`, the source files and the quarantine files hold clear values. |
| D14 | Reconciliation input | Reconcile against all payment rows (deduplicated by `payment_id`), including payments whose order was rejected or does not exist. |
| D15 | Returns | Revenue is current-restated: a September order later RETURNED drops out of September revenue. No history table is built (see Part E, question 9). |

## 4. Part 0: Discovery and data profiling

Profiler: `include/nammamart/profiling.py`. It reads every value as text (`dtype=str, keep_default_na=False`) so blanks and literal `N/A` are not silently turned into NaN, then checks headers, blanks, padding, duplicate keys, ranges, category values, referential integrity (anti-joins) and every business rule.

### 4.1 Profile summary

| File | Rows | Columns | Header as expected | Duplicate keys (extra copies) |
|---|---:|---:|---|---:|
| orders.csv | 2,412 | 13 | yes | 12 (exact duplicates) |
| payments.csv | 2,228 | 7 | yes | 4 (exact duplicates) |
| customers.csv | 405 | 10 | yes | 5 (copies differ) |
| products.csv | 152 | 10 | yes | 2 (copies differ) |
| stores.csv | 15 | 8 | yes | 0 |
| orders_2026-10-01_increment.csv | 160 | 13 | yes | 0 |
| **Total** | **5,372** | | | |

Category values found:

| Column | Values (count) |
|---|---|
| orders.order_status | DELIVERED 2,182 · CANCELLED 156 · RETURNED 69 · `Pending??` 3 · `delivered` 2 |
| orders.payment_mode | UPI 1,329 · Card 493 · Cash on Delivery 326 · Wallet 254 · `upi` 4 · `Cashh` 2 · `UPI ` 2 · `CARD` 1 · `cod` 1 |
| payments.payment_mode | UPI 1,238 · Card 445 · Cash on Delivery 301 · Wallet 234 · `upi` 4 · `UPI ` 2 · `Cashh` 2 · `cod` 1 · `CARD` 1 |
| payments.payment_status | SUCCESS 2,116 · REFUNDED 68 · FAILED 44 |
| customers.loyalty_tier | Bronze 185 · Silver 133 · Gold 54 · Platinum 25 · `BRONZE` 3 · `bronze` 3 · `SILVER` 2 |
| stores.zone | East 4 · West 4 · North 3 · South 3 · blank 1 |
| products.category | Dairy & Bakery 26 · Household 26 · Fruits & Vegetables 25 · Snacks & Beverages 25 · Personal Care 25 · Staples 22 · blank 3 |

Ranges worth noting: `orders.order_ts` runs to 2027-01-01; `quantity` −3 to 5; `discount_pct` 0 to 250; `order_amount` −1,593 to 9,576; `delivery_minutes` 8 to 1,440; `products.mrp` down to −203; `customers.signup_date` up to 2027-01-05; `stores.capacity_orders_per_day` down to 0.

### 4.2 Data Issues Log

Severity: **Critical** rows are quarantined (ETL) or marked `dq_status = 'FAIL'` (ELT); **Warning** rows load and are counted in `dq_results`; **Info** items are logged here but need no pipeline rule.

| # | File | Row key(s) | Column | Problem | Quality dimension | Proposed rule | Severity |
|---|---|---|---|---|---|---|---|
| 1 | orders.csv | ORD000539, ORD000783, ORD001110, ORD001239, ORD001326, ORD001434, ORD001441, ORD001462, ORD001549, ORD001692, ORD001870, ORD002129 | order_id | 12 exact duplicate rows | Uniqueness | `order_id` unique; keep first copy, quarantine the extra | Critical |
| 2 | orders.csv | ORD002255, ORD000701, ORD000997, ORD000580, ORD002019, ORD002262, ORD002149, ORD002367, ORD001332, ORD001918, ORD002156, ORD000583, ORD000790, ORD000764, ORD002219, ORD001532, ORD000022, ORD001373 | customer_id | 18 blank | Completeness | required fields non-blank | Critical |
| 3 | orders.csv | 9 rows with P9455, P9212, P9816, P9893, P9989, P9784, P9822, P9399, P9761 | product_id | not in products.csv | Consistency | key exists in product master | Critical |
| 4 | orders.csv | 5 rows with S17 ×3, S99, S00 | store_id | not in stores.csv | Consistency | key exists in store master | Critical |
| 5 | orders.csv | ORD000469, ORD001296, ORD001459, ORD000867, ORD000462, ORD002360, ORD001724, ORD000099, ORD001124, ORD001384 | quantity | 0 (4 rows) or negative (6 rows) | Accuracy | `quantity > 0` | Critical |
| 6 | orders.csv | ORD002103, ORD001468, ORD001237 | discount_pct | 120, 150, 250 | Validity | 0 ≤ `discount_pct` ≤ 100 | Critical |
| 7 | orders.csv | 18 rows incl. ORD002305, ORD002004, ORD000845, ORD000964 (full list in profile report) | order_amount | ≠ `quantity × unit_price × (1 − discount_pct/100)`; ratios 10× (6), 1.5× (5), 0.5× (4), others from the bad discounts | Accuracy | formula match within ±0.01 | Critical |
| 8 | orders.csv | ORD000153, ORD001066 | order_status | `delivered` (lower case) | Validity | standardise with `UPPER(TRIM())` and log | Warning |
| 9 | orders.csv | ORD000500, ORD001970, ORD001637 | order_status | `Pending??` (not an allowed status; all three have a SUCCESS payment) | Validity | status in DELIVERED/CANCELLED/RETURNED | Critical |
| 10 | orders.csv | 8 rows (`upi` ×4, `UPI ` ×2, `CARD`, `cod`) | payment_mode | non-standard spelling | Validity | standardise via alias map and log | Warning |
| 11 | orders.csv | ORD001812, ORD001414 | payment_mode | `Cashh` | Validity | payment mode in allowed list after aliasing | Critical |
| 12 | orders.csv | ORD000403, ORD001610, ORD000534, ORD000362, ORD002012, ORD000946 | order_ts | `N/A`, blank, `2026-13-05 11:00:00` ×2, `31/09/2026 10:00` ×2 | Validity | parses with strict `YYYY-MM-DD HH:MM:SS` | Critical |
| 13 | orders.csv | ORD002119, ORD001693, ORD001089, ORD001121, ORD001765, ORD001965, ORD001725 | order_ts | 2027-01-01 ×3, 2026-11-02 ×2, 2026-10-21 ×2: outside September; 5 are in the future today | Validity / Timeliness | within expected period; not after run time | Critical |
| 14 | orders.csv | ORD001554, ORD001800, ORD000731, ORD001277, ORD000326, ORD001515, ORD002229, ORD000878 | delivery_minutes | blank on a DELIVERED order | Completeness | DELIVERED ⇒ `delivery_minutes` present | Critical |
| 15 | orders.csv | ORD001037, ORD002101, ORD000455, ORD001104, ORD000020, ORD000424 | delivery_minutes | 480, 900, 1,440 (hours, not minutes) | Accuracy | `delivery_minutes ≤ 120`; outside 8–35 is a Warning | Critical |
| 16 | orders.csv | 156 CANCELLED orders | delivery_minutes | blank | — | expected for a cancelled order; not flagged | Info |
| 17 | payments.csv | PAY0000509, PAY0000755, PAY0001651, PAY0002000 | payment_id | 4 exact duplicate rows | Uniqueness | `payment_id` unique; keep first copy | Critical |
| 18 | payments.csv | ORD970544, ORD968496, ORD988106, ORD941071, ORD981600, ORD997176, ORD953136, ORD966342 | order_id | payment for an order that does not exist | Consistency | `order_id` exists in orders | Critical |
| 19 | payments.csv | payments for ORD001031, ORD001384, ORD000469, ORD001459, ORD000462, ORD000867, ORD001724, ORD000099, ORD002360, ORD001296 | amount_paid | SUCCESS with `amount_paid ≤ 0` (matches the bad-quantity orders) | Accuracy | SUCCESS ⇒ `amount_paid > 0` | Critical |
| 20 | payments.csv | ORD001031, ORD001937, ORD001840, ORD001864, ORD001753, ORD001832, ORD001360, ORD000481, ORD001039, ORD000371, ORD001812, ORD001572, ORD000178, ORD002185 | amount_paid | SUCCESS amount ≠ `order_amount` | Consistency | listed in the reconciliation table | Warning |
| 21 | payments.csv | 5 rows dated 2027-01-01 / 2026-11-02 (the future-dated orders) | payment_ts | in the future | Timeliness | `payment_ts` not after run time | Critical |
| 22 | payments.csv | 8 rows (`upi` ×4, `UPI ` ×2, `cod`, `CARD`) and 2 `Cashh` | payment_mode | non-standard / unknown spelling | Validity | as rows 10–11 | Warning / Critical |
| 23 | payments.csv × orders.csv | 44 DELIVERED orders | payment_status | only payment is FAILED | Consistency | reconciliation row `PAYMENT_FAILED` | Warning |
| 24 | payments.csv × orders.csv | 27 DELIVERED orders | — | no payment row at all | Completeness | reconciliation row `MISSING_PAYMENT` | Warning |
| 25 | payments.csv × orders.csv | ORD001675 | — | RETURNED order with no payment (no money moved; 68 of 69 RETURNED have a REFUNDED row) | Completeness | logged; not a money mismatch | Info |
| 26 | payments.csv | 301 rows | gateway_ref | blank; all are Cash on Delivery | — | expected; not flagged | Info |
| 27 | customers.csv | C00036, C00103, C00226, C00252, C00313 | customer_id | duplicate; copies differ in phone (and area, pincode) | Uniqueness | one row per customer; keep the valid copy (D4) | Critical |
| 28 | customers.csv | 12 rows (`N/A` ×4; missing `@` ×5, e.g. C00307; no domain dot ×3) | email | invalid format | Validity | email regex; masked value set to NULL | Warning |
| 29 | customers.csv | 7 rows (7–9 digits) | phone | not 10 digits | Validity | `^\d{10}$` | Warning |
| 30 | customers.csv | 3 rows | pincode | `5600XX` | Validity | `^\d{6}$` | Warning |
| 31 | customers.csv | 6 rows | city | blank | Completeness | logged; not needed downstream | Info |
| 32 | customers.csv | C00400 (2027-01-05), C00398 (2026-11-12), C00038 (2026-12-20) | signup_date | in the future; these customers already placed 18 September orders | Timeliness | `signup_date` not after run time (customer row only, orders kept per D7) | Critical |
| 33 | customers.csv | 8 rows (`BRONZE` ×3, `bronze` ×3, `SILVER` ×2) | loyalty_tier | inconsistent casing | Consistency | standardise to title case and log | Warning |
| 34 | products.csv | P0030, P0140 | product_id | duplicate; copies differ in name and mrp | Uniqueness | keep first copy (orders use its price) | Critical |
| 35 | products.csv | P0051, P0060, P0068 | category | blank | Completeness | `category` non-blank | Critical |
| 36 | products.csv | P0109 | mrp | −203 (orders are priced at +203: a sign error) | Accuracy | `mrp > 0` | Critical |
| 37 | products.csv | P0019, P0073, P0074, P0117, P0109 | cost_price | `cost_price > mrp` (1.2× mrp) | Accuracy | `cost_price ≤ mrp` | Critical |
| 38 | stores.csv | S07 (BTM Layout) | zone | blank; 176 orders | Completeness | keep store, zone `Unknown` | Warning |
| 39 | stores.csv | S12 | capacity_orders_per_day | 0 | Accuracy | logged only (no capacity check is built) | Info |
| 40 | orders_2026-10-01_increment.csv | ORD000017, ORD000271, ORD000332, ORD000337, ORD000584, ORD000708, ORD000822, ORD001025, ORD001067, ORD001990 | order_status | corrections: DELIVERED → RETURNED (same amount), dated in September | Timeliness (late change) | upsert on `order_id` | Info |
| 41 | orders_2026-10-01_increment.csv | ORD002401–ORD002550 | — | 150 new 1 Oct orders; no payments for them in `payments.csv` and no REFUNDED rows for the 10 returns | Timeliness | reconciliation reasons `PAYMENT_NOT_YET_RECEIVED`, `RETURNED_NOT_REFUNDED` | Warning |
| 42 | orders_2026-10-01_increment.csv | ORD000822 and 13 new orders | product_id | on products that fail their own checks (e.g. P0060, blank category) | Consistency | order kept (D7) | Info |

Impact of D7 on September orders: 126 orders reference a product that fails its own checks; 176 orders are on S07; 18 orders belong to customers with a future `signup_date`. None of them are dropped for that reason.

### 4.3 Questions for the client

1. Is a zero or negative `quantity` a return, a cancellation or a data-entry error? Should it ever reduce revenue?
2. Which zone is S07 (BTM Layout) in? The pipeline reports its 176 orders under zone `Unknown` until you say otherwise.
3. Returned orders (for example ORD000017) still show a SUCCESS payment and no REFUNDED row. When does the refund appear, and should the return reduce September revenue or be booked in October?

## 5. Part A: ETL pipeline

DAG: `dags/nammamart_etl_samarth.py` (`nammamart_etl_samarth`). Helpers: `include/nammamart/` (`sources.py` file access, `quality.py` rules, `masking.py`, `etl.py` transforms, `warehouse.py` DuckDB, `runtime.py` run identity). Control-table DDL: `include/sql/00_create_control_tables.sql`.

### 5.1 Task graph

```text
extract_orders ─────┐
extract_payments ───┤
extract_customers ──┤
extract_products ───┼──► validate ─► transform ─► load_star_schema* ─► reconcile_payments* ─► build_reports* ─► audit_run*
extract_stores ─────┤   (+quarantine)
extract_increments ─┘                                                        * = pool duckdb_samarth
```

A straight line after the extracts: if any task fails, every later task becomes `upstream_failed`, and `audit_run` (`trigger_rule="all_done"`) still runs.

| Step | What it does |
|---|---|
| Extract | Five parallel tasks, one per file, plus `extract_increments` (returns `[]` when no `orders_*_increment.csv` exists). Each checks the header first: a missing file raises `FileNotFoundError` (retried); a missing or renamed column raises `AirflowFailException` (fails at once, no retries). Values are read as text (`keep_default_na=False`), so `N/A` and blanks are kept as received. |
| Validate | One task applies every rule from section 7 in pandas, file by file: stores, products, customers, then orders (the foreign-key checks need the master keys), then payments (they need the orders). Every row gets `failure_reason` (Critical) and `warning_reason` (Warning); the task log lists each failing check per file. |
| Quarantine | Failed rows exactly as received, plus `failure_reason`, `source_row`, `run_id`, to `include/quarantine/samarth/<file>_failed_<run_id>.csv`. A file is written even when empty, so every run leaves evidence. The keys and reasons (no PII) also go to `etl_rejected_rows` for business question 6. |
| Transform | Standardise (trim, case, payment-mode aliases), dedupe customers and products (D4), mask email and phone, join orders to the three dimensions with a left join plus an assertion that no key is missing, derive `order_date`, `net_revenue`, `gross_margin`, `within_15_min`. |
| Load | One DuckDB transaction replaces the whole star (`CREATE OR REPLACE TABLE` ×4), so a reader never sees a half-loaded star and a re-run gives identical tables. Afterwards it asserts that `order_id` is unique in the fact. |
| Reconcile | `etl_payment_reconciliation`: one row per order/payment mismatch with a reason (section 5.3). |
| Report | `etl_daily_zone_revenue` and `etl_category_margin`, printed in the task log with totals. |
| Run audit | Writes this run's `dq_results` (every check, plus the volume check) and its `pipeline_runs` row; keeps a failed run red (section 10). |

Intermediate DataFrames travel between tasks as pickle files under `include/tmp/samarth/<run_id>/` (pyarrow is not installed in the image, so not Parquet); XCom carries only paths, counts and check results. The folder is deleted when a run succeeds and kept after a failure, so a Clear can resume.

### 5.2 Star schema

| Table | Grain | Columns |
|---|---|---|
| `etl_fact_orders` | one row per valid order | order_id, order_ts, order_date, customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes, net_revenue, gross_margin, within_15_min, source_file, source_row, run_id, loaded_at |
| `etl_dim_customer` | one row per customer_id | customer_id, email_masked, phone_masked, city, area, loyalty_tier, signup_date, is_active, dq_status, failure_reason, warning_reason |
| `etl_dim_product` | one row per product_id | product_id, product_name, category, sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date, dq_status, failure_reason, warning_reason |
| `etl_dim_store` | one row per store_id | store_id, store_name, zone, area, pincode, open_date, capacity_orders_per_day, dq_status, failure_reason, warning_reason |
| `etl_rejected_rows` | one row per quarantined row per run (history) | run_id, dag_id, run_date_ist, file_name, row_key, failure_reason, rejected_at |

The fact holds only orders that passed every Critical check (acceptance criterion 2). A dimension holds one canonical row per key, **including** a master row that failed its own checks (for example P0060, blank category): that row is also written to the quarantine file, and it carries `dq_status = 'FAIL'` with its reason in the dimension. This is how decision D7 is implemented: the 126 orders on such products stay in the fact and still join to a product row, with a blank category shown as `Unknown`. Extra duplicate copies never reach a dimension.

Money columns are `DECIMAL(12,2)`. No reporting table contains `email`, `phone` or `full_name`.

### 5.3 Payment reconciliation

All payment rows are reconciled (decision D14), including duplicates and payments for rejected or unknown orders. `difference` = SUCCESS amount paid − amount owed (owed = `order_amount` for a DELIVERED order, else 0). One reason per row:

| Reason | Meaning |
|---|---|
| `ORDER_NOT_FOUND` | payment for an `order_id` not in any orders file |
| `ORDER_REJECTED` | payment for an order that was quarantined, so it is not in the fact |
| `DUPLICATE_PAYMENT_ID` | an extra copy of a payment row (would double-count if summed) |
| `AMOUNT_MISMATCH` | DELIVERED order whose SUCCESS amount ≠ `order_amount` (±0.01) |
| `PAYMENT_FAILED` | DELIVERED order whose payments are all FAILED |
| `MISSING_PAYMENT` | DELIVERED order with no payment row |
| `PAYMENT_NOT_YET_RECEIVED` | DELIVERED order with no payment, dated after the last day the payment feed covers (timing, not an error) |
| `RETURNED_NOT_REFUNDED` | RETURNED order with a SUCCESS payment and no REFUNDED row (timing: refund pending) |
| `CANCELLED_WITH_PAYMENT` | CANCELLED order with a SUCCESS payment |

Not mismatches: CANCELLED with no payment, RETURNED with REFUNDED, RETURNED with no payment (ORD001675: no money moved), and blank `gateway_ref` on Cash on Delivery.

The payment feed's last day is the latest `payment_ts` among payments for valid orders (30 Sept). An unpaid order counts as "not yet received" only if it is dated strictly **after** that day, so the unpaid 30 Sept order stays `MISSING_PAYMENT`.

### 5.4 Happy-path result (`as_of_date = 2026-10-01`, September files only)

| File | Rows read | Passed (incl. warnings) | Of which warnings | Quarantined |
|---|---:|---:|---:|---:|
| orders.csv | 2,412 | 2,308 | 10 | 104 |
| payments.csv | 2,228 | 2,199 | 20 | 29 |
| customers.csv | 405 | 397 | 30 | 8 |
| products.csv | 152 | 142 | 0 | 10 |
| stores.csv | 15 | 15 | 1 | 0 |
| **Total** | **5,212** | | | **151** |

`etl_fact_orders`: 2,308 orders (2,086 DELIVERED, 154 CANCELLED, 68 RETURNED). September net revenue **₹11,34,962.80**, gross margin **₹2,60,826.90**. `etl_payment_reconciliation`: 184 rows (AMOUNT_MISMATCH 13, DUPLICATE_PAYMENT_ID 4, MISSING_PAYMENT 27, ORDER_NOT_FOUND 8, ORDER_REJECTED 90, PAYMENT_FAILED 42).

## 6. Part B: ELT pipeline

DAG: `dags/nammamart_elt_samarth.py` (`nammamart_elt_samarth`). SQL: `include/sql/1*_raw`, `2*_stg`, `30_dq_results_elt.sql`, `4*_mart`. Python only checks the files, copies them into DuckDB and runs the `.sql` files (`include/nammamart/elt.py`); every transformation is SQL.

### 6.1 Task graph (one task per layer)

```text
check_orders ───────┐
check_payments ─────┤
check_customers ────┤
check_products ─────┼──► load_raw* ─► transform_staging* ─► transform_marts* ─► audit_run*
check_stores ───────┤
check_increments ───┘                                            * = pool duckdb_samarth
     Extract              Load         └──── Transform (SQL, in the warehouse) ────┘
```

The six `check_*` tasks (file present, header as expected, row count) don't touch the warehouse, so they run in parallel. The warehouse tasks run one after another by design: DuckDB allows a single writer.

### 6.2 Layers

| Layer | Tables | How it is built |
|---|---|---|
| Raw | `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` | `10_raw_create.sql` + `read_csv` with every column as `VARCHAR`, `auto_detect = false` and a null marker that never occurs, so blanks stay `''`, `N/A` stays `N/A`, `UPI ` keeps its space. Each row gets `_source_row` (1 = first data line), `_loaded_at`, `_source_file`, `_run_id`. The increment file goes into `raw_orders`, told apart by `_source_file`. After each load the row count must equal the file's data rows. |
| Staging | `stg_stores`, `stg_products`, `stg_customers`, `stg_orders`, `stg_payments`, `stg_rejected_duplicates` | `20`–`27_*.sql`: cast types, standardise, keep one canonical row per key (extra copies → `stg_rejected_duplicates`), mask email and phone, apply every rule from section 7, set `dq_status` (PASS/WARN/FAIL), `failure_reason`, `warning_reason`. Original text of parsed columns is kept as `<column>_raw`. No staging table has `email`, `phone` or `full_name`. |
| Mart | `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers` | `40`–`44_mart_*.sql`, built only from staging rows with `dq_status` PASS or WARN. |

Staging and marts read **only this run's raw batch** (`WHERE _run_id = <this run>`, decision D11). The raw layer keeps every batch: it is the audit log. A retry or Clear of the same run first deletes that run's own batch for the file, so it is never loaded twice; no other run's rows are ever deleted.

### 6.3 Incremental load with MERGE

`stg_orders` is built in three SQL steps:

1. `24_stg_orders_build.sql`: canonical rows from this run's `orders.csv` batch, still as text.
2. `25_stg_orders_merge_increment.sql`, once per increment file (oldest first): `MERGE INTO stg_orders ... ON t.order_id = s.order_id WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`. A correction overwrites the existing order; a new order is added. Before the MERGE the task logs how many increment orders are new vs existing.
3. `26_stg_orders_apply_dq.sql`: types, standardises and validates the merged rows, so base and increment rows get exactly the same rules.

Tested with the 1 October file: **150 inserted, 10 updated** (ORD000017, ORD000822, … now RETURNED), `COUNT(*) = COUNT(DISTINCT order_id)` in `stg_orders`.

### 6.4 ETL ↔ ELT parity

Both pipelines were run on the same input, then compared row by row (`EXCEPT` both ways), not just by totals.

| Measure | ETL | ELT | Match |
|---|---:|---:|---|
| Orders passing (September files) | 2,308 | 2,308 | yes, same `order_id` set and same values |
| Delivered orders | 2,086 | 2,086 | yes |
| September net revenue | ₹11,34,962.80 | ₹11,34,962.80 | yes |
| Gross margin | ₹2,60,826.90 | ₹2,60,826.90 | yes |
| Rows set aside (quarantine / FAIL + duplicates) | 151 | 151 | yes, per file |
| Reconciliation rows | 184 | 184 | yes, identical rows and reasons |
| `dq_results` | 41 checks | 41 checks | identical counts and statuses |
| Masked customers | 400 | 400 | identical masked email/phone/tier |
| After the increment: orders passing | 2,458 | 2,458 | yes |
| After the increment: net revenue (current-restated) | ₹12,10,421.30 | ₹12,10,421.30 | yes |

They match because every rule is defined once (`config.py`) and mirrored exactly: strict timestamp format, values read as text in both, the same duplicate copy kept, the same reason codes, and decision D7 applied in both.

## 7. Part D: Data quality dimensions

Every check below runs in both pipelines with the same `check_name` (pandas in `include/nammamart/quality.py`, SQL in the ELT staging files). **Critical** rows are quarantined (ETL) or get `dq_status = 'FAIL'` (ELT); **Warning** rows load and are counted. A row can fail several rules: all reason codes are kept, separated by `;`. Standardisation runs before validation (`UPPER(TRIM(status))`, payment-mode alias map, title-case loyalty tier).

| check_name | Dimension | Severity | Rule (reason code) |
|---|---|---|---|
| `orders_required_fields` | Completeness | Critical | order_id, customer_id, store_id, product_id, order_ts non-blank (`missing_<col>`) |
| `orders_delivered_has_minutes` | Completeness | Critical | DELIVERED ⇒ delivery_minutes present (`delivered_missing_delivery_minutes`) |
| `products_category_present` | Completeness | Critical | category non-blank (`missing_category`) |
| `stores_zone_present` | Completeness | Warning | zone non-blank, else `Unknown` (`missing_zone`) |
| `orders_volume_vs_previous` | Completeness (volume) | Warning | orders this run < 10% of the previous run's `orders_required_fields.rows_checked` (dataset-level: rows_checked = 1) |
| `orders_order_id_unique` | Uniqueness | Critical | extra copies quarantined, first kept (`duplicate_order_id`) |
| `payments_payment_id_unique` | Uniqueness | Critical | `duplicate_payment_id` |
| `customers_customer_id_unique` | Uniqueness | Critical | keep the copy with valid email and phone, else the first (`duplicate_customer_id`) |
| `products_product_id_unique` / `stores_store_id_unique` | Uniqueness | Critical | `duplicate_product_id` / `duplicate_store_id` |
| `orders_status_valid` | Validity | Critical | status ∈ DELIVERED, CANCELLED, RETURNED after standardising (`invalid_order_status`) |
| `orders_status_standardised` | Validity | Warning | raw value differed from the standard one (`order_status_standardised`) |
| `orders_payment_mode_valid` / `payments_payment_mode_valid` | Validity | Critical | mode ∈ allowed list after aliasing (`invalid_payment_mode`) |
| `orders_payment_mode_standardised` / `payments_payment_mode_standardised` | Validity | Warning | `payment_mode_standardised` |
| `orders_ts_parses` / `payments_ts_parses` | Validity | Critical | strict `YYYY-MM-DD HH:MM:SS` (`invalid_order_ts`, `invalid_payment_ts`) |
| `orders_in_expected_period` | Validity | Critical | orders.csv in 1–30 Sept; increment rows not after the file date (`outside_expected_period`) |
| `orders_discount_range` | Validity | Critical | 0 ≤ discount_pct ≤ 100 (`invalid_discount_pct`) |
| `customers_email_format` | Validity | Warning | `^[^@\s]+@[^@\s]+\.[^@\s]+$` (`invalid_email`; masked value becomes NULL) |
| `customers_phone_10_digits` | Validity | Warning | `^\d{10}$` (`invalid_phone`) |
| `customers_pincode_6_digits` / `stores_pincode_6_digits` | Validity | Warning | `^\d{6}$` (`invalid_pincode`) |
| `orders_quantity_positive` | Accuracy | Critical | integer and > 0 (`invalid_quantity`, `nonpositive_quantity`) |
| `orders_amount_formula` | Accuracy | Critical | \|round(quantity × unit_price × (1 − discount_pct/100), 2) − order_amount\| ≤ 0.01 (`order_amount_mismatch`) |
| `orders_delivery_max` | Accuracy | Critical | delivery_minutes integer and ≤ 120 (`invalid_delivery_minutes`, `delivery_minutes_over_120`) |
| `orders_delivery_normal_range` | Accuracy | Warning | outside 8–35 but ≤ 120 (`delivery_minutes_outside_8_35`) |
| `payments_amount_positive` | Accuracy | Critical | SUCCESS ⇒ amount_paid > 0 (`nonpositive_amount_paid`) |
| `products_mrp_positive` / `products_cost_le_mrp` | Accuracy | Critical | mrp > 0 (`nonpositive_mrp`); cost_price ≤ mrp (`cost_above_mrp`) |
| `orders_fk_customer` / `orders_fk_product` / `orders_fk_store` | Consistency | Critical | key exists in the source master file (`unknown_customer_id`, …); a master row failing its own checks does not fail the order (D7) |
| `payments_fk_order` | Consistency | Critical | order_id exists in an orders file (`unknown_order_id`) |
| `payments_amount_matches_order` | Consistency | Warning | SUCCESS amount = order_amount ± 0.01 (`amount_differs_from_order`); also a reconciliation row |
| `customers_loyalty_tier_casing` | Consistency | Warning | raw tier ≠ title-cased tier (`loyalty_tier_standardised`) |
| `orders_not_future` / `payments_not_future` / `customers_signup_not_future` | Timeliness | Critical | not later than the run's start time in IST (`future_order_ts`, …) |
| `orders_freshness` | Timeliness | Warning | latest valid order_ts within 24 h of the reference time (D10; dataset-level: rows_checked = 1) |
| header check (extract task) | Schema | fails the task | every expected column present; missing/renamed → `AirflowFailException`, extra → logged warning |

`dq_results` gets one row per check per run, including checks with zero failures (41 rows per ETL run). `status = 'FAIL'` means "rows were quarantined", not "the DAG failed"; `WARN` means a Warning check found rows.

## 8. Part C: Scheduling

Both DAGs use:

```python
schedule="0 9 * * 1-6"                                   # 09:00, Monday to Saturday
start_date=pendulum.datetime(2026, 10, 1, tz=IST)        # IST = pendulum.timezone("Asia/Kolkata")
catchup=False
max_active_runs=1
dagrun_timeout=timedelta(minutes=25)
default_args={"retries": 2, "retry_delay": timedelta(minutes=2), "execution_timeout": timedelta(minutes=20)}
```

- **Timezone:** the cron is evaluated in the `start_date` timezone, so `0 9` means 09:00 IST.
- **`catchup=False`:** every run reprocesses the full current files (D11), so backfilling the missed weekday slots since 1 October would redo identical work, compete for the single DuckDB writer, and add nothing.
- **Retries:** 2 retries, 2 minutes apart, cover transient problems such as a file still being copied or a locked warehouse, and still finish well before the 09:30 stand-up. A schema change raises `AirflowFailException`, which skips retries, because retrying cannot fix it.
- **`execution_timeout` / `dagrun_timeout`:** a hung task or run fails visibly instead of silently blocking the morning report. Airflow 3 has no task SLA, so the 25-minute run timeout is the guard that the report is either ready or visibly failed by about 09:25.

_Answers to the four Part C questions, verified in the UI, follow in Milestone 5._

## 9. Business questions

_Milestone 5._

## 10. Part F: Observability tests

### 10.1 Run audit (`pipeline_runs`)

`audit_run` is the last task of each DAG, with `trigger_rule="all_done"`, so it runs even after a failure. It records what happened in two tables, both with delete-then-insert on `(dag_id, run_id)` so a Clear or retry replaces the rows instead of duplicating them:

- `dq_results`: in the ETL, `audit_run` writes every check from the validate step plus the volume check, so the scorecard is saved even when transform or load fails. In the ELT the checks are SQL, so `transform_staging` writes them (`30_dq_results_elt.sql`).
- `pipeline_runs`: one row per run, defined below.

| Column | ETL | ELT |
|---|---|---|
| `start_time` / `end_time` | DAG run start / audit time, IST | same |
| `rows_extracted` | rows read from all source files this run | same |
| `rows_loaded` | rows in `etl_fact_orders` | rows inserted into `raw_*` this run |
| `rows_quarantined` | rows written to quarantine files | rows with `dq_status = 'FAIL'` + extra duplicate copies |
| `status` | `FAILED` if any task produced no result; else `SUCCESS_WITH_WARNINGS` if any check is WARN/FAIL; else `SUCCESS` | same |

Task code cannot query Airflow's metadata database in Airflow 3, so the audit task reads each task's XCom: a missing result means that task did not succeed. After writing the row it raises `AirflowFailException` when anything failed. Without this, the always-successful audit leaf would turn a failed run green.

### 10.2 How the experiments were run

- Inputs were never edited by hand. `include/nammamart/make_experiment_fixtures.py` copies the five base files into `include/data/nammamart_experiments/<experiment>/` (gitignored) and applies exactly one change per folder.
- Each run was triggered through the scheduler with config, for example `{"data_dir": "include/data/nammamart_experiments/exp05_bad_rows", "as_of_date": "2026-10-01"}`. ETL first, then ELT.
- "Restore the original file" = the next run uses the default `data_dir`. Every run rebuilds its outputs from the files it is given (D11), so a happy-path run after each experiment restored every table. For Experiment 3 the increment was copied into `include/data/nammamart/` and removed afterwards.
- `dq_results` and `pipeline_runs` keep every experiment run as history.

### 10.3 Results (20 runs)

| # | DAG | Expected | Observed | Where I saw it | What I would change |
|---|---|---|---|---|---|
| 1 Happy path | ETL | All green; 5 extracts in parallel; per-file counts | All 12 tasks green in 6.2 s. The 6 extract tasks all started at +0.00 s (0.2–0.45 s each); then validate 0.3 s, transform 0.2 s, load 0.3 s, reconcile 0.3 s, reports 0.2 s. In / passed / quarantined: orders 2,412 / 2,308 / 104; payments 2,228 / 2,199 / 29; customers 405 / 397 / 8; products 152 / 142 / 10; stores 15 / 15 / 0 | Graph + Gantt view; `validate` log summary table; `pipeline_runs` | Most time is scheduler gaps (~0.7 s between tasks), not work; fine at this size |
| 1 Happy path | ELT | All green; checks in parallel; raw loads serial | All 10 tasks green in 3.4 s. The 6 check tasks started at +0.00 s (0.1–0.16 s); then load_raw 0.3 s, staging 0.5 s, marts 0.3 s, one at a time by design (single DuckDB writer). Raw 5,212 rows; staging FAIL 128 + 23 duplicate copies = 151 | Graph + Gantt; `load_raw` and `transform_staging` logs | Same |
| 2 Re-run | ETL | Fact counts and revenue unchanged | Fact 2,308 rows, ₹11,34,962.80, identical; quarantine files and `etl_rejected_rows` get a new batch for the new run | `pipeline_runs`; query on `etl_fact_orders` | — |
| 2 Re-run | ELT | Marts unchanged; raw grows | Marts identical (₹11,34,962.80); `raw_orders` grew by one 2,412-row batch. Correct: raw is an append-only ingestion log and staging reads only the current batch | `SELECT _run_id, COUNT(*) FROM raw_orders GROUP BY 1` | Partition raw by load date and set a retention period (see Part E) |
| 3 Increment | ETL | 150 inserted, 10 updated, no duplicates | `validate` log: "160 rows, inserted 150, updated 10, rejected 0". Fact 2,458 rows, 0 duplicate `order_id`. ORD000017 and ORD000822 now RETURNED (net revenue 0, `source_file` = increment). September revenue ₹11,34,962.80 → ₹11,30,769.10 (−₹4,193.70 for the 10 returns); 1 October adds ₹79,652.20 | `validate` log; fact queries | The earlier "as reported" September figure is gone (see Part E question 9) |
| 3 Increment | ELT | MERGE: 150 inserted, 10 updated | `transform_staging` log: "MERGE inserted 150, updated 10". Same 2,458 orders, same revenue split as ETL. Raw keeps both files of the run (2,412 + 160 rows). Reconciliation gains 10 `RETURNED_NOT_REFUNDED` (+₹4,193.70) and 150 `PAYMENT_NOT_YET_RECEIVED` (−₹79,652.20) | `transform_staging` log; `stg_orders`; `mart_payment_reconciliation` | Load the next day's payments file with the orders, so timing rows clear themselves |
| 4 Missing file | ETL | `extract_products` retries then fails; downstream `upstream_failed`; audit row FAILED | `extract_products` tried 3 times at 13:44:10, 13:46:10, 13:48:10 UTC (2-minute `retry_delay`, `up_for_retry` in between), `FileNotFoundError: Source file not found: …/products.csv` each time, then `failed`. Every later task `upstream_failed`. `audit_run` still ran, wrote `FAILED` (5,060 rows extracted, 0 loaded) and turned the run red. Finance tables kept the last good load | Grid view (task states, try numbers); task log; `pipeline_runs` | Nothing alerted anyone: someone has to open the UI. Add a failure alert (the trainer's Slack notifier or email) and an alert on a *missing* 09:00 run |
| 4 Missing file | ELT | Same on `check_products` | Same pattern: 3 attempts 2 minutes apart, then `failed`; `load_raw` onwards `upstream_failed`; audit `FAILED`; nothing written to raw | Same | Same |
| 5 New bad data | ETL | Each of the 5 rows quarantined with the right reason | ORD900001 `missing_customer_id`; ORD900002 `nonpositive_quantity`; ORD900003 `unknown_store_id`; ORD900004 `invalid_order_status` (SHIPPED); ORD900005 `outside_expected_period;future_order_ts`. `dq_results` +1 on exactly the matching checks: required_fields 19→20, quantity_positive 10→11, fk_store 5→6, status_valid 3→4, in_expected_period 7→8, not_future 5→6 | `orders_failed_<run>.csv`; `dq_results` compared with the happy path | — |
| 5 New bad data | ELT | Each row in `raw_orders`, FAIL in `stg_orders` with the reason | All 5 rows kept in `raw_orders` as received and FAIL in `stg_orders` with the same reasons as the ETL; same `dq_results` deltas | `stg_orders WHERE dq_status = 'FAIL'`; `dq_results` | — |
| 6 Schema change | ETL | Loud failure, no retries | `extract_orders` failed on attempt 1 (no retries: `AirflowFailException`): "Schema check failed for orders.csv: missing column(s) ['order_amount']; header was [… 'total' …]". Downstream `upstream_failed`, audit `FAILED`, nothing loaded | Task log; Grid view | Loud is right. A data contract agreed with the OMS team would turn this into a planned change |
| 6 Schema change | ELT | Loud failure before raw | `check_orders` failed the same way on attempt 1; 0 rows in `raw_orders` for that run. Had the raw load used `SELECT *` / `BY NAME`, `order_amount` would have loaded as NULL and every order would have failed the formula check instead: a confusing, silent-looking failure | Task log; raw row count for the run | Same |
| 7 Volume (50 orders) | ETL | Volume warning | `orders_volume_vs_previous` = WARN (50 vs 2,405 in the previous run, −98%). But the run stayed green and `etl_fact_orders` was **replaced by 48 orders**: the COO's report would have shown ~2% of the real business | `dq_results`; `pipeline_runs` (`SUCCESS_WITH_WARNINGS`) | Make a >90% drop Critical: stop before load so the previous day's tables stay published, and alert |
| 7 Volume (50 orders) | ELT | Volume warning | Same WARN; marts rebuilt from 48 orders; raw kept the 50-row batch | Same | Same: skip `transform_marts` when volume fails |
| 8 Distribution drift (S03 price ×100) | ETL | Some check catches it | Caught only because `order_amount` no longer matched the formula: `orders_amount_formula` 18 → 175 (+157 S03 orders). All of S03 was quarantined, so S03 vanished from the report: North revenue ₹2,26,807.50 → ₹1,49,552.65; total ₹11,34,962.80 → ₹10,57,707.95 (−₹77,254.85, −6.8%); `ORDER_REJECTED` reconciliation rows 90 → 234 | `dq_results`; `etl_daily_zone_revenue`; reconciliation | If the source had also multiplied `order_amount`, nothing would catch it and S03 revenue would be ×100. Add a distribution check: `unit_price` vs catalogue `mrp`, or each store's average order value vs its history |
| 8 Distribution drift | ELT | Same | Identical numbers in `mart_daily_store_kpis` and `stg_orders` (FAIL, `order_amount_mismatch`, raw `unit_price` 20500.0 kept for audit) | Same | Same |
| 9 Freshness (30 days old) | ETL | Timeliness warning | `orders_freshness` = WARN; log "latest valid order 2026-09-30 23:46:37, reference 2026-10-31 09:00, lag 729.2 h (limit 24 h)". Run still green | `validate` log; `dq_results` | A WARN on stale data should alert (or block) — month-old numbers at the 09:30 review is the problem this project exists to fix |
| 9 Freshness | ELT | Same | Same WARN, same lag | `transform_staging` log; `dq_results` | Same |
| 10 Recovery | ETL | Only the failed task and downstream re-run | After putting `products.csv` back in the same fixture folder, **Clear** on `extract_products` with downstream (Airflow REST API `clearTaskInstances`, the call behind the UI button). The 5 other extracts kept their original state (attempt 1). `extract_products` ran as attempt 4, then validate → reports, `audit_run` attempt 2. `pipeline_runs` still has **one** row for the run, updated to `SUCCESS_WITH_WARNINGS`. Works because the failed run's intermediate files were kept | Grid view try numbers; `pipeline_runs` | — |
| 10 Recovery | ELT | Same | Cleared `check_products`, `load_raw`, `transform_staging`, `transform_marts`, `audit_run`; the other checks untouched; run green, one audit row | Same | — |

### 10.4 Five pillars of data observability

| Pillar | What covers it | Observed |
|---|---|---|
| Freshness | `orders_freshness` (lag of the latest valid order vs 09:00 on the reference day) | Caught the 30-day-old data (729 h) — but only as a WARN in a green run |
| Volume | `orders_volume_vs_previous` | Caught the −98% drop — but the 48-order report was still published |
| Schema | Header check in every extract / check task, `AirflowFailException` | Strong: failed at once with the exact missing column, no retries, nothing loaded |
| Distribution | Rule checks only: formula, delivery-minute limits, mrp/cost | Caught the ×100 price only indirectly; no statistical check would catch a consistent shift |
| Lineage | ELT: `_source_file`, `_source_row`, `_run_id` on every raw row and carried into staging; ETL: `source_file`, `source_row`, `run_id` on the fact and in quarantine files | Every failed row traced to its file and line in both; ELT can also show the row exactly as received for any past run |

**Best covered: schema** (and lineage in the ELT). A broken file fails loudly and safely every time.
**Weakest: distribution**, then freshness/volume *alerting*: those checks detect the problem but don't stop publication or tell anyone.

## 11. Part E: Data governance

_Milestone 5._

## 12. Part G: Recommendation to Sameer (CTO)

_Milestone 5._

## 13. Screenshots

_Milestone 5._
