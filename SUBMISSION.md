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

- Input folder: `include/data/nammamart/` (the five base files). The 1 October increment is added in Experiment 3 by copying `assignment/nammamart_dataset/orders_2026-10-01_increment.csv` into that folder.
- Warehouse: `include/warehouse_samarth.duckdb` (gitignored).

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

_Milestone 2._

## 6. Part B: ELT pipeline

_Milestone 3._

## 7. Part D: Data quality dimensions

_Milestone 2–3._

## 8. Part C: Scheduling

_Milestone 2 (configuration) and 5 (answers verified in the UI)._

## 9. Business questions

_Milestone 5._

## 10. Part F: Observability tests

_Milestone 4._

## 11. Part E: Data governance

_Milestone 5._

## 12. Part G: Recommendation to Sameer (CTO)

_Milestone 5._

## 13. Screenshots

_Milestone 5._
