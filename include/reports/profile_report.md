# NammaMart data profile

| file | rows | columns |
|---|---:|---:|
| orders.csv | 2412 | 13 |
| payments.csv | 2228 | 7 |
| customers.csv | 405 | 10 |
| products.csv | 152 | 10 |
| stores.csv | 15 | 8 |
| orders_2026-10-01_increment.csv | 160 | 13 |
| **total** | **5372** | |

## orders.csv: 2412 rows × 13 columns

- Header matches expected: **True**
- Blank values per column: {'order_ts': 1, 'customer_id': 18, 'delivery_minutes': 164}
- Literal N/A/NULL per column: {'order_ts': 1}
- Leading/trailing spaces per column: {'payment_mode': 2}
- Duplicate `order_id`: 12 extra copies | exact duplicate rows: 12
  - ORD000539: 2 copies, differing columns: none (exact)
  - ORD000783: 2 copies, differing columns: none (exact)
  - ORD001110: 2 copies, differing columns: none (exact)
  - ORD001239: 2 copies, differing columns: none (exact)
  - ORD001326: 2 copies, differing columns: none (exact)
  - ORD001434: 2 copies, differing columns: none (exact)
  - ORD001441: 2 copies, differing columns: none (exact)
  - ORD001462: 2 copies, differing columns: none (exact)
  - ORD001549: 2 copies, differing columns: none (exact)
  - ORD001692: 2 copies, differing columns: none (exact)
  - ORD001870: 2 copies, differing columns: none (exact)
  - ORD002129: 2 copies, differing columns: none (exact)

| column | profile |
|---|---|
| order_id | distinct=2400 sample='ORD001913', 'ORD001791', 'ORD002203' |
| order_ts | min=2026-09-01 06:30:15 max=2027-01-01 09:00:00 unparsable='N/A', '2026-13-05 11:00:00', '2026-13-05 11:00:00', '', '31/09/2026 10:00', '31/09/2026 10:00' |
| customer_id | distinct=401 sample='C00005', 'C00369', 'C00010' |
| store_id | distinct=18 sample='S10', 'S05', 'S08' |
| product_id | distinct=159 sample='P0023', 'P0136', 'P0135' |
| quantity | min=-3 max=5 non-numeric= |
| unit_price | min=10.0 max=883.0 non-numeric= |
| discount_pct | min=0 max=250 non-numeric= |
| order_amount | min=-1593.0 max=9576.0 non-numeric= |
| payment_mode | `'UPI'`:1329, `'Card'`:493, `'Cash on Delivery'`:326, `'Wallet'`:254, `'upi'`:4, `'Cashh'`:2, `'UPI '`:2, `'CARD'`:1, `'cod'`:1 |
| order_status | `'DELIVERED'`:2182, `'CANCELLED'`:156, `'RETURNED'`:69, `'Pending??'`:3, `'delivered'`:2 |
| channel | `'App'`:2044, `'Web'`:368 |
| delivery_minutes | min=8.0 max=1440.0 non-numeric= |

## payments.csv: 2228 rows × 7 columns

- Header matches expected: **True**
- Blank values per column: {'gateway_ref': 301}
- Literal N/A/NULL per column: none
- Leading/trailing spaces per column: {'payment_mode': 2}
- Duplicate `payment_id`: 4 extra copies | exact duplicate rows: 4
  - PAY0000509: 2 copies, differing columns: none (exact)
  - PAY0000755: 2 copies, differing columns: none (exact)
  - PAY0001651: 2 copies, differing columns: none (exact)
  - PAY0002000: 2 copies, differing columns: none (exact)

| column | profile |
|---|---|
| payment_id | distinct=2224 sample='PAY0001316', 'PAY0000574', 'PAY0000133' |
| order_id | distinct=2224 sample='ORD001367', 'ORD001855', 'ORD000027' |
| payment_ts | min=2026-09-01 06:30:24 max=2027-01-01 09:01:46 unparsable= |
| payment_mode | `'UPI'`:1238, `'Card'`:445, `'Cash on Delivery'`:301, `'Wallet'`:234, `'upi'`:4, `'UPI '`:2, `'Cashh'`:2, `'cod'`:1, `'CARD'`:1 |
| amount_paid | min=-1593.0 max=9576.0 non-numeric= |
| payment_status | `'SUCCESS'`:2116, `'REFUNDED'`:68, `'FAILED'`:44 |
| gateway_ref | distinct=1924 sample='GW8048515503', 'GW6532349731', '' |

## customers.csv: 405 rows × 10 columns

- Header matches expected: **True**
- Blank values per column: {'city': 6}
- Literal N/A/NULL per column: {'email': 4}
- Leading/trailing spaces per column: none
- Duplicate `customer_id`: 5 extra copies | exact duplicate rows: 0
  - C00036: 2 copies, differing columns: ['phone', 'area', 'pincode']
  - C00103: 2 copies, differing columns: ['phone', 'area', 'pincode']
  - C00226: 2 copies, differing columns: ['phone']
  - C00252: 2 copies, differing columns: ['phone', 'area', 'pincode']
  - C00313: 2 copies, differing columns: ['phone', 'area', 'pincode']

| column | profile |
|---|---|
| customer_id | distinct=400 sample='C00199', 'C00182', 'C00355' |
| full_name | personal data, values not shown: distinct=280 blank=0 |
| email | personal data, values not shown: distinct=397 blank=0 |
| phone | personal data, values not shown: distinct=405 blank=0 |
| city | `'Bengaluru'`:399, `''`:6 |
| area | distinct=15 sample='Whitefield', 'Hebbal', 'RT Nagar' |
| pincode | min=0.0 max=560102.0 non-numeric='5600XX', '5600XX', '5600XX' |
| signup_date | min=2024-06-01 00:00:00 max=2027-01-05 00:00:00 unparsable= |
| loyalty_tier | `'Bronze'`:185, `'Silver'`:133, `'Gold'`:54, `'Platinum'`:25, `'BRONZE'`:3, `'bronze'`:3, `'SILVER'`:2 |
| is_active | `'Y'`:352, `'N'`:53 |

## products.csv: 152 rows × 10 columns

- Header matches expected: **True**
- Blank values per column: {'category': 3}
- Literal N/A/NULL per column: none
- Leading/trailing spaces per column: none
- Duplicate `product_id`: 2 extra copies | exact duplicate rows: 0
  - P0030: 2 copies, differing columns: ['product_name', 'mrp']
  - P0140: 2 copies, differing columns: ['product_name', 'mrp']

| column | profile |
|---|---|
| product_id | distinct=150 sample='P0001', 'P0002', 'P0003' |
| product_name | distinct=152 sample='Desi Farms Fresh Vegetables 1', 'Desi Farms Fresh Fruits 2', 'Desi Farms Fresh Fruits 3' |
| category | `'Dairy & Bakery'`:26, `'Household'`:26, `'Fruits & Vegetables'`:25, `'Snacks & Beverages'`:25, `'Personal Care'`:25, `'Staples'`:22, `''`:3 |
| sub_category | `'Detergent'`:14, `'Shampoo'`:10, `'Fresh Fruits'`:10, `'Soft Drinks'`:8, `'Herbs'`:8, `'Kitchen Supplies'`:8, `'Milk'`:8, `'Soap'`:8, `'Rice'`:8, `'Tea & Coffee'`:8, `'Fresh Vegetables'`:7, `'Oral Care'`:7, `'Atta'`:7, `'Biscuits'`:7, `'Curd & Paneer'`:7, `'Bread'`:6, `'Oil'`:5, `'Dal'`:5, `'Cheese'`:5, `'Cleaners'`:4, `'Chips'`:2 |
| brand | distinct=23 sample='Desi Farms', 'Desi Farms', 'Desi Farms' |
| mrp | min=-203.0 max=883.0 non-numeric= |
| cost_price | min=6.79 max=631.79 non-numeric= |
| unit | `'pack'`:76, `'unit'`:51, `'kg'`:25 |
| is_perishable | `'False'`:101, `'True'`:51 |
| launch_date | min=2023-01-03 00:00:00 max=2025-09-20 00:00:00 unparsable= |

## stores.csv: 15 rows × 8 columns

- Header matches expected: **True**
- Blank values per column: {'zone': 1}
- Literal N/A/NULL per column: none
- Leading/trailing spaces per column: none
- Duplicate `store_id`: 0 extra copies | exact duplicate rows: 0

| column | profile |
|---|---|
| store_id | distinct=15 sample='S01', 'S02', 'S03' |
| store_name | distinct=15 sample='NammaMart Hebbal', 'NammaMart Yelahanka', 'NammaMart RT Nagar' |
| zone | `'East'`:4, `'West'`:4, `'North'`:3, `'South'`:3, `''`:1 |
| area | distinct=15 sample='Hebbal', 'Yelahanka', 'RT Nagar' |
| pincode | min=560003 max=560102 non-numeric= |
| open_date | min=2024-02-14 00:00:00 max=2025-07-28 00:00:00 unparsable= |
| manager_name | personal data, values not shown: distinct=15 blank=0 |
| capacity_orders_per_day | min=0 max=600 non-numeric= |

## orders_2026-10-01_increment.csv: 160 rows × 13 columns

- Header matches expected: **True**
- Blank values per column: none
- Literal N/A/NULL per column: none
- Leading/trailing spaces per column: none
- Duplicate `order_id`: 0 extra copies | exact duplicate rows: 0

| column | profile |
|---|---|
| order_id | distinct=160 sample='ORD000017', 'ORD000271', 'ORD000332' |
| order_ts | min=2026-09-01 09:38:13 max=2026-10-01 22:33:00 unparsable= |
| customer_id | distinct=143 sample='C00298', 'C00003', 'C00346' |
| store_id | distinct=15 sample='S12', 'S06', 'S12' |
| product_id | distinct=98 sample='P0081', 'P0113', 'P0035' |
| quantity | min=1 max=4 non-numeric= |
| unit_price | min=10.0 max=883.0 non-numeric= |
| discount_pct | min=0 max=20 non-numeric= |
| order_amount | min=11.0 max=2052.0 non-numeric= |
| payment_mode | `'UPI'`:46, `'Card'`:44, `'Cash on Delivery'`:40, `'Wallet'`:30 |
| order_status | `'DELIVERED'`:150, `'RETURNED'`:10 |
| channel | `'App'`:160 |
| delivery_minutes | min=8 max=34 non-numeric= |

# Cross-file checks

## Referential integrity (anti-joins)

- orders.csv.customer_id not in customers: **0** — 
- orders.csv.product_id not in products: **9** — ('P9455', 1), ('P9212', 1), ('P9816', 1), ('P9893', 1), ('P9989', 1), ('P9784', 1), ('P9822', 1), ('P9399', 1), ('P9761', 1)
- orders.csv.store_id not in stores: **5** — ('S17', 3), ('S99', 1), ('S00', 1)
- orders_2026-10-01_increment.csv.customer_id not in customers: **0** — 
- orders_2026-10-01_increment.csv.product_id not in products: **0** — 
- orders_2026-10-01_increment.csv.store_id not in stores: **0** — 
- payments.order_id not in orders: **8** — ORD970544, ORD968496, ORD988106, ORD941071, ORD981600, ORD997176, ORD953136, ORD966342
- orders with no payment, by status: {'CANCELLED': 156, 'DELIVERED': 27, 'RETURNED': 1}

## Business rules


### Business rules: orders.csv

- blank customer_id: **18** — ORD002255, ORD000701, ORD000997, ORD000580, ORD002019, ORD002262, ORD002149, ORD002367, ORD001332, ORD001918, ORD002156, ORD000583, ORD000790, ORD000764, ORD002219, ORD001532, ORD000022, ORD001373
- quantity <= 0: **10** — ORD000469, ORD001296, ORD001459, ORD000867, ORD000462, ORD002360, ORD001724, ORD000099, ORD001124, ORD001384
- discount_pct outside 0–100: **3** — ORD002103, ORD001468, ORD001237
- order_amount ≠ formula (±0.01): **18** — ORD002103, ORD002305, ORD002004, ORD000845, ORD000964, ORD002298, ORD002031, ORD000801, ORD000880, ORD001274, ORD000677, ORD002223, ORD002128, ORD001624, ORD002244, ORD001755, ORD001468, ORD001237
- order_status raw not in allowed (before standardising): **5** — ORD001066, ORD000500, ORD000153, ORD001970, ORD001637
- order_status invalid after UPPER(TRIM): **3** — ORD000500, ORD001970, ORD001637
- payment_mode raw not in allowed: **10** — ORD001812, ORD001414, ORD000159, ORD001364, ORD000433, ORD002356, ORD001640, ORD001874, ORD000934, ORD000691
- payment_mode invalid after aliasing: **2** — ORD001812, ORD001414
- order_ts unparsable (strict): **6** — ORD000403, ORD001610, ORD000534, ORD000362, ORD002012, ORD000946
- DELIVERED without delivery_minutes: **8** — ORD001554, ORD001800, ORD000731, ORD001277, ORD000326, ORD001515, ORD002229, ORD000878
- delivery_minutes > 120: **6** — ORD001037, ORD002101, ORD000455, ORD001104, ORD000020, ORD000424
- delivery_minutes outside 8–35 (non-blank): **6** — ORD001037, ORD002101, ORD000455, ORD001104, ORD000020, ORD000424
- CANCELLED with blank delivery_minutes (expected): **156** 
- order_ts outside 1–30 Sept: **7** — ORD002119, ORD001693, ORD001089, ORD001121, ORD001765, ORD001965, ORD001725
- order_ts after 2026-10-21 (future at profiling date): **5** — ORD002119, ORD001121, ORD001765, ORD001965, ORD001725
- non-standard `order_status` values: {"'Pending??'": 3, "'delivered'": 2}
- non-standard `payment_mode` values: {"'upi'": 4, "'Cashh'": 2, "'UPI '": 2, "'CARD'": 1, "'cod'": 1}
- non-standard `order_ts` values: {"'2026-13-05 11:00:00'": 2, "'31/09/2026 10:00'": 2, "'N/A'": 1, "''": 1}
- order_amount / formula ratio on mismatches: {10.0: 6, 1.5: 5, 0.5: 4, -0.67: 1, -4.0: 1, -1.6: 1}

### Business rules: orders_2026-10-01_increment.csv

- blank customer_id: **0** 
- quantity <= 0: **0** 
- discount_pct outside 0–100: **0** 
- order_amount ≠ formula (±0.01): **0** 
- order_status raw not in allowed (before standardising): **0** 
- order_status invalid after UPPER(TRIM): **0** 
- payment_mode raw not in allowed: **0** 
- payment_mode invalid after aliasing: **0** 
- order_ts unparsable (strict): **0** 
- DELIVERED without delivery_minutes: **0** 
- delivery_minutes > 120: **0** 
- delivery_minutes outside 8–35 (non-blank): **0** 
- CANCELLED with blank delivery_minutes (expected): **0** 
- order_ts after 2026-10-01 23:59:59: **0** 
- order_ts after 2026-10-21 (future at profiling date): **0** 
- non-standard `order_status` values: {}
- non-standard `payment_mode` values: {}
- non-standard `order_ts` values: {}
- order_amount / formula ratio on mismatches: {}

### payments vs orders

- SUCCESS with amount_paid <= 0: **10** — ORD001031, ORD001384, ORD000469, ORD001459, ORD000462, ORD000867, ORD001724, ORD000099, ORD002360, ORD001296
- SUCCESS amount ≠ order_amount: **14** — ORD001031, ORD001937, ORD001840, ORD001864, ORD001753, ORD001832, ORD001360, ORD000481, ORD001039, ORD000371, ORD001812, ORD001572, ORD000178, ORD002185
- payment_mode differs between payment and order (after aliasing): **2**

Order status × payment status (deduplicated payments):

```
payment_status  FAILED  REFUNDED  SUCCESS
order_status                             
DELIVERED           44         0     2101
PENDING??            0         0        3
RETURNED             0        68        0
```

- DELIVERED orders whose payments are all FAILED: **44**
- DELIVERED orders with no payment: **27**
- payment_ts after 2026-10-21: **5**
- blank gateway_ref by payment_mode: {'Cash on Delivery': 301}

### products

- mrp <= 0: P0109
- cost_price > mrp: P0019, P0073, P0074, P0109, P0117
- blank category: P0051, P0060, P0068
- orders where unit_price ≠ product mrp (first copy): **11**
  - P0030: copies' mrp=['50.0', '45.0'], order unit_price={'50.0': 17}
  - P0140: copies' mrp=['364.0', '328.0'], order unit_price={'364.0': 18}
- September orders on a product that fails its own checks: **126**

### customers

- invalid email (values not shown): {'missing @': 5, 'N/A': 4, 'no dot in domain': 3} — customers C00337, C00307, C00063, C00045, C00363, C00310, C00327, C00112, C00193, C00392, C00062, C00067
- phone not 10 digits (values not shown): digit counts {9: 5, 8: 1, 7: 1} — customers C00151, C00240, C00095, C00357, C00130, C00290, C00339
- pincode not 6 digits: '5600XX', '5600XX', '5600XX'
- loyalty_tier needing case fix: {'BRONZE': 3, 'bronze': 3, 'SILVER': 2}
- signup_date after 2026-10-21: [{'customer_id': 'C00400', 'signup_date': '2027-01-05'}, {'customer_id': 'C00398', 'signup_date': '2026-11-12'}, {'customer_id': 'C00038', 'signup_date': '2026-12-20'}]
- September orders by those customers: **18**
- customers whose first order precedes signup_date: **3**

### stores

- blank zone: S07 (orders on them: 176)
- capacity_orders_per_day <= 0: S12

### orders_2026-10-01_increment.csv vs orders.csv

- new order_ids: **150** (ORD002401–ORD002550)
- existing order_ids (corrections): **10**
  - ORD000017: {'order_status': "'DELIVERED'→'RETURNED'"}
  - ORD000271: {'order_status': "'DELIVERED'→'RETURNED'"}
  - ORD000332: {'order_status': "'DELIVERED'→'RETURNED'"}
  - ORD000337: {'order_status': "'DELIVERED'→'RETURNED'"}
  - ORD000584: {'order_status': "'DELIVERED'→'RETURNED'"}
  - ORD000708: {'order_status': "'DELIVERED'→'RETURNED'"}
  - ORD000822: {'order_status': "'DELIVERED'→'RETURNED'"}
  - ORD001025: {'order_status': "'DELIVERED'→'RETURNED'"}
  - ORD001067: {'order_status': "'DELIVERED'→'RETURNED'"}
  - ORD001990: {'order_status': "'DELIVERED'→'RETURNED'"}
- increment rows on products failing own checks: ORD000822, ORD002431, ORD002483, ORD002516, ORD002482, ORD002491, ORD002542, ORD002411, ORD002526, ORD002477, ORD002465, ORD002544, ORD002443, ORD002422
