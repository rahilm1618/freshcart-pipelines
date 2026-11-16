# Part B1 & B2: FreshCart Baseline ELT Execution & Record Tracing

**Date:** 2026-11-15  
**Platform:** Snowflake Data Cloud (`ap-southeast-7.aws`, Account: `fk57668`)  
**Warehouse:** `FRESHCART_WH` | **Database:** `FRESHCART`

---

## B1. Baseline Execution Results

The pipeline was executed top-to-bottom from a clean state using the instructor-supplied ELT flow:
$$\text{Extract (API mock)} \longrightarrow \text{Load (PUT + COPY INTO)} \longrightarrow \text{Transform (SQL Silver, Dim, Ref, Gold)} \longrightarrow \text{Check (quality\_checks)}$$

### Verification Table: Expected vs Actual

| Metric / Check | Expected Result | Actual Snowflake Output | Status |
|:---|:---:|:---:|:---:|
| **Raw rows in `bronze.orders_raw`** | 8 (6 web + 2 mobile) | **8 (6 web + 2 mobile)** | ✅ **MATCH** |
| **Rows passing cleaning in `silver.orders_clean`** | 6 | **6** | ✅ **MATCH** |
| **Quarantined rows in `quarantine.orders`** | 2 (`ORD-1004`, `ORD-1006`) | **2 (`ORD-1004`, `ORD-1006`)** | ✅ **MATCH** |
| **Orders after deduplication (`silver.orders_dedup`)** | 4 | **4** | ✅ **MATCH** |
| **Gold orders (`gold.fct_orders`)** | 4 | **4** | ✅ **MATCH** |
| **Gold revenue (`gold.fct_orders`)** | ₹7,785.00 | **₹7,785.00** | ✅ **MATCH** |
| **Gold loyalty points (`gold.fct_orders`)** | 126 | **126** | ✅ **MATCH** |
| **Share of rows rejected** | 25% | **25%** | ✅ **MATCH** |

### Data Quality Checks Output
```text
rows gained or lost while enriching    0   OK
duplicate orders in gold               0   OK
orders with no customer match          0   OK
orders with an amount but no FX rate   0   OK
orders with no city                    0   OK
share of rows rejected                 25%
GOLD: 4 orders, revenue ₹7785.00, 126 loyalty points
```

---

## B2. Deep-Dive Record Tracing: ORD-1003 & ORD-1005

### 1. Tracing Order ORD-1003 (Deduplication & Repair)

#### A. Raw Ingest in `bronze.orders_raw`
`ORD-1003` was ingested twice from two independent channels:
- **Web Order (`ingest_seq = 2`):**
  ```json
  {
    "order_id": "ORD-1003",
    "cust_email": "meera@outlook.com",
    "cust_name": "MEERA S",
    "order_date": "2026-09-02",
    "amount": null,
    "currency": "INR",
    "pincode": "400050",
    "updated_at": "2026-09-02 09:10:00"
  }
  ```
- **Mobile Order (`ingest_seq = 8`):**
  ```json
  {
    "orderId": "ORD-1003",
    "customer": {
      "email": "Meera@Outlook.com",
      "name": "Meera S"
    },
    "orderDate": "02/09/2026",
    "total": {
      "value": "2,100.00",
      "currency": "INR"
    },
    "deliveryPin": "400050",
    "updatedAt": "2026-09-02T18:40:00"
  }
  ```

#### B. Why Web and Mobile Field Names Differ
The website and mobile applications were created by different engineering teams or client SDKs with distinct serialization conventions:
- The **Web API** produces flat JSON using `snake_case` attributes (`order_id`, `cust_email`, `order_date`, `amount`, `pincode`, `updated_at`).
- The **Mobile App API** produces nested JSON using `camelCase` attributes (`orderId`, `customer.email`, `orderDate`, `total.value`, `deliveryPin`, `updatedAt`).

#### C. SQL Normalization in `silver.orders_checked`
In `SQL_10_ORDERS_CLEAN`, the `WITH unified` CTE unifies both schemas:
- **Mapping expressions:**
  - `raw:order_id::STRING` $\leftrightarrow$ `raw:orderId::STRING`
  - `raw:cust_email::STRING` $\leftrightarrow$ `raw:customer.email::STRING`
  - `raw:order_date::STRING` $\leftrightarrow$ `raw:orderDate::STRING`
  - `raw:amount::STRING` $\leftrightarrow$ `raw:total.value::STRING`
  - `raw:pincode::STRING` $\leftrightarrow$ `raw:deliveryPin::STRING`
  - `raw:updated_at::STRING` $\leftrightarrow$ `raw:updatedAt::STRING`
- **Data type casting & cleansing:**
  - **Date format harmonization:**
    ```sql
    COALESCE(
      TRY_TO_DATE(order_date, 'YYYY-MM-DD'),
      TRY_TO_DATE(order_date, 'DD/MM/YYYY'),    -- parses mobile day-first '02/09/2026'
      TRY_TO_DATE(order_date, 'MON DD, YYYY')
    ) AS order_date
    ```
  - **Amount sanitization:**
    ```sql
    TRY_TO_DECIMAL(REGEXP_REPLACE(amount, '[^0-9.-]', ''), 12, 2) AS amount
    ```
    Strips commas (`2,100.00` $\rightarrow$ `2100.00`) and handles the null web amount cleanly.
  - **Email normalization:** `LOWER(TRIM(cust_email))` converts `Meera@Outlook.com` to `meera@outlook.com`.

#### D. Deduplication in `SQL_20_ORDERS_DEDUP`: QUALIFY ROW_NUMBER()
```sql
CREATE OR REPLACE TABLE silver.orders_dedup AS
SELECT *
FROM silver.orders_clean
QUALIFY ROW_NUMBER() OVER (
          PARTITION BY order_id
          ORDER BY updated_at DESC, ingest_seq DESC
        ) = 1;
```
- **Evaluation for ORD-1003:**
  - Record 1 (`web`, `ingest_seq=2`): `updated_at = 2026-09-02 09:10:00`
  - Record 2 (`mobile`, `ingest_seq=8`): `updated_at = 2026-09-02 18:40:00`
- **Which survives:** The mobile record has a later timestamp (`18:40:00 > 09:10:00`), so it ranks #1 (`ROW_NUMBER() = 1`). The incomplete web record (with `amount = null`) is automatically discarded.

#### E. Gold Metrics Calculation for ORD-1003
- **Amount & FX Rate:** `amount = 2100.00`, `currency = 'INR'`. Joined to `ref.fx_daily` where `rate_to_inr = 1.0000`.
  $$\text{amount\_inr} = 2100.00 \times 1.0000 = \mathbf{2,100.00}$$
- **Customer & Tier:** Joined to `dim.customers_scd2` for `meera@outlook.com` on `order_date = 2026-09-02`.
  - Meera has been **Platinum** since `2022-11-01`.
  - Joined to `ref.tier_multiplier`: Platinum multiplier = **3**.
- **Loyalty Points Arithmetic:**
  $$\text{loyalty\_points} = \lfloor \frac{\text{amount\_inr}}{100} \rfloor \times \text{multiplier} = \lfloor \frac{2100.00}{100} \rfloor \times 3 = 21 \times 3 = \mathbf{63}$$

---

### 2. Tracing Order ORD-1005 vs ORD-1001 (Point-in-Time SCD Type 2 Join)

Both orders belong to customer Priya (`CUST-01`, `priya.r@gmail.com`).

#### Customer Tier History in `dim.customers_scd2`
| CUST_ID | EMAIL | TIER | VALID_FROM | VALID_TO | IS_CURRENT |
|:---:|:---:|:---:|:---:|:---:|:---:|
| `CUST-01` | `priya.r@gmail.com` | **Silver** | `2023-04-10` | `2026-09-02` | `False` |
| `CUST-01` | `priya.r@gmail.com` | **Gold** | `2026-09-03` | `9999-12-31` | `True` |

#### Relevant Table & Point-in-Time Join Condition
Defined in `SQL_40_GOLD`:
```sql
FROM silver.orders_dedup o
LEFT JOIN dim.customers_scd2 c
       ON c.email = o.email
      AND o.order_date BETWEEN c.valid_from AND c.valid_to
LEFT JOIN ref.tier_multiplier m
       ON m.tier = c.tier
```

#### Why ORD-1001 Earns Points via Silver Multiplier
- **`order_date`:** `2026-09-01`.
- The condition `2026-09-01 BETWEEN c.valid_from AND c.valid_to` joins to Priya's **Silver** tier (`2023-04-10` to `2026-09-02`).
- Silver multiplier = **1**.
- Points: $\lfloor 1250.00 / 100 \rfloor \times 1 = 12 \times 1 = \mathbf{12}$.

#### Why ORD-1005 Earns Points via Gold Multiplier
- **`order_date`:** `2026-09-03`.
- On September 3, 2026, Priya upgraded to Gold. The condition `2026-09-03 BETWEEN c.valid_from AND c.valid_to` matches her **Gold** tier (`2026-09-03` to `9999-12-31`).
- Gold multiplier = **2**.
- Points: $\lfloor 899.00 / 100 \rfloor \times 2 = 8 \times 2 = \mathbf{16}$.
- **Core ELT Principle:** Point-in-time SCD2 joins prevent retroactive inflation of loyalty points for past historical transactions.
