# Part B5: Sixth Data Quality Check & ELT vs. ETL Architectural Analysis

**Date:** 2026-11-15  
**Platform:** Snowflake Data Cloud  
**Database:** `FRESHCART` | **Schemas:** `gold`, `silver`

---

## 1. The Sixth Data Quality Check Implementation

A sixth quality validation check was added to `quality_checks()` to protect the financial and rewards integrity of the business:

```python
"gold orders with negative loyalty": "SELECT COUNT_IF(loyalty_points < 0) FROM gold.fct_orders"
```

### Rationale
In FreshCart's rewards architecture, loyalty points represent real customer monetary value and discount currency. Because `silver.orders_clean` filters out non-positive amounts (`amount <= 0`), no order reaching Gold should ever calculate negative points. A negative value indicates upstream calculation corruption, negative multiplier joining, or faulty manual adjustments.

---

## 2. Experimental Execution Evidence

The required three-phase experiment was executed live on Snowflake:

### Phase 1: Baseline Quality Check (Normal Run)
All six quality checks executed across the production tables:
```text
rows gained or lost while enriching        0   OK
duplicate orders in gold                   0   OK
orders with no customer match              0   OK
orders with an amount but no FX rate       0   OK
orders with no city                        0   OK
gold orders with negative loyalty (6th)    0   OK
```
- **Result:** `0 -> PASS (OK)`.

---

### Phase 2: Intentional Failure Injection
A temporary corrupted mock order (`order_id = 'MOCK-FAIL-999'`, `loyalty_points = -10`) was inserted into `gold.fct_orders`:
```sql
INSERT INTO gold.fct_orders (
    order_id, order_date, cust_id, email, tier, multiplier,
    amount, currency, fx_rate, amount_inr, loyalty_points,
    is_high_value, city, state, region, updated_at, row_hash, published_at
) VALUES (
    'MOCK-FAIL-999', '2026-09-05', 'CUST-MOCK', 'mock@test.com', 'Silver', 1,
    -500.00, 'INR', 1.0, -500.00, -10,
    FALSE, 'Bengaluru', 'Karnataka', 'South', CURRENT_TIMESTAMP(), 999999, CURRENT_TIMESTAMP()
);
```

Re-executing `quality_checks()`:
```text
rows gained or lost while enriching        0   OK
duplicate orders in gold                   0   OK
orders with no customer match              0   OK
orders with an amount but no FX rate       0   OK
orders with no city                        0   OK
gold orders with negative loyalty (6th)    1   PROBLEM
```
- **Result:** `1 -> PROBLEM`. The check caught the corrupted record immediately.

---

### Phase 3: Cleanup and Restored Verification
The mock record was deleted:
```sql
DELETE FROM gold.fct_orders WHERE order_id = 'MOCK-FAIL-999';
```

Final verification run:
```text
rows gained or lost while enriching        0   OK
duplicate orders in gold                   0   OK
orders with no customer match              0   OK
orders with an amount but no FX rate       0   OK
orders with no city                        0   OK
gold orders with negative loyalty (6th)    0   OK
```
- **Result:** `0 -> PASS (OK)`.
- **Database Hygiene:** Verified zero mock records remaining in `gold.fct_orders` (`SELECT COUNT(*) = 0`).

---

## 3. Why ELT is Better than ETL for FreshCart, and Where ETL Wins

1. **Why ELT is Superior for FreshCart:**
   FreshCart receives messy, semi-structured JSON payloads from disparate systems (flat web JSON, nested mobile JSON, international currencies, and varying date conventions). In an **ELT (Extract-Load-Transform)** architecture, raw data lands immediately into Snowflake's immutable `bronze.orders_raw` with zero loss of original context. Transforming with SQL inside Snowflake leverages elastic cloud compute for schema mapping, deduplication (`QUALIFY ROW_NUMBER()`), and point-in-time dimension enrichment. Crucially, when new business logic is introduced (e.g. adding the `unsupported_currency` rule in Part B3 or replaying Arjun's tier change in Part B4), engineers can simply re-execute the SQL transformations over existing Bronze data without requesting upstream API teams to re-extract or re-send historic feeds.

2. **The One Situation Where ETL Still Wins:**
   **ETL (Extract-Transform-Load)** remains the mandatory choice when handling **strict privacy, regulatory compliance, or sensitive PII/PCI-DSS data (such as raw credit card CVVs, plaintext passwords, or health records under HIPAA/GDPR)**. In this scenario, sensitive data must be scrubbed, masked, or tokenized in-memory *before* it leaves the source boundary or is written to disk, because landing raw unmasked PII at rest in a cloud data lakehouse represents an immediate legal and security violation.
