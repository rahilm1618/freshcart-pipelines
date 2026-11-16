# Part B3: Add `unsupported_currency` Validation Rule

**Date:** 2026-11-15  
**Platform:** Snowflake Data Cloud  
**Database:** `FRESHCART` | **Schemas:** `silver`, `quarantine`, `gold`

---

## 1. Rule Implementation in `SQL_10_ORDERS_CLEAN`

FreshCart accepts only Indian Rupees (`INR`) and US Dollars (`USD`). To reject unsupported currencies (such as `EUR` in order `ORD-1006`), an additional validation check was introduced inside the `ARRAY_CONSTRUCT_COMPACT(...)` expression in `SQL_10_ORDERS_CLEAN`:

```sql
ARRAY_CONSTRUCT_COMPACT(
  IFF(order_id IS NULL, 'missing_order_id', NULL),
  IFF(email IS NULL OR NOT REGEXP_LIKE(email, '^[^@\\s]+@[^@\\s]+\\.[a-z]{2,}$'), 'bad_email', NULL),
  IFF(pincode IS NULL OR NOT REGEXP_LIKE(pincode, '^[1-9][0-9]{5}$'), 'bad_pincode', NULL),
  IFF(order_date IS NULL, 'unparseable_date', NULL),
  IFF(updated_at IS NULL, 'unparseable_updated_at', NULL),
  IFF(currency NOT IN ('INR', 'USD'), 'unsupported_currency', NULL),  -- NEW RULE
  IFF(amount <= 0, 'non_positive_amount', NULL)
) AS reasons
```

---

## 2. Before vs. After Comparison for ORD-1006

Order `ORD-1006` was placed in `EUR` with `cust_email = "vikram@@mail"` and `pincode = "99999"`.

### Comparison Table

| Attribute / Metric | Before Adding Rule (Part B1) | After Adding Rule (Part B3) | Change Observed |
|:---|:---|:---|:---|
| **`ORD-1006` Reasons Array** | `["bad_email", "bad_pincode"]` | `["bad_email", "bad_pincode", "unsupported_currency"]` | ✅ **`unsupported_currency` appended** |
| **Quarantine Total Rows** | 2 (`ORD-1004`, `ORD-1006`) | 2 (`ORD-1004`, `ORD-1006`) | **Unchanged (No duplicate inserted)** |
| **Gold Orders Count** | 4 | 4 | **Unchanged** |
| **Gold Revenue** | ₹7,785.00 | ₹7,785.00 | **Unchanged** |
| **Gold Loyalty Points** | 126 | 126 | **Unchanged** |

---

## 3. Analysis & Verification Questions

### Did the Gold total change?
**No.** `ORD-1006` was already quarantined in the baseline run due to `bad_email` and `bad_pincode`. Because records with `ARRAY_SIZE(reasons) > 0` are excluded from `silver.orders_clean`, adding an extra rejection reason to an already rejected order has zero impact on downstream `gold.fct_orders`.

### Did the new run create another quarantine row for ORD-1006?
**No.** The quarantine row count remained strictly at 2.

### What SQL logic is responsible for duplicate prevention in quarantine?
In `SQL_10_ORDERS_CLEAN`, the `INSERT INTO quarantine.orders` statement includes an anti-join condition:
```sql
AND NOT EXISTS (
      SELECT 1 FROM quarantine.orders q
      WHERE q.source = c.source
        AND EQUAL_NULL(q.order_id, c.order_id)
        AND EQUAL_NULL(q.row_data:UPDATED_AT::TIMESTAMP_NTZ, c.updated_at))
```
- When the pipeline is re-executed, the incoming record for `ORD-1006` has `source = 'web'`, `order_id = 'ORD-1006'`, and `updated_at = '2026-09-04 08:30:00'`.
- The `NOT EXISTS` subquery finds the existing quarantined row recorded in Run 1 and evaluates to `FALSE`.
- Consequently, Snowflake does not re-insert a duplicate quarantine row for the same order version. This ensures that quarantine storage does not balloon across scheduled pipeline runs.
