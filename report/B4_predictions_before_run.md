# Part B4: Pre-Execution Predictions (SCD Type 2 & Idempotency)

**Date:** 2026-11-15  
**Course:** Data Engineering · Pipeline Orchestration  
**Academic Integrity Requirement:**  
*These predictions are formulated and committed to version control strictly prior to executing the modified pipeline with the new CRM tier event for Arjun (`CUST-02`).*

---

## 1. Description of Incoming Event
- **Customer:** Arjun K (`CUST-02`, `arjun.k@yahoo.in`)
- **New Tier:** `Gold`
- **Effective From:** `2026-08-15`

Prior tier history: `CUST-02` held `Silver` effective from `2024-01-15`.

---

## 2. Pre-Execution Predictions Table

| Quantity | Baseline Value (Part B1) | Predicted Value (Part B4) | Rationale / Mathematical Proof |
|:---|:---:|:---:|:---|
| **Rows for `CUST-02` in `dim.customers_scd2`** | 1 | **2** | Arjun will have two distinct time slices: Silver (`2024-01-15` to `2026-08-14`) and Gold (`2026-08-15` to `9999-12-31`). |
| **Tier used for `ORD-1002`** | Silver | **Gold** | `ORD-1002` was placed on `2026-09-01`. Because `2026-09-01` falls after `2026-08-15`, the point-in-time join matches his new Gold tier. |
| **Loyalty points for `ORD-1002`** | 35 | **70** | USD 42.50 at FX 83.20 = ₹3,536.00. Base units = $\lfloor 3536 / 100 \rfloor = 35$. Multiplied by Gold multiplier ($2$) = $35 \times 2 = \mathbf{70}$. |
| **Total loyalty points in Gold** | 126 | **161** | Baseline total ($126$) + Arjun's incremental points ($+35$) = $126 + 35 = \mathbf{161}$. |
| **Number of Gold rows whose `published_at` changed** | — | **1** | The `MERGE` statement updates rows only when `t.row_hash <> s.row_hash`. Only `ORD-1002` has changed attributes; the other 3 orders retain identical hashes and untouched `published_at` timestamps. |
