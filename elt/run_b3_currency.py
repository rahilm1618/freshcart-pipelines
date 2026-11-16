import snowflake.connector
import pandas as pd
from elt.pipeline_runner import (
    get_connection, run_pipeline, query, show
)

def run_b3():
    print("==================================================================")
    print("  PART B3: ADD UNSUPPORTED_CURRENCY VALIDATION")
    print("==================================================================")
    
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE FRESHCART_WH")
    cur.execute("USE DATABASE FRESHCART")
    
    # 1. Capture BEFORE state
    print("\n--- 1. BEFORE State: Inspecting ORD-1006 in Quarantine ---")
    before_reasons = query(cur, "SELECT reasons FROM quarantine.orders WHERE order_id = 'ORD-1006'")[0][0]
    before_quarantine_rows = query(cur, "SELECT COUNT(*) FROM quarantine.orders")[0][0]
    before_gold_orders, before_gold_rev, before_gold_pts = query(cur, "SELECT COUNT(*), SUM(amount_inr), SUM(loyalty_points) FROM gold.fct_orders")[0]
    print(f"Quarantine rows: {before_quarantine_rows}")
    print(f"ORD-1006 reasons BEFORE: {before_reasons}")
    print(f"Gold: {before_gold_orders} orders, revenue ₹{before_gold_rev}, {before_gold_pts} points")

    # 2. Re-run pipeline WITH unsupported_currency rule
    print("\n--- 2. Executing Pipeline with unsupported_currency Validation Rule ---")
    res = run_pipeline(conn, include_unsupported_currency=True, include_sixth_check=False)

    # 3. Capture AFTER state
    print("\n--- 3. AFTER State: Inspecting Quarantine & Gold ---")
    after_reasons_checked = query(cur, "SELECT reasons FROM silver.orders_checked WHERE order_id = 'ORD-1006'")[0][0]
    after_quarantine_rows = query(cur, "SELECT COUNT(*) FROM quarantine.orders")[0][0]
    after_gold_orders, after_gold_rev, after_gold_pts = query(cur, "SELECT COUNT(*), SUM(amount_inr), SUM(loyalty_points) FROM gold.fct_orders")[0]
    quarantine_dump = show(cur, "SELECT run_id, ingest_seq, source, order_id, reasons FROM quarantine.orders ORDER BY order_id, ingest_seq")
    
    print(f"Quarantine rows AFTER rerun: {after_quarantine_rows}")
    print(f"silver.orders_checked ORD-1006 reasons AFTER: {after_reasons_checked}")
    print(f"Gold AFTER: {after_gold_orders} orders, revenue ₹{after_gold_rev}, {after_gold_pts} points")
    print("\n[quarantine.orders table content]:")
    print(quarantine_dump.to_string(index=False))

    conn.close()

if __name__ == "__main__":
    run_b3()
