import sys
from pathlib import Path
import snowflake.connector
import pandas as pd

from elt.pipeline_runner import (
    get_connection, run_sql, SQL_00_SETUP, run_pipeline, query, show
)

def run_b1():
    print("==================================================================")
    print("  PART B1: FRESHCART BASELINE ELT EXECUTION (SNOWFLAKE)")
    print("==================================================================")
    
    conn = get_connection()
    cur = conn.cursor()
    
    print("\n--- 1. Resetting database to clean baseline state ---")
    cur.execute("DROP DATABASE IF EXISTS FRESHCART")
    run_sql(conn, SQL_00_SETUP)
    cur.execute("USE WAREHOUSE FRESHCART_WH")
    cur.execute("USE DATABASE FRESHCART")
    
    print("\n--- 2. Executing run_pipeline() ---")
    res = run_pipeline(conn, include_unsupported_currency=False, include_sixth_check=False)
    
    print("\n==================================================================")
    print("  PART B1 BASELINE VERIFICATION RESULTS")
    print("==================================================================")
    
    raw_count = query(cur, "SELECT COUNT(*) FROM bronze.orders_raw")[0][0]
    web_count = query(cur, "SELECT COUNT(*) FROM bronze.orders_raw WHERE source = 'web'")[0][0]
    mobile_count = query(cur, "SELECT COUNT(*) FROM bronze.orders_raw WHERE source = 'mobile'")[0][0]
    clean_count = query(cur, "SELECT COUNT(*) FROM silver.orders_clean")[0][0]
    quarantine_count = query(cur, "SELECT COUNT(*) FROM quarantine.orders")[0][0]
    quarantine_orders = [r[0] for r in query(cur, "SELECT order_id FROM quarantine.orders ORDER BY order_id")]
    dedup_count = query(cur, "SELECT COUNT(*) FROM silver.orders_dedup")[0][0]
    gold_orders, gold_revenue, gold_points = query(cur, "SELECT COUNT(*), SUM(amount_inr), SUM(loyalty_points) FROM gold.fct_orders")[0]
    rejected_share = query(cur, "SELECT COUNT_IF(ARRAY_SIZE(reasons) > 0) / NULLIF(COUNT(*), 0) FROM silver.orders_checked")[0][0]
    rejected_pct = f"{float(rejected_share):.0%}"

    metrics = [
        ("Raw rows in bronze.orders_raw", "8 (6 web + 2 mobile)", f"{raw_count} ({web_count} web + {mobile_count} mobile)"),
        ("Rows passing cleaning in silver.orders_clean", "6", str(clean_count)),
        ("Quarantined rows", "2 (ORD-1004, ORD-1006)", f"{quarantine_count} ({', '.join(quarantine_orders)})"),
        ("Rows after deduplication", "4", str(dedup_count)),
        ("Gold orders", "4", str(gold_orders)),
        ("Gold revenue", "₹7,785.00", f"₹{gold_revenue:,.2f}"),
        ("Gold loyalty points", "126", str(gold_points)),
        ("Rejected share", "25%", rejected_pct),
    ]

    df = pd.DataFrame(metrics, columns=["Check", "Expected Result", "Actual Result"])
    print(df.to_string(index=False))
    print("==================================================================\n")

    # Save details of each table for report screenshots
    print("--- Detailed Table Dumps ---")
    print("\n[bronze.orders_raw]:")
    df_bronze = show(cur, "SELECT ingest_seq, source, raw:order_id::STRING AS order_id, raw:amount::STRING AS amount FROM bronze.orders_raw ORDER BY ingest_seq")
    print(df_bronze.to_string(index=False))

    print("\n[quarantine.orders]:")
    df_quarantine = show(cur, "SELECT run_id, ingest_seq, source, order_id, reasons FROM quarantine.orders ORDER BY order_id")
    print(df_quarantine.to_string(index=False))

    print("\n[silver.orders_dedup]:")
    df_dedup = show(cur, "SELECT order_id, ingest_seq, source, email, order_date, amount, currency, updated_at FROM silver.orders_dedup ORDER BY order_id")
    print(df_dedup.to_string(index=False))

    print("\n[gold.fct_orders]:")
    df_gold = show(cur, "SELECT order_id, order_date, cust_id, tier, multiplier, amount, currency, fx_rate, amount_inr, loyalty_points FROM gold.fct_orders ORDER BY order_id")
    print(df_gold.to_string(index=False))

    conn.close()
    return metrics

if __name__ == "__main__":
    run_b1()
