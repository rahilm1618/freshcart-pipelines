import snowflake.connector
import pandas as pd
from elt.pipeline_runner import (
    get_connection, run_pipeline, query, show, MOCK_CRM_TIER_EVENTS_BASE
)

def run_b4():
    print("==================================================================")
    print("  PART B4: SCD TYPE 2 CHANGE & IDEMPOTENCY EXECUTION")
    print("==================================================================")
    
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE FRESHCART_WH")
    cur.execute("USE DATABASE FRESHCART")

    # Capture timestamps BEFORE B4 run
    print("\n--- 1. Capturing published_at timestamps BEFORE B4 run ---")
    before_gold = show(cur, "SELECT order_id, tier, multiplier, loyalty_points, row_hash, published_at FROM gold.fct_orders ORDER BY order_id")
    print(before_gold.to_string(index=False))

    # Add the new CRM tier event for Arjun
    arjun_event = {"cust_id": "CUST-02", "email": "arjun.k@yahoo.in", "tier": "Gold", "effective_from": "2026-08-15"}
    updated_crm_events = MOCK_CRM_TIER_EVENTS_BASE + [arjun_event]

    print(f"\n--- 2. Executing pipeline with new CRM tier event for Arjun ---")
    print(f"Added event: {arjun_event}")
    res = run_pipeline(conn, crm_events=updated_crm_events, include_unsupported_currency=True, include_sixth_check=False)

    # Capture AFTER state
    print("\n--- 3. Querying dim.customers_scd2 for CUST-02 ---")
    df_scd2_arjun = show(cur, "SELECT cust_id, email, tier, valid_from, valid_to, is_current FROM dim.customers_scd2 WHERE cust_id = 'CUST-02' ORDER BY valid_from")
    print(df_scd2_arjun.to_string(index=False))

    print("\n--- 4. Querying ORD-1002 in gold.fct_orders ---")
    df_ord1002 = show(cur, "SELECT order_id, order_date, cust_id, tier, multiplier, amount, currency, fx_rate, amount_inr, loyalty_points FROM gold.fct_orders WHERE order_id = 'ORD-1002'")
    print(df_ord1002.to_string(index=False))

    print("\n--- 5. Querying Gold totals ---")
    orders, rev, points = query(cur, "SELECT COUNT(*), SUM(amount_inr), SUM(loyalty_points) FROM gold.fct_orders")[0]
    print(f"Gold Totals: {orders} orders, revenue ₹{rev:,.2f}, {points} loyalty points")

    print("\n--- 6. Inspecting published_at changes across all Gold orders ---")
    after_gold = show(cur, "SELECT order_id, tier, multiplier, loyalty_points, row_hash, published_at FROM gold.fct_orders ORDER BY order_id")
    print(after_gold.to_string(index=False))

    # Compare published_at timestamps
    merged_comp = pd.merge(before_gold[["ORDER_ID", "PUBLISHED_AT"]], after_gold[["ORDER_ID", "PUBLISHED_AT"]], on="ORDER_ID", suffixes=("_BEFORE", "_AFTER"))
    merged_comp["CHANGED"] = merged_comp["PUBLISHED_AT_BEFORE"] != merged_comp["PUBLISHED_AT_AFTER"]
    print("\n[published_at comparison]:")
    print(merged_comp.to_string(index=False))

    # Verification Table: Predictions vs Actual
    cust_02_rows = len(df_scd2_arjun)
    tier_ord1002 = df_ord1002["TIER"].iloc[0]
    points_ord1002 = int(df_ord1002["LOYALTY_POINTS"].iloc[0])
    total_points = int(points)
    changed_rows = int(merged_comp["CHANGED"].sum())

    comparison = [
        ("Rows for CUST-02 in dim.customers_scd2", "2", str(cust_02_rows)),
        ("Tier used for ORD-1002", "Gold", str(tier_ord1002)),
        ("Loyalty points for ORD-1002", "70", str(points_ord1002)),
        ("Total loyalty points in Gold", "161", str(total_points)),
        ("Number of Gold rows whose published_at changed", "1", str(changed_rows)),
    ]
    df_comp = pd.DataFrame(comparison, columns=["Quantity", "Predicted Value", "Actual Value"])
    print("\n==================================================================")
    print("  PREDICTIONS VS ACTUAL RESULTS (Part B4)")
    print("==================================================================")
    print(df_comp.to_string(index=False))
    print("==================================================================\n")

    conn.close()

if __name__ == "__main__":
    run_b4()
