import snowflake.connector
import pandas as pd
from elt.pipeline_runner import (
    get_connection, query, show
)

def run_b5():
    print("==================================================================")
    print("  PART B5: SIXTH QUALITY CHECK & ELT VS ETL EXPERIMENT")
    print("==================================================================")
    
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE FRESHCART_WH")
    cur.execute("USE DATABASE FRESHCART")

    check_name = "gold orders with negative loyalty"
    check_sql = "SELECT COUNT_IF(loyalty_points < 0) FROM gold.fct_orders"

    def run_all_checks():
        checks = {
            "rows gained or lost while enriching":
                "SELECT (SELECT COUNT(*) FROM silver.orders_enriched) - (SELECT COUNT(*) FROM silver.orders_dedup)",
            "duplicate orders in gold":
                "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM gold.fct_orders",
            "orders with no customer match":
                "SELECT COUNT_IF(cust_id IS NULL) FROM silver.orders_enriched",
            "orders with an amount but no FX rate":
                "SELECT COUNT_IF(amount IS NOT NULL AND fx_rate IS NULL) FROM silver.orders_enriched",
            "orders with no city":
                "SELECT COUNT_IF(city IS NULL) FROM silver.orders_enriched",
            "gold orders with negative loyalty (6th)":
                check_sql,
        }
        res_list = []
        for name, sql in checks.items():
            val = query(cur, sql)[0][0]
            st = "OK" if val == 0 else "PROBLEM"
            print(f"   {name:42s} {val}   {st}")
            res_list.append((name, val, st))
        return res_list

    # Step 1: Normal PASS run
    print("\n--- 1. Baseline Quality Checks (with 6th check added) ---")
    res_normal = run_all_checks()
    val_normal = query(cur, check_sql)[0][0]
    assert val_normal == 0, f"Expected 0, got {val_normal}"
    print(f"\n[Result 1] Normal Check Output: {val_normal} -> PASS (OK)")

    # Step 2: Insert mock order to trigger intentional failure
    mock_order_id = "MOCK-FAIL-999"
    print(f"\n--- 2. Injecting temporary mock order '{mock_order_id}' with loyalty_points = -10 ---")
    cur.execute(f"""
        INSERT INTO gold.fct_orders (
            order_id, order_date, cust_id, email, tier, multiplier,
            amount, currency, fx_rate, amount_inr, loyalty_points,
            is_high_value, city, state, region, updated_at, row_hash, published_at
        ) VALUES (
            '{mock_order_id}', '2026-09-05', 'CUST-MOCK', 'mock@test.com', 'Silver', 1,
            -500.00, 'INR', 1.0, -500.00, -10,
            FALSE, 'Bengaluru', 'Karnataka', 'South', CURRENT_TIMESTAMP(), 999999, CURRENT_TIMESTAMP()
        )
    """)
    
    print("\n--- 3. Running Quality Checks with Mock Order Present ---")
    res_fail = run_all_checks()
    val_fail = query(cur, check_sql)[0][0]
    assert val_fail > 0, f"Expected > 0, got {val_fail}"
    print(f"\n[Result 2] Intentional Failure Output: {val_fail} -> PROBLEM (Triggered as expected!)")

    # Step 3: Remove mock order and verify cleanup
    print(f"\n--- 4. Removing temporary mock order '{mock_order_id}' ---")
    cur.execute(f"DELETE FROM gold.fct_orders WHERE order_id = '{mock_order_id}'")

    print("\n--- 5. Restoring and Verifying Quality Checks ---")
    res_restored = run_all_checks()
    val_restored = query(cur, check_sql)[0][0]
    assert val_restored == 0, f"Expected 0, got {val_restored}"
    print(f"\n[Result 3] Restored Check Output: {val_restored} -> PASS (OK, Database Clean!)")

    # Confirm mock order is completely absent
    mock_count = query(cur, f"SELECT COUNT(*) FROM gold.fct_orders WHERE order_id = '{mock_order_id}'")[0][0]
    print(f"Mock orders remaining in database: {mock_count}")
    assert mock_count == 0

    conn.close()

if __name__ == "__main__":
    run_b5()
