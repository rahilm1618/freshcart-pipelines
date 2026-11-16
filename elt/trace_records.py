import snowflake.connector
import pandas as pd
from elt.pipeline_runner import get_connection, show

conn = get_connection()
cur = conn.cursor()
cur.execute("USE WAREHOUSE FRESHCART_WH")
cur.execute("USE DATABASE FRESHCART")

print("=== ORD-1003 IN BRONZE ===")
rows = cur.execute("SELECT ingest_seq, source, raw FROM bronze.orders_raw WHERE raw:order_id::STRING = 'ORD-1003' OR raw:orderId::STRING = 'ORD-1003' ORDER BY ingest_seq").fetchall()
for r in rows:
    print(f"ingest_seq={r[0]} | source={r[1]} | raw={r[2]}")

print("\n=== ORD-1003 IN SILVER.ORDERS_CHECKED ===")
print(show(cur, "SELECT ingest_seq, source, order_id, email, name, order_date, amount, currency, pincode, updated_at, reasons FROM silver.orders_checked WHERE order_id = 'ORD-1003' ORDER BY ingest_seq").to_string(index=False))

print("\n=== ORD-1003 IN SILVER.ORDERS_DEDUP ===")
print(show(cur, "SELECT ingest_seq, source, order_id, email, name, order_date, amount, currency, pincode, updated_at FROM silver.orders_dedup WHERE order_id = 'ORD-1003'").to_string(index=False))

print("\n=== ORD-1003 IN GOLD ===")
print(show(cur, "SELECT order_id, order_date, cust_id, tier, multiplier, amount, currency, fx_rate, amount_inr, loyalty_points FROM gold.fct_orders WHERE order_id = 'ORD-1003'").to_string(index=False))

print("\n=== PRIYA TIER HISTORY IN DIM.CUSTOMERS_SCD2 ===")
print(show(cur, "SELECT cust_id, email, tier, valid_from, valid_to, is_current FROM dim.customers_scd2 WHERE cust_id = 'CUST-01' ORDER BY valid_from").to_string(index=False))

print("\n=== PRIYA ORDERS IN GOLD (ORD-1001 vs ORD-1005) ===")
print(show(cur, "SELECT order_id, order_date, cust_id, tier, multiplier, amount, currency, fx_rate, amount_inr, loyalty_points FROM gold.fct_orders WHERE cust_id = 'CUST-01' ORDER BY order_date").to_string(index=False))

conn.close()
