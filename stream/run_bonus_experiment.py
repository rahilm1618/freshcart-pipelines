"""
stream/run_bonus_experiment.py
--------------------------------
Orchestrates the Bonus Experiment:
1. Records Gold Baseline before the stream run.
2. Starts dispatcher and runs the RabbitMQ consumer for a 60-second window.
3. Ingests the 1-minute batch through write_landing() -> load_file() -> Transform SQL -> Gold.
4. Records Gold table metrics after Run 1.
5. Replays the exact same landing batch into Snowflake (Run 2).
6. Records Gold table metrics after Run 2.
7. Formally proves that replaying the batch produces ZERO duplicate rows and ZERO revenue change.
"""

import json
import os
import subprocess
import sys
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from elt.pipeline_runner import (
    get_connection,
    write_landing,
    load_file,
    run_sql,
    get_sql_10,
    SQL_20_ORDERS_DEDUP,
    SQL_30_CUSTOMERS_SCD2,
    SQL_31_FX_DAILY,
    SQL_32_PINCODE_GEO,
    SQL_40_GOLD,
    run_quality_checks,
)
from stream.freshcart_consumer import collect_stream_orders, StreamConsumer, AMQP_URL

LOG_FILE = BASE_DIR / "stream" / "bonus_experiment.log"


def log_print(msg, f):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    f.write(line + "\n")
    f.flush()


def get_gold_metrics(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COALESCE(SUM(amount_inr), 0), COALESCE(SUM(loyalty_points), 0) FROM gold.fct_orders")
        count, revenue, points = cur.fetchone()
        cur.execute("SELECT order_id, cust_id, tier, multiplier, amount_inr, loyalty_points FROM gold.fct_orders ORDER BY order_id")
        orders = cur.fetchall()
        return {
            "count": int(count),
            "revenue": float(revenue),
            "points": int(points),
            "orders": orders,
        }


def run_full_bonus_experiment():
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        log_print("=" * 80, f)
        log_print("  BONUS EXPERIMENT: CONNECT RABBITMQ TO SNOWFLAKE LAKEHOUSE", f)
        log_print("=" * 80, f)

        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("USE WAREHOUSE FRESHCART_WH")
            cur.execute("USE DATABASE FRESHCART")

        # -------------------------------------------------------------
        # STEP 1: Record Baseline Metrics (Before Run 1)
        # -------------------------------------------------------------
        log_print("\n[STEP 1] Recording Baseline Gold Table Metrics...", f)
        baseline = get_gold_metrics(conn)
        log_print(f"  Existing Gold Rows    : {baseline['count']}", f)
        log_print(f"  Existing Gold Revenue : INR {baseline['revenue']:,.2f}", f)
        log_print(f"  Existing Gold Points  : {baseline['points']}", f)
        log_print("  Existing Orders in Gold:", f)
        for o in baseline["orders"]:
            log_print(f"    {o[0]}: cust={o[1]} tier={o[2]} mult={o[3]} inr={o[4]} pts={o[5]}", f)

        # -------------------------------------------------------------
        # STEP 2: Dispatcher + 60s RabbitMQ Consumer Collection
        # -------------------------------------------------------------
        log_print("\n[STEP 2] Launching Dispatcher and Consumer for 60s Batch Window...", f)
        
        # Setup consumer queue first so no messages are dropped
        consumer = StreamConsumer(AMQP_URL, pattern="#.food")
        consumer.setup()
        log_print(f"  Consumer queue '{consumer.queue_name}' declared and bound to '#.food'", f)

        # Start dispatcher subprocess
        disp_script = BASE_DIR / "stream" / "dispatcher.py"
        disp_proc = subprocess.Popen(
            [sys.executable, "-u", str(disp_script)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=str(BASE_DIR),
        )
        log_print(f"  Started Dispatcher (PID {disp_proc.pid}) publishing to 'rapido-dispatch'...", f)

        try:
            # Collect messages for 60 seconds
            collected_orders = collect_stream_orders(consumer=consumer, duration_seconds=60)
        finally:
            disp_proc.terminate()
            disp_proc.wait()
            consumer.cleanup()
            log_print("  Dispatcher stopped and consumer queue cleaned up.", f)

        log_print(f"  Successfully collected {len(collected_orders)} valid FreshCart orders from RabbitMQ.", f)

        # -------------------------------------------------------------
        # STEP 3: Landing & Ingestion (Run 1)
        # -------------------------------------------------------------
        run_id_1 = "bonus_stream_batch_01"
        log_print(f"\n[STEP 3] Writing Landing File and Ingesting Batch 1 (run_id={run_id_1})...", f)
        landing_path_1 = write_landing("web", collected_orders, run_id_1)
        log_print(f"  Landing file generated: {landing_path_1}", f)

        with conn.cursor() as cur:
            # Load into bronze
            stage_dir_1 = f"batch_{run_id_1}"
            load_file(cur, landing_path_1, "bronze.orders_raw", stage_dir_1)
            
            # Execute transformation SQL
            log_print("  Executing Silver & Gold Transformation SQL...", f)
            cur.execute(f"SET run_id = '{run_id_1}'")
            run_sql(conn, get_sql_10(include_unsupported_currency=True))
            run_sql(conn, SQL_20_ORDERS_DEDUP)
            run_sql(conn, SQL_30_CUSTOMERS_SCD2)
            run_sql(conn, SQL_31_FX_DAILY)
            run_sql(conn, SQL_32_PINCODE_GEO)
            run_sql(conn, SQL_40_GOLD)

            # Quality checks
            run_quality_checks(cur, include_sixth_check=True)

        after_run_1 = get_gold_metrics(conn)
        log_print(f"\n--- Metrics After Run 1 (Stream Ingestion) ---", f)
        log_print(f"  Gold Rows    : {after_run_1['count']} (increased by {after_run_1['count'] - baseline['count']})", f)
        log_print(f"  Gold Revenue : INR {after_run_1['revenue']:,.2f} (+INR {after_run_1['revenue'] - baseline['revenue']:,.2f})", f)
        log_print(f"  Gold Points  : {after_run_1['points']} (+{after_run_1['points'] - baseline['points']})", f)
        log_print("  All Current Gold Orders:", f)
        for o in after_run_1["orders"]:
            log_print(f"    {o[0]}: cust={o[1]} tier={o[2]} inr={o[4]} pts={o[5]}", f)

        # -------------------------------------------------------------
        # STEP 4: Mandatory Idempotency Experiment (Run 2 - Replay Batch 1)
        # -------------------------------------------------------------
        run_id_2 = "bonus_stream_batch_01_REPLAY"
        log_print("\n" + "=" * 80, f)
        log_print("[STEP 4] MANDATORY IDEMPOTENCY EXPERIMENT: REPLAYING SAME BATCH", f)
        log_print("=" * 80, f)
        log_print(f"  Replaying raw landing file: {landing_path_1}", f)
        log_print("  Forcing raw load into Bronze with identical records...", f)

        with conn.cursor() as cur:
            # Force load the same file into bronze again to simulate duplicate raw delivery
            uri = "file://" + Path(landing_path_1).resolve().as_posix()
            cur.execute(f"PUT '{uri}' @bronze.landing/replay_stage/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
            cur.execute(f"""
                COPY INTO bronze.orders_raw (source, raw, run_id, extracted_at, src_file, src_row)
                FROM (
                  SELECT $1:source::STRING, $1:data, $1:run_id::STRING,
                         $1:extracted_at::TIMESTAMP_TZ, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
                  FROM @bronze.landing/replay_stage/
                )
                FILE_FORMAT = (FORMAT_NAME = 'bronze.ff_ndjson')
                FORCE = TRUE
            """)
            reloaded = cur.fetchall()
            log_print(f"  Raw Bronze re-load status: {reloaded[0][1]} ({reloaded[0][3]} records)", f)

            # Re-execute transformation SQL pipeline
            log_print("  Re-executing complete ELT pipeline (Silver -> Dedup -> SCD2 -> FX -> Gold MERGE)...", f)
            cur.execute(f"SET run_id = '{run_id_2}'")
            run_sql(conn, get_sql_10(include_unsupported_currency=True))
            run_sql(conn, SQL_20_ORDERS_DEDUP)
            run_sql(conn, SQL_30_CUSTOMERS_SCD2)
            run_sql(conn, SQL_31_FX_DAILY)
            run_sql(conn, SQL_32_PINCODE_GEO)
            run_sql(conn, SQL_40_GOLD)

            run_quality_checks(cur, include_sixth_check=True)

        after_replay = get_gold_metrics(conn)
        log_print(f"\n--- Metrics After Replaying Exact Same Batch (Run 2) ---", f)
        log_print(f"  Gold Rows    : {after_replay['count']} (Expected: {after_run_1['count']})", f)
        log_print(f"  Gold Revenue : INR {after_replay['revenue']:,.2f} (Expected: INR {after_run_1['revenue']:,.2f})", f)
        log_print(f"  Gold Points  : {after_replay['points']} (Expected: {after_run_1['points']})", f)

        # Mathematical verification of idempotency
        rows_diff = after_replay["count"] - after_run_1["count"]
        rev_diff = after_replay["revenue"] - after_run_1["revenue"]
        pts_diff = after_replay["points"] - after_run_1["points"]

        log_print("\n" + "=" * 80, f)
        log_print("  IDEMPOTENCY VERIFICATION RESULTS", f)
        log_print("=" * 80, f)
        log_print(f"  Delta Gold Rows    : {rows_diff}  -> {'PASS (No duplicates created)' if rows_diff == 0 else 'FAIL'}", f)
        log_print(f"  Delta Gold Revenue : INR {rev_diff:,.2f}  -> {'PASS (Revenue did not double)' if rev_diff == 0.0 else 'FAIL'}", f)
        log_print(f"  Delta Gold Points  : {pts_diff}  -> {'PASS (Points unchanged)' if pts_diff == 0 else 'FAIL'}", f)
        log_print("=" * 80, f)

        conn.close()
        log_print("\nBonus Pipeline Experiment Finished Successfully.", f)


if __name__ == "__main__":
    run_full_bonus_experiment()
