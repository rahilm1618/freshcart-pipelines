# Optional Bonus: Connecting RabbitMQ Stream to Snowflake Lakehouse (+10 Marks)

## 1. Architecture Overview

This bonus implementation bridges the real-time event streaming pipeline from **Part D** with the multi-hop Lakehouse ELT architecture from **Part B**:

```text
  [stream/dispatcher.py]
            │
            ▼ (topic routing: *.food / #.food)
  [CloudAMQP Topic Exchange: rapido-dispatch]
            │
            ▼ (60s micro-batch buffer · ACK/NACK gate)
  [stream/freshcart_consumer.py]
            │
            ▼ write_landing()
  [freshcart_landing/web/web__batch_01.json.gz]
            │
            ▼ load_file() & transformation SQL
  [Bronze: orders_raw] ──► [Silver: orders_clean] ──► [Gold: fct_orders]
                               (Dedup & SCD2)            (Idempotent MERGE)
```

---

## 2. Modified & Created Files

| File | Type | Role |
|:---|:---|:---|
| `stream/dispatcher.py` | Modified | Enriches published food messages with FreshCart customer details (`cust_name`, `cust_email`, `order_date`, `amount`, `pincode`, `updated_at`) while retaining standard Part D fields (`id`, `city`, `zone`, `type`, `ts`) and Part D3 malformed injections. |
| `stream/freshcart_consumer.py` | Created | Declares a private queue bound to `#.food` on `rapido-dispatch`, buffers messages for 60 seconds, validates schema, issues ACK / NACK, and triggers ELT ingestion. |
| `stream/run_bonus_experiment.py` | Created | Automated orchestrator executing Baseline recording, 60s stream run (Run 1), raw batch replay (Run 2), and mathematical verification of idempotency. |
| `elt/pipeline_runner.py` | Modified | Updated `get_connection()` to default to `database='FRESHCART'` and `warehouse='FRESHCART_WH'`. |
| `report/bonus_stream_to_snowflake.md` | Created | Technical documentation, architecture description, and execution proof. |

---

## 3. How the Consumer and Batching Work

### 3.1 Topic Exchange Binding
The consumer connects to the `rapido-dispatch` topic exchange using the binding key `#.food`. Under AMQP 0-9-1 rules:
- `#` matches zero or more dot-delimited words.
- This captures all food delivery events (e.g., `bangalore.koramangala.food`, `chennai.anna_nagar.food`, `chennai.tnagar.food`).

### 3.2 60-Second Batching Window
1. The consumer initiates an unbuffered collection loop for 60 seconds.
2. Orders arriving on the queue are evaluated:
   - **Malformed messages (missing `zone`):** NACKed (`basic_nack(requeue=False)`) and dropped from the ingestion batch.
   - **Valid messages:** ACKed (`basic_ack`), normalized, and buffered in memory.
3. At the end of 60 seconds, the consumer flushes all buffered records using `write_landing(source="web", records=buffered, run_id=run_id)` into a timestamped, gzip-compressed JSON Lines file:
   `freshcart_landing/web/web__bonus_stream_batch_01.json.gz`.

---

## 4. How Data Enters Snowflake (Bronze → Silver → Gold)

Data loading strictly follows the instructor-provided FreshCart ELT notebook flow:
1. **Bronze Ingestion:**
   ```sql
   PUT file://freshcart_landing/web/web__bonus_stream_batch_01.json.gz @bronze.landing/batch_bonus_stream_batch_01/
   COPY INTO bronze.orders_raw FROM @bronze.landing/batch_bonus_stream_batch_01/
   ```
2. **Silver Cleaning & Deduplication:**
   - `silver.orders_checked`: Validates email regex, pincode regex, amount positivity, and supported currencies (`INR`, `USD`).
   - `silver.orders_clean`: Filters `ARRAY_SIZE(reasons) = 0`.
   - `silver.orders_dedup`: Resolves duplicate deliveries using `QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, ingest_seq DESC) = 1`.
3. **Dimensions & Reference Enrichment:**
   - `dim.customers_scd2`: Resolves customer loyalty tier based on order date falling between `valid_from` and `valid_to`.
   - `ref.fx_daily`: Applies currency exchange rates.
   - `ref.pincode_geo`: Enriches district and region geography.
4. **Gold MERGE:**
   - Computes `amount_inr` and `loyalty_points = FLOOR(amount_inr / 100) * multiplier`.
   - Merges into `gold.fct_orders` comparing row hashes:
     ```sql
     MERGE INTO gold.fct_orders t
     USING (...) s ON t.order_id = s.order_id
     WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET ...
     WHEN NOT MATCHED THEN INSERT (...)
     ```

---

## 5. Mandatory Idempotency Experiment Results

To prove that raw batch replaying does not duplicate records or inflate revenues, the exact same raw file `web__bonus_stream_batch_01.json.gz` was loaded into Bronze a second time with `FORCE = TRUE` and processed through the full ELT pipeline.

### Actual Execution Numbers

| Metric | Baseline (Pre-Stream) | Run 1 (Stream Ingested) | Run 2 (Forced Replay) | Delta (Run 2 vs Run 1) | Verification Status |
|:---|:---:|:---:|:---:|:---:|:---:|
| **Gold Orders Count** | 4 | 14 | 14 | **0** | **PASS (0 duplicates)** |
| **Gold Revenue (INR)** | ₹7,785.00 | ₹26,185.00 | ₹26,185.00 | **₹0.00** | **PASS (Did not double)** |
| **Gold Loyalty Points** | 161 | 596 | 596 | **0** | **PASS (Points identical)** |
| **Quality Check Errors** | 0 | 0 | 0 | 0 | **PASS (All OK)** |

### Orders in Gold Table After Ingestion

```
ORDER_ID         CUST_ID    TIER       AMOUNT_INR   POINTS   STATUS
---------------------------------------------------------------------------------
ORD-1001         CUST-01    Silver     1250.00      12       Baseline
ORD-1002         CUST-02    Gold       3536.00      70       Baseline (SCD2 Updated)
ORD-1003         CUST-03    Platinum   2100.00      63       Baseline
ORD-1005         CUST-01    Gold       899.00       16       Baseline
ORD-STRM-0001    CUST-01    Gold       950.00       18       Stream Ingested
ORD-STRM-0003    CUST-03    Platinum   2200.00      66       Stream Ingested
ORD-STRM-0004    CUST-01    Gold       3100.00      62       Stream Ingested
ORD-STRM-0006    CUST-03    Platinum   950.00       27       Stream Ingested
ORD-STRM-0009    CUST-03    Platinum   3100.00      93       Stream Ingested
ORD-STRM-0011    CUST-02    Gold       950.00       18       Stream Ingested
ORD-STRM-0013    CUST-01    Gold       2200.00      44       Stream Ingested
ORD-STRM-0021    CUST-03    Platinum   950.00       27       Stream Ingested
ORD-STRM-0023    CUST-02    Gold       2200.00      44       Stream Ingested
ORD-STRM-0025    CUST-01    Gold       1800.00      36       Stream Ingested
---------------------------------------------------------------------------------
TOTALS: 14 Orders | INR 26,185.00 Total Revenue | 596 Total Loyalty Points
```

---

## 6. Why Replay Idempotency Holds

The pipeline achieves mathematical idempotency at two independent layers:
1. **Deduplication Layer (`silver.orders_dedup`):**
   ```sql
   QUALIFY ROW_NUMBER() OVER (
     PARTITION BY order_id 
     ORDER BY updated_at DESC, ingest_seq DESC
   ) = 1;
   ```
   Even when Bronze holds duplicated rows from re-ingestion with higher sequence numbers (`ingest_seq`), only one record per `order_id` survives into `silver.orders_dedup`.

2. **Lakehouse Hash-Based MERGE Layer (`gold.fct_orders`):**
   ```sql
   MERGE INTO gold.fct_orders t
   USING (...) s ON t.order_id = s.order_id
   WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET ...
   WHEN NOT MATCHED THEN INSERT (...)
   ```
   Because replayed records produce the exact same `row_hash` value as already present in `gold.fct_orders`, the condition `t.row_hash <> s.row_hash` evaluates to `FALSE`. Consequently, **0 rows are inserted and 0 rows are updated**, guaranteeing 100% idempotency.

---

## 7. Evidence & Screenshots

All screenshots were captured from live executions and saved under `screenshots/bonus/`:
1. `screenshots/bonus/01_rabbitmq_consumer_batch_collection.png`: Live consumer receiving streamed orders, ACK of valid orders, NACK of malformed records, and batch buffer completion.
2. `screenshots/bonus/02_landing_file_and_snowflake_load.png`: Compressed JSON Lines landing generation, `PUT` + `COPY INTO bronze.orders_raw`, and automated quality check passes.
3. `screenshots/bonus/03_gold_first_batch_vs_replay_idempotency.png`: Gold table baseline vs. Run 1 vs. Run 2 (Replay), showing the delta table with 0 duplicates and zero revenue inflation.
