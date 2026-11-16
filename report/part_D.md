# Part D: Food-Order Stream with RabbitMQ

## Overview
This section implements a distributed event-streaming architecture modeled on real-time dispatch systems (such as Zomato or Rapido). A centralized dispatcher produces orders with hierarchical routing keys (`<city>.<zone>.<type>`) into a RabbitMQ **topic exchange** (`rapido-dispatch`), while individual driver workers declare private queues bound to distinct wildcard patterns.

---

## D1. Routing on Paper (6 Marks)

### Driver Binding Patterns
| Driver | Binding Pattern | Subscribed Semantics |
| :--- | :--- | :--- |
| **Ravi** | `bangalore.koramangala.*` | Any request in Koramangala, Bangalore (exact 3 tokens) |
| **Priya** | `bangalore.indiranagar.*` | Any request in Indiranagar, Bangalore (exact 3 tokens) |
| **Arjun** | `bangalore.*.ride` | Any ride request in any Bangalore zone (exact 3 tokens) |
| **Meena** | `bangalore.*.food` | Any food request in any Bangalore zone (exact 3 tokens) |
| **Karthik** | `chennai.tnagar.food` | Food requests specifically in T-Nagar, Chennai |
| **Control** | `bangalore.#` | All requests within Bangalore regardless of zone, type, or depth |

---

### Routing Table Verification
For each incoming routing key, the topic exchange matches the key against the bound queue patterns using AMQP topic matching semantics:
- `*` (star) matches **exactly one** dot-delimited word.
- `#` (hash) matches **zero or more** dot-delimited words.

| # | Routing Key | Receiving Queues | Matching Logic Explanation |
|---|:---|:---|:---|
| **1** | `bangalore.koramangala.food` | **Ravi, Meena, Control** | - **Ravi** matches: 3rd token `food` matches `*`<br>- **Meena** matches: 2nd token `koramangala` matches `*`, 3rd is `food`<br>- **Control** matches: starts with `bangalore.`, `#` matches `koramangala.food` |
| **2** | `bangalore.indiranagar.ride` | **Priya, Arjun, Control** | - **Priya** matches: 3rd token `ride` matches `*`<br>- **Arjun** matches: 2nd token `indiranagar` matches `*`, 3rd is `ride`<br>- **Control** matches: starts with `bangalore.`, `#` matches `indiranagar.ride` |
| **3** | `bangalore.whitefield.ride` | **Arjun, Control** | - **Arjun** matches: 2nd token `whitefield` matches `*`, 3rd is `ride`<br>- **Control** matches: starts with `bangalore.`, `#` matches `whitefield.ride`<br>*(Neither Ravi nor Priya matches because zone is whitefield)* |
| **4** | `chennai.tnagar.food` | **Karthik** | - **Karthik** exact match (`chennai.tnagar.food`)<br>*(All Bangalore drivers ignore non-Bangalore keys)* |
| **5** | `chennai.whitefield.ride` | **dropped** | - No driver is bound to `chennai.*.ride` or `chennai.whitefield.*`. Since no matching binding exists, the message is unrouted and safely dropped by RabbitMQ. |
| **6** | `bangalore.koramangala` | **Control** | - Has only 2 words. See detailed analysis below. |

---

### Analysis of Routing Key 6: `bangalore.koramangala` (Two Words)
**Why does this key match ONLY `Control` and NOT `Ravi`, `Arjun`, or `Meena`?**

1. **Why `Control` (`bangalore.#`) matches:**
   - The symbol `#` in AMQP topic matching represents **zero or more words**.
   - With `bangalore.#`, matching starts at `bangalore.`. The remainder of the key is `koramangala` (1 word), which falls strictly in the range of $\ge 0$ words.
   - Even if the key were simply `bangalore` (0 words after), `Control` would match. Hence `bangalore.koramangala` is delivered to `Control`.

2. **Why `Ravi` (`bangalore.koramangala.*`) fails to match:**
   - The symbol `*` strictly requires **exactly one word**. It cannot match an empty or non-existent token.
   - `bangalore.koramangala.*` mandates a three-part key (`<city>.<zone>.<type>`).
   - Because `bangalore.koramangala` has only two tokens, there is no third token for `*` to bind to. Therefore, Ravi's queue cannot receive this message.

3. **Why `Arjun` (`bangalore.*.ride`) and `Meena` (`bangalore.*.food`) fail:**
   - Both patterns mandate that the third token must be explicitly `ride` or `food`.
   - In `bangalore.koramangala`, there is neither a third token nor a type identifier. Thus, neither pattern matches.

---

## D2. Implementation Details (`stream/dispatcher.py` & `stream/driver.py`)

- **Topic Exchange:** `rapido-dispatch` (durable).
- **Dispatcher (`stream/dispatcher.py`):**
  - Runs in an infinite loop emitting a dispatch message every 2 seconds.
  - Generates JSON payload:
    ```json
    {
      "id": "e4b1c8a0-...",
      "city": "bangalore",
      "zone": "koramangala",
      "type": "food",
      "ts": 1731671234.56
    }
    ```
  - Publishes using the corresponding dot-separated key `<city>.<zone>.<type>`.
  - Injects a malformed message (missing `zone` field) once every 10 messages for Part D3 validation.
- **Driver (`stream/driver.py <NAME> <PATTERN>`):**
  - Connects to broker, declares an exclusive, transient private queue (`exclusive=True`).
  - Binds the queue to `rapido-dispatch` with the assigned pattern.
  - Processes incoming messages with strict acknowledgement semantics.

---

## D3. Message Acknowledgements: ACK and NACK Semantics

### ACK / NACK Logic
- **Valid Messages:** When `zone` is present and JSON is valid, the driver logs the message and issues `channel.basic_ack(delivery_tag=method.delivery_tag)`. The broker removes the delivered message from the queue.
- **Malformed / Poison Messages:** When the `zone` field is missing, the driver detects data corruption, logs a warning, and issues:
  ```python
  channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
  ```
  Setting `requeue=False` instructs RabbitMQ to discard the message immediately (or route it to a Dead Letter Exchange if configured), preventing unprocessable messages from clogging the system.

### Answers to Core Streaming Questions

#### 1. What happens to a message if the driver crashes before sending an ACK?
RabbitMQ tracks unacknowledged deliveries per channel. When a consumer node/worker process abruptly crashes or loses network connectivity:
- The TCP connection and AMQP channel close automatically.
- RabbitMQ detects the channel closure and flags all unacknowledged deliveries (`unacked`) held by that worker.
- RabbitMQ immediately transitions those messages back to `ready` state and **re-delivers** them to another active consumer bound to the same queue (or holds them in the queue until a new worker reconnects).
- **Benefit:** Guarantees **at-least-once delivery** and prevents message loss during infrastructure or application failure.

#### 2. What would happen with `requeue=True` for a message that is always bad (poison pill)?
- If a malformed message (such as one with missing schema attributes or unparseable syntax) is rejected with `requeue=True`, RabbitMQ puts the message right back into the head of the queue.
- The queue will immediately re-deliver the exact same message to the consumer.
- The consumer will parse it, fail validation again, and issue `basic_nack(requeue=True)` once more.
- This creates an **infinite retry loop (poison-pill storm)**:
  1. CPU consumption spikes to 100%.
  2. Consumer logs get flooded with identical failure traces.
  3. All legitimate messages queued behind the poison pill get completely starved and delayed.
- **Solution:** Always reject permanently unrecoverable messages with `requeue=False` (discard or Dead Letter Queue `DLQ`), and reserve `requeue=True` strictly for transient failures (such as temporary database connection timeouts).
