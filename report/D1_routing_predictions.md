# Part D1: RabbitMQ Topic Routing Predictions

**Date:** 2026-11-15  
**Course:** Data Engineering · Pipeline Orchestration  
**Academic Integrity Statement:**  
*These topic routing predictions are calculated on paper and formally recorded prior to the live execution and benchmarking of the RabbitMQ dispatcher and driver consumers. No predictions have been altered retroactively.*

---

## 1. Driver Binding Patterns (Subscribers)

| Driver Name | Binding Pattern | Subscription Scope | Token Depth Required |
|:---|:---|:---|:---:|
| **Ravi** | `bangalore.koramangala.*` | Any activity in Koramangala, Bangalore | Exactly 3 tokens |
| **Priya** | `bangalore.indiranagar.*` | Any activity in Indiranagar, Bangalore | Exactly 3 tokens |
| **Arjun** | `bangalore.*.ride` | Any ride in any zone across Bangalore | Exactly 3 tokens |
| **Meena** | `bangalore.*.food` | Any food delivery in any zone across Bangalore | Exactly 3 tokens |
| **Karthik** | `chennai.tnagar.food` | Food delivery exclusively in T-Nagar, Chennai | Exactly 3 tokens |
| **Control** | `bangalore.#` | All traffic and events within Bangalore | 1 or more tokens ($\ge 1$) |

---

## 2. Topic Matching Evaluation Table

In AMQP 0-9-1 topic exchanges:
- `*` (asterisk) substitutes for **exactly one** dot-delimited word.
- `#` (hash) substitutes for **zero or more** dot-delimited words.

| # | Incoming Routing Key | Receiving Queues (Drivers) | Routing Logic Breakdown |
|:---:|:---|:---|:---|
| **1** | `bangalore.koramangala.food` | **Ravi, Meena, Control** | - **Ravi** (`bangalore.koramangala.*`): `food` matches `*`<br>- **Meena** (`bangalore.*.food`): `koramangala` matches `*`, ends with `food`<br>- **Control** (`bangalore.#`): `#` matches `koramangala.food`<br>- *Priya, Arjun, Karthik do not match (wrong zone/type/city).* |
| **2** | `bangalore.indiranagar.ride` | **Priya, Arjun, Control** | - **Priya** (`bangalore.indiranagar.*`): `ride` matches `*`<br>- **Arjun** (`bangalore.*.ride`): `indiranagar` matches `*`, ends with `ride`<br>- **Control** (`bangalore.#`): `#` matches `indiranagar.ride`<br>- *Ravi, Meena, Karthik do not match.* |
| **3** | `bangalore.whitefield.ride` | **Arjun, Control** | - **Arjun** (`bangalore.*.ride`): `whitefield` matches `*`, ends with `ride`<br>- **Control** (`bangalore.#`): `#` matches `whitefield.ride`<br>- *Ravi and Priya do not match because zone is whitefield; Meena fails on type (ride $\ne$ food).* |
| **4** | `chennai.tnagar.food` | **Karthik** | - **Karthik** (`chennai.tnagar.food`): exact 3-token match.<br>- *All other drivers subscribe strictly to `bangalore` prefixes.* |
| **5** | `chennai.whitefield.ride` | **dropped** | - No driver binding exists for Chennai rides (`chennai.*.ride`) or Whitefield in Chennai.<br>- RabbitMQ topic exchange drops unroutable messages safely when no bound queue matches. |
| **6** | `bangalore.koramangala` | **Control** | - Has only 2 words. See detailed linguistic and mathematical analysis below. |

---

## 3. Detailed Analysis of Key 6: `bangalore.koramangala` (Two Words)

Routing key #6 consists of only **two tokens**: `[bangalore]` and `[koramangala]`.

### Why `Control` (`bangalore.#`) MATCHES:
- The `#` wildcard represents **zero or more** words.
- The pattern `bangalore.#` requires the first token to be `bangalore`. Following the dot, `#` matches the token sequence `['koramangala']` (length = 1 word).
- Because 1 word satisfies the range $\ge 0$, the key is successfully matched and routed to **Control**.
- *(In fact, even if the routing key were simply `bangalore`, `#` would match 0 words and Control would still receive it).*

### Why `Ravi` (`bangalore.koramangala.*`) FAILS:
- The `*` wildcard represents **strictly one word**. It is non-optional and cannot match the empty string or absence of a token.
- `bangalore.koramangala.*` mandates a structure with exactly **three tokens**:
  1. Token 1 = `bangalore`
  2. Token 2 = `koramangala`
  3. Token 3 = any single word (e.g., `food`, `ride`, `express`)
- In `bangalore.koramangala`, there is no 3rd token. The pattern parser cannot satisfy `*`, causing the match to evaluate to `False`.

### Why `Arjun` (`bangalore.*.ride`) and `Meena` (`bangalore.*.food`) FAIL:
- Both patterns require Token 1 to be `bangalore`, Token 2 to be any word, and Token 3 to be a literal string (`ride` or `food`).
- Because key #6 terminates after Token 2, neither required third token exists. Both evaluate to `False`.

---

## 4. Verification Check
- Total evaluated routing keys: 6
- Unroutable / dropped messages: 1 (`chennai.whitefield.ride`)
- Single-consumer messages: 2 (`chennai.tnagar.food`, `bangalore.koramangala`)
- Multi-consumer messages: 3 (`bangalore.koramangala.food`, `bangalore.indiranagar.ride`, `bangalore.whitefield.ride`)
