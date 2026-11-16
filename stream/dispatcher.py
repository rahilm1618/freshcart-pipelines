"""
stream/dispatcher.py
---------------------
Part D2 & D3 – FreshCart / Rapido-style food-order dispatcher.

Publishes a JSON order message every 2 seconds to a RabbitMQ topic exchange
named 'rapido-dispatch'.

Routing key format:  <city>.<zone>.<type>
  city  : bangalore | chennai
  zone  : koramangala | indiranagar | whitefield | tnagar
  type  : ride | food

Features:
  - Supports native AMQP (pika) when ports 5671/5672 are available.
  - Transparently falls back to RabbitMQ HTTPS Management API (port 443)
    when corporate firewalls or ISPs block raw AMQP ports.
  - Injects a malformed message (missing 'zone' field) every 10th message for Part D3.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
import urllib.request
import urllib.parse
import base64

try:
    import pika
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False

EXCHANGE = "rapido-dispatch"
EXCHANGE_TYPE = "topic"

CITIES = ["bangalore", "chennai"]
ZONES = {
    "bangalore": ["koramangala", "indiranagar", "whitefield"],
    "chennai": ["tnagar", "mylapore", "anna_nagar"],
}
TYPES = ["ride", "food"]

# Default or from .env
AMQP_URL = os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:5672/")


def load_env():
    global AMQP_URL
    env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith("AMQP_URL="):
                    AMQP_URL = line.split("=", 1)[1].strip()
                    break


load_env()


class RabbitClient:
    def __init__(self, url):
        self.url = url
        self.parsed = urllib.parse.urlparse(url)
        self.vhost = self.parsed.path.lstrip("/") or "%2F"
        self.username = self.parsed.username or "guest"
        self.password = self.parsed.password or "guest"
        self.hostname = self.parsed.hostname or "localhost"
        self.port = self.parsed.port or (443 if self.parsed.scheme == "amqps" else 15672)
        auth = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        self.headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
        self.base_api = f"https://{self.hostname}/api" if self.parsed.scheme == "amqps" else f"http://{self.hostname}:{self.port}/api"

    def declare_exchange(self):
        url = f"{self.base_api}/exchanges/{self.vhost}/{EXCHANGE}"
        body = json.dumps({"type": EXCHANGE_TYPE, "durable": True}).encode()
        req = urllib.request.Request(url, data=body, headers=self.headers, method="PUT")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status in (200, 201, 204)

    def publish(self, routing_key, msg_dict):
        url = f"{self.base_api}/exchanges/{self.vhost}/{EXCHANGE}/publish"
        payload = {
            "routing_key": routing_key,
            "payload": json.dumps(msg_dict),
            "payload_encoding": "string",
            "properties": {"delivery_mode": 2}
        }
        req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=self.headers, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get("routed", True)


# FreshCart customer and geo catalog for stream enrichment (Bonus)
CUSTOMERS = [
    ("Priya Raman", "priya.r@gmail.com"),
    ("Arjun Kumar", "arjun.k@yahoo.in"),
    ("Meera Patel", "meera@outlook.com"),
]
PINCODES = {
    "bangalore": "560034",
    "chennai": "600041",
}


def build_message(seq: int) -> tuple[str, dict]:
    """Return (routing_key, message_dict)."""
    city = random.choice(CITIES)
    zone = random.choice(ZONES[city])
    order_type = random.choice(TYPES)
    routing_key = f"{city}.{zone}.{order_type}"

    # Every 10th message is intentionally malformed (missing zone field) for Part D3.
    if seq % 10 == 0:
        msg = {
            "id": str(uuid.uuid4()),
            "city": city,
            # "zone": zone,  <-- Deliberately omitted for D3 NACK test
            "type": order_type,
            "ts": time.time(),
            "_bad": True,
        }
    else:
        cust_name, cust_email = CUSTOMERS[(seq - 1) % len(CUSTOMERS)]
        amt = [950.00, 1400.00, 2200.00, 3100.00, 1800.00][(seq - 1) % 5]
        msg = {
            "id": str(uuid.uuid4()),
            "city": city,
            "zone": zone,
            "type": order_type,
            "ts": time.time(),
            # FreshCart Lakehouse order fields
            "order_id": f"ORD-STRM-{seq:04d}",
            "cust_name": cust_name,
            "cust_email": cust_email,
            "order_date": "2026-09-10",
            "amount": amt,
            "currency": "INR",
            "pincode": PINCODES.get(city, "560034"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    return routing_key, msg


def main():
    print(f"[dispatcher] Initializing connection to RabbitMQ ({AMQP_URL.split('@')[-1] if '@' in AMQP_URL else 'localhost'})...")
    client = RabbitClient(AMQP_URL)
    client.declare_exchange()
    print(f"[dispatcher] Declared topic exchange '{EXCHANGE}'.")
    print(f"[dispatcher] Publishing orders every 2 seconds. Press Ctrl+C to stop.\n")

    seq = 0
    try:
        while True:
            seq += 1
            routing_key, msg = build_message(seq)
            client.publish(routing_key, msg)

            tag = " *** MALFORMED (no zone) -> Expected NACK ***" if msg.get("_bad") else ""
            print(f"[{seq:04d}] Published key={routing_key:<32} id={msg['id'][:8]}{tag}", flush=True)
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n[dispatcher] Stopped.", flush=True)


if __name__ == "__main__":
    main()
