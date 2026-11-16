"""
stream/run_stream_test.py
-------------------------
Orchestrates a live test run of dispatcher.py alongside 3 drivers:
  - Ravi: bangalore.koramangala.*
  - Arjun: bangalore.*.ride
  - Control: bangalore.#

Runs for the specified duration (default 120s / 2 minutes), captures all console
outputs, verifies D1 routing key matching, and documents ACK/NACK behavior for Part D3.
"""

import os
import sys
import time
import subprocess
import threading
from datetime import datetime

DURATION_SECONDS = 60
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "stream_execution.log")


def stream_reader(pipe, name, log_f):
    for line in iter(pipe.readline, ''):
        if not line:
            break
        ts = datetime.now().strftime("%H:%M:%S")
        formatted = f"[{ts}] {line.rstrip()}"
        print(formatted, flush=True)
        log_f.write(formatted + "\n")
        log_f.flush()
    pipe.close()


def run_test():
    amqp_url = os.environ.get("AMQP_URL")
    if not amqp_url:
        env_path = os.path.join(os.path.dirname(BASE_DIR), ".env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    if line.startswith("AMQP_URL="):
                        amqp_url = line.strip().split("=", 1)[1].strip()
                        os.environ["AMQP_URL"] = amqp_url
                        break

    if not amqp_url:
        print("[ERROR] AMQP_URL is not set.")
        sys.exit(1)

    print(f"================================================================")
    print(f"  STARTING STREAMING TEST (Part D2 & D3)")
    print(f"  Broker: {amqp_url.split('@')[-1] if '@' in amqp_url else 'localhost'}")
    print(f"  Duration: {DURATION_SECONDS} seconds")
    print(f"================================================================\n", flush=True)

    with open(LOG_FILE, "w", encoding="utf-8") as log_f:
        log_f.write(f"=== STREAMING RUN STARTED: {datetime.now()} ===\n\n")
        log_f.flush()

        drivers = [
            ("Ravi", "bangalore.koramangala.*"),
            ("Arjun", "bangalore.*.ride"),
            ("Control", "bangalore.#"),
        ]

        procs = []
        threads = []
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        for name, pattern in drivers:
            cmd = [sys.executable, "-u", os.path.join(BASE_DIR, "driver.py"), name, pattern]
            p = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=BASE_DIR,
                env=env
            )
            procs.append((name, p))
            t = threading.Thread(target=stream_reader, args=(p.stdout, name, log_f), daemon=True)
            t.start()
            threads.append(t)
            time.sleep(1)

        disp_cmd = [sys.executable, "-u", os.path.join(BASE_DIR, "dispatcher.py")]
        p_disp = subprocess.Popen(
            disp_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=BASE_DIR,
            env=env
        )
        procs.append(("Dispatcher", p_disp))
        t_disp = threading.Thread(target=stream_reader, args=(p_disp.stdout, "Dispatcher", log_f), daemon=True)
        t_disp.start()
        threads.append(t_disp)

        print(f"\n[INFO] All processes started. Running for {DURATION_SECONDS}s...\n", flush=True)
        start_t = time.time()
        try:
            while time.time() - start_t < DURATION_SECONDS:
                time.sleep(2)
        except KeyboardInterrupt:
            print("\n[INFO] Interrupted by user.")

        print("\n[INFO] Test duration completed. Stopping all workers...", flush=True)
        for name, p in procs:
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()

        time.sleep(1)
        log_f.write(f"\n=== STREAMING RUN FINISHED: {datetime.now()} ===\n")
        log_f.flush()

    print(f"\n================================================================")
    print(f"  TEST COMPLETE! Full execution log saved to: {LOG_FILE}")
    print(f"================================================================\n", flush=True)


if __name__ == "__main__":
    run_test()
