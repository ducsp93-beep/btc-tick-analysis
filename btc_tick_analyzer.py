"""
BTC Tick Pattern Analyzer
==========================
Streams live BTC-USD trade ticks from Coinbase's public WebSocket feed,
maintains rolling gap lists (price / volume / cashflow / time), and detects:

  - Acceleration / Deceleration  (price, volume, cashflow gap magnitudes trending)
  - Aggressive matching / Sparse matching  (time-gap trend: fast vs. slow trading pace)
  - Reluctant (alternating sign / alternating zig-zag) behaviour
  - Flat prices (price) / Steady (volume, cashflow) / Paced matching (time)
    - repeated identical consecutive gap values

Sends a push notification to https://ntfy.sh/btc-1st-notif whenever a
pattern is detected, and runs forever until you close the window (Ctrl+C).

Dependencies (install once):
    pip install websocket-client requests

Run:
    python btc_tick_analyzer.py
"""

import json
import os
import time
import threading
from collections import deque
from datetime import datetime, timezone

import requests
import websocket  # from websocket-client package

# ---------------------------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------------------------

PRODUCT_ID = "BTC-USD"
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
NTFY_URL = "https://ntfy.sh/btc-2nd-notif-good-time-ahead"

# Optional: set the MAX_RUNTIME_MINUTES environment variable to make the
# script exit cleanly after a fixed duration (used for the GitHub Actions
# scheduled run below). Leave unset to run forever, as before, for local use.
MAX_RUNTIME_MINUTES = os.environ.get("MAX_RUNTIME_MINUTES")

# Equality tolerance for float comparisons (harmony / sign checks)
EPS = 1e-9

# ---- Step 2: the numbers you defined -------------------------------------
A_PRICE = 8
A_VOLUME = 9
A_CASHFLOW = 10
A_TIME = 10

R_PRICE = 16
R_VOLUME = 20
R_CASHFLOW = 20
R_TIME = 18

H_PRICE = 55
H_VOLUME = 10
H_CASHFLOW = 20
H_TIME = 32

# ---- Step 3: buffer size ---------------------------------------------------
# Per your spec: Max_count = Max(A_*, R_*). We also fold in H_* for safety,
# so the buffer can never be smaller than any window we need to inspect.
MAX_COUNT = max(
    A_PRICE, A_VOLUME, A_CASHFLOW, A_TIME,
    R_PRICE, R_VOLUME, R_CASHFLOW, R_TIME,
    H_PRICE, H_VOLUME, H_CASHFLOW, H_TIME,
)

print(f"[INIT] Max_count (rolling buffer size) = {MAX_COUNT}")

# ---------------------------------------------------------------------------
# Rolling gap buffers (Step 4). deque(maxlen=...) automatically drops the
# oldest value once full, so "if the list exists, just append" is handled
# for free.
# ---------------------------------------------------------------------------
price_gaps = deque(maxlen=MAX_COUNT)
volume_gaps = deque(maxlen=MAX_COUNT)
cashflow_gaps = deque(maxlen=MAX_COUNT)
time_gaps = deque(maxlen=MAX_COUNT)   # always positive (duration)

_lock = threading.Lock()

_prev_tick = {"price": None, "size": None, "time": None}

# Snapshot of the most recent tick, used to enrich notification detail text.
_last_tick_snapshot = {"price": None, "size": None, "cashflow": None, "time": None}

# Track last-sent (category, type, count) to avoid re-notifying every single
# tick while a streak is being maintained/extended in the exact same way.
_last_notified = {}

# Used to stop cleanly after MAX_RUNTIME_MINUTES (e.g. inside a scheduled CI job).
_shutdown_event = threading.Event()
_current_ws = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def sign(x, eps=EPS):
    if x > eps:
        return 1
    elif x < -eps:
        return -1
    return 0


def build_title(category, value_type):
    """
    Time-based categories (Aggressive matching / Sparse matching / Paced matching /
    Reluctant matching) are already complete phrases - no "market entry" or type
    word needed. "Flat prices" is likewise already a complete phrase.
    Everything else (Steady/Accelerating/Decelerating/Reluctant for
    price/volume/cashflow) gets a single type word appended, e.g. "Accelerating price".
    """
    if value_type == "time":
        return category
    if category == "Flat prices":
        return category
    return f"{category} {value_type}"


def build_detail(category, value_type, count, latest_gap):
    """
    Body: just the tick count plus a few vital details, no "Type:"/"Count:" labels.
    """
    lines = [f"{count} ticks"]

    snap = _last_tick_snapshot
    if snap["price"] is not None:
        lines.append(f"Price: ${snap['price']:,.2f}")

    if latest_gap is not None:
        if value_type == "price":
            lines.append(f"Last price gap: {latest_gap:+.2f}")
        elif value_type == "volume":
            lines.append(f"Last volume gap: {latest_gap:+.6f} BTC")
        elif value_type == "cashflow":
            lines.append(f"Last cashflow gap: {latest_gap:+.2f} USD")
        elif value_type == "time":
            lines.append(f"Last tick gap: {latest_gap:.3f}s")

    if snap["time"] is not None:
        lines.append(f"At: {snap['time'].strftime('%Y-%m-%d %H:%M:%S UTC')}")

    return "\n".join(lines)


def send_notification(category, value_type, count, latest_gap=None):
    key = (category, value_type, count)
    now = time.time()
    last_time = _last_notified.get(key, 0)
    # Throttle identical repeat notifications to once every 5 seconds
    if now - last_time < 5:
        return
    _last_notified[key] = now

    title = build_title(category, value_type)
    message = build_detail(category, value_type, count, latest_gap)
    try:
        requests.post(
            NTFY_URL,
            data=message.encode("utf-8"),
            headers={"Title": title},
            timeout=5,
        )
        print(f"[NOTIFY SENT] {title} -> {count} ticks")
    except Exception as e:
        print(f"[NOTIFY FAILED] {title} -> {e}")


# ---------------------------------------------------------------------------
# 5. Analysis functions
# ---------------------------------------------------------------------------
def check_acceleration_deceleration(gaps, count, label):
    """Same-sign window, magnitude strictly increasing (>) => Accelerating,
    magnitude strictly decreasing (<) => Decelerating.
    Strict comparisons keep this from overlapping with Steady, which requires
    all values in the window to be exactly equal."""
    if len(gaps) < count:
        return
    window = list(gaps)[-count:]
    signs = [sign(v) for v in window]
    if 0 in signs or len(set(signs)) != 1:
        return
    abs_vals = [abs(v) for v in window]
    if all(abs_vals[i] > abs_vals[i - 1] for i in range(1, len(abs_vals))):
        send_notification("Accelerating", label, count, latest_gap=window[-1])
    elif all(abs_vals[i] < abs_vals[i - 1] for i in range(1, len(abs_vals))):
        send_notification("Decelerating", label, count, latest_gap=window[-1])


def check_active_quiet(count=A_TIME):
    """Time-gap window strictly shrinking (<) => Aggressive matching (fast, busy trading);
    strictly growing (>) => Sparse matching (slow, thin trading).
    Strict comparisons (rather than <=/>=) keep this from overlapping with
    Paced matching, which requires the gaps to be exactly equal."""
    if len(time_gaps) < count:
        return
    window = list(time_gaps)[-count:]
    if all(window[i] < window[i - 1] for i in range(1, len(window))):
        send_notification("Aggressive matching", "time", count, latest_gap=window[-1])
    elif all(window[i] > window[i - 1] for i in range(1, len(window))):
        send_notification("Sparse matching", "time", count, latest_gap=window[-1])


def check_reluctant_sign(gaps, count, label):
    """Signs strictly alternate +/-/+/-... across the window."""
    if len(gaps) < count:
        return
    window = list(gaps)[-count:]
    signs = [sign(v) for v in window]
    if 0 in signs:
        return
    if all(signs[i] != signs[i - 1] for i in range(1, len(signs))):
        send_notification("Reluctant", label, count, latest_gap=window[-1])


def check_reluctant_time(count=R_TIME):
    """Time gaps are all positive, so 'reluctant' here means the duration
    zig-zags: bigger-then-smaller-then-bigger, alternating each step."""
    if len(time_gaps) < count:
        return
    window = list(time_gaps)[-count:]
    directions = []
    for i in range(1, len(window)):
        if window[i] > window[i - 1]:
            directions.append(1)
        elif window[i] < window[i - 1]:
            directions.append(-1)
        else:
            directions.append(0)
    if 0 in directions:
        return
    if all(directions[i] != directions[i - 1] for i in range(1, len(directions))):
        send_notification("Reluctant matching", "time", count, latest_gap=window[-1])


def check_harmony(gaps, count, label):
    """count-or-more consecutive (approximately) identical values.
    - time     -> "Paced matching" (trades landing at an even, regular rhythm)
    - price    -> "Flat prices" (a busy trend has paused; price sits still, small size trades)
    - volume/cashflow -> "Steady" """
    if len(gaps) < count:
        return
    window = list(gaps)[-count:]
    first = window[0]
    if all(abs(v - first) < EPS for v in window):
        if label == "time":
            category = "Paced matching"
        elif label == "price":
            category = "Flat prices"
        else:
            category = "Steady"
        send_notification(category, label, count, latest_gap=window[-1])


def run_all_checks():
    # Acceleration / Deceleration
    check_acceleration_deceleration(price_gaps, A_PRICE, "price")
    check_acceleration_deceleration(volume_gaps, A_VOLUME, "volume")
    check_acceleration_deceleration(cashflow_gaps, A_CASHFLOW, "cashflow")

    # Active / Quiet (market entry pace)
    check_active_quiet(A_TIME)

    # Reluctant (alternating)
    check_reluctant_sign(price_gaps, R_PRICE, "price")
    check_reluctant_sign(volume_gaps, R_VOLUME, "volume")
    check_reluctant_sign(cashflow_gaps, R_CASHFLOW, "cashflow")
    check_reluctant_time(R_TIME)

    # Harmony
    check_harmony(price_gaps, H_PRICE, "price")
    check_harmony(volume_gaps, H_VOLUME, "volume")
    check_harmony(cashflow_gaps, H_CASHFLOW, "cashflow")
    check_harmony(time_gaps, H_TIME, "time")


# ---------------------------------------------------------------------------
# 4. Tick handling -> gap computation
# ---------------------------------------------------------------------------
def parse_time(ts_str):
    # Coinbase timestamps look like "2024-05-01T12:34:56.123456Z"
    ts_str = ts_str.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str)


def handle_new_tick(price, size, tick_time):
    cashflow = price * size

    # Print every streaming tick so you can watch/double-check the raw feed live.
    print(
        f"[TICK] {tick_time.strftime('%H:%M:%S.%f')[:-3]} UTC | "
        f"price=${price:,.2f} | size={size:.6f} BTC | cashflow=${cashflow:,.2f}"
    )

    with _lock:
        prev = _prev_tick
        if prev["price"] is not None:
            price_gap = price - prev["price"]                     # signed
            volume_gap = size - prev["size"]                       # signed
            cashflow_gap = cashflow - (prev["price"] * prev["size"])  # signed
            time_gap = abs((tick_time - prev["time"]).total_seconds())      # always positive

            price_gaps.append(price_gap)
            volume_gaps.append(volume_gap)
            cashflow_gaps.append(cashflow_gap)
            time_gaps.append(time_gap)

            # Keep this fresh before running checks, so notifications fired
            # during run_all_checks() reflect the current tick.
            _last_tick_snapshot["price"] = price
            _last_tick_snapshot["size"] = size
            _last_tick_snapshot["cashflow"] = cashflow
            _last_tick_snapshot["time"] = tick_time

            run_all_checks()

        _prev_tick["price"] = price
        _prev_tick["size"] = size
        _prev_tick["time"] = tick_time


# ---------------------------------------------------------------------------
# WebSocket plumbing
# ---------------------------------------------------------------------------
def on_open(ws):
    print("[WS] Connected. Subscribing to matches channel...")
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": [PRODUCT_ID],
        "channels": ["matches"],
    }
    ws.send(json.dumps(subscribe_msg))


def on_message(ws, message):
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return

    if data.get("type") not in ("match", "last_match"):
        return

    try:
        price = float(data["price"])
        size = float(data["size"])
        tick_time = parse_time(data["time"])
    except (KeyError, ValueError):
        return

    handle_new_tick(price, size, tick_time)


def on_error(ws, error):
    print(f"[WS ERROR] {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[WS CLOSED] code={close_status_code} msg={close_msg}")


def _shutdown_timer(minutes):
    """Waits `minutes`, then signals shutdown and closes the active socket
    so run_forever_with_reconnect() exits its loop cleanly."""
    _shutdown_event.wait(timeout=minutes * 60)
    if not _shutdown_event.is_set():
        print(f"[TIMER] Max runtime of {minutes} minutes reached - shutting down.")
        _shutdown_event.set()
        if _current_ws is not None:
            try:
                _current_ws.close()
            except Exception:
                pass


def run_forever_with_reconnect():
    global _current_ws

    if MAX_RUNTIME_MINUTES:
        minutes = float(MAX_RUNTIME_MINUTES)
        print(f"[INIT] Will auto-stop after {minutes} minutes (MAX_RUNTIME_MINUTES set).")
        threading.Thread(target=_shutdown_timer, args=(minutes,), daemon=True).start()

    while not _shutdown_event.is_set():
        ws = websocket.WebSocketApp(
            COINBASE_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        _current_ws = ws
        try:
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except KeyboardInterrupt:
            print("\n[EXIT] Stopped by user.")
            break
        except Exception as e:
            print(f"[WS CRASH] {e}")

        if _shutdown_event.is_set():
            print("[EXIT] Max runtime reached. Clean shutdown.")
            break

        print("[WS] Reconnecting in 3 seconds...")
        time.sleep(3)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"[START] Streaming {PRODUCT_ID} trades from Coinbase. Press Ctrl+C to stop.")
    run_forever_with_reconnect()
