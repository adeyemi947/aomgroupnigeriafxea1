import json
import time
import heapq
import requests
import redis
import logging
import hashlib
from datetime import datetime, timedelta
from cryptography.fernet import Fernet

# === CONFIG ===
ROUTING_TARGETS = {
    "rest": "http://localhost:8002/receive_signal",
    "redis": "module_d:signals",
    "logfile": "routed_signals.log"
}
MAX_SIGNAL_AGE_MINUTES = 5
FERNET_KEY = b'YourGeneratedKeyHere'  # 32 url-safe base64-encoded bytes
ENABLE_ENCRYPTION = True
MODULE_F_FEEDBACK_ENDPOINT = "http://localhost:8006/feedback"
DEDUPLICATION_WINDOW_SECONDS = 300  # 5 minutes

# === INIT ===
redis_client = redis.Redis(host="localhost", port=6379, db=0)
fernet = Fernet(FERNET_KEY)
logging.basicConfig(filename="router_audit.log", level=logging.INFO)

# === Signal Queue and Deduplication ===
signal_queue = []
dedup_cache = {}  # {hash: timestamp}


# === Utilities ===
def generate_signal_hash(signal: dict) -> str:
    """Create a unique hash of the signal content for deduplication."""
    hashable = f"{signal['symbol']}|{signal['direction']}|{signal['confidence']}|{signal['reason']}|{signal['timestamp']}"
    return hashlib.sha256(hashable.encode()).hexdigest()


def is_duplicate(signal_hash: str) -> bool:
    """Check for recent duplicate signals."""
    now = time.time()
    if signal_hash in dedup_cache:
        if now - dedup_cache[signal_hash] <= DEDUPLICATION_WINDOW_SECONDS:
            return True
        else:
            del dedup_cache[signal_hash]
    return False


def mark_as_seen(signal_hash: str):
    """Store the signal hash for deduplication purposes."""
    dedup_cache[signal_hash] = time.time()


def cleanup_dedup_cache():
    """Clean up expired entries from dedup cache."""
    now = time.time()
    expired_keys = [k for k, ts in dedup_cache.items() if now - ts > DEDUPLICATION_WINDOW_SECONDS]
    for k in expired_keys:
        del dedup_cache[k]


# === Signal Processing ===

def enqueue_signal(signal):
    """Add a signal to the priority queue based on confidence (descending)."""
    try:
        confidence = float(signal.get("confidence", 0))
        heapq.heappush(signal_queue, (-confidence, signal))
    except Exception as e:
        print(f"[Router] Enqueue Error: {e}")

def is_signal_valid(signal):
    """Check if signal is still valid based on timestamp and expiry."""
    try:
        ts = datetime.fromisoformat(signal["timestamp"])
        if datetime.utcnow() - ts > timedelta(minutes=MAX_SIGNAL_AGE_MINUTES):
            return False
        expiry = datetime.fromisoformat(signal["expires"])
        if datetime.utcnow() > expiry:
            return False
        return True
    except:
        return False

def encrypt_payload(payload):
    if ENABLE_ENCRYPTION:
        return fernet.encrypt(json.dumps(payload).encode()).decode()
    return json.dumps(payload)

def route_signal(signal):
    """Route signal to destinations and log failures."""
    signal_hash = generate_signal_hash(signal)
    if is_duplicate(signal_hash):
        logging.warning(f"[Router] Duplicate signal skipped: {signal['symbol']}")
        return
    mark_as_seen(signal_hash)

    payload = encrypt_payload(signal)

    # 1. REST Forward
    try:
        res = requests.post(ROUTING_TARGETS["rest"], json={"payload": payload}, timeout=5)
        if not res.ok:
            raise Exception(f"HTTP {res.status_code}")
    except Exception as e:
        logging.error(f"[Router] REST route failed: {e}")
        feedback_to_module_f(signal, f"REST forward failed: {e}")

    # 2. Redis Forward
    try:
        redis_client.rpush(ROUTING_TARGETS["redis"], payload)
    except Exception as e:
        logging.error(f"[Router] Redis push failed: {e}")
        feedback_to_module_f(signal, f"Redis forward failed: {e}")

    # 3. File Log
    try:
        with open(ROUTING_TARGETS["logfile"], "a") as f:
            f.write(json.dumps(signal) + "\n")
    except Exception as e:
        logging.error(f"[Router] Logfile write failed: {e}")

def feedback_to_module_f(signal, reason):
    """Send failed signals to Module F for learning."""
    try:
        data = {
            "signal": signal,
            "status": "failed",
            "reason": reason,
            "timestamp": datetime.utcnow().isoformat()
        }
        requests.post(MODULE_F_FEEDBACK_ENDPOINT, json=data, timeout=3)
    except Exception as e:
        logging.error(f"[Router] Feedback to Module F failed: {e}")

def router_loop():
    print("[Router] Started signal router loop.")
    while True:
        cleanup_dedup_cache()
        if signal_queue:
            _, signal = heapq.heappop(signal_queue)
            if not is_signal_valid(signal):
                feedback_to_module_f(signal, "expired")
                continue
            route_signal(signal)
        else:
            time.sleep(1)

# === Entry API ===

def receive_signal(signal):
    """External API to receive new signals (from Module B)."""
    enqueue_signal(signal)

# === Example Simulation ===
if __name__ == "__main__":
    now = datetime.utcnow()
    test_signals = [
        {
            "symbol": "EURUSD",
            "direction": "BUY",
            "confidence": 0.91,
            "reason": "RSI breakout",
            "timestamp": now.isoformat(),
            "expires": (now + timedelta(minutes=3)).isoformat()
        },
        {
            "symbol": "EURUSD",  # Duplicate
            "direction": "BUY",
            "confidence": 0.91,
            "reason": "RSI breakout",
            "timestamp": now.isoformat(),
            "expires": (now + timedelta(minutes=3)).isoformat()
        },
    ]
    for sig in test_signals:
        receive_signal(sig)

    router_loop()
