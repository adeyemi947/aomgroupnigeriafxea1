import json
import time
import logging
import requests
import redis
from datetime import datetime, timedelta

# === Configuration ===
ECONOMIC_FILTER_ENDPOINT = "http://localhost:8007/economic_filter"
FAILED_STRATEGY_CACHE = "module_e:failed_strategies"
SIGNAL_CACHE_KEY = "module_e:signal_cache"
OUTPUT_QUEUE = "module_e:to_module_f"
TO_MODULE_C_REST = "http://localhost:8003/filter_signal"
MAX_STRATEGY_AGE = 600  # seconds

redis_client = redis.Redis(host="localhost", port=6379, db=0)
logging.basicConfig(level=logging.INFO, filename="module_e.log", filemode="a")

# === Utility Functions ===
def fetch_cached_signals():
    """Fetch recent signals from cache"""
    try:
        cached = redis_client.lrange(SIGNAL_CACHE_KEY, 0, -1)
        return [json.loads(c.decode()) for c in cached]
    except Exception as e:
        logging.error(f"[Module E] Fetch signal cache failed: {e}")
        return []

def fetch_failed_strategies():
    """Fetch previously failed strategies"""
    try:
        raw = redis_client.get(FAILED_STRATEGY_CACHE)
        if raw:
            return json.loads(raw.decode())
        return {}
    except:
        return {}

def update_failed_strategies(strategy_name, symbol, reason):
    """Feed back failed strategy for future review"""
    try:
        current = fetch_failed_strategies()
        if symbol not in current:
            current[symbol] = []
        entry = {
            "strategy": strategy_name,
            "timestamp": datetime.utcnow().isoformat(),
            "reason": reason
        }
        current[symbol].append(entry)
        redis_client.set(FAILED_STRATEGY_CACHE,
