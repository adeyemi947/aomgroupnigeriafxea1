# module_f_feedback_learner.py

import redis
import json
import time
import threading
from datetime import datetime, timedelta
import logging
import os

# Redis setup
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Logging setup
logging.basicConfig(filename='module_f_feedback.log', level=logging.INFO, format='%(asctime)s - %(message)s')

# Feedback tracking store (fallback memory)
feedback_store = {}
EXPIRY_SECONDS = 3600 * 24 * 5  # 5 days

def load_feedback():
    if os.path.exists("feedback_store.json"):
        with open("feedback_store.json", "r") as f:
            try:
                return json.load(f)
            except:
                return {}
    return {}

def save_feedback():
    with open("feedback_store.json", "w") as f:
        json.dump(feedback_store, f, indent=2)

def track_signal_feedback(symbol, strategy, outcome, reason=""):
    key = f"{symbol}:{strategy}"
    feedback = feedback_store.get(key, {"wins": 0, "losses": 0, "last_updated": None, "last_outcome": None, "reason": ""})

    if outcome == "win":
        feedback["wins"] += 1
    else:
        feedback["losses"] += 1

    feedback["last_updated"] = datetime.utcnow().isoformat()
    feedback["last_outcome"] = outcome
    feedback["reason"] = reason

    feedback_store[key] = feedback
    save_feedback()

    logging.info(f"Feedback tracked: {key} - {outcome} | {reason}")

def mark_strategy_invalid(symbol, strategy):
    redis_key = f"strategy:blacklist:{symbol}:{strategy}"
    redis_client.set(redis_key, "1", ex=EXPIRY_SECONDS)
    logging.warning(f"Blacklisted {strategy} for {symbol} temporarily due to feedback.")

def is_strategy_blacklisted(symbol, strategy):
    redis_key = f"strategy:blacklist:{symbol}:{strategy}"
    return redis_client.exists(redis_key)

def send_learning_feedback_to_module_b(symbol, strategy, stats):
    redis_client.publish("feedback:to:module_b", json.dumps({
        "symbol": symbol,
        "strategy": strategy,
        "feedback": stats
    }))
    logging.info(f"Sent feedback to Module B for adaptation: {symbol} - {strategy} - {stats}")

def send_learning_feedback_to_module_e(symbol, strategy, stats):
    redis_client.publish("feedback:to:module_e", json.dumps({
        "symbol": symbol,
        "strategy": strategy,
        "feedback": stats
    }))
    logging.info(f"Sent feedback to Module E for filtering update.")

def adaptive_learning_loop():
    while True:
        time.sleep(60)  # Evaluate feedback every 60 seconds

        for key, stats in feedback_store.items():
            wins, losses = stats.get("wins", 0), stats.get("losses", 0)
            total = wins + losses

            if total >= 5:
                win_rate = wins / total
                symbol, strategy = key.split(":")

                if win_rate < 0.3:
                    mark_strategy_invalid(symbol, strategy)
                elif win_rate > 0.7:
                    send_learning_feedback_to_module_b(symbol, strategy, stats)
                    send_learning_feedback_to_module_e(symbol, strategy, stats)

def receive_trade_outcome():
    pubsub = redis_client.pubsub()
    pubsub.subscribe("trade_feedback")

    for message in pubsub.listen():
        if message['type'] != 'message':
            continue

        try:
            data = json.loads(message['data'])
            symbol = data.get("symbol")
            strategy = data.get("strategy")
            outcome = data.get("outcome")  # "win" or "loss"
            reason = data.get("reason", "")

            if symbol and strategy and outcome:
                track_signal_feedback(symbol, strategy, outcome, reason)
        except Exception as e:
            logging.error(f"Failed to process feedback: {e}")

def purge_expired_feedback():
    """Optional maintenance task to remove stale feedback (not used recently)."""
    to_delete = []
    now = datetime.utcnow()

    for key, stats in feedback_store.items():
        try:
            last_updated = datetime.fromisoformat(stats["last_updated"])
            if (now - last_updated) > timedelta(days=10):
                to_delete.append(key)
        except:
            continue

    for key in to_delete:
        del feedback_store[key]
    save_feedback()

# Start feedback receiver + learning loop
if __name__ == "__main__":
    feedback_store = load_feedback()

    threading.Thread(target=adaptive_learning_loop, daemon=True).start()
    threading.Thread(target=receive_trade_outcome, daemon=True).start()

    print("[MODULE F] Feedback Learner running...")
    try:
        while True:
            time.sleep(60)
            purge_expired_feedback()
    except KeyboardInterrupt:
        print("[MODULE F] Stopped.")
