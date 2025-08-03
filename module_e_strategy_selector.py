import json
import time
from datetime import datetime
import requests
import redis
import logging

# === CONFIG ===
REDIS_HOST = 'localhost'
SIGNAL_EVAL_QUEUE = 'module_e:evaluation'
ROUTE_TO_MODULE_I = "http://localhost:8009/execute_trade"
FEEDBACK_TO_MODULE_F = "http://localhost:8006/feedback"
ECONOMIC_FILTER_ENDPOINT = "http://localhost:8010/filter_signal"
STRATEGY_FAILURE_LOG = 'failed_strategies.json'

# === INIT ===
redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
logging.basicConfig(filename="module_e.log", level=logging.INFO)

# === STRATEGY FAILURE MEMORY ===
def load_failed_strategies():
    try:
        with open(STRATEGY_FAILURE_LOG, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_failed_strategies(failed_strategies):
    with open(STRATEGY_FAILURE_LOG, 'w') as f:
        json.dump(failed_strategies, f, indent=4)

failed_strategies = load_failed_strategies()


# === ECONOMIC FILTER ===
def passes_economic_filter(signal):
    try:
        res = requests.post(ECONOMIC_FILTER_ENDPOINT, json=signal, timeout=3)
        data = res.json()
        return data.get("pass", False), data.get("adjustment", 0.0)
    except Exception as e:
        logging.error(f"[EconomicFilter] Error: {e}")
        return False, -0.2  # Penalize confidence on failure

# === STRATEGY TRACKING ===
def strategy_failed_before(strategy_name):
    return failed_strategies.get(strategy_name, 0) >= 2

def mark_strategy_failure(strategy_name):
    failed_strategies[strategy_name] = failed_strategies.get(strategy_name, 0) + 1
    save_failed_strategies(failed_strategies)

def mark_strategy_success(strategy_name):
    if strategy_name in failed_strategies:
        del failed_strategies[strategy_name]
        save_failed_strategies(failed_strategies)

# === MAIN EVALUATION ===
def evaluate_signal(signal):
    strategy = signal.get("strategy", "unknown")
    confidence = float(signal.get("confidence", 0))
    
    # 1. Skip if strategy has failed multiple times
    if strategy_failed_before(strategy):
        reason = f"strategy previously failed ({strategy})"
        send_feedback(signal, reason)
        return

    # 2. Check economic context
    eco_pass, confidence_adj = passes_economic_filter(signal)
    if not eco_pass:
        reason = "blocked by economic filter"
        send_feedback(signal, reason)
        mark_strategy_failure(strategy)
        return
    
    signal['confidence'] = round(confidence + confidence_adj, 4)

    # 3. Confidence threshold
    if signal['confidence'] < 0.75:
        reason = f"confidence too low after adjustment ({signal['confidence']})"
        send_feedback(signal, reason)
        mark_strategy_failure(strategy)
        return

    # 4. Success
    route_to_module_i(signal)
    mark_strategy_success(strategy)

# === ROUTING ===
def route_to_module_i(signal):
    try:
        res = requests.post(ROUTE_TO_MODULE_I, json=signal, timeout=3)
        if res.ok:
            logging.info(f"[Eval] Routed to Module I: {signal}")
        else:
            raise Exception(f"HTTP {res.status_code}")
    except Exception as e:
        logging.error(f"[Eval] Routing error: {e}")
        send_feedback(signal, f"routing error: {e}")

def send_feedback(signal, reason):
    feedback = {
        "signal": signal,
        "status": "rejected",
        "reason": reason,
        "timestamp": datetime.utcnow().isoformat()
    }
    try:
        requests.post(FEEDBACK_TO_MODULE_F, json=feedback, timeout=3)
    except Exception as e:
        logging.error(f"[Feedback] Failed to send: {e}")

# === MAIN LOOP ===
def evaluator_loop():
    print("[Module E] Strategy evaluator running...")
    while True:
        signal_data = redis_client.lpop(SIGNAL_EVAL_QUEUE)
        if signal_data:
            try:
                signal = json.loads(signal_data)
                evaluate_signal(signal)
            except Exception as e:
                logging.error(f"[Evaluator] Bad signal: {e}")
        else:
            time.sleep(1)

# === ENTRY POINT ===
if __name__ == "__main__":
    evaluator_loop()
