import os
import json
import sqlite3
import datetime
import traceback
import pandas as pd
import requests
import redis
from flask import Flask, jsonify, render_template_string
from cryptography.fernet import Fernet
from tenacity import retry, stop_after_attempt, wait_fixed

# === CONFIGURATION ===
DATA_FOLDER = "market_data"
SIGNAL_OUTPUT_FILE = "active_signals.json"
AI_WEEKLY_SIGNAL_FILE = "ai_weekly_signal.csv"
DB_FILE = "signal_logs.db"
REDIS_CHANNEL = "fx_signals"
MODULE_C_ENDPOINT = "http://localhost:8002/receive_signal"
MODULE_I_ENDPOINT = "http://localhost:8004/execute_trade"
EXPIRY_MINUTES = 60
SYMBOLS = ["EURUSD", "GBPUSD", "USDJPY"]
AUTO_TRIGGER_MODULE_I = True
ENABLE_ENCRYPTION = True
ENCRYPTION_KEY = Fernet.generate_key()
fernet = Fernet(ENCRYPTION_KEY)

# === REDIS CONNECTION ===
try:
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
except:
    redis_client = None

# === DATABASE INIT ===
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        symbol TEXT,
        direction TEXT,
        reason TEXT,
        confidence REAL,
        expires TEXT
    )''')
    conn.commit()
    conn.close()

# === SIGNAL GENERATOR ===
class SignalGenerator:
    def __init__(self):
        self.signals = []

    def load_data(self, symbol):
        try:
            file_path = os.path.join(DATA_FOLDER, f"{symbol}_H1.csv")
            return pd.read_csv(file_path, parse_dates=["time"], index_col="time")
        except Exception as e:
            print(f"[ERROR] Loading {symbol}: {e}")
            return None

    def build_signal(self, symbol, direction, reason, confidence):
        signal = {
            "symbol": symbol,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "direction": "BUY" if direction == 1 else "SELL",
            "confidence": confidence,
            "reason": reason,
            "expires": (datetime.datetime.utcnow() + datetime.timedelta(minutes=EXPIRY_MINUTES)).isoformat()
        }

        self.log_signal_to_db(signal)
        self.send_to_module_c(signal)
        self.push_to_redis(signal)
        return signal

    def generate_momentum_signal(self, df, symbol):
        try:
            if df["momentum"].iloc[-1] > 0 and df["rsi"].iloc[-1] > 55:
                return self.build_signal(symbol, 1, "Momentum Long", 0.75)
            elif df["momentum"].iloc[-1] < 0 and df["rsi"].iloc[-1] < 45:
                return self.build_signal(symbol, -1, "Momentum Short", 0.75)
        except: pass
        return None

    def generate_ma_crossover(self, df, symbol):
        try:
            if df["ma_20"].iloc[-2] < df["ma_50"].iloc[-2] and df["ma_20"].iloc[-1] > df["ma_50"].iloc[-1]:
                return self.build_signal(symbol, 1, "MA Bullish Crossover", 0.65)
            elif df["ma_20"].iloc[-2] > df["ma_50"].iloc[-2] and df["ma_20"].iloc[-1] < df["ma_50"].iloc[-1]:
                return self.build_signal(symbol, -1, "MA Bearish Crossover", 0.65)
        except: pass
        return None

    def include_ai_weekly_signal(self):
        try:
            with open(AI_WEEKLY_SIGNAL_FILE, "r") as f:
                line = f.readline().strip()
                direction, confidence = line.split(",")
                signal = self.build_signal("EURUSD", int(direction), "AI Weekly Signal", float(confidence))
                self.signals.append(signal)
        except Exception as e:
            print(f"[AI SIGNAL] Error: {e}")

    def log_signal_to_db(self, signal):
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute('''INSERT INTO signals (timestamp, symbol, direction, reason, confidence, expires)
                              VALUES (?, ?, ?, ?, ?, ?)''',
                           (signal["timestamp"], signal["symbol"], signal["direction"],
                            signal["reason"], signal["confidence"], signal["expires"]))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[DB] Logging error: {e}")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def send_to_module_c(self, signal):
        try:
            payload = json.dumps(signal)
            if ENABLE_ENCRYPTION:
                payload = fernet.encrypt(payload.encode()).decode()
            res = requests.post(MODULE_C_ENDPOINT, json={"payload": payload}, timeout=3)
            res.raise_for_status()

            if res.ok and AUTO_TRIGGER_MODULE_I:
                self.auto_trigger_module_i(signal)
        except Exception as e:
            print(f"[Module C] Send error: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def auto_trigger_module_i(self, signal):
        try:
            payload = json.dumps(signal)
            if ENABLE_ENCRYPTION:
                payload = fernet.encrypt(payload.encode()).decode()
            res = requests.post(MODULE_I_ENDPOINT, json={"payload": payload}, timeout=3)
            res.raise_for_status()
        except Exception as e:
            print(f"[Module I] Trigger failed: {e}")
            raise

    @retry(stop=stop_after_attempt(2), wait=wait_fixed(2))
    def push_to_redis(self, signal):
        if redis_client:
            try:
                payload = json.dumps(signal)
                if ENABLE_ENCRYPTION:
                    payload = fernet.encrypt(payload.encode()).decode()
                redis_client.publish(REDIS_CHANNEL, payload)
            except Exception as e:
                print(f"[REDIS] Publish error: {e}")
                raise

    def run(self):
        print("[Module B] Running...")
        for symbol in SYMBOLS:
            df = self.load_data(symbol)
            if df is None: continue

            for strategy in [self.generate_momentum_signal, self.generate_ma_crossover]:
                signal = strategy(df, symbol)
                if signal:
                    self.signals.append(signal)

        self.include_ai_weekly_signal()
        self.signals.sort(key=lambda x: x["confidence"], reverse=True)

        with open(SIGNAL_OUTPUT_FILE, "w") as f:
            json.dump(self.signals, f, indent=2)

        print(f"[Module B] {len(self.signals)} signals exported.")

# === MONITORING DASHBOARD ===
app = Flask(__name__)
@app.route("/")
def dashboard():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT * FROM signals ORDER BY timestamp DESC LIMIT 20", conn)
    conn.close()
    html = df.to_html(index=False)
    return render_template_string("""
    <html>
    <head><title>Signal Monitor</title></head>
    <body>
        <h2>📡 Latest FX Signals (Top 20)</h2>
        {{ table | safe }}
    </body>
    </html>
    """, table=html)

# === MAIN ENTRY ===
if __name__ == "__main__":
    import threading

    init_db()
    gen = SignalGenerator()

    # Run Flask dashboard in background
    flask_thread = threading.Thread(target=app.run, kwargs={"port": 8050, "debug": False})
    flask_thread.start()

    # Run signal generator
    gen.run()
