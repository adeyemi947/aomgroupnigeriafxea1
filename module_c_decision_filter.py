import json, sqlite3, datetime, threading, time, unittest
from flask import Flask, request, jsonify, render_template_string
import pandas as pd
import matplotlib.pyplot as plt
import io, base64
import requests
import redis
import yagmail
from cryptography.fernet import Fernet
from telegram import Bot

# === CONFIG ===
DB_FILE = "decision_log.db"
CONFIDENCE_THRESHOLD = 0.6
ENCRYPTION_KEY = b'YourGeneratedEncryptionKeyHere'
fernet = Fernet(ENCRYPTION_KEY)
MODULE_I_ENDPOINT = "http://localhost:8004/execute_trade"
EVENT_FILE = "event_calendar.json"
EVENT_FILTER = True
TELEGRAM_BOT_TOKEN = "your_bot_token"
TELEGRAM_CHAT_ID = "your_chat_id"
EMAIL_USER = "you@gmail.com"
EMAIL_PASSWORD = "your_email_app_password"
EMAIL_RECEIVER = "target@example.com"
ENABLE_ENCRYPTION = True

# === INIT ===
app = Flask(__name__)
r = redis.Redis(host='localhost', port=6379, db=0)
bot = Bot(token=TELEGRAM_BOT_TOKEN)
yag = yagmail.SMTP(EMAIL_USER, EMAIL_PASSWORD)

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        symbol TEXT,
        direction TEXT,
        decision TEXT,
        reason TEXT,
        confidence REAL,
        triggered INTEGER
    )''')
    conn.commit()
    conn.close()

def log_decision(signal, decision, triggered):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO decisions (timestamp, symbol, direction, decision, reason, confidence, triggered)
                      VALUES (?, ?, ?, ?, ?, ?, ?)''',
                   (signal["timestamp"], signal["symbol"], signal["direction"], decision,
                    signal["reason"], signal["confidence"], triggered))
    conn.commit()
    conn.close()

def is_valid(signal):
    try:
        expiry = datetime.datetime.fromisoformat(signal["expires"])
        if datetime.datetime.utcnow() > expiry:
            return False
        return signal["confidence"] >= CONFIDENCE_THRESHOLD
    except:
        return False

def correlate_with_event(symbol, timestamp):
    if not EVENT_FILTER: return True
    try:
        with open(EVENT_FILE) as f:
            events = json.load(f).get("events", [])
        for e in events:
            if symbol in e["affected_symbols"]:
                event_time = datetime.datetime.fromisoformat(e["time"])
                sig_time = datetime.datetime.fromisoformat(timestamp)
                if abs((sig_time - event_time).total_seconds()) < 3600:
                    return False
    except:
        pass
    return True

def forward_to_module_i(signal):
    try:
        payload = json.dumps(signal)
        if ENABLE_ENCRYPTION:
            payload = fernet.encrypt(payload.encode()).decode()
        res = requests.post(MODULE_I_ENDPOINT, json={"payload": payload})
        return res.ok
    except Exception as e:
        print(f"[ERROR] Forward failed: {e}")
        return False

def send_alerts(signal):
    msg = f"""
    🔔 Signal Alert!
    {signal['symbol']} - {signal['direction']} ({signal['reason']})
    Confidence: {signal['confidence']}
    Time: {signal['timestamp']}
    """
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg)
    except Exception as e:
        print(f"[Alert] Telegram failed: {e}")
    try:
        yag.send(to=EMAIL_RECEIVER, subject="Signal Alert", contents=msg)
    except Exception as e:
        print(f"[Alert] Email failed: {e}")

def process_signal(signal):
    if not is_valid(signal):
        log_decision(signal, "REJECTED", 0)
        return {"status": "rejected"}

    if not correlate_with_event(signal["symbol"], signal["timestamp"]):
        log_decision(signal, "REJECTED: news event", 0)
        return {"status": "rejected_event"}

    triggered = forward_to_module_i(signal)
    log_decision(signal, "ACCEPTED", int(triggered))

    if signal["confidence"] >= 0.85:
        threading.Thread(target=send_alerts, args=(signal,)).start()

    return {"status": "accepted", "triggered": triggered}

# === ROUTES ===
@app.route("/receive_signal", methods=["POST"])
def receive_signal():
    try:
        payload = request.json.get("payload")
        if ENABLE_ENCRYPTION:
            payload = fernet.decrypt(payload.encode()).decode()
        signal = json.loads(payload)
        return jsonify(process_signal(signal)), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/receive_signals", methods=["POST"])
def receive_signals():
    try:
        payload = request.json.get("payload")
        if ENABLE_ENCRYPTION:
            payload = fernet.decrypt(payload.encode()).decode()
        signals = json.loads(payload)
        results = [process_signal(sig) for sig in signals]
        return jsonify({"results": results}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/")
def dashboard():
    try:
        df = pd.read_sql_query("SELECT * FROM decisions ORDER BY id DESC LIMIT 100", sqlite3.connect(DB_FILE))
        # Plot confidence vs triggered
        fig, ax = plt.subplots(figsize=(6, 3))
        df.plot(x="id", y="confidence", ax=ax, label="Confidence", color='blue')
        df.plot(x="id", y="triggered", ax=ax, secondary_y=True, label="Triggered", color='red')
        ax.set_title("Signal Confidence vs Trigger")
        buf = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buf, format="png")
        buf.seek(0)
        encoded = base64.b64encode(buf.read()).decode("utf-8")
        html_table = df.to_html(classes="table table-striped", index=False)
        return render_template_string("""
        <html><head><title>Module C</title>
        <meta http-equiv="refresh" content="20">
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css"></head>
        <body class="p-4">
        <h3>📊 Recent Decision Log</h3>
        {{ table|safe }}
        <h4>📈 Signal Confidence</h4>
        <img src="data:image/png;base64,{{ plot }}" width="100%">
        </body></html>""", table=html_table, plot=encoded)
    except Exception as e:
        return f"<pre>{e}</pre>"

# === TESTS ===
class TestSignalFilter(unittest.TestCase):
    def test_valid_signal(self):
        signal = {
            "symbol": "EURUSD",
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "direction": "BUY",
            "reason": "RSI breakout",
            "confidence": 0.7,
            "expires": (datetime.datetime.utcnow() + datetime.timedelta(minutes=2)).isoformat()
        }
        self.assertTrue(is_valid(signal))

    def test_expired_signal(self):
        signal = {
            "symbol": "EURUSD",
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "direction": "SELL",
            "reason": "test",
            "confidence": 0.9,
            "expires": (datetime.datetime.utcnow() - datetime.timedelta(minutes=1)).isoformat()
        }
        self.assertFalse(is_valid(signal))

if __name__ == "__main__":
    init_db()
    unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(TestSignalFilter))
    print("[Module C] Running with dashboard on port 8002")
    app.run(host="0.0.0.0", port=8002)
