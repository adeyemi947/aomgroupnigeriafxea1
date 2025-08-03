import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import json
import redis
import time
from ta.trend import MACD
from ta.momentum import RSIIndicator

TIMEFRAMES = {
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "H1": mt5.TIMEFRAME_H1,
}

class MTFConfirmation:
    def __init__(self, symbol="EURUSD", lookback=100, redis_host="localhost", redis_port=6379):
        self.symbol = symbol
        self.lookback = lookback
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        if not mt5.initialize():
            raise ConnectionError("MetaTrader5 initialization failed.")

    def get_data(self, timeframe):
        rates = mt5.copy_rates_from_pos(self.symbol, timeframe, 0, self.lookback)
        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        return df

    def compute_indicators(self, df):
        df['macd'] = MACD(df['close']).macd()
        df['rsi'] = RSIIndicator(df['close']).rsi()
        df['momentum'] = df['close'] - df['close'].shift(4)
        df.dropna(inplace=True)
        return df

    def determine_trend(self, df):
        df = self.compute_indicators(df)
        recent = df.iloc[-1]
        score = 0

        if recent['macd'] > 0:
            score += 1
        else:
            score -= 1

        if recent['rsi'] > 55:
            score += 1
        elif recent['rsi'] < 45:
            score -= 1

        if recent['momentum'] > 0:
            score += 1
        else:
            score -= 1

        if score >= 2:
            trend = "bullish"
        elif score <= -2:
            trend = "bearish"
        else:
            trend = "neutral"

        return trend, score

    def confirm_signal(self, signal_direction: str, signal_type: str = "momentum"):
        confirmation = True
        trend_summary = {}
        score_sum = 0

        for label, tf in TIMEFRAMES.items():
            df = self.get_data(tf)
            trend, score = self.determine_trend(df)
            trend_summary[label] = {"trend": trend, "score": score}
            score_sum += score

            if signal_type == "momentum":
                if signal_direction == "buy" and trend != "bullish":
                    confirmation = False
                elif signal_direction == "sell" and trend != "bearish":
                    confirmation = False

            elif signal_type == "reversal":
                if signal_direction == "buy" and trend == "bearish":
                    confirmation = True
                elif signal_direction == "sell" and trend == "bullish":
                    confirmation = True
                else:
                    confirmation = False

            elif signal_type == "breakout":
                if abs(score) < 3:
                    confirmation = False

            elif signal_type == "news":
                if "H1" in trend_summary:
                    if abs(trend_summary["H1"]["score"]) >= 3:
                        confirmation = True
                    else:
                        confirmation = False

        return {
            "confirmed": confirmation,
            "overall_score": score_sum,
            "trend_summary": trend_summary,
            "signal_direction": signal_direction,
            "signal_type": signal_type,
            "symbol": self.symbol
        }

    def process_queue(self):
        print("🔁 [MTF Confirmation] Waiting for signals from Redis queue...")
        while True:
            try:
                job = self.redis.blpop("queue:signals:confirm", timeout=5)
                if job:
                    _, payload = job
                    signal_data = json.loads(payload)
                    self.symbol = signal_data.get("symbol", "EURUSD")
                    result = self.confirm_signal(
                        signal_direction=signal_data["signal_direction"],
                        signal_type=signal_data.get("signal_type", "momentum")
                    )
                    self.redis.rpush("queue:signals:confirmed", json.dumps(result))
                    print(f"✅ Confirmed signal: {result}")
                else:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("🛑 Shutdown requested.")
                break
            except Exception as e:
                print(f"⚠️ Error processing signal: {e}")
                continue

    def shutdown(self):
        mt5.shutdown()
