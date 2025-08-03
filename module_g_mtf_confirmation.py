import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import time
import logging
from ta.trend import MACD
from ta.momentum import RSIIndicator
from ta.momentum import StochasticOscillator

# Logger setup
logging.basicConfig(filename="module_g_mtf_confirmation.log", level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

TIMEFRAMES = {
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "H1": mt5.TIMEFRAME_H1
}

class MTFConfirmation:
    def __init__(self, symbol="EURUSD", lookback=100):
        self.symbol = symbol
        self.lookback = lookback
        if not mt5.initialize():
            logging.error("Failed to initialize MetaTrader5 connection")
            raise Exception("MT5 connection failed")

    def shutdown(self):
        mt5.shutdown()

    def get_data(self, timeframe):
        rates = mt5.copy_rates_from_pos(self.symbol, timeframe, 0, self.lookback)
        if rates is None or len(rates) == 0:
            logging.warning(f"Empty rates from MT5 for {self.symbol} - {timeframe}")
            return None
        df = pd.DataFrame(rates)
        df["time"] = pd.to_datetime(df["time"], unit="s")
        return df

    def compute_indicators(self, df):
        df['ma_fast'] = df['close'].rolling(window=5).mean()
        df['ma_slow'] = df['close'].rolling(window=20).mean()

        macd = MACD(df['close'])
        df['macd'] = macd.macd()
        df['macd_signal'] = macd.macd_signal()

        rsi = RSIIndicator(df['close'])
        df['rsi'] = rsi.rsi()

        df['momentum'] = df['close'].diff()

        return df

    def determine_trend(self, df):
        if df is None or len(df) < 25:
            return "neutral", 0

        df = self.compute_indicators(df)

        trend_score = 0

        # MA Slope
        if df['ma_fast'].iloc[-1] > df['ma_slow'].iloc[-1]:
            trend_score += 1
        else:
            trend_score -= 1

        # MACD
        if df['macd'].iloc[-1] > df['macd_signal'].iloc[-1]:
            trend_score += 1
        else:
            trend_score -= 1

        # RSI
        if df['rsi'].iloc[-1] > 55:
            trend_score += 1
        elif df['rsi'].iloc[-1] < 45:
            trend_score -= 1

        # Momentum
        if df['momentum'].iloc[-1] > 0:
            trend_score += 1
        else:
            trend_score -= 1

        if trend_score >= 2:
            return "bullish", trend_score
        elif trend_score <= -2:
            return "bearish", trend_score
        else:
            return "neutral", trend_score

    def confirm_signal(self, signal_direction: str):
        confirmation = True
        trend_summary = {}
        score_sum = 0

        for label, tf in TIMEFRAMES.items():
            df = self.get_data(tf)
            trend, score = self.determine_trend(df)
            trend_summary[label] = {"trend": trend, "score": score}
            score_sum += score

            if signal_direction == "buy" and trend != "bullish":
                confirmation = False
            elif signal_direction == "sell" and trend != "bearish":
                confirmation = False

        return {
            "confirmed": confirmation,
            "overall_score": score_sum,
            "trend_summary": trend_summary
        }

    def loop_for_signal_verification(self, signal_queue, output_queue):
        logging.info("✅ MTF confirmation loop started.")
        while True:
            if not signal_queue.empty():
                signal = signal_queue.get()
                signal_id = signal.get("id", "unknown")
                symbol = signal.get("symbol", "EURUSD")
                direction = signal.get("direction", "").lower()

                self.symbol = symbol
                result = self.confirm_signal(direction)

                output = {
                    "signal_id": signal_id,
                    "symbol": symbol,
                    "direction": direction,
                    "confirmed": result["confirmed"],
                    "score": result["overall_score"],
                    "trend_summary": result["trend_summary"]
                }

                output_queue.put(output)

                if result["confirmed"]:
                    logging.info(f"✅ Signal {signal_id} CONFIRMED by MTF")
                else:
                    logging.info(f"❌ Signal {signal_id} REJECTED by MTF")

            time.sleep(1)

    def socket_ready_stub(self, data):
        """
        Optional: Replace this with actual socket logic to bridge to brokers or APIs.
        Example placeholder for sending data to another Python service or broker endpoint.
        """
        try:
            # TODO: Use socket/socket.io/REST to send data to external system
            logging.info(f"[Socket Relay] → {data}")
        except Exception as e:
            logging.error(f"Socket relay error: {e}")
