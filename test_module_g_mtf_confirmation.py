import pytest
import pandas as pd
from module_g_mtf_confirmation import MTFConfirmation

# Mock MTFConfirmation for local testing without MT5
class DummyMTF(MTFConfirmation):
    def __init__(self, symbol="EURUSD", lookback=100):
        self.symbol = symbol
        self.lookback = lookback

    def get_data(self, timeframe):
        # Return dummy OHLCV data
        data = {
            'time': pd.date_range(start='2023-01-01', periods=100, freq='H'),
            'open': pd.Series([1.1 + 0.001*i for i in range(100)]),
            'high': pd.Series([1.12 + 0.001*i for i in range(100)]),
            'low': pd.Series([1.08 + 0.001*i for i in range(100)]),
            'close': pd.Series([1.1 + 0.001*i for i in range(100)]),
            'tick_volume': [100]*100,
            'spread': [2]*100,
            'real_volume': [0]*100,
        }
        return pd.DataFrame(data)

@pytest.fixture
def dummy_mtf():
    return DummyMTF()

def test_compute_indicators(dummy_mtf):
    df = dummy_mtf.get_data('H1')
    df = dummy_mtf.compute_indicators(df)
    assert 'macd' in df.columns
    assert 'rsi' in df.columns
    assert 'momentum' in df.columns
    assert not df['rsi'].isnull().all()

def test_determine_trend(dummy_mtf):
    df = dummy_mtf.get_data('H1')
    trend, score = dummy_mtf.determine_trend(df)
    assert trend in ["bullish", "bearish", "neutral"]
    assert isinstance(score, int)

def test_confirm_signal(dummy_mtf):
    result = dummy_mtf.confirm_signal("buy")
    assert isinstance(result, dict)
    assert "confirmed" in result
    assert "trend_summary" in result
