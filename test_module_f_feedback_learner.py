# test_module_f_feedback_learner.py
import unittest
from unittest.mock import patch, MagicMock
import module_f_feedback_learner as mffl
import json
import os

class TestModuleFFeedbackLearner(unittest.TestCase):

    def setUp(self):
        mffl.feedback_store.clear()
        self.symbol = "EURUSD"
        self.strategy = "momentum"
        self.key = f"{self.symbol}:{self.strategy}"

    def test_track_signal_feedback_win(self):
        mffl.track_signal_feedback(self.symbol, self.strategy, "win")
        self.assertIn(self.key, mffl.feedback_store)
        self.assertEqual(mffl.feedback_store[self.key]["wins"], 1)

    def test_track_signal_feedback_loss(self):
        mffl.track_signal_feedback(self.symbol, self.strategy, "loss", reason="stoploss hit")
        self.assertEqual(mffl.feedback_store[self.key]["losses"], 1)
        self.assertEqual(mffl.feedback_store[self.key]["reason"], "stoploss hit")

    def test_blacklisting(self):
        with patch.object(mffl.redis_client, 'set') as mock_set:
            mffl.mark_strategy_invalid(self.symbol, self.strategy)
            mock_set.assert_called()

    def test_feedback_learning_threshold(self):
        mffl.feedback_store[self.key] = {"wins": 1, "losses": 4, "last_updated": None, "last_outcome": None, "reason": ""}
        with patch.object(mffl, 'mark_strategy_invalid') as mock_blacklist:
            with patch.object(mffl, 'send_learning_feedback_to_module_b') as mock_b:
                with patch.object(mffl, 'send_learning_feedback_to_module_e') as mock_e:
                    mffl.adaptive_learning_loop()
                    mock_blacklist.assert_called()

    def tearDown(self):
        if os.path.exists("feedback_store.json"):
            os.remove("feedback_store.json")

if __name__ == '__main__':
    unittest.main()
