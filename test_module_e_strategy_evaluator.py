import pytest
import json
from unittest.mock import patch
from module_e_strategy_evaluator import (
    strategy_failed_before,
    mark_strategy_failure,
    mark_strategy_success,
    passes_economic_filter,
    evaluate_signal,
    failed_strategies
)

# ------------------------------
# Strategy Memory Tests
# ------------------------------
def test_strategy_failure_memory():
    strategy_name = "TestStrategy"
    
    mark_strategy_failure(strategy_name)
    assert failed_strategies[strategy_name] >= 1

    mark_strategy_success(strategy_name)
    assert strategy_name not in failed_strategies

# ------------------------------
# Economic Filter Test (mocked)
# ------------------------------
@patch('module_e_strategy_evaluator.requests.post')
def test_economic_filter_pass(mock_post):
    mock_post.return_value.ok = True
    mock_post.return_value.json.return_value = {"pass": True, "adjustment": 0.1}

    dummy_signal = {"symbol": "EURUSD", "strategy": "momentum"}
    result, adj = passes_economic_filter(dummy_signal)

    assert result is True
    assert isinstance(adj, float)

# ------------------------------
# Signal Evaluation Logic Test
# ------------------------------
@patch('module_e_strategy_evaluator.passes_economic_filter')
@patch('module_e_strategy_evaluator.route_to_module_i')
@patch('module_e_strategy_evaluator.send_feedback')
def test_evaluate_signal_valid(mock_feedback, mock_route, mock_eco_filter):
    mock_eco_filter.return_value = (True, 0.1)
    mock_route.return_value = True

    signal = {
        "symbol": "EURUSD",
        "strategy": "momentum",
        "confidence": 0.8
    }

    evaluate_signal(signal)
    mock_route.assert_called_once()

@patch('module_e_strategy_evaluator.passes_economic_filter')
@patch('module_e_strategy_evaluator.route_to_module_i')
@patch('module_e_strategy_evaluator.send_feedback')
def test_evaluate_signal_low_confidence(mock_feedback, mock_route, mock_eco_filter):
    mock_eco_filter.return_value = (True, -0.3)

    signal = {
        "symbol": "EURUSD",
        "strategy": "volatility",
        "confidence": 0.6
    }

    evaluate_signal(signal)
    mock_feedback.assert_called_once()
    mock_route.assert_not_called()

# ------------------------------
# Failed Strategy Bypass Test
# ------------------------------
@patch('module_e_strategy_evaluator.send_feedback')
def test_evaluate_blocked_failed_strategy(mock_feedback):
    strategy = "rejected_strategy"
    failed_strategies[strategy] = 3

    signal = {
        "symbol": "USDJPY",
        "strategy": strategy,
        "confidence": 0.9
    }

    evaluate_signal(signal)
    mock_feedback.assert_called_once()
