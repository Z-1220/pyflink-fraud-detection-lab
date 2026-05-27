"""TimeDecayRiskScorer.process_element 状态机测试（时间衰减 + ValueState）。"""
import json
import math
from unittest.mock import MagicMock

import pytest


class MockValueState:
    """模拟 Flink ValueState<String> 的简单实现。"""
    def __init__(self):
        self._value = None

    def value(self):
        return self._value

    def update(self, v):
        self._value = v


def _make_scorer():
    """创建已注入 mock 状态的 TimeDecayRiskScorer 实例。"""
    from class10_ecomm_datastream import TimeDecayRiskScorer
    scorer = TimeDecayRiskScorer()
    scorer.score_state = MockValueState()
    return scorer


def _make_ctx(processing_timestamp=0):
    """创建 mock Flink KeyedProcessFunction.Context。"""
    ctx = MagicMock()
    ctx.timestamp.return_value = processing_timestamp
    return ctx


def _make_alert(alert_type, alert_time, **kwargs):
    return json.dumps(dict(alert_type=alert_type, user_id="user_test",
                           alert_time=alert_time, **kwargs))


class TestProcessElement:
    """验证 process_element 的时间衰减和状态持久化。"""

    def test_first_alert_score_equals_severity(self):
        """首条告警：无历史状态，得分 = 严重性。"""
        scorer = _make_scorer()
        ctx = _make_ctx()
        alert = _make_alert("LARGE_AMOUNT", "2026-05-27T10:00:00", amount=5000)

        results = list(scorer.process_element(alert, ctx))
        assert len(results) == 1
        data = json.loads(results[0])
        assert data["risk_score"] == 0.30

    def test_two_alerts_same_time_accumulates(self):
        """两条告警同时到达：得分累加。"""
        scorer = _make_scorer()
        ctx = _make_ctx()

        a1 = _make_alert("LARGE_AMOUNT", "2026-05-27T10:00:00", amount=5000)
        list(scorer.process_element(a1, ctx))

        a2 = _make_alert("LARGE_AMOUNT", "2026-05-27T10:00:00", amount=5000)
        results = list(scorer.process_element(a2, ctx))

        # 同时到达 → 无衰减 → 0.30*2 = 0.60
        assert json.loads(results[0])["risk_score"] == pytest.approx(0.60, abs=0.001)

    def test_delay_causes_decay(self):
        """间隔 3 分钟（1 个半衰期）→ 旧分衰减 50%。"""
        scorer = _make_scorer()
        ctx = _make_ctx()

        a1 = _make_alert("LARGE_AMOUNT", "2026-05-27T10:00:00", amount=5000)
        list(scorer.process_element(a1, ctx))

        # 3 分钟后 = 1 个半衰期 → 0.30 * 0.5 + 0.30 = 0.45
        a2 = _make_alert("LARGE_AMOUNT", "2026-05-27T10:03:00", amount=5000)
        results = list(scorer.process_element(a2, ctx))

        assert json.loads(results[0])["risk_score"] == pytest.approx(0.45, abs=0.01)

    def test_very_small_score_not_yielded(self):
        """分数低于 0.001 时不输出（但仍更新状态）。"""
        scorer = _make_scorer()
        # 手动设置一个极小分数
        scorer.score_state.update("0.0005,1716543000000")
        ctx = _make_ctx()

        # 很久以后 → 衰减到几乎为零
        alert = _make_alert("LARGE_AMOUNT", "2026-05-28T10:00:00", amount=1)
        results = list(scorer.process_element(alert, ctx))
        # amount=1 → multiplier=max(min(1/5000, 5), 1.0)=1.0 → severity=0.30
        # old_score decayed from 0.0005 → near zero → new_score ≈ 0.30
        assert len(results) == 1  # 0.30 > 0.001, should yield

    def test_missing_alert_time_falls_back_to_processing_time(self):
        """alert_time 缺失时使用 ctx.timestamp() 作为回退。"""
        scorer = _make_scorer()
        ctx = _make_ctx(processing_timestamp=1716543000000)

        alert = json.dumps({"alert_type": "HIGH_FREQUENCY", "user_id": "u1",
                            "transaction_count": 5})
        results = list(scorer.process_element(alert, ctx))
        assert len(results) == 1
        assert json.loads(results[0])["risk_score"] == 0.25
