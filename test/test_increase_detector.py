"""ContinuousIncreaseDetector 递增检测算法测试。"""
import json
from unittest.mock import MagicMock, patch

import pytest


class MockListState:
    """模拟 Flink ListState<Double>。"""
    def __init__(self):
        self._items = []

    def get(self):
        return self._items[:] if self._items else None

    def update(self, items):
        self._items = list(items)

    def add(self, value):
        self._items.append(value)

    def clear(self):
        self._items = []


def _make_detector():
    """创建已注入 mock 状态的 ContinuousIncreaseDetector。"""
    with patch("class10_ecomm_datastream.pymysql.connect"):
        from class10_ecomm_datastream import ContinuousIncreaseDetector
        detector = ContinuousIncreaseDetector()
        detector.last_amounts = MockListState()
        detector.ads_conn = MagicMock()  # 阻止真实 MySQL 写入
        return detector


def _make_value(amount, user_id="u1", ts=1716543000000, txn_id="txn_001"):
    """构造 ParseTransaction 输出的 12 元组。"""
    return ("u1", amount, "electronics", ts, txn_id,
            "success", "purchase", "10.0.0.1", "prod_01", "Product", "广东省", "深圳")


class TestContinuousIncrease:
    """验证连续递增检测算法的边界条件。"""

    def test_three_increasing_triggers_alert(self):
        """3 笔连续递增 → 触发告警。"""
        detector = _make_detector()
        ctx = MagicMock()

        results = []
        for amt, txn_id in [(100, "t1"), (120, "t2"), (150, "t3")]:
            val = _make_value(amt, txn_id=txn_id)
            results.extend(detector.process_element(val, ctx))

        assert len(results) == 1
        alert = json.loads(results[0])
        assert alert["alert_type"] == "CONTINUOUS_INCREASE"
        assert alert["sequence_length"] == 3
        assert alert["amounts"] == [100, 120, 150]

    def test_two_transactions_no_alert(self):
        """仅 2 笔交易 → 不足 INCREASE_MIN_SEQ，不触发。"""
        detector = _make_detector()
        ctx = MagicMock()

        results = []
        for amt, txn_id in [(100, "t1"), (130, "t2")]:
            results.extend(detector.process_element(_make_value(amt, txn_id=txn_id), ctx))

        assert len(results) == 0

    def test_decrease_breaks_sequence(self):
        """递增中断（金额回落）→ 不触发。"""
        detector = _make_detector()
        ctx = MagicMock()

        results = []
        for amt, txn_id in [(100, "t1"), (120, "t2"), (90, "t3")]:
            results.extend(detector.process_element(_make_value(amt, txn_id=txn_id), ctx))

        assert len(results) == 0

    def test_state_cleared_after_alert(self):
        """触发告警后 ListState 被清空，后续需要重新累积。"""
        detector = _make_detector()
        ctx = MagicMock()

        # 第一轮：触发告警
        for amt, txn_id in [(100, "t1"), (120, "t2"), (150, "t3")]:
            detector.process_element(_make_value(amt, txn_id=txn_id), ctx)

        # 状态已清空，再发 1 笔 → 需要重新积累
        results = list(detector.process_element(_make_value(200, txn_id="t4"), ctx))
        assert len(results) == 0

    def test_three_out_of_four_increasing(self):
        """4 笔中前 2 笔不满足递增，后 3 笔形成序列 → 触发，seq_len=3。"""
        detector = _make_detector()
        ctx = MagicMock()

        # 50→55 增幅不足 1.1，阻断链条；55→65→80 满足
        results = []
        for amt, txn_id in [(50, "t1"), (55, "t2"), (65, "t3"), (80, "t4")]:
            results.extend(detector.process_element(_make_value(amt, txn_id=txn_id), ctx))

        assert len(results) == 1
        assert json.loads(results[0])["sequence_length"] == 3
        assert json.loads(results[0])["amounts"] == [55, 65, 80]
