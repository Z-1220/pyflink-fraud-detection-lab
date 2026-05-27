"""class10_server 辅助查询函数测试。"""
from unittest.mock import MagicMock

import pymysql
import pytest


class TestQueryTopRiskyUsers:
    """_query_top_risky_users：跨库 JOIN + fallback。"""

    def test_join_success(self, mock_cursor):
        """跨库 JOIN 成功时直接返回结果。"""
        mock_cursor.fetchall.return_value = [
            ("user_a", "Alice", 15),
            ("user_b", "Bob", 10),
        ]
        from class10_server import _query_top_risky_users
        result = _query_top_risky_users(mock_cursor, limit=2)

        assert len(result) == 2
        assert result[0] == {"user_id": "user_a", "user_name": "Alice", "alert_count": 15}
        assert result[1] == {"user_id": "user_b", "user_name": "Bob", "alert_count": 10}

    def test_fallback_on_join_error(self, mock_cursor):
        """跨库 JOIN 失败时走分步查询路径。"""
        call_count = [0]

        def _side_effect(sql, params=None):
            call_count[0] += 1
            if call_count[0] == 1:
                raise pymysql.Error("Cross-db JOIN not supported")
            # 第二步：按 user_id 分组查询
            return None

        mock_cursor.execute = MagicMock(side_effect=_side_effect)

        # step 1: fallback 分组查询 → 返回 alert rows
        # step 2: ecommerce users 查询 → 返回 name rows
        mock_cursor.fetchall.side_effect = [
            [("user_a", 15)],                     # alert count
            [("user_a", "Alice")],                # user name
        ]

        # 需要 mock pymysql.connect 用于 fallback 中的 ecommerce 连接
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr("class10_server.pymysql.connect", MagicMock())
            from class10_server import _query_top_risky_users
            result = _query_top_risky_users(mock_cursor, limit=5)

        assert len(result) == 1
        assert result[0]["user_id"] == "user_a"

    def test_empty_result(self, mock_cursor):
        """无告警数据时返回空列表。"""
        mock_cursor.fetchall.return_value = []
        from class10_server import _query_top_risky_users
        result = _query_top_risky_users(mock_cursor)
        assert result == []


class TestQueryAlertTypeDistribution:
    """_query_alert_type_distribution：告警类型分布查询。"""

    def test_returns_correct_structure(self, mock_cursor):
        mock_cursor.fetchall.return_value = [
            ("LARGE_AMOUNT", 42),
            ("HIGH_FREQUENCY", 18),
        ]
        from class10_server import _query_alert_type_distribution
        result = _query_alert_type_distribution(mock_cursor)

        assert len(result) == 2
        assert result[0] == {"alert_type": "LARGE_AMOUNT", "count": 42}


class TestQueryRegionAlerts:
    """_query_region_alerts：省份告警统计查询。"""

    def test_returns_correct_structure(self, mock_cursor):
        mock_cursor.fetchall.return_value = [
            ("广东省", 30),
            ("北京市", 5),
        ]
        from class10_server import _query_region_alerts
        result = _query_region_alerts(mock_cursor)

        assert len(result) == 2
        assert result[0] == {"province": "广东省", "alert_count": 30}


class TestQueryRiskScores:
    """_query_risk_scores：从 Kafka 缓存读取 + 用户名 JOIN。"""

    def test_empty_cache_returns_empty_list(self):
        from class10_server import _query_risk_scores, risk_score_cache
        risk_score_cache.clear()
        result = _query_risk_scores(10)
        assert result == []

    def test_returns_sorted_by_score(self):
        from class10_server import _query_risk_scores, risk_score_cache
        risk_score_cache.clear()
        risk_score_cache["user_a"] = 0.5
        risk_score_cache["user_b"] = 1.2

        # 需要 mock ecommerce 查询以获取用户名
        with pytest.MonkeyPatch().context() as mp:
            mock_conn = MagicMock()
            mock_cur = MagicMock()
            mock_cur.fetchall.return_value = [
                ("user_b", "Bob"),
                ("user_a", "Alice"),
            ]
            mock_conn.cursor.return_value.__enter__.return_value = mock_cur
            mp.setattr("class10_server.pymysql.connect", MagicMock(return_value=mock_conn))

            result = _query_risk_scores(10)

        assert len(result) == 2
        assert result[0]["user_id"] == "user_b"   # 高分在前
        assert result[0]["risk_score"] == 1.2
        assert result[1]["user_id"] == "user_a"
        assert result[1]["risk_score"] == 0.5
