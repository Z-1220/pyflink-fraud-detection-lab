"""FastAPI REST 端点集成测试 — 全量 9 类端点 + SQL 参数断言。"""
from datetime import datetime
from unittest.mock import ANY, MagicMock, call, patch

import pytest


def _mock_connect(fetchall_return):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fetchall_return
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    return MagicMock(return_value=mock_conn)


def _get_mock_cursor(mock_connect):
    """从 mock connect 中取出 cursor 供断言 execute 调用参数。"""
    return mock_connect.return_value.cursor.return_value.__enter__.return_value


# ---- 类别 ----
class TestCategories:
    def test_returns_list(self, client):
        with patch("class10_server.pymysql.connect",
                   _mock_connect([("electronics", "电子产品"), ("clothing", "服装")])):
            resp = client.get("/api/categories")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0] == {"category": "electronics", "description": "电子产品"}


# ---- 告警历史 ----
class TestAlertsHistory:
    def test_no_filter(self, client):
        now = datetime.now()
        rows = [("LARGE_AMOUNT", "user_a", "txn_1", 6000.0, None,
                 "exceeds threshold", now)]
        with patch("class10_server.pymysql.connect", _mock_connect(rows)):
            resp = client.get("/api/alerts/history")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert data[0]["alert_type"] == "LARGE_AMOUNT"
        assert data[0]["amount"] == 6000.0

    def test_filter_by_type_passes_param_to_sql(self, client):
        """验证 alert_type 参数传入 SQL execute。"""
        now = datetime.now()
        rows = [("LARGE_AMOUNT", "u1", "t1", 7000.0, None, "detail", now)]
        mock_connect = _mock_connect(rows)
        with patch("class10_server.pymysql.connect", mock_connect):
            resp = client.get("/api/alerts/history?alert_type=LARGE_AMOUNT")
        assert resp.status_code == 200
        cur = _get_mock_cursor(mock_connect)
        # execute 被调用，第一个参数含 WHERE alert_type =
        sql_called = cur.execute.call_args[0][0]
        assert "alert_type = %s" in sql_called

    def test_filter_by_keyword_passes_like_to_sql(self, client):
        """验证 keyword 查询使用 LIKE 模式。"""
        mock_connect = _mock_connect([])
        with patch("class10_server.pymysql.connect", mock_connect):
            resp = client.get("/api/alerts/history?keyword=user_abc")
        assert resp.status_code == 200
        cur = _get_mock_cursor(mock_connect)
        sql_called = cur.execute.call_args[0][0]
        assert "LIKE %s" in sql_called

    def test_empty_result(self, client):
        with patch("class10_server.pymysql.connect", _mock_connect([])):
            resp = client.get("/api/alerts/history")
        assert resp.status_code == 200
        assert resp.json() == []


# ---- 告警统计 ----
class TestAlertStats:
    def test_returns_by_type_and_by_hour(self, client):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [("LARGE_AMOUNT", 30), ("HIGH_FREQUENCY", 20)],
            [("10:00", 15), ("11:00", 10)],
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        with patch("class10_server.pymysql.connect", MagicMock(return_value=mock_conn)):
            resp = client.get("/api/alerts/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["by_type"]) == 2
        assert len(data["by_hour"]) == 2


# ---- Top5 排行 ----
class TestTopRiskyUsers:
    def test_respects_limit(self, client):
        rows = [("u1", "Alice", 10), ("u2", "Bob", 8)]
        with patch("class10_server.pymysql.connect", _mock_connect(rows)):
            resp = client.get("/api/top-risky-users?limit=2")
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    def test_db_error_returns_error_json(self, client):
        with patch("class10_server.pymysql.connect",
                   MagicMock(side_effect=Exception("connection refused"))):
            resp = client.get("/api/top-risky-users")
        assert resp.status_code == 200
        assert "error" in resp.json()


# ---- Dashboard Snapshot ----
class TestDashboardSnapshot:
    def test_contains_four_keys(self, client):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [("u1", "Alice", 5)],
            [("LARGE_AMOUNT", 10)],
            [("广东省", 8)],
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        with patch("class10_server.pymysql.connect", MagicMock(return_value=mock_conn)):
            with patch("class10_server.risk_score_cache", {}):
                resp = client.get("/api/dashboard/snapshot")
        assert resp.status_code == 200
        data = resp.json()
        assert "top_risky_users" in data
        assert "risk_scores" in data
        assert "alert_stats" in data
        assert "region_alerts" in data


# ---- 导出 ----
class TestExportAlerts:
    def test_returns_csv_content_type(self, client):
        rows = [("LARGE_AMOUNT", "u1", "t1", 6000.0, None, "details", datetime.now())]
        with patch("class10_server.pymysql.connect", _mock_connect(rows)):
            resp = client.get("/api/export/alerts")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]


class TestExportStats:
    def test_returns_csv(self, client):
        rows = [(datetime.now(), datetime.now(), "electronics", 5000.0, 20)]
        with patch("class10_server.pymysql.connect", _mock_connect(rows)):
            resp = client.get("/api/export/stats")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]


# ---- 省份 ----
class TestRegionStats:
    def test_reads_from_cache(self, client):
        with patch("class10_server.region_cache", {
            "广东省": {"province": "广东省", "total_amount": 5000.0, "transaction_count": 20},
        }):
            resp = client.get("/api/region-stats")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["province"] == "广东省"


class TestRegionAlertStats:
    def test_returns_province_list(self, client):
        rows = [("广东省", 30), ("北京市", 5)]
        with patch("class10_server.pymysql.connect", _mock_connect(rows)):
            resp = client.get("/api/region-alert-stats")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0] == {"province": "广东省", "alert_count": 30}


# ---- 风险评分 ----
class TestUserRiskScores:
    def test_returns_sorted_from_cache(self, client):
        with patch("class10_server.risk_score_cache", {"u_a": 0.8, "u_b": 1.5}):
            mock_cur = MagicMock()
            mock_cur.fetchall.return_value = [("u_b", "Bob"), ("u_a", "Alice")]
            mock_conn = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cur
            with patch("class10_server.pymysql.connect", MagicMock(return_value=mock_conn)):
                resp = client.get("/api/user-risk-scores?limit=2")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["user_id"] == "u_b"   # 高分在前
        assert data[0]["risk_score"] == 1.5

    def test_empty_cache(self, client):
        with patch("class10_server.risk_score_cache", {}):
            resp = client.get("/api/user-risk-scores")
        assert resp.status_code == 200
        assert resp.json() == []


# ---- 历史窗口统计 ----
class TestStatsHistory:
    def test_no_filter(self, client):
        now = datetime.now()
        rows = [(now, now, "electronics", 5000.0, 20)]
        with patch("class10_server.pymysql.connect", _mock_connect(rows)):
            resp = client.get("/api/stats/history")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert data[0]["category"] == "electronics"

    def test_filter_by_window(self, client):
        with patch("class10_server.pymysql.connect", _mock_connect([])):
            resp = client.get("/api/stats/history?window_start=2026-01-01&window_end=2026-06-01")
        assert resp.status_code == 200
        assert resp.json() == []
