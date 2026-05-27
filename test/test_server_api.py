"""FastAPI REST 端点集成测试。"""
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest


def _mock_connect(fetchall_return):
    """创建 mock pymysql.connect，cursor.fetchall 返回指定数据。"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fetchall_return
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    return MagicMock(return_value=mock_conn)


class TestCategories:
    def test_returns_list(self, client):
        with patch("class10_server.pymysql.connect",
                   _mock_connect([("electronics", "电子产品"), ("clothing", "服装")])):
            resp = client.get("/api/categories")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0] == {"category": "electronics", "description": "电子产品"}


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

    def test_filter_by_type(self, client):
        now = datetime.now()
        rows = [("LARGE_AMOUNT", "u1", "t1", 7000.0, None, "detail", now)]
        with patch("class10_server.pymysql.connect", _mock_connect(rows)):
            resp = client.get("/api/alerts/history?alert_type=LARGE_AMOUNT")
        assert resp.status_code == 200
        assert all(r["alert_type"] == "LARGE_AMOUNT" for r in resp.json())

    def test_filter_by_keyword(self, client):
        with patch("class10_server.pymysql.connect", _mock_connect([])):
            resp = client.get("/api/alerts/history?keyword=user_abc")
        assert resp.status_code == 200


class TestAlertStats:
    def test_returns_by_type_and_by_hour(self, client):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.side_effect = [
            [("LARGE_AMOUNT", 30), ("HIGH_FREQUENCY", 20)],   # by_type
            [("10:00", 15), ("11:00", 10)],                    # by_hour
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        with patch("class10_server.pymysql.connect", MagicMock(return_value=mock_conn)):
            resp = client.get("/api/alerts/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["by_type"]) == 2
        assert len(data["by_hour"]) == 2


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


class TestDashboardSnapshot:
    def test_contains_four_keys(self, client):
        """snapshot 返回 4 个子模块数据。"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # _query_top_risky_users, _query_alert_type_distribution, _query_region_alerts
        mock_cursor.fetchall.side_effect = [
            [("u1", "Alice", 5)],             # top risky
            [("LARGE_AMOUNT", 10)],           # alert type distribution
            [("广东省", 8)],                   # region alerts
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        with patch("class10_server.pymysql.connect", MagicMock(return_value=mock_conn)):
            # 清空风险评分缓存避免不必要的 DB 连接
            with patch("class10_server.risk_score_cache", {}):
                resp = client.get("/api/dashboard/snapshot")
        assert resp.status_code == 200
        data = resp.json()
        assert "top_risky_users" in data
        assert "risk_scores" in data
        assert "alert_stats" in data
        assert "region_alerts" in data


class TestExportAlerts:
    def test_returns_csv_content_type(self, client):
        rows = [("LARGE_AMOUNT", "u1", "t1", 6000.0, None, "details", datetime.now())]
        with patch("class10_server.pymysql.connect", _mock_connect(rows)):
            resp = client.get("/api/export/alerts")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]


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
