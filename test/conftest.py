"""共享 fixtures：TestClient、算法实例、mock cursor。"""
import sys
import os
from contextlib import asynccontextmanager
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "code"))


@pytest.fixture
def mock_cursor():
    """可预设 fetchall 返回值的 mock pymysql cursor。"""
    cur = MagicMock()
    cur.fetchall.return_value = []
    return cur


@pytest.fixture(scope="module")
def risk_scorer():
    from class10_ecomm_datastream import TimeDecayRiskScorer
    return TimeDecayRiskScorer()


@pytest.fixture(scope="module")
def parse_txn():
    from class10_ecomm_datastream import ParseTransaction
    return ParseTransaction()


@pytest.fixture(scope="module")
def client():
    """FastAPI TestClient，Kafka 已禁用。每个测试需自行 patch pymysql.connect。"""
    with patch("class10_server.Thread"):
        from class10_server import app

        @asynccontextmanager
        async def _noop_lifespan(_app):
            yield

        app.router.lifespan_context = _noop_lifespan

        from fastapi.testclient import TestClient
        with TestClient(app) as tc:
            yield tc
