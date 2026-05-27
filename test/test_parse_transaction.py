"""ParseTransaction.map 数据解析测试。"""
import json


class TestParseTransaction:
    """验证 JSON → 12 元组解析的正确性和默认值。"""

    def test_complete_json_all_fields(self, parse_txn):
        """完整 JSON 的 12 个字段均正确解析。"""
        msg = json.dumps({
            "user_id": "user_abc",
            "amount": 1234.56,
            "category": "electronics",
            "timestamp": 1716543000000,
            "transaction_id": "txn_001",
            "result": "success",
            "transaction_type": "purchase",
            "ip_address": "10.0.1.5",
            "product_id": "prod_99",
            "product_name": "Sensor Electronics",
            "province": "广东省",
            "city": "深圳",
        })
        result = parse_txn.map(msg)
        assert result[T_IDX_USER_ID] == "user_abc"
        assert result[T_IDX_AMOUNT] == 1234.56
        assert result[T_IDX_CATEGORY] == "electronics"
        assert result[T_IDX_TIMESTAMP] == 1716543000000
        assert result[T_IDX_TXN_ID] == "txn_001"
        assert result[T_IDX_RESULT] == "success"
        assert result[T_IDX_IP_ADDRESS] == "10.0.1.5"
        assert result[T_IDX_PROVINCE] == "广东省"

    def test_minimal_json_defaults_applied(self, parse_txn):
        """仅含必填字段时可选字段取默认值。"""
        msg = json.dumps({
            "user_id": "u1",
            "amount": 100,
            "timestamp": 1716543000000,
            "transaction_id": "t1",
        })
        result = parse_txn.map(msg)
        assert result[T_IDX_CATEGORY] == "unknown"
        assert result[T_IDX_RESULT] == "success"
        assert result[T_IDX_IP_ADDRESS] == "0.0.0.0"
        assert result[T_IDX_PROVINCE] == "未知"
        assert result[T_IDX_CITY] == "未知"

    def test_amount_is_float_not_string(self, parse_txn):
        """amount 字段转为 float 类型。"""
        msg = json.dumps({
            "user_id": "u1", "amount": "99.99",
            "timestamp": 1716543000000, "transaction_id": "t1",
        })
        result = parse_txn.map(msg)
        assert isinstance(result[T_IDX_AMOUNT], float)
        assert result[T_IDX_AMOUNT] == 99.99

    def test_timestamp_is_int(self, parse_txn):
        """timestamp 字段转为 int 类型。"""
        msg = json.dumps({
            "user_id": "u1", "amount": 100,
            "timestamp": "1716543000000", "transaction_id": "t1",
        })
        result = parse_txn.map(msg)
        assert isinstance(result[T_IDX_TIMESTAMP], int)
        assert result[T_IDX_TIMESTAMP] == 1716543000000


# 直接 import 索引常量供测试使用
from class10_ecomm_datastream import (
    T_IDX_USER_ID, T_IDX_AMOUNT, T_IDX_CATEGORY, T_IDX_TIMESTAMP,
    T_IDX_TXN_ID, T_IDX_RESULT, T_IDX_IP_ADDRESS,
    T_IDX_PROVINCE, T_IDX_CITY,
)
