"""TimeDecayRiskScorer._compute_severity 算法测试。"""


class TestComputeSeverity:
    """验证 5 种告警类型的动态严重性计算。"""

    # ---- LARGE_AMOUNT ----
    def test_large_amount_baseline(self, risk_scorer):
        """5000 元刚好触发阈值，乘数 = 1.0。"""
        alert = {"alert_type": "LARGE_AMOUNT", "amount": 5000}
        assert risk_scorer._compute_severity(alert) == 0.30

    def test_large_amount_capped(self, risk_scorer):
        """25000 元 = 5 倍阈值，触及 5x 上限。"""
        alert = {"alert_type": "LARGE_AMOUNT", "amount": 25000}
        assert risk_scorer._compute_severity(alert) == 1.50  # 0.30 * 5.0

    def test_large_amount_zero_still_gets_base_weight(self, risk_scorer):
        """金额为 0（异常数据）仍获得基础权重。"""
        alert = {"alert_type": "LARGE_AMOUNT", "amount": 0}
        assert risk_scorer._compute_severity(alert) == 0.30

    def test_large_amount_missing_field_safe(self, risk_scorer):
        """缺少 amount 字段不崩溃，获得基础权重。"""
        alert = {"alert_type": "LARGE_AMOUNT"}
        assert risk_scorer._compute_severity(alert) == 0.30

    # ---- HIGH_FREQUENCY ----
    def test_high_frequency(self, risk_scorer):
        """10 笔交易 = 2 倍阈值。"""
        alert = {"alert_type": "HIGH_FREQUENCY", "transaction_count": 10}
        assert risk_scorer._compute_severity(alert) == 0.50  # 0.25 * 2.0

    # ---- FAILED_SURGE（系统级告警，不参与用户评分） ----
    def test_failed_surge_uses_default_weight(self, risk_scorer):
        """失败飙升是系统级指标，落入默认权重 0.10。
        实际运行时 process_element 会直接跳过 FAILED_SURGE，
        此测试仅验证 _compute_severity 的兜底行为。"""
        alert = {"alert_type": "FAILED_SURGE", "transaction_count": 16}
        assert risk_scorer._compute_severity(alert) == 0.10

    # ---- IP_SHARING ----
    def test_ip_sharing_capped(self, risk_scorer):
        """15 个用户共用 IP = 5 倍阈值，触及 5x 上限。"""
        alert = {"alert_type": "IP_SHARING", "user_count": 15}
        assert risk_scorer._compute_severity(alert) == 1.25  # 0.25 * 5.0

    # ---- 边界 ----
    def test_unknown_type_default(self, risk_scorer):
        """未知告警类型 → 默认权重 0.10。"""
        alert = {"alert_type": "UNKNOWN_TYPE"}
        assert risk_scorer._compute_severity(alert) == 0.10
