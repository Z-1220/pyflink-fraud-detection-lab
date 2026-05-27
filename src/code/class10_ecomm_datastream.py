# class10_ecomm_datastream.py
"""
电商交易风险检测系统 - Flink 流处理作业（最终稳定版）
修复 on_timer 返回 None 导致 TimerException 的问题。
"""

import json
import math
import os
import traceback
from datetime import datetime, timezone

import pymysql
from config_loader import MYSQL_CONFIG
from pyflink.common import Duration, Time, WatermarkStrategy, Types, RestartStrategies
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import (
    MapFunction,
    ProcessWindowFunction,
    KeyedProcessFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

# ==================== 环境变量 ====================
os.environ["JAVA_TOOL_OPTIONS"] = "-Dfile.encoding=UTF-8"
_PYFLINK_VENV = os.environ.get(
    "PYFLINK_VENV",
    r"D:\PythonProject\00_Learning\pyflink_project\.venv",
)
PYTHON_EXEC = os.environ.get(
    "PYFLINK_PYTHON_EXEC",
    os.path.join(_PYFLINK_VENV, "Scripts", "python.exe"),
)
os.environ.setdefault("PYFLINK_CLIENT_EXECUTABLE", PYTHON_EXEC)
os.environ.setdefault("python.executable", PYTHON_EXEC)
os.environ.setdefault("python.client.executable", PYTHON_EXEC)
os.environ.setdefault("BEAM_PYTHON", PYTHON_EXEC)
os.environ["PYTHON_LOOPBACK_MODE"] = "1"
os.environ["FLINK_PYTHON_WORKER_EXIT_TIMEOUT"] = "60000"

# ==================== 配置 ====================
KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC = "transaction_events"
OUTPUT_ALARM_TOPIC = "alarm_events"
OUTPUT_GLOBAL_ACC_TOPIC = "total_amount_and_count_events"
OUTPUT_WINDOW_GLOBAL_TOPIC = "window_count_and_amount_events"
OUTPUT_CATEGORY_TOPIC = "category_aggregated_events"
OUTPUT_REGION_TOPIC = "region_aggregated_events"
OUTPUT_RISK_SCORE_TOPIC = "user_risk_scores"

HIGH_AMOUNT_THRESHOLD = 5000.0          # 大额交易阈值：单笔交易金额 > 5000 即触发告警
FREQ_WINDOW_MS = 300_000                # 高频交易检测窗口：5 分钟（300,000 毫秒）
FREQ_THRESHOLD = 5                      # 高频交易告警触发条件：同一用户在窗口内交易次数 ≥ 5 笔
INCREASE_MIN_SEQ = 3                    # 连续递增交易序列最小长度：至少连续 3 笔递增才触发告警
INCREASE_FACTOR = 1.1                   # 递增比例：下一笔金额必须大于前一笔的 1.1 倍（即增长 10% 以上）
FAILED_SURGE_WINDOW_SECONDS = 30        # 失败交易飙升检测窗口：30 秒内失败笔数超过阈值则告警
FAILED_SURGE_THRESHOLD = 8              # 失败交易飙升阈值：30 秒内失败交易 ≥ 8 笔
IP_SHARING_WINDOW_SECONDS = 60          # IP 共用检测窗口：60 秒内同 IP 不同用户数超过阈值则告警
IP_SHARING_THRESHOLD = 3                # IP 共用阈值：同 IP 出现 ≥ 3 个不同用户

ADS_MYSQL_CONFIG = {**MYSQL_CONFIG, "database": "ads_ecommerce", "autocommit": True}

# 解析后元组字段索引（ParseTransaction 返回的 12 元组）
T_IDX_USER_ID = 0
T_IDX_AMOUNT = 1
T_IDX_CATEGORY = 2
T_IDX_TIMESTAMP = 3
T_IDX_TXN_ID = 4
T_IDX_RESULT = 5
T_IDX_TXN_TYPE = 6
T_IDX_IP_ADDRESS = 7
T_IDX_PRODUCT_ID = 8
T_IDX_PRODUCT_NAME = 9
T_IDX_PROVINCE = 10
T_IDX_CITY = 11


class TransactionTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return int(value[3])


class ParseTransaction(MapFunction):
    def map(self, value: str):
        txn = json.loads(value)
        return (
            txn["user_id"],                          # T_IDX_USER_ID
            float(txn["amount"]),                    # T_IDX_AMOUNT
            txn.get("category", "unknown"),          # T_IDX_CATEGORY
            int(txn["timestamp"]),                   # T_IDX_TIMESTAMP
            txn["transaction_id"],                   # T_IDX_TXN_ID
            txn.get("result", "success"),            # T_IDX_RESULT
            txn.get("transaction_type", "purchase"), # T_IDX_TXN_TYPE
            txn.get("ip_address", "0.0.0.0"),        # T_IDX_IP_ADDRESS
            txn.get("product_id", "unknown"),        # T_IDX_PRODUCT_ID
            txn.get("product_name", "unknown"),      # T_IDX_PRODUCT_NAME
            txn.get("province", "未知"),              # T_IDX_PROVINCE
            txn.get("city", "未知"),                  # T_IDX_CITY
        )


class GlobalAccumulator(MapFunction):
    def __init__(self):
        self.total = 0.0
        self.count = 0

    def map(self, value):
        self.total += value[T_IDX_AMOUNT]
        self.count += 1
        return json.dumps({
            "total_amount": round(self.total, 2),
            "transaction_count": self.count,
            "update_time": datetime.now(timezone.utc).isoformat(),
        })


class HighFrequencyDetector(KeyedProcessFunction):
    def __init__(self):
        self.timestamps_state = None
        self.ads_conn = None

    def open(self, runtime_context: RuntimeContext):
        self.timestamps_state = runtime_context.get_list_state(
            ListStateDescriptor("timestamps_v2", Types.LONG())
        )
        self.ads_conn = pymysql.connect(**ADS_MYSQL_CONFIG)

    def process_element(self, value, ctx):
        try:
            user_id = value[T_IDX_USER_ID]
            ts = value[T_IDX_TIMESTAMP]

            ts_list = self.timestamps_state.get()
            if ts_list is None:
                ts_list = []

            cutoff = ts - FREQ_WINDOW_MS
            ts_list = [t for t in ts_list if t > cutoff]
            ts_list.append(ts)
            self.timestamps_state.update(ts_list)

            ctx.timer_service().register_event_time_timer(ts + FREQ_WINDOW_MS)

            if len(ts_list) >= FREQ_THRESHOLD:
                alert = {
                    "alert_type": "HIGH_FREQUENCY",
                    "user_id": user_id,
                    "transaction_count": len(ts_list),
                    "window_start": datetime.fromtimestamp((ts - FREQ_WINDOW_MS) / 1000, tz=timezone.utc).isoformat(),
                    "window_end": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat(),
                    "alert_time": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat(),
                    "details": f"User {user_id} has {len(ts_list)} transactions in last {FREQ_WINDOW_MS/1000:.0f} seconds",
                }
                yield json.dumps(alert)
                self._write_ads_alert(alert)
        except Exception as e:
            print(f"❌ HighFrequencyDetector error: {e}")
            traceback.print_exc()
            raise

    def on_timer(self, timestamp: int, ctx):
        try:
            ts_list = self.timestamps_state.get()
            if ts_list is None:
                ts_list = []
            cutoff = timestamp - FREQ_WINDOW_MS
            ts_list = [t for t in ts_list if t > cutoff]
            self.timestamps_state.update(ts_list)
        except Exception as e:
            print(f"❌ HighFrequencyDetector.on_timer error: {e}")
            traceback.print_exc()
            raise
        return []

    def _write_ads_alert(self, alert):
        try:
            with self.ads_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO risk_alerts
                       (alert_type, user_id, transaction_id, amount, transaction_count,
                        window_start, window_end, details, alert_time)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        alert["alert_type"],
                        alert["user_id"],
                        None,
                        None,
                        alert["transaction_count"],
                        alert["window_start"],
                        alert["window_end"],
                        alert["details"],
                        alert["alert_time"],
                    ),
                )
        except Exception as e:
            print(f"⚠️ 写入高频告警失败: {e}")

    def close(self):
        if self.ads_conn:
            self.ads_conn.close()


class ContinuousIncreaseDetector(KeyedProcessFunction):
    def __init__(self):
        self.last_amounts = None
        self.ads_conn = None

    def open(self, runtime_context: RuntimeContext):
        self.last_amounts = runtime_context.get_list_state(
            ListStateDescriptor("last_amounts_v2", Types.DOUBLE())
        )
        self.ads_conn = pymysql.connect(**ADS_MYSQL_CONFIG)

    def process_element(self, value, ctx):
        try:
            user_id = value[T_IDX_USER_ID]
            amount = value[T_IDX_AMOUNT]
            ts = value[T_IDX_TIMESTAMP]
            txn_id = value[T_IDX_TXN_ID]

            amounts = list(self.last_amounts.get() or [])
            amounts.append(amount)
            if len(amounts) > 10:
                amounts = amounts[-10:]
            self.last_amounts.update(amounts)

            if len(amounts) >= INCREASE_MIN_SEQ:
                inc_count = 1
                inc_amounts = [amounts[-1]]
                for i in range(len(amounts)-2, -1, -1):
                    if amounts[i+1] >= amounts[i] * INCREASE_FACTOR:
                        inc_count += 1
                        inc_amounts.insert(0, amounts[i])
                    else:
                        break
                if inc_count >= INCREASE_MIN_SEQ:
                    alert = {
                        "alert_type": "CONTINUOUS_INCREASE",
                        "user_id": user_id,
                        "transaction_id": txn_id,
                        "amount": amount,
                        "sequence_length": inc_count,
                        "amounts": inc_amounts,
                        "alert_time": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat(),
                        "details": f"User {user_id} has {inc_count} consecutive increasing transactions",
                    }
                    yield json.dumps(alert)
                    self._write_ads_alert(alert)
                    self.last_amounts.clear()
        except Exception as e:
            print(f"❌ ContinuousIncreaseDetector error: {e}")
            traceback.print_exc()
            raise

    def on_timer(self, timestamp: int, ctx):
        return []

    def _write_ads_alert(self, alert):
        try:
            with self.ads_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO risk_alerts
                       (alert_type, user_id, transaction_id, amount, transaction_count,
                        window_start, window_end, details, alert_time)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        alert["alert_type"],
                        alert["user_id"],
                        alert["transaction_id"],
                        alert["amount"],
                        None,
                        None,
                        None,
                        alert["details"],
                        alert["alert_time"],
                    ),
                )
        except Exception as e:
            print(f"⚠️ 写入连续递增告警失败: {e}")

    def close(self):
        if self.ads_conn:
            self.ads_conn.close()


# ==================== 大额交易、窗口函数等保持不变，与之前完全相同 ====================
class LargeAmountAlertSink(MapFunction):
    def __init__(self):
        self.ads_conn = None

    def open(self, runtime_context):
        self.ads_conn = pymysql.connect(**ADS_MYSQL_CONFIG)

    def map(self, value):
        user_id, amount, category, timestamp_ms, txn_id, *_ = value
        alert = {
            "alert_type": "LARGE_AMOUNT",
            "user_id": user_id,
            "transaction_id": txn_id,
            "amount": amount,
            "category": category,
            "alert_time": datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat(),
            "details": f"Transaction {txn_id} amount {amount} exceeds {HIGH_AMOUNT_THRESHOLD}",
        }
        self._write_ads_alert(alert)
        return json.dumps(alert)

    def _write_ads_alert(self, alert):
        try:
            with self.ads_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO risk_alerts
                       (alert_type, user_id, transaction_id, amount, details, alert_time)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        alert["alert_type"],
                        alert["user_id"],
                        alert["transaction_id"],
                        alert["amount"],
                        alert["details"],
                        alert["alert_time"],
                    ),
                )
        except Exception as e:
            print(f"⚠️ 写入大额告警失败: {e}")

    def close(self):
        if self.ads_conn:
            self.ads_conn.close()


class FailedTransactionSurgeDetector(ProcessWindowFunction):
    """30 秒滚动窗口内失败交易数量超过阈值时告警"""
    def __init__(self):
        self.ads_conn = None

    def open(self, runtime_context):
        self.ads_conn = pymysql.connect(**ADS_MYSQL_CONFIG)

    def process(self, key: str, context, elements) -> list:
        count = 0
        for e in elements:
            count += 1
        if count < FAILED_SURGE_THRESHOLD:
            return []
        window_start = context.window().start
        window_end = context.window().end
        alert = {
            "alert_type": "FAILED_SURGE",
            "user_id": "GLOBAL",
            "transaction_count": count,
            "window_start": datetime.fromtimestamp(window_start / 1000, tz=timezone.utc).isoformat(),
            "window_end": datetime.fromtimestamp(window_end / 1000, tz=timezone.utc).isoformat(),
            "alert_time": datetime.now(timezone.utc).isoformat(),
            "details": f"{count} failed transactions in last {FAILED_SURGE_WINDOW_SECONDS}s",
        }
        try:
            with self.ads_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO risk_alerts
                       (alert_type, user_id, transaction_count, window_start, window_end, details, alert_time)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (alert["alert_type"], alert["user_id"], alert["transaction_count"],
                     alert["window_start"], alert["window_end"], alert["details"], alert["alert_time"]),
                )
        except Exception as e:
            print(f"⚠️ 写入失败飙升告警失败: {e}")
        return [json.dumps(alert)]

    def close(self):
        if self.ads_conn:
            self.ads_conn.close()


class IPSharingDetector(ProcessWindowFunction):
    """60 秒滚动窗口内同 IP 出现多个不同用户时告警"""
    def __init__(self):
        self.ads_conn = None

    def open(self, runtime_context):
        self.ads_conn = pymysql.connect(**ADS_MYSQL_CONFIG)

    def process(self, ip: str, context, elements) -> list:
        user_ids = set()
        for e in elements:
            user_ids.add(e[T_IDX_USER_ID])
        if len(user_ids) < IP_SHARING_THRESHOLD:
            return []
        window_start = context.window().start
        window_end = context.window().end
        alert = {
            "alert_type": "IP_SHARING",
            "user_id": next(iter(user_ids)),
            "ip_address": ip,
            "user_count": len(user_ids),
            "shared_users": list(user_ids),
            "window_start": datetime.fromtimestamp(window_start / 1000, tz=timezone.utc).isoformat(),
            "window_end": datetime.fromtimestamp(window_end / 1000, tz=timezone.utc).isoformat(),
            "alert_time": datetime.now(timezone.utc).isoformat(),
            "details": f"IP {ip} has {len(user_ids)} distinct users in {IP_SHARING_WINDOW_SECONDS}s: {', '.join(user_ids)}",
        }
        try:
            with self.ads_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO risk_alerts
                       (alert_type, user_id, transaction_count, window_start, window_end, details, alert_time)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (alert["alert_type"], alert["user_id"], alert["user_count"],
                     alert["window_start"], alert["window_end"], alert["details"], alert["alert_time"]),
                )
        except Exception as e:
            print(f"⚠️ 写入IP共用告警失败: {e}")
        return [json.dumps(alert)]

    def close(self):
        if self.ads_conn:
            self.ads_conn.close()


class RegionWindowFunction(ProcessWindowFunction):
    """5 秒滚动窗口按省份聚合交易额和笔数（用于中国地图热力图）"""
    def open(self, runtime_context):
        pass

    def process(self, province: str, context, elements) -> list:
        total = 0.0
        count = 0
        for e in elements:
            total += e[T_IDX_AMOUNT]
            count += 1
        result = {
            "province": province,
            "window_start": context.window().start,
            "window_end": context.window().end,
            "total_amount": round(total, 2),
            "transaction_count": count,
        }
        return [json.dumps(result)]


class GlobalWindowFunction(ProcessWindowFunction):
    def __init__(self):
        self.ads_conn = None

    def open(self, runtime_context):
        self.ads_conn = pymysql.connect(**ADS_MYSQL_CONFIG)

    def process(self, key: str, context, elements) -> list:
        total = 0.0
        count = 0
        for e in elements:
            total += e[T_IDX_AMOUNT]
            count += 1
        window_start = context.window().start
        window_end = context.window().end
        result = {
            "window_start": window_start,
            "window_end": window_end,
            "total_amount": round(total, 2),
            "transaction_count": count,
        }
        try:
            with self.ads_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO transaction_stats
                       (window_start, window_end, category, total_amount, transaction_count)
                       VALUES (%s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                           total_amount = VALUES(total_amount),
                           transaction_count = VALUES(transaction_count)""",
                    (
                        datetime.fromtimestamp(window_start / 1000, tz=timezone.utc),
                        datetime.fromtimestamp(window_end / 1000, tz=timezone.utc),
                        "ALL",
                        total,
                        count,
                    ),
                )
        except Exception as e:
            print(f"⚠️ 写入全量窗口失败: {e}")
        return [json.dumps(result)]

    def close(self):
        if self.ads_conn:
            self.ads_conn.close()


class CategoryWindowFunction(ProcessWindowFunction):
    def __init__(self):
        self.ads_conn = None

    def open(self, runtime_context):
        self.ads_conn = pymysql.connect(**ADS_MYSQL_CONFIG)

    def process(self, category: str, context, elements) -> list:
        total = 0.0
        count = 0
        for e in elements:
            total += e[T_IDX_AMOUNT]
            count += 1
        window_start = context.window().start
        window_end = context.window().end
        result = {
            "window_start": window_start,
            "window_end": window_end,
            "category": category,
            "total_amount": round(total, 2),
            "transaction_count": count,
        }
        try:
            with self.ads_conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO transaction_stats
                       (window_start, window_end, category, total_amount, transaction_count)
                       VALUES (%s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                           total_amount = VALUES(total_amount),
                           transaction_count = VALUES(transaction_count)""",
                    (
                        datetime.fromtimestamp(window_start / 1000, tz=timezone.utc),
                        datetime.fromtimestamp(window_end / 1000, tz=timezone.utc),
                        category,
                        total,
                        count,
                    ),
                )
        except Exception as e:
            print(f"⚠️ 写入类别窗口失败: {e}")
        return [json.dumps(result)]

    def close(self):
        if self.ads_conn:
            self.ads_conn.close()


class TimeDecayRiskScorer(KeyedProcessFunction):
    """时间衰减风险评分。每条告警触发：旧分衰减 + 动态严重性累加。

    严重性 = 基础权重 x 动态乘数，乘数基于告警具体指标超出阈值的程度。
    例如：大额交易 50000 元比 5001 元严重 10 倍（上限 5x）。
    """

    def __init__(self):
        self.score_state = None
        self._base_weights = {
            "LARGE_AMOUNT": 0.30,
            "HIGH_FREQUENCY": 0.25,
            "CONTINUOUS_INCREASE": 0.20,
            "FAILED_SURGE": 0.25,
            "IP_SHARING": 0.25,
        }
        self._lambda = math.log(2) / 180_000.0  # T_half=3min

    def _compute_severity(self, alert):
        """根据告警具体指标计算动态严重性。乘数范围 [1.0, 上限]，保证至少获得基础权重。"""
        alert_type = alert.get("alert_type", "UNKNOWN")
        base = self._base_weights.get(alert_type, 0.10)
        multiplier = 1.0
        if alert_type == "LARGE_AMOUNT":
            amount = alert.get("amount", 0) or 0
            multiplier = max(min(amount / float(HIGH_AMOUNT_THRESHOLD), 5.0), 1.0)
        elif alert_type == "HIGH_FREQUENCY":
            count = alert.get("transaction_count", 0) or 0
            multiplier = max(min(count / float(FREQ_THRESHOLD), 4.0), 1.0)
        elif alert_type == "CONTINUOUS_INCREASE":
            seq_len = alert.get("sequence_length", 0) or 0
            multiplier = max(min(seq_len / float(INCREASE_MIN_SEQ), 4.0), 1.0)
        elif alert_type == "FAILED_SURGE":
            count = alert.get("transaction_count", 0) or 0
            multiplier = max(min(count / float(FAILED_SURGE_THRESHOLD), 5.0), 1.0)
        elif alert_type == "IP_SHARING":
            users = alert.get("user_count", 0) or 0
            multiplier = max(min(users / float(IP_SHARING_THRESHOLD), 5.0), 1.0)
        return base * multiplier

    def open(self, runtime_context: RuntimeContext):
        self.score_state = runtime_context.get_state(
            ValueStateDescriptor("decay_score", Types.STRING())
        )

    def process_element(self, value, ctx):
        alert = json.loads(value)
        user_id = alert.get("user_id", "unknown")
        severity = self._compute_severity(alert)

        try:
            now = datetime.fromisoformat(alert["alert_time"]).timestamp() * 1000.0
        except (KeyError, ValueError):
            now = float(ctx.timestamp())

        raw = self.score_state.value()
        last_score = 0.0
        last_ts = 0.0
        if raw:
            parts = raw.split(",")
            last_score = float(parts[0])
            last_ts = float(parts[1])

        if last_ts > 0 and now > last_ts:
            last_score *= math.exp(-self._lambda * (now - last_ts))

        new_score = last_score + severity
        self.score_state.update(f"{new_score},{now}")

        if new_score > 0.001:
            yield json.dumps({
                "user_id": user_id,
                "risk_score": round(new_score, 4),
                "update_time": alert.get("alert_time", ""),
            })

    def on_timer(self, timestamp, ctx):
        return []


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_restart_strategy(RestartStrategies.no_restart())
    env.get_config().set_global_job_parameters({
        "python.fn-execution.bundle.size": "1",
        "python.fn-execution.bundle.time": "0",
    })

    _jars_dir = os.environ.get(
        "PYFLINK_JARS_DIR",
        r"D:\PythonProject\00_Learning\pyflink_project\jars",
    )
    env.add_jars(
        f"file:///{_jars_dir}/flink-connector-kafka-3.1.0-1.18.jar",
        f"file:///{_jars_dir}/kafka-clients-3.6.1.jar",
    )

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(INPUT_TOPIC)
        .set_group_id("flink-txn-consumer")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    raw_stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    parsed_stream = (
        raw_stream.map(ParseTransaction())
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(2))
            .with_timestamp_assigner(TransactionTimestampAssigner())
        )
    )

    global_acc_stream = parsed_stream.map(GlobalAccumulator(), output_type=Types.STRING())

    high_freq_alarm_stream = (
        parsed_stream.key_by(lambda v: v[T_IDX_USER_ID])
        .process(HighFrequencyDetector(), output_type=Types.STRING())
    )

    increase_alarm_stream = (
        parsed_stream.key_by(lambda v: v[T_IDX_USER_ID])
        .process(ContinuousIncreaseDetector(), output_type=Types.STRING())
    )

    large_alarm_stream = (
        parsed_stream.filter(lambda t: t[T_IDX_AMOUNT] > HIGH_AMOUNT_THRESHOLD)
        .map(LargeAmountAlertSink(), output_type=Types.STRING())
    )

    failed_surge_stream = (
        parsed_stream.filter(lambda t: t[T_IDX_RESULT] == "failed")
        .key_by(lambda x: "global")
        .window(TumblingEventTimeWindows.of(Time.seconds(FAILED_SURGE_WINDOW_SECONDS)))
        .process(FailedTransactionSurgeDetector(), output_type=Types.STRING())
    )

    ip_sharing_stream = (
        parsed_stream.key_by(lambda x: x[T_IDX_IP_ADDRESS])
        .window(TumblingEventTimeWindows.of(Time.seconds(IP_SHARING_WINDOW_SECONDS)))
        .process(IPSharingDetector(), output_type=Types.STRING())
    )

    all_alarms = large_alarm_stream.union(
        high_freq_alarm_stream, increase_alarm_stream,
        failed_surge_stream, ip_sharing_stream,
    )

    risk_score_stream = (
        all_alarms.key_by(lambda v: json.loads(v).get("user_id", "unknown"))
        .process(TimeDecayRiskScorer(), output_type=Types.STRING())
    )

    global_window_stream = (
        parsed_stream.key_by(lambda x: "global")
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .process(GlobalWindowFunction(), output_type=Types.STRING())
    )

    category_window_stream = (
        parsed_stream.key_by(lambda x: x[T_IDX_CATEGORY])
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .process(CategoryWindowFunction(), output_type=Types.STRING())
    )

    region_window_stream = (
        parsed_stream.key_by(lambda x: x[T_IDX_PROVINCE])
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .process(RegionWindowFunction(), output_type=Types.STRING())
    )

    def create_kafka_sink(topic):
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

    all_alarms.sink_to(create_kafka_sink(OUTPUT_ALARM_TOPIC))
    global_acc_stream.sink_to(create_kafka_sink(OUTPUT_GLOBAL_ACC_TOPIC))
    global_window_stream.sink_to(create_kafka_sink(OUTPUT_WINDOW_GLOBAL_TOPIC))
    category_window_stream.sink_to(create_kafka_sink(OUTPUT_CATEGORY_TOPIC))
    region_window_stream.sink_to(create_kafka_sink(OUTPUT_REGION_TOPIC))
    risk_score_stream.sink_to(create_kafka_sink(OUTPUT_RISK_SCORE_TOPIC))

    env.execute("Ecommerce Risk Detection")

if __name__ == "__main__":
    main()