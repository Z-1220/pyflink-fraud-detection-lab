# class10_ecomm_datastream.py
"""
电商交易风险检测系统 - Flink 流处理作业（最终稳定版）
修复 on_timer 返回 None 导致 TimerException 的问题。
"""

import json
import os
import traceback
from datetime import datetime, timezone

import pymysql
from fastapi import FastAPI
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
from pyflink.datastream.state import ListStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows
from starlette.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

# ==================== 环境变量 ====================
os.environ["JAVA_TOOL_OPTIONS"] = "-Dfile.encoding=UTF-8"
PYTHON_EXEC = r"D:\PythonProject\00_Learning\pyflink_project\.venv\Scripts\python.exe"
os.environ["PYFLINK_CLIENT_EXECUTABLE"] = PYTHON_EXEC
os.environ["python.executable"] = PYTHON_EXEC
os.environ["python.client.executable"] = PYTHON_EXEC
os.environ["BEAM_PYTHON"] = PYTHON_EXEC
os.environ["PYTHON_LOOPBACK_MODE"] = "1"
os.environ["FLINK_PYTHON_WORKER_EXIT_TIMEOUT"] = "60000"

# ==================== 配置 ====================
KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC = "transaction_events"
OUTPUT_ALARM_TOPIC = "alarm_events"
OUTPUT_GLOBAL_ACC_TOPIC = "total_amount_and_count_events"
OUTPUT_WINDOW_GLOBAL_TOPIC = "window_count_and_amount_events"
OUTPUT_CATEGORY_TOPIC = "category_aggregated_events"
OUTPUT_PRODUCT_TOPIC = "product_aggregated_events"

HIGH_AMOUNT_THRESHOLD = 5000.0          # 大额交易阈值：单笔交易金额 > 5000 即触发告警
FREQ_WINDOW_MS = 300_000                # 高频交易检测窗口：5 分钟（300,000 毫秒）
FREQ_THRESHOLD = 5                      # 高频交易告警触发条件：同一用户在窗口内交易次数 ≥ 5 笔
INCREASE_MIN_SEQ = 3                    # 连续递增交易序列最小长度：至少连续 3 笔递增才触发告警
INCREASE_FACTOR = 1.1                   # 递增比例：下一笔金额必须大于前一笔的 1.1 倍（即增长 10% 以上）
FAILED_SURGE_WINDOW_SECONDS = 30        # 失败交易飙升检测窗口：30 秒内失败笔数超过阈值则告警
FAILED_SURGE_THRESHOLD = 8              # 失败交易飙升阈值：30 秒内失败交易 ≥ 8 笔
IP_SHARING_WINDOW_SECONDS = 60          # IP 共用检测窗口：60 秒内同 IP 不同用户数超过阈值则告警
IP_SHARING_THRESHOLD = 3                # IP 共用阈值：同 IP 出现 ≥ 3 个不同用户

ADS_MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "ads_ecommerce",
    "charset": "utf8mb4",
    "autocommit": True,
}

app = FastAPI()

current_dir = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(current_dir, "static")

ECOM_MYSQL_CONFIG = {
    "host": "localhost", "port": 3306, "user": "root", "password": "123456",
    "database": "ecommerce", "charset": "utf8mb4",
}

# ---- API 路由必须在 StaticFiles mount 之前注册 ----
@app.get("/api/top-risky-users")
def _top_risky_users(limit: int = 5):
    try:
        conn = pymysql.connect(**ADS_MYSQL_CONFIG)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT ra.user_id, u.user_name, COUNT(*) as alert_count
                   FROM risk_alerts ra JOIN ecommerce.users u ON ra.user_id = u.user_id
                   GROUP BY ra.user_id, u.user_name
                   ORDER BY alert_count DESC LIMIT %s""", (limit,))
            rows = cur.fetchall()
        conn.close()
        return [{"user_id": r[0], "user_name": r[1], "alert_count": r[2]} for r in rows]
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/alerts/stats")
def _alert_stats():
    try:
        conn = pymysql.connect(**ADS_MYSQL_CONFIG)
        with conn.cursor() as cur:
            cur.execute("SELECT alert_type, COUNT(*) as cnt FROM risk_alerts GROUP BY alert_type ORDER BY cnt DESC")
            by_type = [{"alert_type": r[0], "count": r[1]} for r in cur.fetchall()]
            cur.execute(
                """SELECT DATE_FORMAT(alert_time, '%%H:00') as hour, COUNT(*) as cnt
                   FROM risk_alerts WHERE alert_time >= NOW() - INTERVAL 24 HOUR
                   GROUP BY DATE_FORMAT(alert_time, '%%H:00') ORDER BY hour""")
            by_hour = [{"hour": r[0], "count": r[1]} for r in cur.fetchall()]
        conn.close()
        return {"by_type": by_type, "by_hour": by_hour}
    except Exception as e:
        return {"error": str(e)}

@app.get("/favicon.ico")
async def _favicon():
    from fastapi.responses import Response
    return Response(status_code=204)


@app.get("/", response_class=HTMLResponse)
async def _root():
    with open(os.path.join(static_dir, "index.html"), "r", encoding="utf-8") as f:
        return f.read()

# StaticFiles mount 必须在最后
app.mount("/static", StaticFiles(directory=static_dir), name="static")

class TransactionTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp: int) -> int:
        return int(value[3])


class ParseTransaction(MapFunction):
    def map(self, value: str):
        txn = json.loads(value)
        return (
            txn["user_id"],                          # [0]
            float(txn["amount"]),                    # [1]
            txn.get("category", "unknown"),          # [2]
            int(txn["timestamp"]),                   # [3]
            txn["transaction_id"],                   # [4]
            txn.get("result", "success"),            # [5]
            txn.get("transaction_type", "purchase"), # [6]
            txn.get("ip_address", "0.0.0.0"),        # [7]
            txn.get("product_id", "unknown"),        # [8]
            txn.get("product_name", "unknown"),      # [9]
        )


class GlobalAccumulator(MapFunction):
    def __init__(self):
        self.total = 0.0
        self.count = 0

    def map(self, value):
        self.total += value[1]
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
            user_id = value[0]
            ts = value[3]

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
            user_id = value[0]
            amount = value[1]
            ts = value[3]
            txn_id = value[4]

            self.last_amounts.add(amount)
            amounts = list(self.last_amounts.get() or [])
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
            user_ids.add(e[0])
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


class ProductWindowFunction(ProcessWindowFunction):
    """5 秒滚动窗口按商品聚合交易额和笔数"""
    def open(self, runtime_context):
        pass

    def process(self, product_name: str, context, elements) -> list:
        total = 0.0
        count = 0
        category = ""
        for e in elements:
            total += e[1]
            count += 1
            if not category:
                category = e[2]
        result = {
            "window_start": context.window().start,
            "window_end": context.window().end,
            "product_name": product_name,
            "category": category,
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
            total += e[1]
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
            total += e[1]
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


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_restart_strategy(RestartStrategies.no_restart())
    env.get_config().set_global_job_parameters({
        "python.fn-execution.bundle.size": "1",
        "python.fn-execution.bundle.time": "0",
    })

    env.add_jars(
        "file:///D:/PythonProject/00_Learning/pyflink_project/jars/flink-connector-kafka-3.1.0-1.18.jar",
        "file:///D:/PythonProject/00_Learning/pyflink_project/jars/kafka-clients-3.6.1.jar",
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
        parsed_stream.key_by(lambda v: v[0])
        .process(HighFrequencyDetector(), output_type=Types.STRING())
    )

    increase_alarm_stream = (
        parsed_stream.key_by(lambda v: v[0])
        .process(ContinuousIncreaseDetector(), output_type=Types.STRING())
    )

    large_alarm_stream = (
        parsed_stream.filter(lambda t: t[1] > HIGH_AMOUNT_THRESHOLD)
        .map(LargeAmountAlertSink(), output_type=Types.STRING())
    )

    failed_surge_stream = (
        parsed_stream.filter(lambda t: t[5] == "failed")
        .key_by(lambda x: "global")
        .window(TumblingEventTimeWindows.of(Time.seconds(FAILED_SURGE_WINDOW_SECONDS)))
        .process(FailedTransactionSurgeDetector(), output_type=Types.STRING())
    )

    ip_sharing_stream = (
        parsed_stream.key_by(lambda x: x[7])
        .window(TumblingEventTimeWindows.of(Time.seconds(IP_SHARING_WINDOW_SECONDS)))
        .process(IPSharingDetector(), output_type=Types.STRING())
    )

    all_alarms = large_alarm_stream.union(
        high_freq_alarm_stream, increase_alarm_stream,
        failed_surge_stream, ip_sharing_stream,
    )

    global_window_stream = (
        parsed_stream.key_by(lambda x: "global")
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .process(GlobalWindowFunction(), output_type=Types.STRING())
    )

    category_window_stream = (
        parsed_stream.key_by(lambda x: x[2])
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .process(CategoryWindowFunction(), output_type=Types.STRING())
    )

    product_window_stream = (
        parsed_stream.key_by(lambda x: x[9])
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .process(ProductWindowFunction(), output_type=Types.STRING())
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
    product_window_stream.sink_to(create_kafka_sink(OUTPUT_PRODUCT_TOPIC))

    env.execute("Ecommerce Risk Detection")

if __name__ == "__main__":
    main()