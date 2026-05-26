# data_generator.py
"""
电商交易风险检测系统 - 实时交易数据生成器 (DWD + Kafka)
职责：
  - 持续生成符合业务规则的真实交易数据
  - 写入 MySQL ecommerce 库 (DWD 层)
  - 发送交易事件到 Kafka topic 'transaction_events'
  - 发送用户信息到 Kafka topic 'user_info' (启动时全量，定期刷新)
"""

import pymysql
import time
import random
import uuid
import json
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ==================== 配置 ====================
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "ecommerce",
    "charset": "utf8mb4",
}

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_TRANSACTION = "transaction_events"
TOPIC_USER_INFO = "user_info"

BATCH_SIZE = 15
SLEEP_INTERVAL = 1.5
HIGH_AMOUNT_THRESHOLD = 5000
# 异常注入概率（每批次）
HIGH_FREQ_CHANCE = 0.25         # 25% 概率注入高频交易
HIGH_FREQ_MIN_TXNS = 3
HIGH_FREQ_MAX_TXNS = 5
INCREASE_SEQ_CHANCE = 0.15      # 15% 概率注入连续递增
INCREASE_SEQ_LENGTH_MIN = 3
INCREASE_SEQ_LENGTH_MAX = 4
LARGE_TXN_CHANCE = 0.25         # 25% 概率注入大额交易
LARGE_TXN_COUNT_MIN = 1
LARGE_TXN_COUNT_MAX = 2

CATEGORIES = [
    "electronics", "clothing", "food", "home", "books",
    "sports", "toys", "health", "automotive", "music"
]
TRANS_TYPES = ["purchase", "refund", "transfer"]
# 结果加权：80% 成功，12% 失败，8% 处理中
RESULT_WEIGHTS = [0.80, 0.12, 0.08]
RESULTS = ["success", "failed", "processing"]


class TransactionGenerator:
    def __init__(self, mysql_conn, kafka_producer):
        self.conn = mysql_conn
        self.kafka_producer = kafka_producer
        self.user_ids = []
        self.product_info = []          # (product_id, category)
        self.product_name_map = {}      # product_id → product_name
        self.all_users = []             # 完整用户信息，用于发送 user_info
        self.user_ip_map = {}           # user_id → ip_address
        self.seq_states = {}
        self.seq_committed = set()
        self.reconnect_attempts = 3
        self._load_metadata()
        self._send_all_user_info()

    def _load_metadata(self):
        """从维表中加载用户和商品信息"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            users = cur.fetchall()
            if not users:
                raise RuntimeError("users 表为空，请先运行 create_tables.py")
            self.user_ids = [row[0] for row in users]

            cur.execute("SELECT product_id, category, product_name FROM products")
            prods = cur.fetchall()
            if not prods:
                raise RuntimeError("products 表为空")
            self.product_info = [(r[0], r[1]) for r in prods]
            self.product_name_map = {r[0]: r[2] for r in prods}

            cur.execute("SELECT user_id, user_name, ip_address, account_type, device FROM users")
            self.all_users = cur.fetchall()
            self.user_ip_map = {row[0]: row[2] for row in self.all_users}
        print(f"✅ 元数据加载完成：{len(self.user_ids)} 个用户，{len(self.product_info)} 个商品")

    def _send_all_user_info(self):
        """将全量用户信息发送到 Kafka"""
        for user in self.all_users:
            msg = {
                "user_id": user[0],
                "user_name": user[1],
                "ip_address": user[2],
                "account_type": user[3],
                "device": user[4]
            }
            try:
                self.kafka_producer.send(TOPIC_USER_INFO, value=msg)
            except Exception as e:
                print(f"⚠️ 发送用户信息失败: {e}")
        self.kafka_producer.flush()
        print(f"✅ 已发送 {len(self.all_users)} 条用户信息到 Kafka")

    def refresh_metadata(self):
        """定期刷新元数据（适应新增用户）"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            self.user_ids = [row[0] for row in cur.fetchall()]
            cur.execute("SELECT user_id, user_name, ip_address, account_type, device FROM users")
            self.all_users = cur.fetchall()
            self.user_ip_map = {row[0]: row[2] for row in self.all_users}
            cur.execute("SELECT product_id, product_name FROM products")
            self.product_name_map = {row[0]: row[1] for row in cur.fetchall()}
        self._send_all_user_info()
        print(f"🔄 元数据已刷新，当前用户数: {len(self.user_ids)}")

    @staticmethod
    def _txn_to_kafka_msg(txn_tuple, ip_address="0.0.0.0", product_name="unknown"):
        """将交易元组转为 Kafka JSON 消息"""
        return {
            "transaction_id": txn_tuple[0],
            "user_id": txn_tuple[1],
            "product_id": txn_tuple[2],
            "category": txn_tuple[3],
            "amount": float(txn_tuple[4]),
            "transaction_type": txn_tuple[5],
            "result": txn_tuple[6],
            "ip_address": ip_address,
            "product_name": product_name,
            "timestamp": int(datetime.strptime(txn_tuple[7], '%Y-%m-%d %H:%M:%S.%f').timestamp() * 1000)
        }

    @staticmethod
    def _build_txn(user_id, prod_id, category, amount, txn_type, result, event_time):
        txn_id = f"txn_{uuid.uuid4().hex[:12]}"
        event_str = event_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        return txn_id, user_id, prod_id, category, amount, txn_type, result, event_str

    # ---------- 各类交易生成逻辑 ----------
    def _generate_normal_transaction(self):
        user_id = random.choice(self.user_ids)
        prod_id, category = random.choice(self.product_info)
        amount = round(random.uniform(10, 3000), 2)
        txn_type = random.choice(TRANS_TYPES)
        result = random.choices(RESULTS, weights=RESULT_WEIGHTS)[0]
        event_time = datetime.now() + timedelta(seconds=random.randint(-5, 5))
        return self._build_txn(user_id, prod_id, category, amount, txn_type, result, event_time)

    def _generate_large_transaction(self):
        user_id = random.choice(self.user_ids)
        prod_id, category = random.choice(self.product_info)
        amount = round(random.uniform(HIGH_AMOUNT_THRESHOLD, 20000), 2)
        event_time = datetime.now() + timedelta(seconds=random.randint(-3, 3))
        return self._build_txn(user_id, prod_id, category, amount, "purchase", "success", event_time)

    def _generate_increase_transaction(self, user_id):
        prod_id, category = random.choice(self.product_info)
        if user_id in self.seq_states:
            last_amt, cnt = self.seq_states[user_id]
            new_amt = round(last_amt * random.uniform(1.1, 1.5), 2)
            cnt += 1
        else:
            new_amt = round(random.uniform(10, 200), 2)
            cnt = 1
        self.seq_states[user_id] = (new_amt, cnt)
        event_time = datetime.now() + timedelta(seconds=random.randint(-2, 2))
        return self._build_txn(user_id, prod_id, category, new_amt, "purchase", "success", event_time)

    def generate_batch(self):
        """生成一批交易（概率注入异常），写入 MySQL 和 Kafka"""
        cursor = self.conn.cursor()
        transactions = []
        anomaly_log = []

        try:
            # 1. 高频交易（概率注入）
            if random.random() < HIGH_FREQ_CHANCE:
                user = random.choice(self.user_ids)
                tx_count = random.randint(HIGH_FREQ_MIN_TXNS, HIGH_FREQ_MAX_TXNS)
                for _ in range(tx_count):
                    txn = self._generate_normal_transaction()
                    txn = list(txn)
                    txn[1] = user
                    txn[0] = f"txn_{uuid.uuid4().hex[:12]}"
                    txn[7] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    transactions.append(tuple(txn))
                anomaly_log.append(f"高频：用户 {user} 产生 {tx_count} 笔")

            # 2. 连续递增（概率注入）
            if random.random() < INCREASE_SEQ_CHANCE:
                inc_users = random.sample(self.user_ids, min(2, len(self.user_ids)))
                for user in inc_users:
                    if user in self.seq_committed:
                        continue
                    seq_len = random.randint(INCREASE_SEQ_LENGTH_MIN, INCREASE_SEQ_LENGTH_MAX)
                    for _ in range(seq_len):
                        txn = self._generate_increase_transaction(user)
                        transactions.append(txn)
                    self.seq_committed.add(user)
                    self.seq_states.pop(user, None)
                    anomaly_log.append(f"连续递增：用户 {user} 产生 {seq_len} 笔")
                    break  # 只取一个用户

            # 3. 大额交易（概率注入）
            if random.random() < LARGE_TXN_CHANCE:
                large_count = random.randint(LARGE_TXN_COUNT_MIN, LARGE_TXN_COUNT_MAX)
                for _ in range(large_count):
                    transactions.append(self._generate_large_transaction())
                anomaly_log.append(f"大额交易：{large_count} 笔")

            # 4. 普通交易填充
            remaining = BATCH_SIZE - len(transactions)
            for _ in range(remaining):
                transactions.append(self._generate_normal_transaction())

            # ---------- 写入 MySQL ----------
            sql = """INSERT INTO transactions 
                     (transaction_id, user_id, product_id, category, amount, 
                      transaction_type, result, event_time)
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
            try:
                cursor.executemany(sql, transactions)
                self.conn.commit()
            except pymysql.IntegrityError:
                self.conn.rollback()
                print("⚠️ 主键冲突，尝试逐条插入...")
                success = 0
                for txn in transactions:
                    try:
                        cursor.execute(sql, txn)
                        self.conn.commit()
                        success += 1
                    except pymysql.IntegrityError:
                        self.conn.rollback()
                print(f"逐条插入完成：成功 {success}/{len(transactions)}")

            # ---------- 发送到 Kafka ----------
            for txn in transactions:
                ip = self.user_ip_map.get(txn[1], "0.0.0.0")
                pname = self.product_name_map.get(txn[2], "unknown")
                kafka_msg = self._txn_to_kafka_msg(txn, ip_address=ip, product_name=pname)
                try:
                    self.kafka_producer.send(TOPIC_TRANSACTION, value=kafka_msg)
                except Exception as e:
                    print(f"⚠️ 发送 Kafka 失败: {e}")
            self.kafka_producer.flush()


        except pymysql.Error as e:
            self.conn.rollback()
            print(f"❌ 数据库错误: {e}")
            self._reconnect()
        except Exception as e:
            self.conn.rollback()
            print(f"❌ 批次处理失败: {e}")
        finally:
            cursor.close()

    def _reconnect(self):
        """数据库重连"""
        for i in range(self.reconnect_attempts):
            try:
                self.conn.ping(reconnect=True)
                print("✅ 数据库重连成功")
                return
            except pymysql.Error:
                time.sleep(2)
        raise ConnectionError("❌ 数据库连接丢失，重试失败")


def main():
    # Kafka Producer（JSON序列化）
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3,
        acks='all'
    )

    # MySQL 连接
    mysql_conn = pymysql.connect(**MYSQL_CONFIG, autocommit=False)

    try:
        gen = TransactionGenerator(mysql_conn, kafka_producer)
    except RuntimeError as e:
        print(f"❌ 初始化失败: {e}")
        return

    print("🚀 数据生成器启动，写入 MySQL 和 Kafka ...")
    refresh_counter = 0
    start_time = time.time()          # 记录启动时间
    RUN_DURATION = 600                # 10 分钟 = 600 秒

    try:
        while True:
            gen.generate_batch()
            refresh_counter += 1
            if refresh_counter % 100 == 0:
                gen.refresh_metadata()
            time.sleep(SLEEP_INTERVAL)

            # 检查是否已运行超过 10 分钟
            if time.time() - start_time > RUN_DURATION:
                print(f"⏰ 已运行 {RUN_DURATION} 秒，生成器自动停止。")
                break
    except KeyboardInterrupt:
        print("\n🛑 生成器已手动停止")
    finally:
        mysql_conn.close()
        kafka_producer.close()

if __name__ == "__main__":
    main()