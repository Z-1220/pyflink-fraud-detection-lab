# data_generator.py
"""
电商交易风险检测系统 - 实时交易数据生成器 (DWD 层)
作用：
  持续生成符合业务规则的真实交易数据，直接写入 ecommerce 库（DWD 明细层）。
  前提：ecommerce 库中的 categories、products、users 维表已由 create_tables.py 初始化。
  不依赖 Kafka，不涉及 ads_ecommerce 库。
"""

import pymysql
import time
import random
import uuid
from datetime import datetime, timedelta

# ==================== 配置 ====================
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "ecommerce",
    "charset": "utf8mb4",
}

BATCH_SIZE = 15
SLEEP_INTERVAL = 0.8
ANOMALY_RATE = 0.15               # 未用，可保留用于控制异常注入
HIGH_AMOUNT_THRESHOLD = 5000
HIGH_FREQ_USER_COUNT = 2
HIGH_FREQ_MIN_TXNS = 3
HIGH_FREQ_MAX_TXNS = 6

CATEGORIES = [                     # 仅用于参考，实际从 products 维表获取
    "electronics", "clothing", "food", "home", "books",
    "sports", "toys", "health", "automotive", "music"
]
TRANS_TYPES = ["purchase", "refund", "transfer"]
RESULTS = ["success", "failed", "processing"]


class TransactionGenerator:
    def __init__(self, conn):
        self.conn = conn
        self.user_ids = []
        self.product_info = []      # list of (product_id, category)
        self.seq_states = {}        # 用于连续递增，key: user_id, value: (last_amount, count)
        self.seq_committed = set()  # 已完成递增序列的用户（本轮不再重复）
        self.reconnect_attempts = 3
        self._load_metadata()

    def _load_metadata(self):
        """从 DWD 维表中加载用户 ID 和商品信息，若为空则报错退出"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            users = cur.fetchall()
            if not users:
                raise RuntimeError("users 表为空，请先运行 create_tables.py 初始化基础数据")
            self.user_ids = [row[0] for row in users]

            cur.execute("SELECT product_id, category FROM products")
            prods = cur.fetchall()
            if not prods:
                raise RuntimeError("products 表为空，请先运行 create_tables.py 初始化基础数据")
            self.product_info = prods
        print(f"✅ 元数据加载完成：{len(self.user_ids)} 个用户，{len(self.product_info)} 个商品")

    def refresh_metadata(self):
        """定期刷新（例如新增用户后）"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users")
            self.user_ids = [row[0] for row in cur.fetchall()]
        print(f"🔄 元数据已刷新，当前用户数: {len(self.user_ids)}")

    # ---- 交易构建 ----
    def _build_txn(self, user_id, prod_id, category, amount, txn_type, result, event_time):
        txn_id = f"txn_{uuid.uuid4().hex[:12]}"
        # event_time 使用传递的 datetime，格式化到毫秒
        event_str = event_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        return (
            txn_id, user_id, prod_id, category, amount,
            txn_type, result, event_str
        )

    # ---- 各类交易生成逻辑 ----
    def _generate_normal_transaction(self):
        user_id = random.choice(self.user_ids)
        prod_id, category = random.choice(self.product_info)
        amount = round(random.uniform(10, 3000), 2)
        txn_type = random.choice(TRANS_TYPES)
        result = random.choice(RESULTS)
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
        """生成一个批次的交易数据并写入 DWD 层（transactions 表）"""
        cursor = self.conn.cursor()
        transactions = []
        anomaly_log = []

        try:
            # 1. 高频交易异常模拟
            high_freq_users = random.sample(self.user_ids, min(HIGH_FREQ_USER_COUNT, len(self.user_ids)))
            for user in high_freq_users:
                tx_count = random.randint(HIGH_FREQ_MIN_TXNS, HIGH_FREQ_MAX_TXNS)
                for _ in range(tx_count):
                    txn = self._generate_normal_transaction()
                    # 强制覆盖为用户和时间，使模拟更自然
                    txn = list(txn)
                    txn[1] = user
                    txn[0] = f"txn_{uuid.uuid4().hex[:12]}"
                    txn[7] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    transactions.append(tuple(txn))
                anomaly_log.append(f"高频交易：用户 {user} 产生 {tx_count} 笔")

            # 2. 连续递增交易异常模拟
            inc_users = random.sample(self.user_ids, min(2, len(self.user_ids)))
            for user in inc_users:
                if user in self.seq_committed:
                    continue
                seq_len = random.randint(3, 5)
                for _ in range(seq_len):
                    txn = self._generate_increase_transaction(user)
                    transactions.append(txn)
                self.seq_committed.add(user)
                self.seq_states.pop(user, None)
                anomaly_log.append(f"连续递增：用户 {user} 产生 {seq_len} 笔递增交易")

            # 3. 大额交易异常模拟
            large_count = random.randint(2, 4)
            for _ in range(large_count):
                transactions.append(self._generate_large_transaction())
            anomaly_log.append(f"大额交易：{large_count} 笔")

            # 4. 填充普通交易至批次大小
            remaining = BATCH_SIZE - len(transactions)
            for _ in range(remaining):
                transactions.append(self._generate_normal_transaction())

            # 5. 批量写入 DWD 事务表
            sql = """INSERT INTO transactions 
                     (transaction_id, user_id, product_id, category, amount, 
                      transaction_type, result, event_time)
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
            try:
                cursor.executemany(sql, transactions)
                self.conn.commit()
            except pymysql.IntegrityError as e:
                self.conn.rollback()
                print(f"⚠️ 主键冲突或完整性错误: {e}，尝试逐条插入...")
                success = 0
                for txn in transactions:
                    try:
                        cursor.execute(sql, txn)
                        self.conn.commit()
                        success += 1
                    except pymysql.IntegrityError:
                        self.conn.rollback()
                        # 忽略重复或引用错误，继续
                print(f"逐条插入完成：成功 {success}/{len(transactions)}")

            # 日志输出
            print(f"[{datetime.now().strftime('%H:%M:%S')}] DWD 写入完成，异常摘要: {', '.join(anomaly_log)}")

        except pymysql.Error as e:
            self.conn.rollback()
            print(f"❌ 数据库错误: {e}")
            # 可选：尝试重连
            self._reconnect()
        except Exception as e:
            self.conn.rollback()
            print(f"❌ 批次插入失败: {e}")
        finally:
            cursor.close()

    def _reconnect(self):
        """尝试重新连接数据库"""
        for i in range(self.reconnect_attempts):
            try:
                self.conn.ping(reconnect=True)
                print("✅ 数据库重连成功")
                return
            except pymysql.Error:
                time.sleep(2)
        raise ConnectionError("❌ 数据库连接丢失，重试失败")


def main():
    conn = pymysql.connect(**MYSQL_CONFIG, autocommit=False)
    try:
        gen = TransactionGenerator(conn)
    except RuntimeError as e:
        print(f"❌ 初始化失败: {e}")
        return

    print("🚀 数据生成器启动，持续向 DWD 层（ecommerce 库）写入交易流...")
    refresh_counter = 0
    try:
        while True:
            gen.generate_batch()
            refresh_counter += 1
            if refresh_counter % 100 == 0:
                gen.refresh_metadata()
            time.sleep(SLEEP_INTERVAL)
    except KeyboardInterrupt:
        print("\n🛑 生成器已手动停止")
    except ConnectionError as ce:
        print(ce)
    finally:
        conn.close()


if __name__ == "__main__":
    main()