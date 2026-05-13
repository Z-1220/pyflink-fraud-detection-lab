# create_tables.py
"""
电商交易风险检测系统 - 数据库初始化脚本 (双库版)
- ecommerce:      维度表 + 交易事实表 (ODS/DWD)
- ads_ecommerce:  聚合统计表 + 风险告警表 (ADS)
逻辑：
  - 若目标数据库为空（无任何表） → 创建所有表
  - 若目标数据库非空 → 清空所有表数据（TRUNCATE）
"""

import pymysql
import uuid
import random
from faker import Faker

# ==================== 数据库连接配置 ====================
DB_CONFIG_BASE = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "charset": "utf8mb4",
}

# 两个数据库的完整配置
DB_ECOM = {**DB_CONFIG_BASE, "database": "ecommerce"}
DB_ADS  = {**DB_CONFIG_BASE, "database": "ads_ecommerce"}

# ==================== ecommerce 库的表定义 ====================
ECOM_TABLES_SQL = [
    # 维度表：类别字典
    """CREATE TABLE IF NOT EXISTS categories (
        category    VARCHAR(50) PRIMARY KEY COMMENT '商品类别名称',
        description VARCHAR(200) DEFAULT NULL COMMENT '类别描述'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品类别字典表'""",

    # 维度表：用户
    """CREATE TABLE IF NOT EXISTS users (
        user_id      VARCHAR(50)  PRIMARY KEY COMMENT '用户唯一标识',
        user_name    VARCHAR(100) NOT NULL COMMENT '用户名称',
        ip_address   VARCHAR(45)  DEFAULT NULL COMMENT '客户端IP地址',
        account_type VARCHAR(20)  DEFAULT 'normal' COMMENT '账户类别 (normal/vip)',
        device       VARCHAR(200) DEFAULT NULL COMMENT '登录设备信息',
        created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
        updated_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
        INDEX idx_account_type (account_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户基本信息表'""",

    # 维度表：商品
    """CREATE TABLE IF NOT EXISTS products (
        product_id   VARCHAR(50)   PRIMARY KEY COMMENT '商品唯一标识',
        category     VARCHAR(50)   NOT NULL COMMENT '所属商品类别',
        product_name VARCHAR(200)  DEFAULT NULL COMMENT '商品名称',
        price        DECIMAL(10,2) NOT NULL COMMENT '标准单价',
        status       VARCHAR(20)   DEFAULT 'active' COMMENT '商品状态 (active/inactive)',
        created_at   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
        updated_at   TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
        CONSTRAINT fk_product_category FOREIGN KEY (category) REFERENCES categories(category) ON DELETE RESTRICT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品信息表'""",

    # 事实表：交易明细
    """CREATE TABLE IF NOT EXISTS transactions (
        transaction_id   VARCHAR(50)   PRIMARY KEY COMMENT '交易唯一标识',
        user_id          VARCHAR(50)   NOT NULL COMMENT '用户ID',
        product_id       VARCHAR(50)   DEFAULT NULL COMMENT '商品ID（可为空）',
        category         VARCHAR(50)   NOT NULL COMMENT '商品类别',
        amount           DECIMAL(12,2) NOT NULL COMMENT '交易金额',
        transaction_type VARCHAR(20)   DEFAULT 'purchase' COMMENT '交易类型 (purchase/refund/transfer)',
        result           VARCHAR(20)   DEFAULT 'success' COMMENT '交易结果 (success/failed/processing)',
        event_time       TIMESTAMP(3)  NOT NULL COMMENT '业务事件时间（毫秒精度）',
        process_time     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP COMMENT '数据写入时间',
        INDEX idx_event_time (event_time),
        INDEX idx_user_event (user_id, event_time),
        CONSTRAINT fk_txn_user     FOREIGN KEY (user_id)     REFERENCES users(user_id)       ON DELETE CASCADE,
        CONSTRAINT fk_txn_product  FOREIGN KEY (product_id)  REFERENCES products(product_id) ON DELETE SET NULL,
        CONSTRAINT fk_txn_category FOREIGN KEY (category)    REFERENCES categories(category) ON DELETE RESTRICT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='交易事件明细表'"""
]

# ==================== ads_ecommerce 库的表定义 ====================
ADS_TABLES_SQL = [
    # 聚合表：商品类别滚动窗口统计
    """CREATE TABLE IF NOT EXISTS transaction_stats (
        id                BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
        window_start      TIMESTAMP(3) NOT NULL COMMENT '滚动窗口开始时间',
        window_end        TIMESTAMP(3) NOT NULL COMMENT '滚动窗口结束时间',
        category          VARCHAR(50)  NOT NULL COMMENT '商品类别（逻辑关联 ecommerce.categories.category）',
        total_amount      DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT '窗口内总交易金额',
        transaction_count BIGINT       NOT NULL DEFAULT 0 COMMENT '窗口内交易笔数',
        created_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP COMMENT '统计写入时间',
        UNIQUE KEY uk_window_category (window_start, window_end, category),
        INDEX idx_category (category),
        INDEX idx_window (window_start, window_end)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品类别维度滚动窗口聚合统计表'""",

    # 告警表：风险告警记录
    """CREATE TABLE IF NOT EXISTS risk_alerts (
        alert_id          BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '告警自增ID',
        alert_type        VARCHAR(50)  NOT NULL COMMENT '告警类型',
        user_id           VARCHAR(50)  NOT NULL COMMENT '涉及用户ID（逻辑关联 ecommerce.users.user_id）',
        transaction_id    VARCHAR(50)  DEFAULT NULL COMMENT '触发告警的交易ID（逻辑关联 ecommerce.transactions.transaction_id）',
        amount            DECIMAL(12,2) DEFAULT NULL COMMENT '涉及金额',
        transaction_count INT          DEFAULT NULL COMMENT '高频交易次数',
        window_start      TIMESTAMP(3) DEFAULT NULL COMMENT '高频检测窗口开始',
        window_end        TIMESTAMP(3) DEFAULT NULL COMMENT '高频检测窗口结束',
        details           TEXT         COMMENT '告警详细信息',
        alert_time        TIMESTAMP(3) NOT NULL COMMENT '告警事件时间',
        created_at        TIMESTAMP   DEFAULT CURRENT_TIMESTAMP COMMENT '告警入库时间',
        INDEX idx_alert_type (alert_type),
        INDEX idx_alert_time (alert_time),
        INDEX idx_user (user_id),
        INDEX idx_transaction (transaction_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='风险交易告警记录表'"""
]

# 清空表时的顺序（从叶到根）
TRUNCATE_ORDER_ECOM = ["transactions", "products", "users", "categories"]
TRUNCATE_ORDER_ADS  = ["risk_alerts", "transaction_stats"]


def create_databases_if_not_exists():
    """创建两个数据库（如果不存在）"""
    conn = pymysql.connect(**DB_CONFIG_BASE)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS ecommerce")
            cur.execute("CREATE DATABASE IF NOT EXISTS ads_ecommerce")
        conn.commit()
    finally:
        conn.close()


def is_database_empty(conn):
    """判断当前连接的数据库是否为空（无任何表）"""
    with conn.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        return len(cursor.fetchall()) == 0


def create_tables(conn, tables_sql):
    """在指定连接上执行建表语句"""
    with conn.cursor() as cursor:
        for sql in tables_sql:
            cursor.execute(sql)
        conn.commit()


def truncate_tables(conn, table_list):
    """清空表数据（按顺序，临时关闭外键检查）"""
    with conn.cursor() as cursor:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")
        for table in table_list:
            cursor.execute(f"TRUNCATE TABLE `{table}`")
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()


def init_base_data(conn):
    """初始化基础数据：类别、商品、用户（仅限 ecommerce 库）"""
    fake = Faker()
    CATEGORIES = ["electronics", "clothing", "food", "home", "books",
                  "sports", "toys", "health", "automotive", "music"]

    with conn.cursor() as cur:
        # 1. 插入类别
        for cat in CATEGORIES:
            cur.execute("INSERT IGNORE INTO categories (category) VALUES (%s)", (cat,))

        # 2. 为每个类别生成 6~12 个商品
        for cat in CATEGORIES:
            for _ in range(random.randint(6, 12)):
                pid = f"prod_{uuid.uuid4().hex[:8]}"
                name = fake.word().capitalize() + " " + cat.capitalize()
                price = round(random.uniform(5, 500), 2)
                cur.execute(
                    "INSERT IGNORE INTO products (product_id, category, product_name, price) VALUES (%s,%s,%s,%s)",
                    (pid, cat, name, price)
                )

        # 3. 创建 300 个初始用户
        for _ in range(300):
            uid = f"user_{uuid.uuid4().hex[:8]}"
            uname = fake.user_name()
            ip = fake.ipv4()
            atype = random.choice(["normal", "vip"])
            device = random.choice(["Chrome/Windows", "Safari/Mac", "Chrome/Android", "iOS App"])
            cur.execute(
                "INSERT IGNORE INTO users (user_id, user_name, ip_address, account_type, device) VALUES (%s,%s,%s,%s,%s)",
                (uid, uname, ip, atype, device)
            )
        conn.commit()
    print("✅ 基础数据初始化完成（ecommerce 库）")


def process_database(db_config, db_name, tables_sql, truncate_order, init_func=None):
    """
    统一处理一个数据库：检查是否为空 → 建表/清空 → 可选的基础数据填充
    """
    print(f"\n📦 开始处理数据库: {db_name}")
    conn = pymysql.connect(**db_config)
    try:
        if is_database_empty(conn):
            print(f"ℹ️  [{db_name}] 数据库为空，开始建表...")
            create_tables(conn, tables_sql)
            print(f"✅ [{db_name}] 所有表创建完成")
            # 若有初始化函数则调用（仅 ecommerce 库需要）
            if init_func:
                init_func(conn)
        else:
            print(f"⚠️  [{db_name}] 数据库非空，执行清空操作...")
            truncate_tables(conn, truncate_order)
            print(f"✅ [{db_name}] 清空完成")
            # 清空后如果需要重新填充基础数据
            if init_func:
                print(f"ℹ️  [{db_name}] 重新填充基础数据...")
                init_func(conn)
    finally:
        conn.close()
    print(f"🎉 [{db_name}] 处理完毕")


def main():
    # 1. 确保两个数据库存在
    create_databases_if_not_exists()
    print("✅ 数据库 ecommerce 和 ads_ecommerce 已就绪")

    # 2. 处理 ecommerce 库（含基础数据初始化）
    process_database(
        db_config=DB_ECOM,
        db_name="ecommerce",
        tables_sql=ECOM_TABLES_SQL,
        truncate_order=TRUNCATE_ORDER_ECOM,
        init_func=init_base_data
    )

    # 3. 处理 ads_ecommerce 库（无基础数据）
    process_database(
        db_config=DB_ADS,
        db_name="ads_ecommerce",
        tables_sql=ADS_TABLES_SQL,
        truncate_order=TRUNCATE_ORDER_ADS,
        init_func=None
    )

    print("\n🎉 全部数据库初始化完成")


if __name__ == "__main__":
    main()