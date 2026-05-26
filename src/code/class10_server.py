# class10_server.py
"""
电商交易风险检测系统 - 实时数据推送服务
消费 Kafka 输出主题，通过 WebSocket 向前端推送数据。
静态资源（HTML/CSS/JS）存放于 static/ 目录。
使用 lifespan 事件处理器替代已弃用的 on_event。
"""

import json
import asyncio
import os
from contextlib import asynccontextmanager
from threading import Thread
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer
import pymysql

# ==================== 配置 ====================
KAFKA_BOOTSTRAP = "localhost:9092"
TOPICS = [
    "alarm_events",
    "total_amount_and_count_events",
    "window_count_and_amount_events",
    "category_aggregated_events",
]
GROUP_ID = "websocket-server"

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "ecommerce",
    "charset": "utf8mb4",
}

# ==================== 全局变量 ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(BASE_DIR, "static")
os.makedirs(static_dir, exist_ok=True)

clients: set[WebSocket] = set()


async def broadcast(message: str):
    """向所有连接的客户端发送消息"""
    disconnected = []
    for ws in clients:
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        clients.discard(ws)


def kafka_consumer_thread(loop: asyncio.AbstractEventLoop):
    """后台线程：消费 Kafka 并调度广播到主事件循环"""
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    print(f"📡 Kafka 消费者启动，监听主题: {TOPICS}")
    for msg in consumer:
        payload = {"topic": msg.topic, "data": msg.value}
        asyncio.run_coroutine_threadsafe(broadcast(json.dumps(payload)), loop)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # startup
    loop = asyncio.get_running_loop()
    Thread(target=kafka_consumer_thread, args=(loop,), daemon=True).start()
    print("✅ WebSocket 服务已就绪")
    yield
    # shutdown
    print("🛑 服务正在关闭")


app = FastAPI(lifespan=lifespan)

# 挂载静态文件
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    print(f"🔗 新客户端连接，当前连接数: {len(clients)}")
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        print(f"❌ 客户端断开，当前连接数: {len(clients) - 1}")
    finally:
        clients.discard(websocket)


# ==================== REST API ====================
@app.get("/api/categories")
def get_categories():
    """返回商品类别列表"""
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        with conn.cursor() as cur:
            cur.execute("SELECT category, description FROM categories")
            rows = cur.fetchall()
        conn.close()
        return [{"category": r[0], "description": r[1]} for r in rows]
    except Exception as e:  # noqa: PIE786
        return {"error": str(e)}


@app.get("/api/stats/history")
def get_history_stats(window_start: str = None, window_end: str = None):
    """查询历史窗口统计（ADS 库）"""
    ads_config = MYSQL_CONFIG.copy()
    ads_config["database"] = "ads_ecommerce"
    try:
        conn = pymysql.connect(**ads_config)
        with conn.cursor() as cur:
            if window_start and window_end:
                cur.execute(
                    """SELECT window_start, window_end, category, total_amount, transaction_count
                       FROM transaction_stats
                       WHERE window_start >= %s AND window_end <= %s
                       ORDER BY window_start DESC LIMIT 200""",
                    (window_start, window_end),
                )
            else:
                cur.execute(
                    """SELECT window_start, window_end, category, total_amount, transaction_count
                       FROM transaction_stats ORDER BY window_start DESC LIMIT 200"""
                )
            rows = cur.fetchall()
        conn.close()
        return [
            {
                "window_start": r[0].isoformat() if r[0] else None,
                "window_end": r[1].isoformat() if r[1] else None,
                "category": r[2],
                "total_amount": float(r[3]),
                "transaction_count": r[4],
            }
            for r in rows
        ]
    except Exception as e:  # noqa: PIE786
        return {"error": str(e)}


@app.get("/api/alerts/history")
def get_alert_history(alert_type: str = None, limit: int = 100):
    """查询历史告警记录（ADS 库）"""
    ads_config = MYSQL_CONFIG.copy()
    ads_config["database"] = "ads_ecommerce"
    try:
        conn = pymysql.connect(**ads_config)
        with conn.cursor() as cur:
            if alert_type:
                cur.execute(
                    """SELECT alert_type, user_id, transaction_id, amount, transaction_count,
                              details, alert_time
                       FROM risk_alerts WHERE alert_type = %s
                       ORDER BY alert_time DESC LIMIT %s""",
                    (alert_type, limit),
                )
            else:
                cur.execute(
                    """SELECT alert_type, user_id, transaction_id, amount, transaction_count,
                              details, alert_time
                       FROM risk_alerts ORDER BY alert_time DESC LIMIT %s""",
                    (limit,),
                )
            rows = cur.fetchall()
        conn.close()
        return [
            {
                "alert_type": r[0],
                "user_id": r[1],
                "transaction_id": r[2],
                "amount": float(r[3]) if r[3] else None,
                "transaction_count": r[4],
                "details": r[5],
                "alert_time": r[6].isoformat() if r[6] else None,
            }
            for r in rows
        ]
    except Exception as e:  # noqa: PIE786
        return {"error": str(e)}


@app.get("/api/top-risky-users")
def get_top_risky_users(limit: int = 5):
    """返回告警次数最多的用户排名（跨库 JOIN，fallback 分步查询）"""
    ads_config = MYSQL_CONFIG.copy()
    ads_config["database"] = "ads_ecommerce"
    try:
        conn = pymysql.connect(**ads_config)
        with conn.cursor() as cur:
            try:
                # 尝试跨库 JOIN
                cur.execute(
                    """SELECT ra.user_id, u.user_name, COUNT(*) as alert_count
                       FROM risk_alerts ra
                       JOIN ecommerce.users u ON ra.user_id = u.user_id
                       GROUP BY ra.user_id, u.user_name
                       ORDER BY alert_count DESC
                       LIMIT %s""",
                    (limit,),
                )
                rows = cur.fetchall()
            except pymysql.Error:
                # fallback: 分步查询再合并
                cur.execute(
                    """SELECT user_id, COUNT(*) as alert_count
                       FROM risk_alerts
                       GROUP BY user_id
                       ORDER BY alert_count DESC
                       LIMIT %s""",
                    (limit,),
                )
                alert_rows = cur.fetchall()
                if not alert_rows:
                    rows = []
                else:
                    user_ids = [r[0] for r in alert_rows]
                    alert_map = {r[0]: r[1] for r in alert_rows}
                    conn_ecom = pymysql.connect(**MYSQL_CONFIG)
                    try:
                        with conn_ecom.cursor() as cur2:
                            placeholders = ",".join(["%s"] * len(user_ids))
                            cur2.execute(
                                f"SELECT user_id, user_name FROM users WHERE user_id IN ({placeholders})",
                                user_ids,
                            )
                            name_map = {r[0]: r[1] for r in cur2.fetchall()}
                        rows = [
                            (uid, name_map.get(uid, "unknown"), cnt)
                            for uid, cnt in alert_rows
                        ]
                    finally:
                        conn_ecom.close()
        conn.close()
        return [
            {"user_id": r[0], "user_name": r[1], "alert_count": r[2]}
            for r in rows
        ]
    except Exception as e:  # noqa: PIE786
        return {"error": str(e)}


@app.get("/", response_class=HTMLResponse)
async def root():
    """返回监控大屏首页"""
    html_path = os.path.join(static_dir, "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()