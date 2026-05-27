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
    "product_aggregated_events",
    "region_aggregated_events",
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

# 内存缓存：省份聚合数据（由 Kafka 消费者线程更新）
import threading
region_cache: dict[str, dict] = {}
region_cache_lock = threading.Lock()


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
        # 缓存省份聚合数据
        if msg.topic == "region_aggregated_events":
            data = msg.value
            prov = data.get("province", "")
            with region_cache_lock:
                region_cache[prov] = {
                    "province": prov,
                    "total_amount": data.get("total_amount", 0),
                    "transaction_count": data.get("transaction_count", 0),
                }


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
def get_alert_history(alert_type: str = None, keyword: str = None, limit: int = 500):
    """查询历史告警记录（ADS 库），支持类型和关键词筛选"""
    ads_config = MYSQL_CONFIG.copy()
    ads_config["database"] = "ads_ecommerce"
    try:
        conn = pymysql.connect(**ads_config)
        with conn.cursor() as cur:
            conditions = []
            params = []
            if alert_type:
                conditions.append("alert_type = %s")
                params.append(alert_type)
            if keyword:
                conditions.append(
                    "(user_id LIKE %s OR transaction_id LIKE %s OR details LIKE %s)")
                kw = f"%{keyword}%"
                params.extend([kw, kw, kw])
            where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
            sql = f"SELECT alert_type, user_id, transaction_id, amount, transaction_count, details, alert_time FROM risk_alerts {where} ORDER BY alert_time DESC LIMIT %s"
            params.append(limit)
            cur.execute(sql, params)
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


@app.get("/api/alerts/stats")
def get_alert_stats():
    """返回告警类型分布和近24小时按小时统计"""
    ads_config = MYSQL_CONFIG.copy()
    ads_config["database"] = "ads_ecommerce"
    try:
        conn = pymysql.connect(**ads_config)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT alert_type, COUNT(*) as cnt
                   FROM risk_alerts
                   GROUP BY alert_type
                   ORDER BY cnt DESC"""
            )
            by_type = [{"alert_type": r[0], "count": r[1]} for r in cur.fetchall()]

            cur.execute(
                """SELECT DATE_FORMAT(alert_time, '%%H:00') as hour, COUNT(*) as cnt
                   FROM risk_alerts
                   WHERE alert_time >= NOW() - INTERVAL 24 HOUR
                   GROUP BY DATE_FORMAT(alert_time, '%%H:00')
                   ORDER BY hour"""
            )
            by_hour = [{"hour": r[0], "count": r[1]} for r in cur.fetchall()]
        conn.close()
        return {"by_type": by_type, "by_hour": by_hour}
    except Exception as e:  # noqa: PIE786
        return {"error": str(e)}


@app.get("/api/region-stats")
def get_region_stats():
    """返回各省份实时聚合数据（从 Kafka 缓存读取）"""
    with region_cache_lock:
        return list(region_cache.values())


@app.get("/api/region-alert-stats")
def get_region_alert_stats():
    """返回各省份风险告警数量（JOIN risk_alerts + users）"""
    ads_config = MYSQL_CONFIG.copy()
    ads_config["database"] = "ads_ecommerce"
    try:
        conn = pymysql.connect(**ads_config)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT u.province, COUNT(*) as alert_count
                   FROM risk_alerts ra
                   JOIN ecommerce.users u ON ra.user_id = u.user_id
                   WHERE u.province IS NOT NULL
                   GROUP BY u.province"""
            )
            rows = cur.fetchall()
        conn.close()
        return [{"province": r[0], "alert_count": r[1]} for r in rows]
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/user-risk-scores")
def get_user_risk_scores(limit: int = 10):
    """时间衰减TOPSIS风险评分
    5项指标按指数衰减累计 → 向量归一化 → 加权 → 正负理想解 → 贴近度
    λ = ln(2)/180000,  T_half=3min=180,000ms
    """
    import math
    import time as _time

    ads_config = MYSQL_CONFIG.copy()
    ads_config["database"] = "ads_ecommerce"
    LAMBDA = math.log(2) / 180_000.0           # 衰减系数 per ms
    WEIGHTS = [0.25, 0.20, 0.25, 0.15, 0.15]   # C1大额 C2高频 C3失败率 C4IP共用 C5递增

    try:
        conn = pymysql.connect(**ads_config)
        now_ms = _time.time() * 1000.0

        # ---- 1. 拉取告警记录（排除 GLOBAL）----
        with conn.cursor() as cur:
            cur.execute(
                """SELECT user_id, alert_type, alert_time
                   FROM risk_alerts
                   WHERE user_id != 'GLOBAL'
                     AND alert_time >= DATE_SUB(NOW(), INTERVAL 10 MINUTE)"""
            )
            alert_rows = cur.fetchall()

            # ---- 2. 拉取各用户最近5分钟失败率 ----
            cur.execute(
                """SELECT user_id,
                          SUM(CASE WHEN result='failed' THEN 1 ELSE 0 END) as fc,
                          COUNT(*) as tc
                   FROM ecommerce.transactions
                   WHERE event_time >= DATE_SUB(NOW(), INTERVAL 5 MINUTE)
                   GROUP BY user_id"""
            )
            fail_rows = cur.fetchall()
        conn.close()

        # ---- 3. 按用户计算5项时间衰减指标 ----
        TYPE_TO_IDX = {
            "LARGE_AMOUNT": 0,
            "HIGH_FREQUENCY": 1,
            "CONTINUOUS_INCREASE": 3,
            "IP_SHARING": 4,
        }
        user_scores = {}  # user_id → [S1, S2, S3, S4, S5]

        for row in alert_rows:
            uid, atype, atime = row
            idx = TYPE_TO_IDX.get(atype)
            if idx is None:
                continue
            dt_ms = now_ms - atime.timestamp() * 1000.0
            if dt_ms < 0:
                dt_ms = 0
            decay = math.exp(-LAMBDA * dt_ms)
            if uid not in user_scores:
                user_scores[uid] = [0.0, 0.0, 0.0, 0.0, 0.0]
            user_scores[uid][idx] += decay

        # C3 失败率
        for uid, fc, tc in fail_rows:
            if tc > 0:
                fail_rate = fc / tc
            else:
                fail_rate = 0.0
            if uid not in user_scores:
                user_scores[uid] = [0.0, 0.0, 0.0, 0.0, 0.0]
            user_scores[uid][2] = fail_rate

        if not user_scores:
            return []

        # ---- 4. TOPSIS ----
        uids = list(user_scores.keys())
        n = len(uids)
        m = 5
        X = [user_scores[uid] for uid in uids]  # n×5 matrix

        # 向量归一化
        col_sqsum = [0.0] * m
        for i in range(n):
            for j in range(m):
                col_sqsum[j] += X[i][j] ** 2
        col_norm = [math.sqrt(s) if s > 0 else 1.0 for s in col_sqsum]

        R = [[0.0]*m for _ in range(n)]  # normalized
        for i in range(n):
            for j in range(m):
                R[i][j] = X[i][j] / col_norm[j]

        # 加权
        V = [[0.0]*m for _ in range(n)]
        for i in range(n):
            for j in range(m):
                V[i][j] = R[i][j] * WEIGHTS[j]

        # 正/负理想解
        A_plus = [max(V[i][j] for i in range(n)) for j in range(m)]
        A_minus = [min(V[i][j] for i in range(n)) for j in range(m)]

        # 距离
        D_plus = [0.0] * n
        D_minus = [0.0] * n
        for i in range(n):
            dp = dm = 0.0
            for j in range(m):
                dp += (V[i][j] - A_plus[j]) ** 2
                dm += (V[i][j] - A_minus[j]) ** 2
            D_plus[i] = math.sqrt(dp)
            D_minus[i] = math.sqrt(dm)

        # 贴近度
        scores = []
        for i in range(n):
            denom = D_plus[i] + D_minus[i]
            closeness = D_minus[i] / denom if denom > 0 else 0.0
            scores.append((uids[i], round(closeness, 4)))

        scores.sort(key=lambda x: x[1], reverse=True)
        top = scores[:limit]

        # ---- 5. 获取用户名称 ----
        ecom_config = MYSQL_CONFIG.copy()
        ecom_config["database"] = "ecommerce"
        conn2 = pymysql.connect(**ecom_config)
        with conn2.cursor() as cur:
            placeholders = ",".join(["%s"] * len(top))
            uid_list = [t[0] for t in top]
            cur.execute(
                f"SELECT user_id, user_name FROM users WHERE user_id IN ({placeholders})",
                uid_list,
            )
            name_map = {r[0]: r[1] for r in cur.fetchall()}
        conn2.close()

        return [
            {"user_id": t[0], "user_name": name_map.get(t[0], t[0]), "risk_score": t[1]}
            for t in top
        ]

    except Exception as e:
        return {"error": str(e)}


@app.get("/api/export/alerts")
def export_alerts(alert_type: str = None, keyword: str = None):
    """导出告警数据为CSV"""
    from fastapi.responses import Response as FastResponse
    ads_config = MYSQL_CONFIG.copy()
    ads_config["database"] = "ads_ecommerce"
    try:
        conn = pymysql.connect(**ads_config)
        with conn.cursor() as cur:
            conditions = []
            params = []
            if alert_type:
                conditions.append("alert_type = %s")
                params.append(alert_type)
            if keyword:
                conditions.append(
                    "(user_id LIKE %s OR transaction_id LIKE %s OR details LIKE %s)")
                kw = f"%{keyword}%"
                params.extend([kw, kw, kw])
            where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
            sql = f"SELECT alert_type, user_id, transaction_id, amount, transaction_count, details, alert_time FROM risk_alerts {where} ORDER BY alert_time DESC LIMIT 5000"
            cur.execute(sql, params)
            rows = cur.fetchall()
        conn.close()

        import io
        output = io.StringIO()
        output.write('﻿')  # BOM for Excel
        output.write("告警类型,用户ID,交易ID,金额,交易次数,详情,告警时间\n")
        for r in rows:
            output.write(",".join([
                str(r[0] or ""),
                str(r[1] or ""),
                str(r[2] or ""),
                str(r[3] or ""),
                str(r[4] or ""),
                f'"{str(r[5] or "").replace(chr(34), chr(34)+chr(34))}"',
                str(r[6] or ""),
            ]) + "\n")
        csv_content = output.getvalue()
        output.close()
        return FastResponse(
            content=csv_content.encode("utf-8-sig"),
            media_type="text/csv; charset=utf-8-sig",
            headers={"Content-Disposition": "attachment; filename=risk_alerts.csv"},
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/export/stats")
def export_stats(window_start: str = None, window_end: str = None):
    """导出窗口统计数据为CSV"""
    from fastapi.responses import Response as FastResponse
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
                       ORDER BY window_start DESC LIMIT 5000""",
                    (window_start, window_end),
                )
            else:
                cur.execute(
                    """SELECT window_start, window_end, category, total_amount, transaction_count
                       FROM transaction_stats ORDER BY window_start DESC LIMIT 5000"""
                )
            rows = cur.fetchall()
        conn.close()

        import io
        output = io.StringIO()
        output.write('﻿')
        output.write("窗口开始,窗口结束,类别,总金额,交易笔数\n")
        for r in rows:
            output.write(",".join([
                str(r[0] or ""),
                str(r[1] or ""),
                str(r[2] or ""),
                str(r[3] or ""),
                str(r[4] or ""),
            ]) + "\n")
        csv_content = output.getvalue()
        output.close()
        return FastResponse(
            content=csv_content.encode("utf-8-sig"),
            media_type="text/csv; charset=utf-8-sig",
            headers={"Content-Disposition": "attachment; filename=transaction_stats.csv"},
        )
    except Exception as e:
        return {"error": str(e)}


@app.get("/favicon.ico")
async def _favicon():
    from fastapi.responses import Response
    return Response(status_code=204)


@app.get("/", response_class=HTMLResponse)
async def root():
    """返回监控大屏首页"""
    html_path = os.path.join(static_dir, "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()