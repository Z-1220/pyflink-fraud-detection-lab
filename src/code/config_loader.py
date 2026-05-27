"""共享配置加载：MySQL 连接 + 日志初始化。"""
import json
import logging
import os

# ---- 日志：统一格式，INFO 级别 ----
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

# ---- MySQL 配置，密码优先从环境变量 ----
_config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mysql_config.json")

with open(_config_path, "r", encoding="utf-8") as _f:
    _cfg = json.load(_f)

MYSQL_CONFIG = {
    "host": _cfg["host"],
    "port": _cfg["port"],
    "user": _cfg["user"],
    "password": os.environ.get("MYSQL_PASSWORD", _cfg["password"]),
    "charset": _cfg.get("charset", "utf8mb4"),
}
