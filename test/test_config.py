"""config_loader 配置加载健壮性测试。"""
import json
import os
from unittest.mock import mock_open, patch

import pytest


class TestMysqlConfig:

    def test_loads_from_json(self):
        """从 JSON 文件加载配置。"""
        cfg = {"host": "db.example.com", "port": 3307,
               "user": "admin", "password": "secret"}
        with patch("builtins.open", mock_open(read_data=json.dumps(cfg))):
            import importlib
            import config_loader
            importlib.reload(config_loader)
            assert config_loader.MYSQL_CONFIG["host"] == "db.example.com"
            assert config_loader.MYSQL_CONFIG["port"] == 3307

    def test_env_var_overrides_password(self):
        """MYSQL_PASSWORD 环境变量覆盖 JSON 中的密码。"""
        cfg = {"host": "localhost", "port": 3306,
               "user": "root", "password": "file_password"}
        with patch("builtins.open", mock_open(read_data=json.dumps(cfg))):
            with patch.dict(os.environ, {"MYSQL_PASSWORD": "env_password"}):
                import importlib
                import config_loader
                importlib.reload(config_loader)
                assert config_loader.MYSQL_CONFIG["password"] == "env_password"

    def test_default_charset_utf8mb4(self):
        """charset 未指定时默认 utf8mb4。"""
        cfg = {"host": "localhost", "port": 3306, "user": "root", "password": "pwd"}
        with patch("builtins.open", mock_open(read_data=json.dumps(cfg))):
            import importlib
            import config_loader
            importlib.reload(config_loader)
            assert config_loader.MYSQL_CONFIG["charset"] == "utf8mb4"
