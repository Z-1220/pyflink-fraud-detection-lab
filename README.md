# 电商交易风险实时检测系统

基于 PyFlink + Kafka + FastAPI + ECharts 的实时流处理教学演示项目，模拟电商交易场景下的多维度风险检测与大屏可视化。

## 一、代码架构

### 后端脚本

1. **`src/code/create_tables.py`** — 数据库初始化
   清空并重建 `ecommerce`（DWD 明细层）和 `ads_ecommerce`（ADS 应用层）两个数据库。初始化 300 个用户（按人口权重分配至 31 个省级行政区，共享 30 个 IP 池）、10 个商品类别、每类 6~12 个商品（按类别分价格档位）。

2. **`src/code/data_generator.py`** — 实时交易数据生成器
   持续生成模拟电商交易数据（每 1.5 秒一批，每批 15 条），以概率注入三种异常（高频交易 25%、连续递增 15%、大额交易 25%），双路输出至 MySQL `ecommerce.transactions` 表和 Kafka `transaction_events` 主题。运行 10 分钟后自动停止。

3. **`src/code/class10_ecomm_datastream.py`** — PyFlink 流处理作业
   消费 Kafka 原始交易数据，解析为 12 元组后分叉为 11 条并行处理链路：
   - **5 路风险检测**：大额交易、高频交易（KeyedProcessFunction + Timer）、连续递增（反向扫描算法）、失败飙升（30s 滚动窗口）、IP 共用（60s 滚动窗口）
   - **时间衰减风险评分**：基于指数衰减函数（半衰期 3 分钟）和动态乘数模型（按指标超出阈值程度线性放大）的用户评分
   - **4 路窗口聚合**：全局累计（每条实时输出）、5 秒窗口全量聚合、5 秒窗口按类别聚合、5 秒窗口按省份聚合
   结果写入 MySQL `ads_ecommerce` 库，同时输出至 6 个 Kafka 下游 Topic。

4. **`src/code/class10_server.py`** — WebSocket + REST 服务
   在后台线程中消费 7 个 Kafka Topic，通过 WebSocket 向所有连接的客户端实时推送数据（异常自动重连）。提供 11 个 REST API 端点：类别字典、告警历史（支持类型和关键词筛选）、告警统计、Top 5 排行、Dashboard 快照（合并轮询）、省份实时交易、省份告警、用户风险评分、告警 CSV 导出、统计 CSV 导出、历史窗口统计。新客户端连接时主动推送累计状态快照，确保浏览器刷新后数据不丢失。

5. **`src/code/config_loader.py`** — 共享配置加载
   从 `mysql_config.json` 加载数据库连接参数，支持 `MYSQL_PASSWORD` 环境变量覆盖密码。提供统一的日志初始化函数。

### 前端资源（`src/code/static/`）

6. **`index.html`** — 监控大屏页面
   4 张指标卡 + 用户风险评分 Top 10 柱状图 + 风险用户排行 Top 5 + 告警仪表盘 2x3（6 个 Gauge） + 类别饼图 + 趋势双 Y 轴折线图 + 中国地图热力图（交易金额/风险告警双模式切换，650px） + 可筛选可滚动可导出的实时告警列表。

7. **`app.js`** — 前端业务逻辑（548 行）
   WebSocket 接收 7 路实时数据 + 状态快照，ECharts 渲染全部图表。每 15 秒通过单一 `/api/dashboard/snapshot` 端点合并轮询 4 组历史统计数据。WebSocket 断线采用指数退避策略自动重连（1s→2s→4s→...→30s 上限）。

8. **`style.css`** — 大屏样式（170 行）
   "水墨丹青"中国风暗色主题，CSS Grid + Flexbox 混合布局。六区域垂直流式排列：标题 → 指标卡 → 三列图表行（2:1:1.8）→ 双列图表行 → 中国地图 → 告警列表。页面可滚动适配不同投影分辨率。

### 测试（`test/`）

9. `test/` 目录 — 7 个测试文件，49 条用例
   覆盖风险评分算法（静态严重性 + 状态机）、连续递增检测、JSON 解析、全部 REST 端点（含 SQL 参数断言和错误降级）、数据库查询辅助函数、配置加载。全部 49 条用例在 1.2 秒内完成，不依赖 MySQL/Kafka/Flink 外部服务。

## 二、数据管道

1. Python 生成器 → MySQL（DWD 明细层 `ecommerce` 库）
2. Python 生成器 → Kafka `transaction_events` → Flink 流计算 → MySQL（ADS 应用层 `ads_ecommerce` 库）
3. Flink 流计算 → Kafka（6 个下游 Topic） → `class10_server` WebSocket → 前端 ECharts 大屏
4. 前端 → REST API `/api/dashboard/snapshot` → MySQL + 内存缓存 → 前端（15 秒轮询）

## 三、项目启动流程

| 步骤 | 操作 | 命令 |
|------|------|------|
| 1 | 启动 Zookeeper | `bin\windows\zookeeper-server-start.bat config\zookeeper.properties`（在 Kafka 目录下） |
| 2 | 启动 Kafka | `bin\windows\kafka-server-start.bat config\server.properties`（在 Kafka 目录下） |
| 3 | 初始化数据库 | `uv run .\src\code\create_tables.py` |
| 4 | 启动数据生成器 | `uv run src\code\data_generator.py` |
| 5 | 启动 Flink 作业 | `uv run .\src\code\class10_ecomm_datastream.py` |
| 6 | 启动 Web 服务 | `cd src/code && uv run uvicorn class10_server:app --host 0.0.0.0 --port 8000 --reload` |
| 7 | 打开大屏 | 浏览器访问 [http://localhost:8000](http://localhost:8000) |

## 四、运行测试

```shell
uv run pytest test/ -v
```

## 五、技术文档

详细设计文档见项目根目录 [技术文档.md](技术文档.md)。
