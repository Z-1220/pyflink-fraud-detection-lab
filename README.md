## 基于pyflink数仓模拟工程项目  
### 一、代码架构设计  

#### 后端脚本

1. **`src/code/create_tables.py`** — 数据库初始化  
   清空并重建 `ecommerce`（DWD 层）和 `ads_ecommerce`（ADS 层）两个数据库。初始化 300 个用户（共享 30 个 IP 池）、10 个商品类别、每类 6~12 个商品（按类别分价格档位）。

2. **`src/code/data_generator.py`** — 实时交易数据生成器  
   持续生成模拟电商交易数据，以概率注入异常（高频、递增、大额），写入 MySQL `ecommerce.transactions` 表同时发送至 Kafka `transaction_events`。全量用户信息发送至 Kafka `user_info`。10 分钟后自动停止。

3. **`src/code/class10_ecomm_datastream.py`** — PyFlink 流处理作业  
   消费 Kafka 交易数据，完成 5 路风险检测 + 3 路统计分析：
   - **检测**：大额交易 `LARGE_AMOUNT`、高频交易 `HIGH_FREQUENCY`、连续递增 `CONTINUOUS_INCREASE`、失败飙升 `FAILED_SURGE`、IP 共用 `IP_SHARING`
   - **统计**：全局累计 `GlobalAccumulator`、5 秒窗口全量 `GlobalWindowFunction`、5 秒窗口按类别 `CategoryWindowFunction`  
   结果写入 MySQL `ads_ecommerce` 库，同时推送至 4 个 Kafka 下游 topic。内嵌 FastAPI 提供 API 端点和静态资源服务。

4. **`src/code/class10_server.py`** — WebSocket + REST 服务  
   消费 4 个 Kafka 输出 topic，通过 WebSocket 实时推送至前端大屏。提供 5 个 REST API 端点：`/categories`、`/stats/history`、`/alerts/history`、`/alerts/stats`、`/top-risky-users`。挂载静态资源目录。

#### 前端资源（`src/code/static/`）

5. **`index.html`** — 监控大屏页面  
   4 张指标卡 + 类别饼图 + 趋势双 Y 轴折线图 + 风险用户排行 + 可筛选可滚动的实时告警列表。

6. **`app.js`** — 前端逻辑  
   WebSocket 接收 4 路实时数据，ECharts 渲染图表，轮询 REST API 获取历史告警统计和用户排行，筛选按钮调用后端搜索。

7. **`style.css`** — 大屏样式  
   暗色主题 Flex 布局，视口撑满，告警区域 `flex: 1` 自动填充剩余高度并独立滚动。

### 二、数据管道设计
1. Python生成器 → MySQL（DWD层）
2. Python生成器 → Kafka → PyFlink DWS计算 → MySQL(ADS结果表) 
3. PyFlink DWS计算 → Kafka→ 前端可视化
### 三、项目启动流程
1. 启动zookeeper服务：
   ```shell
    deactivate
    cd D:\Tools\kafka_2.12-2.7.0
    bin\windows\zookeeper-server-start.bat config\zookeeper.properties
   ```
2. 启动Kafka服务：
   ```shell
    deactivate
    cd D:\Tools\kafka_2.12-2.7.0
    bin\windows\kafka-server-start.bat config\server.properties
   ```
3. 初始化数据库：
   ```
    uv run .\src\code\create_tables.py
   ```
4. 模拟生成电商交易数据，并完成送出数据：传入Kafka管道，同时写入 MySQL ecommerce 库 (DWD 层)
   ```shell
    uv run src\code\data_generator.py
   ```
5. PyFlink消费Kafka的数据并进行计算分析，将结果传入Kafka管道，同时发送至ADS层数据库保存。
   ```shell
    uv run .\src\code\class10_ecomm_datastream.py
   ```
6. 前端服务器启动：
   ```shell
    cd src/code
    uv run uvicorn class10_server:app --host 0.0.0.0 --port 8000 --reload
   ```
7. 进入浏览器地址查看大屏：[http://localhost:8000](http://localhost:8000)