## 基于pyflink数仓模拟工程项目  
### 一、代码架构设计  
1. `src/code/create_tables.py` 负责清空数据库内容，并建立数据表
2. `src/code/data_generator.py` 负责生成实时模拟数据并传入kafka
3. `src/code/class10_ecomm_datastream.py` 负责pyflink计算并将结果传入ADS结果表
4. 

### 二、数据管道设计
1. Python生成器 → MySQL
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