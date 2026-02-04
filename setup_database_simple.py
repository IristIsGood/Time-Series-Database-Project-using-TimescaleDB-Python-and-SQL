# 导入需要的库
import psycopg2  # 用于连接PostgreSQL数据库
import time      # 用于时间延迟

# 数据库连接信息
database_host = 'localhost'
database_port = 5433
database_user = 'postgres'
database_password = '1998'

print("="*60)
print("开始设置数据库")
print("="*60)

# ========== 第一步：连接到PostgreSQL ==========
print("\n第1步：连接到PostgreSQL数据库...")

# 先连接到默认的postgres数据库
connection = psycopg2.connect(
    host=database_host,
    port=database_port,
    user=database_user,
    password=database_password
)

# 设置自动提交模式（创建数据库需要）
connection.autocommit = True

# 创建游标（用来执行SQL命令）
cursor = connection.cursor()

print("连接成功！")

# ========== 第二步：创建traffic_data数据库 ==========
print("\n第2步：创建traffic_data数据库...")

# 先删除旧数据库（如果存在）
try:
    cursor.execute("DROP DATABASE IF EXISTS traffic_data")
    print("删除了旧数据库")
except:
    print("没有旧数据库需要删除")

# 创建新数据库
cursor.execute("CREATE DATABASE traffic_data")
print("创建新数据库成功！")

# 关闭连接
cursor.close()
connection.close()

# 等待一下
time.sleep(2)

# ========== 第三步：连接到traffic_data数据库 ==========
print("\n第3步：连接到traffic_data数据库...")

connection = psycopg2.connect(
    host=database_host,
    port=database_port,
    user=database_user,
    password=database_password,
    database='traffic_data'  # 这次连接到我们创建的数据库
)

cursor = connection.cursor()
print("连接成功！")

# ========== 第四步：启用TimescaleDB扩展 ==========
print("\n第4步：启用TimescaleDB扩展...")

cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
connection.commit()
print("TimescaleDB扩展已启用！")

# ========== 第五步：创建数据表 ==========
print("\n第5步：创建detector_data表...")

# SQL命令：创建表
create_table_sql = """
CREATE TABLE IF NOT EXISTS detector_data (
    time TIMESTAMPTZ NOT NULL,
    detector_id VARCHAR(50) NOT NULL,
    nVehContrib INTEGER,
    flow DOUBLE PRECISION,
    occupancy DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    length DOUBLE PRECISION,
    nVehEntered INTEGER
)
"""

cursor.execute(create_table_sql)
connection.commit()
print("数据表创建成功！")

# ========== 第六步：把表转换成超表 ==========
print("\n第6步：转换为时序超表...")

# 这是TimescaleDB的核心功能
# 超表会按时间自动分区，提高查询速度
convert_to_hypertable_sql = """
SELECT create_hypertable(
    'detector_data', 
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
)
"""

cursor.execute(convert_to_hypertable_sql)
connection.commit()
print("超表转换成功！")

# ========== 第七步：创建索引 ==========
print("\n第7步：创建索引...")

# 索引可以加速查询
create_index_sql = """
CREATE INDEX IF NOT EXISTS idx_detector_time 
ON detector_data (detector_id, time DESC)
"""

cursor.execute(create_index_sql)
connection.commit()
print("索引创建成功！")

# ========== 完成 ==========
cursor.close()
connection.close()

print("\n" + "="*60)
print("数据库设置完成！")
print("="*60)