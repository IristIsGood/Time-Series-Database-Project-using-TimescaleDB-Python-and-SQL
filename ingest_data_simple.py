# 导入需要的库
import xml.etree.ElementTree as ET  # 用于解析XML文件
import psycopg2  # 用于连接数据库
from datetime import datetime, timedelta  # 用于处理时间

print("="*60)
print("开始导入数据")
print("="*60)

# ========== 第一步：连接到数据库 ==========
print("\n第1步：连接到数据库...")

# 数据库连接信息
database_host = 'localhost'
database_port = 5433
database_user = 'postgres'
database_password = '1998'
database_name = 'traffic_data'

# 建立连接
connection = psycopg2.connect(
    host=database_host,
    port=database_port,
    user=database_user,
    password=database_password,
    database=database_name
)

# 创建游标
cursor = connection.cursor()

print("数据库连接成功！")

# ========== 第二步：打开XML文件 ==========
print("\n第2步：打开SUMO输出文件...")

xml_filename = 'detector_output.xml'

# 解析XML文件
tree = ET.parse(xml_filename)
root = tree.getroot()

print(f"文件 {xml_filename} 打开成功！")

# ========== 第三步：准备插入数据 ==========
print("\n第3步：开始读取和插入数据...")

# SQL插入语句
insert_sql = """
INSERT INTO detector_data 
(time, detector_id, nVehContrib, flow, occupancy, speed, length, nVehEntered)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# 记录当前时间作为起始点
start_time = datetime.now()

# 计数器
total_records = 0
batch_records = []  # 批量插入的缓存

# ========== 第四步：循环读取每一条XML记录 ==========
print("正在处理数据...")

# 找到所有的<interval>标签
intervals = root.findall('interval')

for interval in intervals:
    # 提取数据
    begin_time = float(interval.get('begin'))  # 开始时间（秒）
    detector_id = interval.get('id')  # 检测器ID
    
    # 计算实际时间戳
    # 例如：begin_time=150表示模拟开始后150秒
    actual_time = start_time + timedelta(seconds=begin_time)
    
    # 提取其他数据（如果XML中没有这个字段，就用0）
    nVehContrib = int(interval.get('nVehContrib', 0))
    flow = float(interval.get('flow', 0.0))
    occupancy = float(interval.get('occupancy', 0.0))
    speed = float(interval.get('speed', 0.0))
    length = float(interval.get('length', 0.0))
    nVehEntered = int(interval.get('nVehEntered', 0))
    
    # 把数据放到缓存中
    one_record = (
        actual_time,
        detector_id,
        nVehContrib,
        flow,
        occupancy,
        speed,
        length,
        nVehEntered
    )
    batch_records.append(one_record)
    
    # 每1000条插入一次（批量插入更快）
    if len(batch_records) >= 1000:
        cursor.executemany(insert_sql, batch_records)
        connection.commit()  # 提交到数据库
        
        total_records = total_records + len(batch_records)
        batch_records = []  # 清空缓存
        
        print(f"已插入 {total_records} 条记录...")

# ========== 第五步：插入剩余的数据 ==========
if len(batch_records) > 0:
    cursor.executemany(insert_sql, batch_records)
    connection.commit()
    total_records = total_records + len(batch_records)
    print(f"已插入 {total_records} 条记录...")

# ========== 第六步：显示统计信息 ==========
print("\n" + "="*60)
print("数据导入完成！")
print("="*60)
print(f"\n总共插入了 {total_records} 条记录")

# 查询每个检测器的记录数
print("\n各检测器的数据量：")
cursor.execute("""
    SELECT detector_id, COUNT(*) 
    FROM detector_data 
    GROUP BY detector_id 
    ORDER BY detector_id
""")

results = cursor.fetchall()
for row in results:
    detector_name = row[0]
    count = row[1]
    print(f"  {detector_name}: {count} 条")

# ========== 第七步：关闭连接 ==========
cursor.close()
connection.close()

print("\n数据库连接已关闭")