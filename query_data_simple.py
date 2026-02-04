# 导入需要的库
import psycopg2
from datetime import datetime, timedelta

print("="*60)
print("交通数据查询程序")
print("="*60)

# ========== 连接数据库 ==========
print("\n连接到数据库...")

connection = psycopg2.connect(
    host='localhost',
    port=5433,
    user='postgres',
    password='1998',
    database='traffic_data'
)

cursor = connection.cursor()
print("连接成功！")

# ========== 启用压缩 ==========
print("\n" + "="*60)
print("启用数据压缩")
print("="*60)

print("\n启用压缩功能...")
cursor.execute("""
ALTER TABLE detector_data SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'detector_id',
  timescaledb.compress_orderby = 'time DESC'
)
""")
connection.commit()
print("✓ 压缩已启用")

print("\n设置压缩策略...")
cursor.execute("SELECT add_compression_policy('detector_data', INTERVAL '7 days')")
connection.commit()
print("✓ 7天后自动压缩")

print("\n压缩现有数据...")
cursor.execute("SELECT compress_chunk(i) FROM show_chunks('detector_data') i")
connection.commit()
print("✓ 压缩完成")



# ========================================================
# 查询1：范围查询 - 查找最近4小时的最大流量
# ========================================================
print("\n" + "="*60)
print("查询1：查找detector_1最近4小时的最大流量")
print("="*60)

# 第一步：找出最新的时间
cursor.execute("SELECT MAX(time) FROM detector_data")
latest_time = cursor.fetchone()[0]
print(f"最新数据时间: {latest_time}")

# 第二步：计算4小时前的时间
hours_ago_4 = latest_time - timedelta(hours=4)
print(f"4小时前时间: {hours_ago_4}")

# 第三步：查询这个时间范围内的最大流量
sql_query_1 = """
    SELECT time, flow, speed, occupancy
    FROM detector_data
    WHERE detector_id = 'detector_1'
      AND time >= %s
      AND time <= %s
    ORDER BY flow DESC
    LIMIT 1
"""

cursor.execute(sql_query_1, (hours_ago_4, latest_time))
result = cursor.fetchone()

if result:
    max_time = result[0]
    max_flow = result[1]
    max_speed = result[2]
    max_occupancy = result[3]
    
    print("\n最大流量出现在：")
    print(f"  时间: {max_time}")
    print(f"  流量: {max_flow:.2f} 车辆/小时")
    print(f"  速度: {max_speed:.2f} m/s")
    print(f"  占用率: {max_occupancy:.2f}%")
else:
    print("没有找到数据")

# 第四步：计算这段时间的统计数据
sql_stats = """
    SELECT 
        COUNT(*) as records,
        AVG(flow) as avg_flow,
        MAX(flow) as max_flow,
        MIN(flow) as min_flow
    FROM detector_data
    WHERE detector_id = 'detector_1'
      AND time >= %s
      AND time <= %s
"""

cursor.execute(sql_stats, (hours_ago_4, latest_time))
stats = cursor.fetchone()

print("\n这段时间的统计：")
print(f"  数据点数: {stats[0]}")
print(f"  平均流量: {stats[1]:.2f} 车辆/小时")
print(f"  最大流量: {stats[2]:.2f} 车辆/小时")
print(f"  最小流量: {stats[3]:.2f} 车辆/小时")

# ========================================================
# 查询2：插值查询 - 估算某个时间点的速度
# ========================================================
print("\n" + "="*60)
print("查询2：插值查询 - 估算detector_2在某时刻的速度")
print("="*60)

# 第一步：找到detector_2的时间范围
cursor.execute("""
    SELECT MIN(time), MAX(time) 
    FROM detector_data 
    WHERE detector_id = 'detector_2'
""")

min_time, max_time = cursor.fetchone()
print(f"数据时间范围: {min_time} 到 {max_time}")

# 第二步：选择一个目标时间（比如开始后150秒）
target_time = min_time + timedelta(seconds=150)
print(f"目标时间: {target_time}")

# 第三步：找到目标时间前后最近的两个数据点

# 找前一个点（时间 <= 目标时间）
cursor.execute("""
    SELECT time, speed
    FROM detector_data
    WHERE detector_id = 'detector_2' 
      AND time <= %s
    ORDER BY time DESC
    LIMIT 1
""", (target_time,))

before_point = cursor.fetchone()

# 找后一个点（时间 >= 目标时间）
cursor.execute("""
    SELECT time, speed
    FROM detector_data
    WHERE detector_id = 'detector_2' 
      AND time >= %s
    ORDER BY time ASC
    LIMIT 1
""", (target_time,))

after_point = cursor.fetchone()

# 第四步：线性插值计算
if before_point and after_point:
    before_time = before_point[0]
    before_speed = before_point[1]
    after_time = after_point[0]
    after_speed = after_point[1]
    
    print(f"\n前一个数据点:")
    print(f"  时间: {before_time}")
    print(f"  速度: {before_speed:.2f} m/s")
    
    print(f"\n后一个数据点:")
    print(f"  时间: {after_time}")
    print(f"  速度: {after_speed:.2f} m/s")
    
    # 计算时间跨度（秒）
    time_span_seconds = (after_time - before_time).total_seconds()
    
    if time_span_seconds > 0:
        # 计算目标时间到前一个点的时间差
        time_to_before = (target_time - before_time).total_seconds()
        
        # 计算插值比例
        ratio = time_to_before / time_span_seconds
        
        # 线性插值公式: v1 + (v2 - v1) * ratio
        interpolated_speed = before_speed + (after_speed - before_speed) * ratio
        
        print(f"\n插值结果:")
        print(f"  估算速度: {interpolated_speed:.2f} m/s")
        print(f"  插值比例: {ratio:.2%}")
    else:
        print("\n两个点的时间相同，无需插值")

# ========================================================
# 查询3：缺口填充 - 检查数据完整性
# ========================================================
print("\n" + "="*60)
print("查询3：检查detector_3的数据缺口")
print("="*60)

# 使用LAG函数找出时间间隔超过10秒的缺口
sql_gaps = """
    WITH time_gaps AS (
        SELECT 
            time as current_time,
            LAG(time) OVER (ORDER BY time) as prev_time,
            speed as current_speed,
            LAG(speed) OVER (ORDER BY time) as prev_speed,
            EXTRACT(EPOCH FROM (time - LAG(time) OVER (ORDER BY time))) as gap_seconds
        FROM detector_data
        WHERE detector_id = 'detector_3'
    )
    SELECT 
        prev_time,
        current_time,
        prev_speed,
        current_speed,
        gap_seconds
    FROM time_gaps
    WHERE gap_seconds > 10
    ORDER BY gap_seconds DESC
    LIMIT 5
"""

cursor.execute(sql_gaps)
gaps = cursor.fetchall()

if len(gaps) > 0:
    print(f"\n发现 {len(gaps)} 个数据缺口：")
    
    for i in range(len(gaps)):
        gap = gaps[i]
        prev_time = gap[0]
        curr_time = gap[1]
        prev_speed = gap[2]
        curr_speed = gap[3]
        gap_seconds = gap[4]
        
        print(f"\n缺口 {i+1}:")
        print(f"  时间范围: {prev_time} -> {curr_time}")
        print(f"  缺口时长: {gap_seconds:.0f} 秒")
        print(f"  前一速度: {prev_speed:.2f} m/s")
        print(f"  后一速度: {curr_speed:.2f} m/s")
        
        # 建议的填充值（前后平均）
        avg_speed = (prev_speed + curr_speed) / 2
        print(f"  建议填充: {avg_speed:.2f} m/s")
else:
    print("\n未发现超过10秒的数据缺口")
    print("数据完整性良好！")

# 显示数据完整性统计
cursor.execute("""
    SELECT 
        COUNT(*) as total_points,
        MAX(time) - MIN(time) as time_span
    FROM detector_data
    WHERE detector_id = 'detector_3'
""")

stats = cursor.fetchone()
total_points = stats[0]
time_span = stats[1]

print(f"\n数据完整性统计:")
print(f"  总数据点: {total_points}")
print(f"  时间跨度: {time_span}")

# 计算数据密度（点/秒）
if time_span:
    total_seconds = time_span.total_seconds()
    data_rate = total_points / total_seconds
    print(f"  数据密度: {data_rate:.2f} 点/秒")

# ========================================================
# 查询4：聚合查询 - 计算5分钟平均值
# ========================================================
print("\n" + "="*60)
print("查询4：计算5分钟聚合数据")
print("="*60)

# 动态计算5分钟聚合（不使用预先创建的视图）
sql_aggregate = """
    SELECT 
        time_bucket('5 minutes', time) AS bucket,
        detector_id,
        AVG(flow) as avg_flow,
        MAX(flow) as max_flow,
        SUM(nVehEntered) as total_vehicles
    FROM detector_data
    GROUP BY bucket, detector_id
    ORDER BY bucket DESC
    LIMIT 10
"""

cursor.execute(sql_aggregate)
results = cursor.fetchall()

if len(results) > 0:
    print("\n最近10个5分钟时间段的统计：")
    print("\n时间桶                    | 检测器    | 平均流量 | 最大流量 | 总车辆")
    print("-" * 75)
    
    for row in results:
        bucket = row[0]
        detector = row[1]
        avg_flow = row[2]
        max_flow = row[3]
        total_veh = row[4]
        
        print(f"{bucket} | {detector} | {avg_flow:>8.1f} | {max_flow:>8.1f} | {total_veh:>6}")
else:
    print("没有找到聚合数据")

# 计算压缩率
cursor.execute("SELECT COUNT(*) FROM detector_data")
original_count = cursor.fetchone()[0]

cursor.execute("""
    SELECT COUNT(DISTINCT time_bucket('5 minutes', time)) * 
           COUNT(DISTINCT detector_id)
    FROM detector_data
""")
aggregated_count = cursor.fetchone()[0]

if original_count > 0:
    compression_ratio = (1 - aggregated_count / original_count) * 100
    print(f"\n数据压缩:")
    print(f"  原始记录: {original_count:,}")
    print(f"  聚合后: {aggregated_count:,}")
    print(f"  压缩率: {compression_ratio:.1f}%")

# ========== 关闭连接 ==========
cursor.close()
connection.close()

print("\n" + "="*60)
print("所有查询完成！")
print("="*60)