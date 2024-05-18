# 更新后的分布式数据库结构（每个节点存储多个不同数据）
nodes = {
    'node1': {'data1': {'name': 'data1', 'size': 300}, 'data2': {'name': 'data2', 'size': 200}, 'data4': {'name': 'data4', 'size': 250}, 'data6': {'name': 'data6', 'size': 20}, 'location': 'x1'},
    'node2': {'data1': {'name': 'data1', 'size': 200}, 'data3': {'name': 'data3', 'size': 400}, 'data5': {'name': 'data5', 'size': 100}, 'data4': {'name': 'data4', 'size': 80}, 'location': 'x2'},
    'node3': {'data1': {'name': 'data1', 'size': 500}, 'data4': {'name': 'data4', 'size': 100}, 'data7': {'name': 'data7', 'size': 20}, 'data6': {'name': 'data6', 'size': 1000}, 'location': 'x3'},
    # 其他数据存储在不同节点上
}

# 传输时间系数
transmission_times = {
    ('node1', 'node2'): 2,  # 从node1到node2传输时间系数为2
    ('node1', 'node3'): 3,  # 从node1到node3传输时间系数为3
    ('node2', 'node3'): 4,  # 从node2到node3传输时间系数为4
    # 其他节点间传输时间系数
}

# 节点处理一行数据的时间
processing_time_per_row = 1

def calculate_cost(transmission_time, size):
    return transmission_time * size + processing_time_per_row * size

def query_algorithm_DPccp(data_keys):
    BestPlan = {}
    InnerCounter = 0
    OnoLohmanCounter = 0
    
    for data_key in data_keys:
        for node_key, node_data in nodes.items():
            if any(data['name'] == data_key for data_key, data in node_data.items()):
                BestPlan[data_key] = [data for data_key, data in node_data.items() if data['name'] == data_key][0]
    
    for S1 in data_keys:
        for S2 in data_keys:
            if S1 != S2:
                InnerCounter += 1
                OnoLohmanCounter += 1
                p1 = BestPlan.get(S1, {})
                p2 = BestPlan.get(S2, {})
                if isinstance(p1, dict) and isinstance(p2, dict):
                    cost1 = calculate_cost(transmission_times.get((p1.get('location'), p2.get('location')), 0), p1['size'])
                    cost2 = calculate_cost(transmission_times.get((p2.get('location'), p1.get('location')), 0), p2['size'])
                    
                    CurrPlan1 = p1['name'] + p2['name']
                    CurrPlan2 = p2['name'] + p1['name']
                    
                    if cost1 < cost2:
                        BestPlan[S1 + S2] = {'name': CurrPlan1, 'size': p1['size'] + p2['size']}
                    else:
                        BestPlan[S1 + S2] = {'name': CurrPlan2, 'size': p2['size'] + p1['size']}
    
    CsgCmpPairCounter = 2 * OnoLohmanCounter
    return BestPlan, CsgCmpPairCounter

# 执行查询操作并计算查询成本
data_keys = ['data1', 'data2', 'data3']
optimized_plan, CsgCmpPairCounter = query_algorithm_DPccp(data_keys)
total_cost = 0
for key, value in optimized_plan.items():
    total_cost += calculate_cost(1, value['size'])

print(f"最优查询计划为: {optimized_plan}")
print(f"查询成本为: {total_cost}")
print(f"查询算法导致的查询成本差异计数: {CsgCmpPairCounter}")
