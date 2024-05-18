nodes = {
    'node0': {},  # 假设node0没有存储数据，仅作为数据汇总点
    'node1': {'data1': {'name': 'data1', 'size': 300}, 'data2': {'name': 'data2', 'size': 200}, 'data4': {'name': 'data4', 'size': 250}, 'data6': {'name': 'data6', 'size': 20}, 'location': 'x1'},
    'node2': {'data1': {'name': 'data1', 'size': 200}, 'data3': {'name': 'data3', 'size': 400}, 'data5': {'name': 'data5', 'size': 100}, 'data4': {'name': 'data4', 'size': 80}, 'location': 'x2'},
    'node3': {'data1': {'name': 'data1', 'size': 500}, 'data4': {'name': 'data4', 'size': 100}, 'data7': {'name': 'data7', 'size': 20}, 'data6': {'name': 'data6', 'size': 1000}, 'location': 'x3'},
}

transmission_times = {
    ('node0', 'node1'): 1, ('node1', 'node0'): 1,
    ('node0', 'node2'): 1.5, ('node2', 'node0'): 1.5,
    ('node0', 'node3'): 2, ('node3', 'node0'): 2,
    ('node1', 'node2'): 2, ('node2', 'node1'): 2,
    ('node1', 'node3'): 3, ('node3', 'node1'): 3,
    ('node2', 'node3'): 4, ('node3', 'node2'): 4,
}

processing_time_per_row = 1  # 节点处理一行数据的时间
#local_search_cost = 100  # 节点传输至查找指令发出本地的成本

def calculate_cost(node_from, node_to, data_size):
    """
    计算从一个节点到另一个节点传输数据的成本，并返回成本及传输路径。
    """
    transmission_time = transmission_times.get((node_from, node_to), 0)  # 获取传输时间系数
    transfer_cost = transmission_time * data_size  # 计算传输成本
    processing_cost = processing_time_per_row * data_size  # 计算处理成本
    total_cost = transfer_cost + processing_cost  # 返回总成本
    return total_cost, f"{node_from} -> {node_to}"


def find_data(data_name):
    """
    查找数据，并计算查找成本及传输方案。
    """
    locations = []  # 存放数据所在节点
    total_size = 0  # 数据总量
    transfer_details = []  # 存储传输细节
    total_cost = 0
    for node, data in nodes.items():
        if data_name in data:
            size = data[data_name]['size']
            locations.append((node, size))
            total_size += size
            if node != 'node0':  # 如果数据不在node0上，计算传输成本
                cost, path = calculate_cost(node, 'node0', size)
                total_cost += cost
                transfer_details.append((path, cost))
    
    if not locations:
        return f"Data {data_name} not found", 0, 0, []
    
    return f"Found {data_name} with total size {total_size}", locations, total_cost, transfer_details

# 示例查找
data_name = 'data5'  # 想要查找的数据名称
result, locations, total_cost, transfer_details = find_data(data_name)
print(result)
print("Locations and sizes:", locations)
print("Total cost:", total_cost)
print("Transfer details:")
for detail in transfer_details:
    print(detail)
