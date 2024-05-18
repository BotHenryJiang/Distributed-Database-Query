import heapq

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



def dijkstra(graph, start):
    """
    Dijkstra算法实现，返回从start到所有其他节点的最短路径和成本。
    """
    # 初始化距离表，所有节点的距离都设置为无穷大
    distances = {node: float('infinity') for node in graph}
    distances[start] = 0  # 起点到自己的距离是0
    # 用优先队列维护当前发现的最短路径的候选节点
    pq = [(0, start)]
    while pq:
        current_distance, current_node = heapq.heappop(pq)
        # 如果当前节点的距离已经是最小了，就不需要处理
        if current_distance > distances[current_node]:
            continue
        # 遍历当前节点的邻居
        for neighbor, weight in graph[current_node].items():
            distance = current_distance + weight
            # 如果找到更短的路径，则更新最短路径表，并将其加入优先队列
            if distance < distances[neighbor]:
                distances[neighbor] = distance
                heapq.heappush(pq, (distance, neighbor))
    return distances

# 构建图
graph = {
    'node0': {},
    'node1': {'node0': 1},
    'node2': {'node0': 1.5},
    'node3': {'node0': 2},
    'node0': {'node1': 1, 'node2': 1.5, 'node3': 2},  # 确保图是双向的
    'node1': {'node2': 2, 'node3': 3, 'node0': 1},
    'node2': {'node1': 2, 'node3': 4, 'node0': 1.5},
    'node3': {'node1': 3, 'node2': 4, 'node0': 2},
}

# 使用Dijkstra算法找到从node1, node2, node3到node0的最低成本路径
for start_node in ['node1', 'node2', 'node3']:
    distances = dijkstra(graph, start_node)
    print(f"Minimum cost from {start_node} to node0 is {distances['node0']}")
