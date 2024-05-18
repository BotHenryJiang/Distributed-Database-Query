import simpy
import random
from generated_nodes_data import nodes_data, transfer_cost_coefficients  # 导入生成的数据库数据

class DistributedDatabaseNode:
    def __init__(self, env, node_id, data, transfer_cost_coefficients, processing_cost_coefficient):
        self.env = env
        self.node_id = node_id
        self.data = data  # 数据格式为 {data_id: size}
        self.transfer_cost_coefficients = transfer_cost_coefficients
        self.processing_cost_coefficient = processing_cost_coefficient

    def query(self, data_id, requesting_node):
        yield self.env.timeout(random.randint(1, 3))  # 模拟查询操作需要随机时间
        data_size = self.data.get(data_id, None)
        if data_size is not None:
            # 计算成本
            transfer_cost = self.transfer_cost_coefficients[requesting_node][self.node_id] * data_size if requesting_node != self.node_id else 0
            processing_cost = self.processing_cost_coefficient * data_size
            total_cost = transfer_cost + processing_cost
            return (f"Node {self.node_id} has data {data_id} with size {data_size}. Total cost: {total_cost}", total_cost)
        else:
            return (f"Data {data_id} not found in Node {self.node_id}", 0)

def query_algorithm1(env, nodes, data_id):
    print(f'Client querying data {data_id} using Query Algorithm 1 at time {env.now}')
    query_events_algo1 = [env.process(node.query(data_id, requesting_node=i)) for i, node in enumerate(nodes)]
    results_algo1 = yield simpy.AllOf(env, query_events_algo1)
    total_query_cost_algo1 = sum(cost for _, cost in results_algo1.values())
    print(f"Total query cost for data {data_id} (Algorithm 1): {total_query_cost_algo1}")

def query_algorithm2(env, nodes, data_id):
    print(f'Client querying data {data_id} using Query Algorithm 2 at time {env.now}')
    query_events_algo2 = [env.process(node.query(data_id, requesting_node=0)) for node in nodes]
    results_algo2 = yield simpy.AllOf(env, query_events_algo2)
    total_query_cost_algo2 = sum(cost for _, cost in results_algo2.values())
    print(f"Total query cost for data {data_id} (Algorithm 2): {total_query_cost_algo2}")

# 创建SimPy环境
env = simpy.Environment()

# 创建分布式数据库节点
nodes = [DistributedDatabaseNode(env, i, data, transfer_cost_coefficients[i], processing_cost_coefficient=0.01) for i, data in enumerate(nodes_data, start=1)]

# 发起客户端查询
data_id = 'data1'
env.process(query_algorithm1(env, nodes, data_id))
env.process(query_algorithm2(env, nodes, data_id))

# 运行模拟
env.run(until=10)
