import random

# 生成10个节点的分布式数据库
num_nodes = 10
nodes_data = []
transfer_cost_coefficients = [[random.uniform(0.05, 0.25) for _ in range(num_nodes)] for _ in range(num_nodes)]

# 生成1000条数据ID和大小
data_ids = [f'data{i+1}' for i in range(1000)]
data_sizes = [random.randint(300, 1500) for _ in range(1000)]

# 随机分片存储数据到10个节点
for _ in range(num_nodes):
    node_data = {}
    for data_id, data_size in zip(data_ids, data_sizes):
        if random.random() < 0.5:  # 随机决定是否在该节点存储该数据
            node_data[data_id] = data_size
    nodes_data.append(node_data)

# 确保每个节点都有对应的传输成本系数
while len(transfer_cost_coefficients) < num_nodes:
    transfer_cost_coefficients.append([random.uniform(0.05, 0.25) for _ in range(num_nodes)])

# 存储生成的数据库数据到文件
with open('generated_nodes_data.py', 'w') as f:
    f.write(f"nodes_data = {nodes_data}\n")
    f.write(f"transfer_cost_coefficients = {transfer_cost_coefficients}")
