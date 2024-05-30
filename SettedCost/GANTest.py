import time
import random
import pandas as pd
import dask.dataframe as dd
import re
from database_setup import ShardManager

import torch
import torch.nn as nn
import torch.optim as optim


# 数据库节点
class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.data = pd.DataFrame(columns=['id', 'data', 'category'])
        self.ddf = dd.from_pandas(self.data, npartitions=1)
        # 模拟传输时间（单位：秒），按节点编号递增
        self.transmission_time_per_unit = 0.001 + 0.001 * node_id

    def load_from_csv(self, filename):
        self.ddf = dd.read_csv(filename)
        self.data = self.ddf.compute()

    def save_to_csv(self, filename):
        self.ddf.to_csv(filename, index=False, single_file=True)

    def query(self, subquery):
        # 模拟本地执行时间（单位：秒）
        subqueries = subquery.split(" and ")
        data_subset = self.data
        total_execution_time = 0

        for sq in subqueries:
            initial_size = len(data_subset)
            if 'id' in sq:
                complexity_factor = 0.001  # 简单查询的复杂度较低
            elif 'category' in sq:
                complexity_factor = 0.003
            else:
                complexity_factor = 0.005  # 复杂查询的复杂度较高
            
            # 执行子查询
            data_subset = data_subset.query(sq)
            filtered_size = len(data_subset)
            
            # 计算当前子查询的执行时间
            execution_time = complexity_factor * initial_size
            total_execution_time += execution_time

        # 模拟传输时间
        transmission_time = self.transmission_time_per_unit * len(data_subset)

        # 模拟总查询时间
        total_time = total_execution_time + transmission_time
        return total_time


# 查询路由器
class QueryRouter:
    def __init__(self, shard_manager):
        self.shard_manager = shard_manager

    def query(self, query, optimizer):
        print(f"\nOriginal Query: {query}")
        
        # 记录优化开始时间
        start_optimization_time = time.time()
        optimized_query = optimizer.optimize(query)
        # 计算优化时间
        optimization_time = time.time() - start_optimization_time
        
        print(f"Optimized Query: {optimized_query}")
        
        results = []
        query_times = []
        for i, node in enumerate(self.shard_manager.nodes):
            # 使用模拟的查询时间
            simulated_time = node.query(optimized_query)
            results.append(node.data.query(optimized_query))
            query_times.append(simulated_time)
            print(f"Node {i+1} Simulated Query Time: {simulated_time} seconds")
            # 打印部分查询结果进行验证
            print(f"Node {i+1} Query Result Sample:")
            print(results[-1].head())
        
        total_results = pd.concat(results)
        total_query_time = sum(query_times)
        print(f"Optimization Time: {optimization_time} seconds")
        print(f"Total Simulated Query Time: {total_query_time} seconds")
        return total_results, query_times, total_query_time, optimization_time


# 优化器基类
class Optimizer:
    def __init__(self):
        pass

    def optimize(self, query):
        raise NotImplementedError("Subclasses should implement this method")

    def calculate_optimization_time(self, query, data_size):
        query_complexity = len(query.split(" and "))
        # 假设优化时间与查询复杂度和数据量成正比
        optimization_time = 0.01 * query_complexity * (data_size / 1000)
        return optimization_time


def evaluate_optimizer(query_router, query, optimizer):
    results, query_times, total_query_time, optimization_time = query_router.query(query, optimizer)
    
    performance_metrics = {
        "Optimization Time": optimization_time,
        "Query Execution Time": total_query_time
    }
    
    return performance_metrics

#GAN Optimizer
class Generator(nn.Module):
    def __init__(self, input_dim, output_dim, activation_fn):
        super(Generator, self).__init__()
        self.model = nn.Sequential(
            nn.Linear(input_dim, 128),
            activation_fn(),
            nn.Linear(128, 256),
            activation_fn(),
            nn.Linear(256, output_dim)
        )

    def forward(self, x):
        return self.model(x)

class Discriminator(nn.Module):
    def __init__(self, output_dim, activation_fn):
        super(Discriminator, self).__init__()
        self.model = nn.Sequential(
            nn.Linear(output_dim, 128),
            activation_fn(),
            nn.Linear(128, 1),
            nn.Sigmoid()
        )

    def forward(self, x):
        return self.model(x)

class GANOptimizer(Optimizer):
    def __init__(self, all_data, learning_rate_g=0.001, learning_rate_d=0.001, activation_fn=nn.ReLU):
        self.all_data = all_data
        self.generator = None
        self.discriminator = None
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.learning_rate_g = learning_rate_g
        self.learning_rate_d = learning_rate_d
        self.activation_fn = activation_fn

    def optimize(self, query):
        subqueries = query.split(" and ")
        input_dim = len(subqueries)
        output_dim = len(subqueries)

        # 初始化生成器和判别器
        self.generator = Generator(input_dim, output_dim, self.activation_fn).to(self.device)
        self.discriminator = Discriminator(output_dim, self.activation_fn).to(self.device)

        # 定义优化器
        g_optimizer = optim.Adam(self.generator.parameters(), lr=self.learning_rate_g)
        d_optimizer = optim.Adam(self.discriminator.parameters(), lr=self.learning_rate_d)

        # 训练GAN
        self.train_gan(subqueries, g_optimizer, d_optimizer)

        # 使用训练好的生成器生成优化后的查询顺序
        optimized_query = self.generate_optimized_query(subqueries)

        return optimized_query

    def train_gan(self, subqueries, g_optimizer, d_optimizer, epochs=1000):
        for epoch in range(epochs):
            # 生成器生成查询顺序
            real_data = torch.FloatTensor([i for i in range(len(subqueries))]).to(self.device)
            fake_data = self.generator(real_data)

            # 计算生成顺序的代价
            cost = self.query_cost(fake_data, subqueries)

            # 训练判别器
            d_optimizer.zero_grad()
            fake_output = self.discriminator(cost)
            d_loss = -torch.mean(torch.log(1.0 - fake_output))
            d_loss.backward()
            d_optimizer.step()

            # 训练生成器
            g_optimizer.zero_grad()
            fake_data = self.generator(real_data)
            fake_output = self.discriminator(self.query_cost(fake_data, subqueries))
            g_loss = -torch.mean(torch.log(fake_output))
            g_loss.backward()
            g_optimizer.step()

            if epoch % 100 == 0:
                print(f"Epoch {epoch}/{epochs}, D Loss: {d_loss.item()}, G Loss: {g_loss.item()}")

    def query_cost(self, fake_data, subqueries):
        # 将张量从计算图中分离出来，防止梯度计算错误
        indices = fake_data.detach().cpu().numpy().argsort()
        sorted_subqueries = [subqueries[i] for i in indices]
        cost = sum(len(sq) for sq in sorted_subqueries)  # 假设代价是查询长度的总和
        cost_tensor = torch.tensor([cost], dtype=torch.float, device=self.device).expand(1, len(subqueries))
        return cost_tensor

    def generate_optimized_query(self, subqueries):
        real_data = torch.FloatTensor([i for i in range(len(subqueries))]).to(self.device)
        with torch.no_grad():
            optimized_order = self.generator(real_data).cpu().numpy().argsort()
        optimized_subqueries = [subqueries[i] for i in optimized_order]
        return " and ".join(optimized_subqueries)

# 加载节点数据
nodes = []
for i in range(10):
    node = Node(i)
    node.load_from_csv(f'node_{i+1}.csv')
    nodes.append(node)

# 合并所有节点的数据到一个DataFrame中
all_data = pd.concat([node.data for node in nodes])

# 创建分片管理器和查询路由器
shard_manager = ShardManager(nodes)
query_router = QueryRouter(shard_manager)


# 实验设置
learning_rates = [0.001,0.005, 0.01]
activation_functions = [nn.ReLU, nn.LeakyReLU]

# 定义查询
query = "data.str.contains('X') and category == 'B' and data.str.contains('AZ') and id >= 500000 and id < 800000"

# 存储实验结果
results = []

# 运行实验
for lr_g in learning_rates:
    for lr_d in learning_rates:
        for activation_fn in activation_functions:
            print(f"\nRunning experiment with lr_g={lr_g}, lr_d={lr_d}, activation_fn={activation_fn.__name__}")
            optimizer = GANOptimizer(all_data, learning_rate_g=lr_g, learning_rate_d=lr_d, activation_fn=activation_fn)
            performance_metrics = evaluate_optimizer(query_router, query, optimizer)
            performance_metrics.update({
                "Learning Rate G": lr_g,
                "Learning Rate D": lr_d,
                "Activation Function": activation_fn.__name__
            })
            results.append(performance_metrics)

# 将结果保存到DataFrame
results_df = pd.DataFrame(results)

# 打印结果
print(results_df)

# 保存结果到CSV文件
results_df.to_csv('gan_optimizer_results.csv', index=False)