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


# 基于规则的优化器
class RuleBasedOptimizer(Optimizer):
    def optimize(self, query):
        # 预定义的优化规则：将选择操作提前
        def is_selection(subquery):
            return "id" in subquery or "category" in subquery

        # 将查询拆分为子查询
        subqueries = query.split(" and ")
        
        # 将选择操作和其他操作分开
        selection_queries = [sq for sq in subqueries if is_selection(sq)]
        other_queries = [sq for sq in subqueries if not is_selection(sq)]
        
        # 重新组合优化后的查询
        optimized_query = " and ".join(selection_queries + other_queries)
        return optimized_query

# 基于成本的优化器
class CostBasedOptimizer(Optimizer):
    def optimize(self, query):
        subqueries = query.split(" and ")
        
        # 定义一个简单的成本模型来评估不同查询操作的开销
        def estimate_cost(subquery):
            if "category" in subquery:
                return 1  # 假设category查询的成本较低
            elif "id" in subquery:
                return 2  # 假设id查询的成本较高
            else:
                return 3  # 其他查询的成本最高
        
        # 动态规划表
        n = len(subqueries)
        dp = [None] * (n + 1)
        dp[0] = ""
        
        for i in range(1, n + 1):
            for j in range(i):
                candidate = " and ".join(subqueries[j:i])
                if dp[i] is None or (dp[j] is not None and estimate_cost(candidate) + estimate_cost(dp[j]) < estimate_cost(dp[i])):
                    dp[i] = dp[j] + " and " + candidate if dp[j] else candidate
        
        optimized_query = dp[n]
        
        # 确保优化后的查询字符串不为空
        if not optimized_query.strip():
            optimized_query = query
        
        return optimized_query

# 动态规划优化器
class DynamicProgrammingOptimizer(Optimizer):
    def optimize(self, query):
        subqueries = query.split(" and ")
        n = len(subqueries)
        
        # 初始化动态规划表
        dp = [None] * (n + 1)
        dp[0] = ""
        
        for i in range(1, n + 1):
            for j in range(i):
                candidate = " and ".join(subqueries[j:i])
                if dp[i] is None or (dp[j] is not None and len(candidate) < len(dp[i])):
                    dp[i] = dp[j] + " and " + candidate if dp[j] else candidate
        
        optimized_query = dp[n]
        
        # 确保优化后的查询字符串不为空
        if not optimized_query.strip():
            optimized_query = query
        
        return optimized_query

# 贪心算法优化器
class GreedyOptimizer(Optimizer):
    def optimize(self, query):
        subqueries = query.split(" and ")
        
        # 定义一个简单的成本模型来评估不同查询操作的开销
        def estimate_cost(subquery):
            if "category" in subquery:
                return 1  # 假设category查询的成本较低
            elif "id" in subquery:
                return 2  # 假设id查询的成本较高
            else:
                return 3  # 其他查询的成本最高
        
        # 按成本对子查询进行排序
        sorted_subqueries = sorted(subqueries, key=estimate_cost)
        
        # 组合成最终的优化查询
        optimized_query = " and ".join(sorted_subqueries)
        
        return optimized_query

# 遗传算法优化器
class GeneticAlgorithmOptimizer(Optimizer):
    def __init__(self, all_data):
        self.all_data = all_data

    def optimize(self, query):
        # 解析查询子语句
        subqueries = query.split(" and ")
        population_size = 10
        generations = 200
        mutation_rate = 0.2  # 增加变异率

        def generate_population(size, subqueries):
            population = []
            for _ in range(size):
                individual = random.sample(subqueries, len(subqueries))
                population.append(individual)
            return population

        def fitness(individual):
            # 使用查询执行时间作为适应度函数
            try:
                individual_query = " and ".join(individual)
                start_time = time.time()
                result = self.all_data.query(individual_query)
                end_time = time.time()
                execution_time = end_time - start_time
                return execution_time
            except Exception as e:
                print(f"Query failed: {' and '.join(individual)}, Error: {e}")
                return float('inf')  # 如果查询失败，返回一个很大的适应度值

        def crossover(parent1, parent2):
            split_point = random.randint(1, len(parent1) - 2)
            child = parent1[:split_point] + [sq for sq in parent2 if sq not in parent1[:split_point]]
            return child

        def mutate(individual):
            if random.random() < mutation_rate:
                idx1, idx2 = random.sample(range(len(individual)), 2)
                individual[idx1], individual[idx2] = individual[idx2], individual[idx1]
            return individual

        population = generate_population(population_size, subqueries)
        for generation in range(generations):
            population = sorted(population, key=fitness)
            next_generation = population[:2]
            for _ in range(population_size - 2):
                parents = random.sample(population[:5], 2)
                offspring = crossover(parents[0], parents[1])
                offspring = mutate(offspring)
                next_generation.append(offspring)
            population = next_generation

            # 输出当前最优个体及其适应度
            best_individual = min(population, key=fitness)
            best_fitness = fitness(best_individual)
            print(f"Generation {generation + 1}: Best Individual = {' and '.join(best_individual)}, Fitness = {best_fitness}")

        best_individual = min(population, key=fitness)
        return " and ".join(best_individual)


#GAN优化器
class Generator(nn.Module):
    def __init__(self, input_dim, output_dim):
        super(Generator, self).__init__()
        self.model = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.Linear(128, output_dim),
            nn.Softmax(dim=-1)
        )

    def forward(self, x):
        return self.model(x)

class Discriminator(nn.Module):
    def __init__(self, input_dim):
        super(Discriminator, self).__init__()
        self.model = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.Linear(128, 1),
            nn.Sigmoid()
        )

    def forward(self, x):
        return self.model(x)

class GANOptimizer(Optimizer):
    def __init__(self, all_data):
        self.all_data = all_data
        self.generator = None
        self.discriminator = None
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    def optimize(self, query):
        subqueries = query.split(" and ")
        input_dim = len(subqueries)
        output_dim = len(subqueries)

        # 初始化生成器和判别器
        self.generator = Generator(input_dim, output_dim).to(self.device)
        self.discriminator = Discriminator(output_dim).to(self.device)

        # 定义优化器
        g_optimizer = optim.Adam(self.generator.parameters(), lr=0.001)
        d_optimizer = optim.Adam(self.discriminator.parameters(), lr=0.001)

        # 训练GAN
        self.train_gan(subqueries, g_optimizer, d_optimizer)

        # 使用训练好的生成器生成优化后的查询顺序
        optimized_query = self.generate_optimized_query(subqueries)

        return optimized_query

    def train_gan(self, subqueries, g_optimizer, d_optimizer, epochs=1000):
        for epoch in range(epochs):
            # 生成器生成查询顺序
            real_data = torch.FloatTensor([i for i in range(len(subqueries))]).to(self.device)
            fake_data = self.generator(real_data).detach()

            # 训练判别器
            d_optimizer.zero_grad()
            real_output = self.discriminator(real_data)
            fake_output = self.discriminator(fake_data)
            d_loss = -torch.mean(torch.log(real_output) + torch.log(1.0 - fake_output))
            d_loss.backward()
            d_optimizer.step()

            # 训练生成器
            g_optimizer.zero_grad()
            fake_data = self.generator(real_data)
            fake_output = self.discriminator(fake_data)
            g_loss = -torch.mean(torch.log(fake_output))
            g_loss.backward()
            g_optimizer.step()

            if epoch % 100 == 0:
                print(f"Epoch {epoch}/{epochs}, D Loss: {d_loss.item()}, G Loss: {g_loss.item()}")

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

# 定义优化器
optimizers = {
    "RBO": RuleBasedOptimizer(),
    "CBO": CostBasedOptimizer(),
    "DP": DynamicProgrammingOptimizer(),
    "GrS": GreedyOptimizer(),
    "GA": GeneticAlgorithmOptimizer(all_data), #传递 all_data 给遗传算法优化器
    "GAN": GANOptimizer(all_data)
}

# 测试和比较不同优化器的性能
query = "categoty == 'B' and data.str.contains('X') and data.str.contains('AZ') and id >=500000 and id<800000 "

for name, optimizer in optimizers.items():
    print(f"\nRunning {name} Optimizer...")
    performance_metrics = evaluate_optimizer(query_router, query, optimizer)
    print(f"{name} Performance Metrics: {performance_metrics}")