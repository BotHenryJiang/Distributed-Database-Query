import time
import random
import pandas as pd
import dask.dataframe as dd
import re
from database_setup import ShardManager

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
        optimized_query = optimizer.optimize(query)
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
        total_simulated_time = sum(query_times)
        print(f"Total Simulated Query Time: {total_simulated_time} seconds")
        return total_results, query_times, total_simulated_time



# 优化器基类
class Optimizer:
    def optimize(self, query):
        raise NotImplementedError

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
        # 这里是一个简单的遗传算法实现
        population_size = 10
        generations = 200
        mutation_rate = 0.2  # 增加变异率

        def generate_population(size, base_query):
            population = []
            for _ in range(size):
                lower_bound = random.randint(0, 900000)
                upper_bound = lower_bound + random.randint(10000, 100000)
                category = random.choice(['A', 'B', 'C', 'D', 'E'])
                data_condition = "data.str.contains('X')" if random.random() < 0.5 else ""
                individual_query = f"id >= {lower_bound} and id < {upper_bound} and category == '{category}'"
                if data_condition:
                    individual_query += f" and {data_condition}"
                population.append(individual_query)
            return population

        def fitness(individual):
            # 使用查询执行时间作为适应度函数
            try:
                start_time = time.time()
                result = self.all_data.query(individual)
                end_time = time.time()
                execution_time = end_time - start_time
                return execution_time
            except Exception as e:
                print(f"Query failed: {individual}, Error: {e}")
                return float('inf')  # 如果查询失败，返回一个很大的适应度值

        def crossover(parent1, parent2):
            split_point = random.randint(1, len(parent1) - 2)
            return parent1[:split_point] + parent2[split_point:]

        def mutate(individual, base_query):
            if random.random() < mutation_rate:
                parts = re.split(r" and ", individual)
                if len(parts) > 1:
                    mutation_point = random.randint(0, len(parts) - 1)
                    lower_bound = random.randint(0, 900000)
                    upper_bound = lower_bound + random.randint(10000, 100000)
                    category = random.choice(['A', 'B', 'C', 'D', 'E'])
                    data_condition = "data.str.contains('X')" if random.random() < 0.5 else ""
                    new_condition = f"id >= {lower_bound} and id < {upper_bound} and category == '{category}'"
                    if data_condition:
                        new_condition += f" and {data_condition}"
                    parts[mutation_point] = new_condition
                    return " and ".join(parts)
            return individual

        population = generate_population(population_size, query)
        for generation in range(generations):
            population = sorted(population, key=fitness)
            next_generation = population[:2]
            for _ in range(population_size - 2):
                parents = random.sample(population[:5], 2)
                offspring = crossover(parents[0], parents[1])
                offspring = mutate(offspring, query)
                next_generation.append(offspring)
            population = next_generation

            # 输出当前最优个体及其适应度
            best_individual = min(population, key=fitness)
            best_fitness = fitness(best_individual)
            print(f"Generation {generation + 1}: Best Individual = {best_individual}, Fitness = {best_fitness}")

        best_individual = min(population, key=fitness)
        return best_individual

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
    "GA": GeneticAlgorithmOptimizer(all_data)  # 传递 all_data 给遗传算法优化器
}

# 测试和比较不同优化器的性能
query = "data.str.contains('X') and data.str.contains('AZ') and data.str.contains('1') and data.str.contains('G') and category == 'A' and id >= 100 and id < 200000"

for name, optimizer in optimizers.items():
    print(f"\nRunning {name} Optimizer...")
    results, query_times, total_simulated_time = query_router.query(query, optimizer)
    print(f"{name} Total Simulated Query Time: {total_simulated_time} seconds")

