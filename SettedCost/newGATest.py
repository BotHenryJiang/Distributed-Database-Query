import time
import random
import pandas as pd
import dask.dataframe as dd
from database_setup import ShardManager
import csv

# 数据库节点
class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.data = pd.DataFrame(columns=['id', 'data', 'category'])
        self.ddf = dd.from_pandas(self.data, npartitions=1)
        self.transmission_time_per_unit = 0.001 + 0.001 * node_id

    def load_from_csv(self, filename):
        self.ddf = dd.read_csv(filename)
        self.data = self.ddf.compute()

    def save_to_csv(self, filename):
        self.ddf.to_csv(filename, index=False, single_file=True)

    def query(self, subquery):
        subqueries = subquery.split(" and ")
        data_subset = self.data
        total_execution_time = 0

        for sq in subqueries:
            initial_size = len(data_subset)
            if 'id' in sq:
                complexity_factor = 0.001
            elif 'category' in sq:
                complexity_factor = 0.003
            else:
                complexity_factor = 0.005
            
            data_subset = data_subset.query(sq)
            filtered_size = len(data_subset)
            execution_time = complexity_factor * initial_size
            total_execution_time += execution_time

        transmission_time = self.transmission_time_per_unit * len(data_subset)
        total_time = total_execution_time + transmission_time
        return total_time

# 查询路由器
class QueryRouter:
    def __init__(self, shard_manager):
        self.shard_manager = shard_manager

    def query(self, query, optimizer):
        optimized_query, iteration_results = optimizer.optimize(query)
        return iteration_results

# 优化器基类
class Optimizer:
    def __init__(self):
        pass

    def optimize(self, query):
        raise NotImplementedError("Subclasses should implement this method")

# 遗传算法优化器
class GeneticAlgorithmOptimizer(Optimizer):
    def __init__(self, all_data, population_size, generations, mutation_rate):
        self.all_data = all_data
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate

    def optimize(self, query):
        subqueries = query.split(" and ")

        def generate_population(size, subqueries):
            population = []
            for _ in range(size):
                individual = random.sample(subqueries, len(subqueries))
                population.append(individual)
            return population

        def fitness(individual):
            try:
                individual_query = " and ".join(individual)
                start_time = time.time()
                self.all_data.query(individual_query)
                end_time = time.time()
                execution_time = end_time - start_time
                return execution_time
            except Exception as e:
                print(f"Query failed: {' and '.join(individual)}, Error: {e}")
                return float('inf')

        def cost(individual):
            try:
                individual_query = " and ".join(individual)
                data_subset = self.all_data.query(individual_query)
                total_execution_time = 0
                for sq in individual:
                    initial_size = len(data_subset)
                    if 'id' in sq:
                        complexity_factor = 0.001
                    elif 'category' in sq:
                        complexity_factor = 0.003
                    else:
                        complexity_factor = 0.005
                    data_subset = data_subset.query(sq)
                    filtered_size = len(data_subset)
                    execution_time = complexity_factor * initial_size
                    total_execution_time += execution_time
                transmission_time = self.all_data.shape[0] * 0.001
                total_time = total_execution_time + transmission_time
                return total_time
            except Exception as e:
                print(f"Query failed: {' and '.join(individual)}, Error: {e}")
                return float('inf')

        def crossover(parent1, parent2):
            split_point = random.randint(1, len(parent1) - 2)
            child = parent1[:split_point] + [sq for sq in parent2 if sq not in parent1[:split_point]]
            return child

        def mutate(individual):
            if random.random() < self.mutation_rate:
                idx1, idx2 = random.sample(range(len(individual)), 2)
                individual[idx1], individual[idx2] = individual[idx2], individual[idx1]
            return individual

        # 保存每代的最优个体和查询代价
        iteration_results = []

        population = generate_population(self.population_size, subqueries)
        for generation in range(self.generations):
            population = sorted(population, key=fitness)
            next_generation = population[:2]
            for _ in range(self.population_size - 2):
                parents = random.sample(population[:5], 2)
                offspring = crossover(parents[0], parents[1])
                offspring = mutate(offspring)
                next_generation.append(offspring)
            population = next_generation

            best_individual = min(population, key=fitness)
            best_cost = cost(best_individual)
            best_query = " and ".join(best_individual)
            print(f"Generation {generation + 1}: Best Query = {best_query}, Cost = {best_cost}")

            iteration_results.append({
                "Generation": generation + 1,
                "Best Query": best_query,
                "Cost": best_cost
            })

        return best_query, iteration_results

def main():
    # 加载节点数据
    nodes = []
    for i in range(10):
        node = Node(i)
        node.load_from_csv(f'node_{i+1}.csv')
        nodes.append(node)

    all_data = pd.concat([node.data for node in nodes])
    shard_manager = ShardManager(nodes)
    query_router = QueryRouter(shard_manager)

    query = "data.str.contains('X') and category == 'B' and data.str.contains('AZ') and id >=500000 and id<800000"

    # 只使用一个参数组合
    params = {"population_size": 10, "generations": 10, "mutation_rate": 0.2}

    print(f"\nRunning GA Optimizer with params: {params}")
    ga_optimizer = GeneticAlgorithmOptimizer(
        all_data, 
        params["population_size"], 
        params["generations"], 
        params["mutation_rate"]
    )

    iteration_results = query_router.query(query, ga_optimizer)

    # 将结果保存到CSV文件中
    with open("ga_optimization_results.csv", "w", newline='') as csvfile:
        fieldnames = ['generation', 'best_query', 'cost']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for result in iteration_results:
            writer.writerow({
                'generation': result['Generation'],
                'best_query': result['Best Query'],
                'cost': result['Cost']
            })

if __name__ == "__main__":
    main()