import time
import random
import pandas as pd
import dask.dataframe as dd
from database_setup import ShardManager
import json
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
        print(f"\nOriginal Query: {query}")
        start_optimization_time = time.time()
        optimized_query, best_individuals, best_fitnesses, best_costs, iteration_results, all_fitnesses, all_costs = optimizer.optimize(query)
        optimization_time = time.time() - start_optimization_time

        print(f"Optimized Query: {optimized_query}")
        
        results = []
        query_times = []
        for i, node in enumerate(self.shard_manager.nodes):
            simulated_time = node.query(optimized_query)
            results.append(node.data.query(optimized_query))
            query_times.append(simulated_time)
            print(f"Node {i+1} Simulated Query Time: {simulated_time} seconds")
            print(f"Node {i+1} Query Result Sample:")
            print(results[-1].head())
        
        total_results = pd.concat(results)
        total_query_time = sum(query_times)
        print(f"Optimization Time: {optimization_time} seconds")
        print(f"Total Simulated Query Time: {total_query_time} seconds")
        return total_results, query_times, total_query_time, optimization_time, best_individuals, best_fitnesses, best_costs, iteration_results, all_fitnesses, all_costs

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
                return self.all_data.query(individual_query)
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

        # 保存每代的最优个体、适应度和查询代价
        best_individuals = []
        best_fitnesses = []
        best_costs = []
        iteration_results = []

        # 保存每次迭代的全部个体的查询代价和适应度
        all_fitnesses = []
        all_costs = []

        population = generate_population(self.population_size, subqueries)
        for generation in range(self.generations):
            fitness_values = [fitness(ind) for ind in population]
            cost_values = [cost(ind) for ind in population]
            all_fitnesses.append(fitness_values)
            all_costs.append(cost_values)

            population = sorted(population, key=fitness)
            next_generation = population[:2]
            for _ in range(self.population_size - 2):
                parents = random.sample(population[:5], 2)
                offspring = crossover(parents[0], parents[1])
                offspring = mutate(offspring)
                next_generation.append(offspring)
            population = next_generation

            best_individual = min(population, key=fitness)
            best_fitness = fitness(best_individual)
            best_cost = cost(best_individual)
            print(f"Generation {generation + 1}: Best Individual = {' and '.join(best_individual)}, Fitness = {best_fitness}, Cost = {best_cost}")
            best_individuals.append(" and ".join(best_individual))
            best_fitnesses.append(best_fitness)
            best_costs.append(best_cost)

            # 保存每次迭代的结果
            iteration_results.append({
                "Generation": generation + 1,
                "Best Individual": " and ".join(best_individual),
                "Fitness": best_fitness,
                "Cost": best_cost
            })

        best_individual = min(population, key=fitness)
        return " and ".join(best_individual), best_individuals, best_fitnesses, best_costs, iteration_results, all_fitnesses, all_costs

def evaluate_optimizer(query_router, query, optimizer):
    results, query_times, total_query_time, optimization_time, best_individuals, best_fitnesses, best_costs, iteration_results, all_fitnesses, all_costs = query_router.query(query, optimizer)
    
    performance_metrics = {
        "Optimization Time": optimization_time,
        "Query Execution Time": total_query_time,
        "Best Individuals": best_individuals,
        "Best Fitnesses": best_fitnesses,
        "Best Costs": best_costs,
        "Iteration Results": iteration_results,
        "All Fitnesses": all_fitnesses,
        "All Costs": all_costs
    }
    
    return performance_metrics


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

    # 定义不同参数组合
    param_combinations = [
        {"population_size": 10, "generations": 50, "mutation_rate": 0.1},
        {"population_size": 10, "generations": 50, "mutation_rate": 0.2},
        {"population_size": 10, "generations": 50, "mutation_rate": 0.3},
        {"population_size": 10, "generations": 50, "mutation_rate": 0.4},
        {"population_size": 20, "generations": 50, "mutation_rate": 0.1},
        {"population_size": 20, "generations": 50, "mutation_rate": 0.2},
        {"population_size": 20, "generations": 50, "mutation_rate": 0.3},
        {"population_size": 20, "generations": 50, "mutation_rate": 0.4},
        {"population_size": 10, "generations": 100, "mutation_rate": 0.1},
        {"population_size": 10, "generations": 100, "mutation_rate": 0.2},
        {"population_size": 10, "generations": 100, "mutation_rate": 0.3},
        {"population_size": 10, "generations": 100, "mutation_rate": 0.4},
        {"population_size": 20, "generations": 200, "mutation_rate": 0.1},
        {"population_size": 20, "generations": 200, "mutation_rate": 0.2},
        {"population_size": 20, "generations": 200, "mutation_rate": 0.3},
        {"population_size": 20, "generations": 200, "mutation_rate": 0.4},
    ]

    all_results = []

    for params in param_combinations:
        print(f"\nRunning GA Optimizer with params: {params}")
        ga_optimizer = GeneticAlgorithmOptimizer(
            all_data, 
            params["population_size"], 
            params["generations"], 
            params["mutation_rate"]
        )
        performance_metrics = evaluate_optimizer(query_router, query, ga_optimizer)
        print(f"GA Optimizer Performance Metrics: {performance_metrics}")

        # 打印每代最优个体、适应度和查询代价
        print(f"Best Individuals per Generation: {performance_metrics['Best Individuals']}")
        print(f"Best Fitnesses per Generation: {performance_metrics['Best Fitnesses']}")
        print(f"Best Costs per Generation: {performance_metrics['Best Costs']}")

        # 保存每次迭代得到的全部查询代价和适应度
        all_results.append({
            "params": params,
            "performance_metrics": performance_metrics
        })

    # 将所有结果保存到CSV文件中
    with open("ga_optimization_results.csv", "w", newline='') as csvfile:
        fieldnames = ['params', 'generation', 'best_individual', 'best_fitness', 'best_cost', 'all_fitnesses', 'all_costs']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for result in all_results:
            params = result["params"]
            performance_metrics = result["performance_metrics"]
            for i, iteration_result in enumerate(performance_metrics["Iteration Results"]):
                row = {
                    'params': json.dumps(params),
                    'generation': iteration_result['Generation'],
                    'best_individual': iteration_result['Best Individual'],
                    'best_fitness': iteration_result['Fitness'],
                    'best_cost': iteration_result['Cost'],
                    'all_fitnesses': json.dumps(performance_metrics['All Fitnesses'][i]),
                    'all_costs': json.dumps(performance_metrics['All Costs'][i])
                }
                writer.writerow(row)

if __name__ == "__main__":
    main()