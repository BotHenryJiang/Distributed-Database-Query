import time
import pandas as pd
from DatabaseSetup import Node, ShardManager
from Query_optimization import QueryRouter, GreedyOptimizer, DynamicProgrammingOptimizer

# 示例数据加载
nodes = [Node() for _ in range(10)]
for i, node in enumerate(nodes):
    node.load_from_csv(f"data{i+1}.csv")

shard_manager = ShardManager(nodes)
query_router = QueryRouter(shard_manager)

# 合并所有节点的数据
all_data = pd.concat([node.data for node in nodes])

# 创建优化器实例
greedy_optimizer = GreedyOptimizer()
dp_optimizer = DynamicProgrammingOptimizer()

# 示例查询
query = "data.str.contains('X') and data.str.contains('AZ') and data.str.contains('1') and data.str.contains('G') and category == 'A' and id >= 100 and id < 200000 "

# 评估函数
def evaluate_optimizer(optimizer, query, query_router, runs=5):
    total_time = 0
    for _ in range(runs):
        start_time = time.time()
        results, query_times = query_router.query(query, optimizer)
        end_time = time.time()
        total_time += (end_time - start_time)
    avg_time = total_time / runs
    return avg_time

# 评估GreedyOptimizer
greedy_time = evaluate_optimizer(greedy_optimizer, query, query_router)
print(f"Average Query Time with GreedyOptimizer: {greedy_time} seconds")

# 评估DynamicProgrammingOptimizer
dp_time = evaluate_optimizer(dp_optimizer, query, query_router)
print(f"Average Query Time with DynamicProgrammingOptimizer: {dp_time} seconds")
