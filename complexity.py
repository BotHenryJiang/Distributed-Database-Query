import matplotlib.pyplot as plt

# 定义查询类型和对应的复杂度
query_types = ['Simple Query', 'Join Query', 'Nested Query', 'Aggregation']
complexity = [1, 3, 5, 4]

# 创建图表
fig, ax = plt.subplots()

# 绘制柱状图
bars = ax.bar(query_types, complexity, color=['lightblue', 'lightgreen', 'lightcoral', 'lightgoldenrodyellow'])

# 添加标签和标题
ax.set_xlabel('Query Type')
ax.set_ylabel('Complexity')
ax.set_title('Query Complexity Comparison')

# 添加复杂度数值标签
for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, height - 0.25, str(height), ha='center', va='bottom')

# 显示图表
plt.show()


'''{"population_size": 10, "generations": 50, "mutation_rate": 0.1},
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
        '''