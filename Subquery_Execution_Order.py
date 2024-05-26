import matplotlib.pyplot as plt
import networkx as nx

# 创建有向图
G = nx.DiGraph()

# 添加节点和边表示子查询执行顺序
# 顺序执行
edges_sequential = [
    ('Subquery 1', 'Subquery 2'),
    ('Subquery 2', 'Subquery 3'),
    ('Subquery 3', 'Final Query')
]

# 并行执行
edges_parallel = [
    ('Subquery 1', 'Final Query'),
    ('Subquery 2', 'Final Query'),
    ('Subquery 3', 'Final Query')
]

# 改变顺序执行
edges_changed_order = [
    ('Subquery 1', 'Subquery 3'),
    ('Subquery 3', 'Subquery 2'),
    ('Subquery 2', 'Final Query')
]

# 定义子图布局
fig, axs = plt.subplots(1, 3, figsize=(18, 6))

# 顺序执行图
G.add_edges_from(edges_sequential)
pos = nx.spring_layout(G)
nx.draw(G, pos, with_labels=True, node_size=3000, node_color='lightblue', font_size=10, font_weight='bold', arrows=True, arrowstyle='-|>', arrowsize=20, ax=axs[0])
axs[0].set_title('Sequential Execution')

# 清空图
G.clear()

# 并行执行图
G.add_edges_from(edges_parallel)
nx.draw(G, pos, with_labels=True, node_size=3000, node_color='lightgreen', font_size=10, font_weight='bold', arrows=True, arrowstyle='-|>', arrowsize=20, ax=axs[1])
axs[1].set_title('Parallel Execution')

# 清空图
G.clear()

# 改变顺序执行图
G.add_edges_from(edges_changed_order)
nx.draw(G, pos, with_labels=True, node_size=3000, node_color='lightcoral', font_size=10, font_weight='bold', arrows=True, arrowstyle='-|>', arrowsize=20, ax=axs[2])
axs[2].set_title('Changed Order Execution')

# 添加标题
fig.suptitle('Subquery Execution Order Comparison', fontsize=16)

# 显示图表
plt.show()