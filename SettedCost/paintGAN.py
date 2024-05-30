import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv('gan_optimizer_results_3.csv')

# 创建一个大图
fig, ax1 = plt.subplots(figsize=(16, 8))

# 绘制 Optimization Time 的折线图
ax1.plot(range(1, 19), df['Optimization Time'], color='blue', label='Optimization Time')
ax1.set_xlabel('Parameter Setting')
ax1.set_ylabel('Optimization Time (s)', color='blue')
ax1.tick_params('y', colors='blue')

# 设置 x 轴刻度标签为具体的参数设置
ax1.set_xticks(range(1, 19))
ax1.set_xticklabels([
    'LR G: 0.001\nLR D: 0.001\nReLU',
    'LR G: 0.001\nLR D: 0.001\nLeakyReLU',
    'LR G: 0.001\nLR D: 0.005\nReLU',
    'LR G: 0.001\nLR D: 0.005\nLeakyReLU',
    'LR G: 0.001\nLR D: 0.01\nReLU',
    'LR G: 0.001\nLR D: 0.01\nLeakyReLU',
    'LR G: 0.005\nLR D: 0.001\n ReLU',
    'LR G: 0.005\nLR D: 0.001\n LeakyReLU',
    'LR G: 0.005\nLR D: 0.005\n ReLU',
    'LR G: 0.005\nLR D: 0.005\n LeakyReLU',
    'LR G: 0.005\nLR D: 0.01\n ReLU',
    'LR G: 0.005\nLR D: 0.01\n LeakyReLU',
    'LR G: 0.01\nLR D: 0.001 \n ReLU',
    'LR G: 0.01\nLR D: 0.001\n LeakyReLU',
    'LR G: 0.01\nLR D: 0.005\n ReLU',
    'LR G: 0.01\nLR D: 0.005\n LeakyReLU',
    'LR G: 0.01\nLR D: 0.01\n ReLU',
    'LR G: 0.01\nLR D: 0.01\n LeakyReLU'
], rotation=0, fontsize=6)

# 绘制 Query Execution Time 的柱状图,增加透明度
ax2 = ax1.twinx()
ax2.bar(range(1, 19), df['Query Execution Time'], color='#ffa07a', alpha=0.5, label='Query Execution Time')
ax2.set_ylabel('Query Execution Time (s)', color='#ffa07a')
ax2.tick_params('y', colors='#ffa07a')

# 添加图例
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

# 添加标题
plt.title('Optimization Time vs Query Execution Time for 18 Parameter Settings')

# 显示图表
plt.show()