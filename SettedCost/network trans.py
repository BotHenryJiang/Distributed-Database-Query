import matplotlib.pyplot as plt
import numpy as np

# 定义网络带宽和数据量的范围
bandwidths = np.array([10, 50, 100, 500, 1000])  # 单位：Mbps
data_sizes = np.array([10, 50, 100, 500, 1000])  # 单位：MB

# 计算传输时间（单位：秒）
# 传输时间 = 数据大小 / 带宽（注意带宽转换为MB/s）
transmission_times = np.array([[data_size / (bandwidth / 8.0) for bandwidth in bandwidths] for data_size in data_sizes])

# 创建图表
fig, ax = plt.subplots()

# 绘制每个数据大小对应的传输时间曲线
for i, data_size in enumerate(data_sizes):
    ax.plot(bandwidths, transmission_times[i], marker='o', label=f'{data_size} MB')

# 设置图表标题和标签
ax.set_title('Impact of Network Bandwidth and Data Size on Transmission Time')
ax.set_xlabel('Network Bandwidth (Mbps)')
ax.set_ylabel('Transmission Time (seconds)')
ax.set_xscale('log')  # 使用对数刻度显示带宽
ax.legend(title='Data Size')

# 显示图表
plt.grid(True)
plt.show()