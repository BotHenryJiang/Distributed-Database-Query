import matplotlib.pyplot as plt
import numpy as np

optimizers = ['RBO', 'CBO', 'DPO', 'GrS', 'GAN', 'GA']
statement1 = [1000.41, 1000.41, 1000.41, 3199.46, 3199.75, 3199.75]
statement2 = [3920.31, 9518.28, 9518.28, 3920.31, 2920.75, 8760.01]
statement3 = [4023.15, 4023.15, 4023.15, 4023.15, 5088.93, 4023.15]
statement4 = [5027.28, 5027.28, 5027.28, 4228.24, 5110.59, 5110.59]
statement5 = [4249.4, 7998.65, 7998.65, 3000, 3250, 9940.83]

x = np.arange(len(optimizers))
width = 0.15

fig, ax = plt.subplots(figsize=(12, 6))

ax.plot(x - 2*width, statement1, label='query1', linewidth=2)
ax.scatter(x - 2*width, statement1, s=100, linewidth=2)

ax.plot(x - width, statement2, label='query2', linewidth=2) 
ax.scatter(x - width, statement2, s=100, linewidth=2)

ax.plot(x, statement3, label='query3', linewidth=2)
ax.scatter(x, statement3, s=100, linewidth=2)

ax.plot(x + width, statement4, label='query4', linewidth=2)
ax.scatter(x + width, statement4, s=100, linewidth=2)

ax.plot(x + 2*width, statement5, label='query5', linewidth=2)
ax.scatter(x + 2*width, statement5, s=100, linewidth=2)

ax.set_xticks(x)
ax.set_xticklabels(optimizers)
ax.legend()
ax.set_xlabel('Optimizer')
ax.set_ylabel('Cost Function')

plt.show()