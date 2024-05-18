import random
import string
import pandas as pd
import dask.dataframe as dd

# 数据库节点
class Node:
    def __init__(self):
        self.data = pd.DataFrame(columns=['id', 'data', 'category'])
        self.ddf = dd.from_pandas(self.data, npartitions=1)

    def load_from_csv(self, filename):
        self.ddf = dd.read_csv(filename)
        self.data = self.ddf.compute()

    def save_to_csv(self, filename):
        self.ddf.to_csv(filename, index=False, single_file=True)

# 分片管理器
class ShardManager:
    def __init__(self, nodes):
        self.nodes = nodes

    def get_node_for_key(self, key):
        # 简单的哈希分片策略
        return key % len(self.nodes)

if __name__ == "__main__":
    def generate_and_save_data(num_records, categories, batch_size, shard_manager):
        data_batches = [[] for _ in range(len(shard_manager.nodes))]

        for i in range(num_records):
            record_id = i
            # 生成随机大小的数据字符串，大小在0.5K到1.5K之间
            data_size = random.randint(512, 1536)  # 0.5K到1.5K字节
            record_data = ''.join(random.choices(string.ascii_uppercase + string.digits, k=data_size))
            category = random.choice(categories)
            node_index = shard_manager.get_node_for_key(record_id)
            data_batches[node_index].append((record_id, record_data, category))

            # 每隔batch_size条数据打印一次
            if (i + 1) % batch_size == 0:
                print(f"Generated {i + 1} records")
                for j, batch in enumerate(data_batches):
                    if batch:
                        print(f"Sample data for node_{j+1}: {batch[:10]}")

        # 保存数据到CSV文件
        for i, batch in enumerate(data_batches):
            df = pd.DataFrame(batch, columns=['id', 'data', 'category'])
            ddf = dd.from_pandas(df, npartitions=1)
            ddf.to_csv(f'node_{i+1}.csv', index=False, single_file=True)
            print(f"Node {i+1} Data saved to node_{i+1}.csv")

    # 初始化十个数据库节点
    nodes = [Node() for _ in range(10)]

    # 创建分片管理器
    shard_manager = ShardManager(nodes)

    # 插入示例数据
    num_records = 1000000  # 进一步扩大数据量
    categories = ['A', 'B', 'C', 'D', 'E']
    batch_size = 100000  # 打印间隔

    # 生成并保存数据到CSV文件
    generate_and_save_data(num_records, categories, batch_size, shard_manager)

    # 加载数据到节点并打印前10条数据进行验证
    for i, node in enumerate(nodes):
        node.load_from_csv(f'node_{i+1}.csv')
        print(f"Sample data from node_{i+1}.csv:")
        print(node.data.head(10))
