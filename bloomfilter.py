import math
import hashlib
import redis
import functools

try:
    # 优先使用c语言写的mmh3，速度最快
    import mmh3 as mmh3
except ImportError:
    # 没有再使用Python写的mmh3
    import pymmh3 as mmh3


class RedisBloomFilter:

    def __init__(self, conn, key='bloomfilter', capacity=100000000, error_rate=0.0000001):
        self.conn = conn
        self.key = key
        self.capacity = capacity
        self.error_rate = error_rate

        self.m = math.ceil(capacity * math.log2(math.e) * math.log2(1 / error_rate))  # 最少需要的总bit位数
        self.k = math.ceil(math.log1p(2) * self.m / capacity)  # 最少需要的hash次数
        self.mem = math.ceil(self.m / 8 / 1024 / 1024)  # 最少需要多少M内存
        self.block_num = math.ceil(self.mem / 512)  # 实际需要多少block分片

        # redis单个string最大为2^32 bit，所以默认大小就设置为2^32
        self.bit_size = 1 << 32
        self.hash_funcs = self.get_hash_funcs(self.k)

    def __repr__(self):
        return '理论容量：{:,}，理论错误率：{}，hash函数数量：{}，block数量：{}，内存占用：{}M'.format(
            self.capacity, self.error_rate, self.k,self.block_num, self.block_num * 512)

    def __contains__(self, value):
        block = self.get_block_key(value)
        locs = self.get_offset(value)
        return all(True if self.conn.getbit(block, loc) else False for loc in locs)

    def get_hash_funcs(self, num):
        '''
        获取指定个数的哈希函数
        '''
        # hash_funcs_list = ["rs_hash", "js_hash", "pjw_hash", "elf_hash", "bkdr_hash", "sdbm_hash", "djb_hash",
        #                    "dek_hash", 'bp_hash', 'fnv_hash', 'ap_hash']
        # return [getattr(GeneralHashFunctions, name) for name in hash_funcs_list[:num]]

        # 100个内置种子，用于生成不同的哈希函数
        seeds = [543, 460, 171, 876, 796, 607, 650, 81, 837, 545, 591, 946, 846, 521, 913, 636, 878, 735, 414, 372,
                 344, 324, 223, 180, 327, 891, 798, 933, 493, 293, 836, 10, 6, 544, 924, 849, 438, 41, 862, 648, 338,
                 465, 562, 693, 979, 52, 763, 103, 387, 374, 349, 94, 384, 680, 574, 480, 307, 580, 71, 535, 300, 53,
                 481, 519, 644, 219, 686, 236, 424, 326, 244, 212, 909, 202, 951, 56, 812, 901, 926, 250, 507, 739, 371,
                 63, 584, 154, 7, 284, 617, 332, 472, 140, 605, 262, 355, 526, 647, 923, 199, 518]

        return [functools.partial(mmh3.hash, seed=i) for i in seeds[:num]]

    def get_block_key(self, value):
        '''
        计算归属分片，返回key name
        '''
        # UTF8编码
        value = str(value).encode()
        # 获取10进制哈希值
        hash_value = int(hashlib.md5(value).hexdigest(), 16)
        return '%s_%s' % (self.key, str(hash_value % self.block_num))

    def get_offset(self, value):
        '''
        获取每个哈希函数在block内计算出的位置
        '''
        results = []
        value = str(value)
        for func in self.hash_funcs:
            # 部分hash获取的是负数，所以取绝对值
            hash_value = abs(func(value))
            # 将hash函数得出的函数值映射到[0, 2^32-1]区间内
            results.append(hash_value % self.bit_size)
        return results

    def add(self, value):
        block = self.get_block_key(value)
        locs = self.get_offset(value)
        for loc in locs:
            self.conn.setbit(block, loc, 1)


if __name__ == '__main__':
    conn = redis.Redis(host='127.0.0.1', port=6379, db=0)
    bf = RedisBloomFilter(conn)
    print(bf)
    bf.add('sbs')
    print('ssbs' in bf)
