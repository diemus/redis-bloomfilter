# redis-bloomfilter
可自行设定理论容量和理论错误率，并根据这2项参数自动调整占用redis的大小和使用的函数数量

## Usage:


    pip install mmh3

sdaf

    conn = redis.Redis(host='127.0.0.1', port=6379, db=0)
    bf = RedisBloomFilter(conn)
    print(bf)
    bf.add('abc')
    print('abc' in bf)
