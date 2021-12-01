import time
from fastparquet import ParquetFile

import dike.core.webhdfs
import dike.core.util

if __name__ == '__main__':
    fname = '/tpch-test-parquet-1g/lineitem.parquet/' \
            'part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet'

    # TPC-H 100 GB
    # fname = '/tpch-test-parquet/lineitem.parquet'

    # dike_file = dike.webhdfs.WebHdfsFile(f'webhdfs://dikehdfs:9860/{fname}', user='peter')
    dike_file = dike.core.webhdfs.WebHdfsFile(f'webhdfs://172.18.0.100:9870/{fname}', user='peter')

    # dike_file = open('../caerus-dikeHDFS/data' + fname, 'rb')
    # f = io.BufferedReader(dike_file, buffer_size=(1 << 20))
    pf = ParquetFile(dike_file)

    projection = ['l_partkey', 'l_extendedprice', 'l_discount']
    # projection = ['l_orderkey']

    print('pf.columns', pf.columns)

    start = time.time()
    df = pf.read_row_group_file(pf.row_groups[0], projection, pf.categories)
    read_time = 0

    end = time.time()
    print(f"Run time is: {end - start} secs")

    print(len(dike_file.read_stats))
    print(dike_file.read_stats[:10])

