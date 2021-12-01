import io
import gc
import time
from fastparquet import ParquetFile
import dask.dataframe as dd
import dike.core.webhdfs
import dike.core.util


def read_row_group(pf, index, columns):
    df = pf.read_row_group_file(pf.row_groups[index], columns, {})
    return dd.from_pandas(df, npartitions=3)


if __name__ == '__main__':
    fname = '/tpch-test-parquet-1g/lineitem.parquet/' \
            'part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet'

    # TPC-H 100 GB
    #fname = '/tpch-test-parquet/lineitem.parquet'

    dike_file = dike.core.webhdfs.WebHdfsFile(f'webhdfs://172.18.0.100:9860/{fname}', user='peter')
    f = io.BufferedReader(dike_file, buffer_size=(1 << 20))
    pf = ParquetFile(f)

    projection = ['l_orderkey', 'l_quantity']

    start = time.time()
    ddf_list = [None]*len(pf.row_groups)
    for i in range(0, len(pf.row_groups)):
        print('Loading rg', i)
        ddf = read_row_group(pf, i, projection)
        ddf.set_index('l_orderkey')
        ddf = ddf.groupby('l_orderkey').l_quantity.sum()
        ddf_list[i] = ddf
        mem_usage = dike.core.util.get_memory_usage_mb()
        print("Memory usage", mem_usage)
        gc.collect()
        if mem_usage > 4096:  # 4 GB limit
            break

    ddf = dd.concat(ddf_list)
    ddf = ddf[ddf.l_quantity > 300]
    end = time.time()
    print(ddf.head(57))
    print(f"Run time is: {end - start} secs")



