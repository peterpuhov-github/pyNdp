import io
import pandas as pd
import numpy
from fastparquet import ParquetFile
import dike.webhdfs
import dike.client.tpch

if __name__ == '__main__':
    fname = '/tpch-test-parquet-1g/lineitem.parquet/' \
            'part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet'

    q14 = dike.client.tpch.TpchQ14(fname, 0)
