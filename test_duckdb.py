import duckdb
import time
import numpy
import pyarrow
import pyarrow.parquet
from dike.core.webhdfs import WebHdfsFile
from concurrent.futures import ThreadPoolExecutor


def read_col(pf, col):
    c = pf.read_row_group(0, columns=[col])
    return c


def read_parallel(f, columns):
    pf = pyarrow.parquet.ParquetFile(f)
    print(pf.schema_arrow.names)
    executor = ThreadPoolExecutor(max_workers=len(columns))
    futures = list()
    for col in columns:
        fin = f.copy()
        pfin = pyarrow.parquet.ParquetFile(fin, metadata=pf.metadata)
        futures.append(executor.submit(read_col, pfin, col))
        # tbl = pfin.read_row_group(0, columns=[col])

    res = [r.result().column(0) for r in futures]
    return res


def read_seq(f, columns):
    pf = pyarrow.parquet.ParquetFile(f)
    tbl = pf.read_row_group(0, columns=columns)
    return tbl

if __name__ == '__main__':
    fname = '/tpch-test-parquet-1g/lineitem.parquet/' \
            'part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet'

    f = WebHdfsFile(f'webhdfs://172.18.0.100:9870/{fname}', user='peter')

    columns = ['l_partkey', 'l_extendedprice', 'l_discount', 'l_shipdate']
    start = time.time()
    results = read_parallel(f, columns)
    tbl = pyarrow.Table.from_arrays(results, names=columns)
    end = time.time()
    print(f"Run time is: {end - start:.3f} secs {(f.read_bytes/(1<<20)) / (end - start):.3f} MB/s")
    start = time.time()
    query = "SELECT * FROM arrow WHERE l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'"

    df = duckdb.from_arrow_table(tbl).query("arrow", query).fetchdf()
    # df = duckdb.from_arrow_table(tbl).query("arrow", query).fetchnumpy()

    for col in df.columns:
        data = df[col].to_numpy()
        if data.dtype == 'object' and isinstance(data[0], str):
            s = data.astype(dtype=numpy.bytes_)
            l = numpy.char.str_len(s).astype(dtype=numpy.ubyte)
            fixed_len = numpy.all(l == l[0])
            if fixed_len:
                print(f'All strings have len {l[0]}')

            print(s[0], numpy.char.str_len(s)[0])
            print(len(s.tobytes()))
            print(s.tobytes()[:20])
            print(l.tobytes()[:4])

    end = time.time()
    print(df[columns[0]].size)
    print(f"Query time is: {end - start:.3f} secs")
