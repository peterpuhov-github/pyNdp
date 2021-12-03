import time
import threading
import sys
from fastparquet import ParquetFile
from fastparquet import core

from dike.core.webhdfs import WebHdfsFile

class ParquetReader(ParquetFile):
    def __init__(self, infile: WebHdfsFile):
        self.pf = ParquetFile(infile)
        self.infile = infile

    def read_rg(self, index: int, columns: list):
        rg = self.pf.row_groups[index]
        #  Allocate memory
        df, assign = self.pf.pre_allocate(rg.num_rows, columns, {}, None)

        threads = []
        files = []
        for column in rg.columns:
            name = column.meta_data.path_in_schema[0]
            if name in columns:
                print(name)
                # core.read_col(column, self.pf.schema, self.infile, assign=assign[name])
                infile = WebHdfsFile.copy(self.infile)
                th = threading.Thread(target=core.read_col,
                                      args=(column, self.pf.schema, infile),
                                      kwargs={'assign': assign[name]})
                th.start()
                threads.append((th, infile))
                # th.join()

        for th, infile in threads:
            th.join()
            self.infile.read_bytes += infile.read_bytes


        return df


if __name__ == '__main__':
    fname = '/tpch-test-parquet-1g/lineitem.parquet/' \
            'part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet'

    f = WebHdfsFile(f'webhdfs://172.18.0.100:9870/{fname}', user='peter')
    f.enable_cache()

    reader = ParquetReader(f)
    start = time.time()
    columns =  ['l_partkey', 'l_extendedprice', 'l_discount', 'l_shipdate']
    df = reader.read_rg(0, columns[:4])

    # df = reader.read_rg(0, reader.pf.columns[:])
    end = time.time()
    print(f"Run time is: {end - start:.3f} secs {(f.read_bytes/(1<<20)) / (end - start):.3f} MB/s")
    print(df.shape)
