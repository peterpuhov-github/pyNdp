import time
import pyarrow
import pyarrow.parquet
import numpy
import duckdb
from dike.core.webhdfs import WebHdfsFile
from concurrent.futures import ThreadPoolExecutor

from pyspark.serializers import write_with_length
import sqlparse


class DataTypes:
    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    INT96 = 3
    FLOAT = 4
    DOUBLE = 5
    BYTE_ARRAY = 6
    FIXED_LEN_BYTE_ARRAY = 7

    type = {'int64': INT64, 'float64': DOUBLE}

def read_col(pf, rg, col):
    return pf.read_row_group(rg, columns=[col])


def read_parallel(f, rg, columns):
    pf = pyarrow.parquet.ParquetFile(f)
    executor = ThreadPoolExecutor(max_workers=len(columns))
    futures = list()
    for col in columns:
        fin = f.copy()
        pfin = pyarrow.parquet.ParquetFile(fin, metadata=pf.metadata)
        futures.append(executor.submit(read_col, pfin, rg, col))

    return [r.result().column(0) for r in futures]

class TpchSQL:
    def __init__(self, config):
        f = WebHdfsFile(config['url'])
        pf = pyarrow.parquet.ParquetFile(f)
        tokens = sqlparse.parse(config['query'])[0].flatten()
        sql_columns = set([t.value for t in tokens if t.ttype in [sqlparse.tokens.Token.Name]])
        columns = [col for col in pf.schema_arrow.names if col in sql_columns]
        print(columns)
        rg = int(config['row_group'])
        tbl = pyarrow.Table.from_arrays(read_parallel(f, rg, columns), names=columns)
        self.df = duckdb.from_arrow_table(tbl).query("arrow", config['query']).fetchdf()
        print(f'Computed df {self.df.shape}')

    def to_spark(self, outfile):
        header = numpy.empty(len(self.df.columns) + 1, numpy.int64)
        dtypes = [DataTypes.type[self.df.dtypes[c].name] for c in self.df.columns]
        header[0] = len(self.df.columns)
        i = 1
        for t in dtypes:
            header[i] = t
            i += 1

        write_with_length(header.byteswap().newbyteorder().tobytes(), outfile)
        for col in self.df.columns:
            data = self.df[col].to_numpy().byteswap().newbyteorder().tobytes()
            header = numpy.empty(4, numpy.int32)
            header[0] = DataTypes.type[self.df.dtypes[col].name]
            header[1] = 0  # Used for FIXED_LEN_BYTE_ARRAY ONLY
            header[2] = len(data)
            header[3] = 0  # Compressed len
            outfile.write(header.byteswap().newbyteorder().tobytes())
            outfile.write(data)
