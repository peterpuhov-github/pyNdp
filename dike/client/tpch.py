import io
import pandas as pd
import numpy
from pyspark.serializers import write_with_length, write_int

from fastparquet import ParquetFile
import dike.webhdfs


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


class TpchQ14:
    def __init__(self, file_name: str, row_group: int):
        dike_file = dike.webhdfs.WebHdfsFile(f'webhdfs://dikehdfs:9860/{file_name}', user='peter')
        f = io.BufferedReader(dike_file, buffer_size=(1 << 20))
        pf = ParquetFile(f)
        filter_columns = ['l_shipdate']
        projection_columns = ['l_partkey', 'l_extendedprice', 'l_discount']
        total_columns = filter_columns + projection_columns
        # Align projection with Parquet scema
        total_columns = [c for c in pf.columns if c in total_columns]
        print(total_columns)
        self.df = pf.read_row_group_file(pf.row_groups[row_group], total_columns, {})
        print(f'Total rows {self.df.shape[0]}')
        self.df = self.df[(self.df['l_shipdate'] >= '1995-09-01') & (self.df['l_shipdate'] < '1995-10-01')]
        print(f'Filtered rows {self.df.shape[0]}')
        self.df = self.df[projection_columns]
        print(f'projected columns {self.df.columns}')

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

