import pandas as pd
import numpy


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


if __name__ == '__main__':
    df = pd.DataFrame({'A': [1, 1, 2, 2],
                       'B': [1, 2, 3, 4],
                       'C': numpy.random.randn(4)
                      })

    # print(df.to_numpy())
    columns = len(df.columns)
    dtypes = [DataTypes.type[df.dtypes[c].name] for c in df.columns]
    print(dtypes)

    header = numpy.empty(columns + 1, numpy.int64)
    header[0] = columns
    i = 1
    for t in dtypes:
        header[i] = t
        i += 1

    print(header)
    print(header.tobytes())
    be_header = header.byteswap().newbyteorder()
    print(be_header)
    print(be_header.tobytes())


