import io
import sys
import time
import gc
import pandas as pd
from fastparquet import ParquetFile

import dike.webhdfs
import dike.util

if __name__ == '__main__':
    fname = '/tpch-test-parquet-1g/lineitem.parquet/' \
            'part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet'

    # TPC-H 100 GB
    # fname = '/tpch-test-parquet/lineitem.parquet'

    dike_file = dike.webhdfs.WebHdfsFile(f'webhdfs://dikehdfs:9860/{fname}', user='peter')
    f = io.BufferedReader(dike_file, buffer_size=(1 << 20))
    pf = ParquetFile(f)

    projection = ['l_orderkey', 'l_quantity']

    print('pf.columns', pf.columns)
    print('pf.categories', pf.categories)
    print('len(pf.row_groups)', len(pf.row_groups))

    start = time.time()
    read_time = 0
    df_list = []
    for i in range(0, len(pf.row_groups)):
        print('Loading rg', i)
        start_read = time.time()
        df = pf.read_row_group_file(pf.row_groups[i], projection, pf.categories)
        read_time += time.time() - start_read
        # df.set_index('l_orderkey')
        df = df.groupby('l_orderkey').sum()
        df_list.append(df)
        memory_usage_mb = dike.util.get_memory_usage_mb()
        print("Memory usage MB", memory_usage_mb)
        if memory_usage_mb > 4096:  # 4GB limit
            break

    # df = pd.concat(df_list, ignore_index=False)
    # print('Combined df size MB', sys.getsizeof(df) >> 20)
    for df in df_list:
        #  print(f'Min {df.index.min()} Max {df.index.max()}')
        pass

    for i in range(0, len(df_list) - 1):
        print('Merging to rg', i)
        df1 = df_list[i]
        df2 = df_list[i+1]
        max_index = df1.index.max()
        df_merge = df2.loc[df2.index <= max_index]
        for idx in df_merge.index:
            #  print(df1.at[idx, 'l_quantity'])
            df1.at[idx, 'l_quantity'] += df2.at[idx, 'l_quantity']
            #  print(df1.at[idx, 'l_quantity'])

        #  print(f'Rows {df2.shape[0]}')
        df2.drop(df_merge.index, axis=0, inplace=True)
        #  print(f'Rows {df2.shape[0]}')

    #  print("Data memory MB", (row * 16) >> 20)

    row = 0
    for i in range(0, len(df_list)):
        #  print('Filtering rg', i)
        df_list[i] = df_list[i].loc[df_list[i]['l_quantity'] > 300]
        row += df_list[i].shape[0]

    #df = pd.concat(df_list, ignore_index=False)

    #row, col = df.shape
    print(f"Rows {row}")

    end = time.time()
    print(f"Run time is: {end - start} secs")

    print(f"Read time is: {read_time} secs")
    # pd.set_option('display.max_rows', df.shape[0] + 1)
    # print(df)

    '''
    for i, row in df.iterrows():
        print(f"{i},{row['l_quantity']},")
    '''
