import time
import dike.client.tpch
import cProfile
import pstats
from pstats import SortKey

if __name__ == '__main__':
    fname = '/tpch-test-parquet-1g/lineitem.parquet/' \
            'part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet'

    start = time.time()
    q14 = dike.client.tpch.TpchQ14(fname, 0)
    # cProfile.run('q14 = dike.client.tpch.TpchQ14(fname, 0)', 'profile.txt')
    end = time.time()
    # p = pstats.Stats('profile.txt')
    # p.sort_stats(SortKey.CUMULATIVE).print_stats(10)
    # p.sort_stats(SortKey.TIME).print_stats(5)

    print(f"Run time is: {end - start} secs")
