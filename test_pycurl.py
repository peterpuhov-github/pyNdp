import time
import threading
import http.client


def read(buffer, offset, length):
    req = '/webhdfs/v1/tpch-test-parquet-1g/lineitem.parquet/part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet?op=OPEN&user.name=peter&namenoderpcaddress=dikehdfs:9000&buffersize=131072'
    r = f"{req}&offset={offset}&length={length}"
    conn = http.client.HTTPConnection('172.18.0.100:9864', blocksize=128 << 10)
    conn.request("GET", r)
    resp = conn.getresponse()
    resp.readinto(buffer)
    conn.close()


if __name__ == '__main__':
    length = 8 << 20
    total_bytes = 0

    start = time.time()
    threads = []
    # buffer = bytearray(8 << 20)
    total_bytes = 0
    for offset in range(0, length*8, length):
        buffer = bytearray(8 << 20)
        th = threading.Thread(target=read, args=(buffer, offset, length))
        total_bytes += length
        th.start()
        threads.append(th)

    for th in threads:
        th.join()

    end = time.time()
    print(f'HTTP \t Received {total_bytes} bytes in {end - start} secs {(total_bytes/(1<<20)) / (end - start)} MB/s')

