import os
import urllib.parse
import http.client
import json
from concurrent.futures import ThreadPoolExecutor


class HttpReader(object):
    def __init__(self, netloc, req, offset, length):
        self
class WebHdfsFile(object):
    def __init__(self, name, user, buffer_size=(128 << 10), size=None, data_url=None, data_req=None):
        self.name = name
        self.user = user
        self.buffer_size = buffer_size
        self.mode = 'rb'
        self.closed = False
        self.offset = 0
        self.cache = None
        # self.executor = ThreadPoolExecutor(max_workers=1)
        self.read_stats = []
        self.read_bytes = 0
        self.verbose = False

        if size is None: # Support for copy constructor
            self.url = urllib.parse.urlparse(self.name)
            self.conn = http.client.HTTPConnection(self.url.netloc)
            req = f'/webhdfs/v1/{self.url.path}?op=GETFILESTATUS'
            self.conn.request("GET", req)
            resp = self.conn.getresponse()
            resp_data = resp.read()
            file_status = json.loads(resp_data)['FileStatus']
            self.size = file_status['length']
        else:
            self.size = size

        if data_req is None or data_url is None: # Support for copy constructor
            req = f'/webhdfs/v1{self.url.path}?op=OPEN&user.name={self.user}&buffersize={self.buffer_size}'
            self.conn.request("GET", req)
            resp = self.conn.getresponse()
            self.data_url = urllib.parse.urlparse(resp.headers['Location'])
            # print(self.data_url)
            self.conn.close()
            # Open connection to Data Node
            # self.conn = http.client.HTTPConnection(self.data_url.netloc)
            query = self.data_url.query.split('&offset=')[0]
            self.data_req = f'{self.data_url.path}?{query}'
        else:
            self.data_req = data_req
            self.data_url = data_url

    def enable_cache(self):
        self.cache = dict()
        self.cache['CACHE_SIZE'] = 4 << 20
        self.cache['_cache_data'] = bytearray(self.cache['CACHE_SIZE'])
        self.cache['_cache_view'] = memoryview(self.cache['_cache_data'])
        self.cache['_cache_offset'] = -1

    def copy(self):  # Copy constructor
        f = WebHdfsFile(self.name, self.user, self.buffer_size, self.size, self.data_url, self.data_req)
        if self.cache is not None:
            f.enable_cache()

        return f

    def seek(self, offset, whence=0):
        #print("Attempt to seek {} from {}".format(offset, whence))
        if whence == os.SEEK_SET:
            self.offset = offset
        elif whence == os.SEEK_CUR:
            self.offset += offset
        elif whence == os.SEEK_END:
            self.offset = self.size + offset

        return self.offset

    def read(self, length=-1):
        if self.verbose:
            print(f"Attempt to read from {self.offset} len {length}")
        pos = self.offset
        if length == -1:
            length = self.size - pos

        self.offset += length
        self.read_bytes += length
        cache_size = 0
        if self.cache is not None:
            cache_size = self.cache['CACHE_SIZE']

        if length > cache_size:  # Cache is disabled or not big enough
            self.read_stats.append((pos, length))
            req = f'{self.data_req}&offset={pos}&length={length}'
            conn = http.client.HTTPConnection(self.data_url.netloc, blocksize=self.buffer_size)
            conn.request("GET", req)
            resp = conn.getresponse()
            data = resp.read(length)
            conn.close()
            return data


        # Check if we have data in cache
        if 0 <= self.cache['_cache_offset'] <= pos and \
            pos + length < self.cache['_cache_offset'] + cache_size:
            begin = pos - self.cache['_cache_offset']
            end = begin + length
            return self.cache['_cache_data'][begin:end]

        # We have a cache miss
        if self.size - pos > self.cache['CACHE_SIZE']:
            self.read_stats.append((pos, self.cache['CACHE_SIZE']))
            req = f'{self.data_req}&offset={pos}&length={cache_size}'
            conn = http.client.HTTPConnection(self.data_url.netloc, blocksize=self.buffer_size)
            conn.request("GET", req)
            resp = conn.getresponse()
            # resp.readinto(self.cache['_cache_data'])
            resp.readinto(self.cache['_cache_view'])
            conn.close()
            self.cache['_cache_offset'] = pos
            return self.cache['_cache_data'][:length]

        self.read_stats.append((pos, length))

        req = f'{self.data_req}&offset={self.size - cache_size}&length={self.cache["CACHE_SIZE"]}'
        conn = http.client.HTTPConnection(self.data_url.netloc)
        conn.request("GET", req)
        resp = conn.getresponse()
        # resp.readinto(self.cache['_cache_data'])
        resp.readinto(self.cache['_cache_view'])
        conn.close()
        self.cache['_cache_offset'] = self.size - cache_size
        begin = pos - self.cache['_cache_offset']
        end = begin + length
        return self.cache['_cache_data'][begin:end]

    def readinto(self, b):
        buffer = self.read(len(b))
        length = len(buffer)
        b[:length] = buffer
        return length

    def tell(self):
        return self.offset

    def seekable(self):
        return True

    def readable(self):
        return True

    def writable(self):
        return False

    def close(self):
        self.conn.close()

