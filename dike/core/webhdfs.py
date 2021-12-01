import os
import urllib.parse
import http.client
import json

'''
from pywebhdfs.webhdfs import PyWebHdfsClient
fs = PyWebHdfsClient(host='dikehdfs', port='9870', user_name='peter')
file_status = fs.get_file_dir_status(path)['FileStatus']
size = self.file_status['length']
fs.read_file(path, offset=pos, length=length, buffersize=8192)
'''

class WebHdfsFile(object):
    def __init__(self, name, user, buffer_size=(128 << 10), size=None, data_url=None, data_req=None):
        self.name = name
        self.user = user
        self.buffer_size = buffer_size
        self.mode = 'rb'
        self.closed = False
        self.offset = 0
        self.CACHE_SIZE = 8 << 20
        self._cache_data = bytearray(self.CACHE_SIZE)
        self._cache_view = memoryview(self._cache_data)
        self._cache_offset = -1
        self.read_stats = []
        self.read_bytes = 0

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

    def copy(original):  # Copy constructor
        return WebHdfsFile(original.name, original.user, original.buffer_size, original.size, original.data_url, original.data_req)

    def seek(self, offset, whence=0):
        # print("Attempt to seek {} from {}".format(offset, whence))
        if whence == os.SEEK_SET:
            self.offset = offset
        elif whence == os.SEEK_CUR:
            self.offset += offset
        elif whence == os.SEEK_END:
            self.offset = self.size + offset

        return self.offset

    def read(self, length=-1):
        # print(f"Attempt to read from {self.offset} len {length}")
        pos = self.offset
        if length == -1:
            length = self.size - pos

        self.offset += length
        self.read_bytes += length
        # Check if we have data in cache
        if 0 <= self._cache_offset <= pos and \
            pos + length < self._cache_offset + self.CACHE_SIZE:
            begin = pos - self._cache_offset
            end = begin + length
            return self._cache_data[begin:end]

        # We have a cache miss
        if self.size - pos > self.CACHE_SIZE:
            self.read_stats.append((pos, self.CACHE_SIZE))
            req = f'{self.data_req}&offset={pos}&length={self.CACHE_SIZE}'
            conn = http.client.HTTPConnection(self.data_url.netloc, blocksize=self.buffer_size)
            conn.request("GET", req)
            resp = conn.getresponse()
            # resp.readinto(self._cache_data)
            resp.readinto(self._cache_view)
            conn.close()
            self._cache_offset = pos
            return self._cache_data[:length]

        self.read_stats.append((pos, length))

        req = f'{self.data_req}&offset={self.size - self.CACHE_SIZE}&length={self.CACHE_SIZE}'
        conn = http.client.HTTPConnection(self.data_url.netloc)
        conn.request("GET", req)
        resp = conn.getresponse()
        # resp.readinto(self._cache_data)
        resp.readinto(self._cache_view)
        conn.close()
        self._cache_offset = self.size - self.CACHE_SIZE
        begin = pos - self._cache_offset
        end = begin + length
        return self._cache_data[begin:end]


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

