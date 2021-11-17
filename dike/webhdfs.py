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
    def __init__(self, name, user, buffer_size=(128 << 10)):
        self.name = name
        self.user = user
        self.buffer_size = buffer_size
        self.mode = 'rb'
        self.closed = False
        self.offset = 0
        self.url = urllib.parse.urlparse(self.name)
        self.conn = http.client.HTTPConnection(self.url.netloc)

        req = f'/webhdfs/v1/{self.url.path}?op=GETFILESTATUS'
        self.conn.request("GET", req)
        resp = self.conn.getresponse()
        resp_data = resp.read()
        file_status = json.loads(resp_data)['FileStatus']
        self.size = file_status['length']

        req = f'/webhdfs/v1{self.url.path}?op=OPEN&user.name={self.user}&buffersize={self.buffer_size}'
        self.conn.request("GET", req)
        resp = self.conn.getresponse()
        self.data_url = urllib.parse.urlparse(resp.headers['Location'])
        # print(self.data_url)
        self.conn.close()
        # Open connection to Data Node
        self.conn = http.client.HTTPConnection(self.data_url.netloc)
        query = self.data_url.query.split('&offset=')[0]
        self.data_req = f'{self.data_url.path}?{query}'

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
        req = f'{self.data_req}&offset={pos}&length={length}'
        self.conn.request("GET", req)
        resp = self.conn.getresponse()
        return resp.read(length)

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

