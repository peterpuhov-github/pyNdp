import io
import json
import threading
import http.server
from http import HTTPStatus
import xml.etree.ElementTree
import urllib.parse
from collections import OrderedDict
from fastparquet import ParquetFile
import pickle
import dike.webhdfs
import dike.code_factory

DIKE_CONFIG = {}


class NdpMasterRequestHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send_name_node_request(self):
        conn = http.client.HTTPConnection(DIKE_CONFIG['dfs.namenode.http-address'])
        conn.request("GET", self.path, '', self.headers)
        response = conn.getresponse()
        data = response.read()
        conn.close()
        return response, data

    def do_GET(self):
        print('Path', self.path)
        if 'ReadParam' in self.headers:
            print('ReadParam', self.headers['ReadParam'])

        url = urllib.parse.urlparse(self.path)
        query = url.query.split('&')
        user = None
        for q in query:
            if 'user.name=' in q:
                user = q.split('user.name=')[1]

        netloc = DIKE_CONFIG['dfs.namenode.http-address']
        f = dike.webhdfs.WebHdfsFile(f'webhdfs://{netloc}/{self.path}', user=user)
        reader = io.BufferedReader(f, buffer_size=(1 << 20))
        pf = ParquetFile(reader)
        finfo = OrderedDict()
        finfo['columns'] = pf.columns
        finfo['dtypes'] = [pf.dtypes[c].name for c in pf.columns]
        finfo['row_group_count'] = len(pf.row_groups)

        spark_worker_command = dike.code_factory.create_spark_worker_command()
        finfo['spark_worker_command'] = spark_worker_command.hex()
        finfo_json = json.dumps(finfo)
        print(finfo_json)
        print(len(pf.row_groups))
        self.send_response(HTTPStatus.OK)
        self.end_headers()
        self.wfile.write(finfo_json.encode())


class NdpDataRequestHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):

        fs = HadoopFileSystem('172.18.0.3', port=9000, user='peter', replication=1, driver='libhdfs3')

        with fs.open_input_file('/tpch-test-parquet/lineitem.parquet') as f:
            parquet_file = pyarrow.parquet.ParquetFile(f)
            print('parquet_file.num_row_groups', parquet_file.num_row_groups)


        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b'OK')


class NdpNode(threading.Thread):
    def __init__(self, address, handler):
        threading.Thread.__init__(self)
        self.address = address
        self.handler = handler

    def run(self):
        httpd = http.server.ThreadingHTTPServer(self.address, self.handler)
        httpd.serve_forever()


def serve_forever(config_file):
    tree = xml.etree.ElementTree.parse(config_file)
    root = tree.getroot()

    for config_property in root[0].findall('property'):
        name = config_property.find('name').text
        value = config_property.find('value').text
        print(name, ':', value)
        DIKE_CONFIG[name] = value

    # Launch HDFS Node servers
    servers = [
        (('', int(DIKE_CONFIG['dike.pyndp.master-port'])), NdpMasterRequestHandler),
        (('', int(DIKE_CONFIG['dike.pyndp.data-port'])), NdpDataRequestHandler)
    ]

    threads = [NdpNode(s[0], s[1]) for s in servers]
    for th in threads:
        th.daemon = True
        th.start()

    for th in threads:
        th.join()

if __name__ == '__main__':
    # Launch Spark worker simulation
    serve_forever('../../dikeHDFS.xml')