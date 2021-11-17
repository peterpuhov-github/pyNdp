import threading
import http.server
import xml.etree.ElementTree
import urllib.parse
import pyarrow
from pyarrow.fs import HadoopFileSystem

DIKE_CONFIG = {}


class NdpMasterRequestHandler(http.server.BaseHTTPRequestHandler):
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

        response, data = self.send_name_node_request()
        self.send_response(response.status, response.reason)
        for key in response.headers.keys():
            if key == 'Location':
                location = response.headers[key]
                url = urllib.parse.urlparse(location)
                url_split = list(urllib.parse.urlsplit(location))
                url_split[1] = "{}:{}".format(DIKE_CONFIG['dike.pyndp.http-ip'], DIKE_CONFIG['dike.pyndp.data-port'])
                ndp_location = urllib.parse.urlunsplit(url_split)
                print(key, ndp_location)
                self.send_header(key, ndp_location)
            else:
                self.send_header(key, response.headers[key])

        self.end_headers()
        self.wfile.write(data)


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

