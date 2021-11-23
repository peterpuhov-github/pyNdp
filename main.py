import dike.server.ndp_server

if __name__ == '__main__':
    # Launch Spark worker simulation
    dike.server.ndp_server.serve_forever('dikeHDFS.xml')


