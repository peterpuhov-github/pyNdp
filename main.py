import dike.ndp_server

if __name__ == '__main__':
    # Launch Spark worker simulation
    dike.ndp_server.serve_forever('dikeHDFS.xml')


