import time
import logging
import socket
import os
from pprint import pprint
from dask.distributed import Client, as_completed, LocalCluster

def sum_square(x,y,z):
    time.sleep(5)
    return {'result': [x,y,z,x**2 + y**2+z**2],
            'host': socket.gethostname(),
            'pid': os.getpid(),
            'time': time.strftime("%H:%M:%S")}

if __name__ == '__main__':
    cluster = LocalCluster(processes=2 ,n_workers=2)
    client = Client(cluster)

    print('client:', client)

    for future in as_completed(client.map(sum_square, range(0,100), tuple(i for i in range(100,200)), tuple(i for i in range(200,300)))): # FIX
        result = future.result()
        pprint(result)