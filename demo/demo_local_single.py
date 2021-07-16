import time
import logging
import socket
import os
from pprint import pprint
from dask.distributed import Client, as_completed, LocalCluster

def slow_increment(x):
    time.sleep(5)
    return {'result': x + 1,
            'host': socket.gethostname(),
            'pid': os.getpid(),
            'time': time.strftime("%H:%M:%S")}#time.strftime("%H:%M:%S")

if __name__ == '__main__':
    cluster = LocalCluster(processes=2 ,n_workers=2)
    client = Client(cluster)

    print('client:', client)

    for future in as_completed(client.map(slow_increment, range(4))): # FIX
        result = future.result()
        pprint(result)