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
            'time': time.strftime("%H:%M:%S")}

if __name__ == '__main__':
    # Set processes=True - Multiple independence processes with different process IDs will be used.
    # Set n_workers=2 - Number of workers to start
    cluster = LocalCluster(processes=True ,n_workers=2)
    client = Client(cluster)

    print('client:', client)

    # Submit 4 jobs. slow_increment(0), slow_increment(1), slow_increment(2), slow_increment(3)
    for future in as_completed(client.map(slow_increment, range(4))): 
        result = future.result()
        pprint(result)
