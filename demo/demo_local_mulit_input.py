import time
import logging
import socket
import os
from pprint import pprint
from dask.distributed import Client, as_completed, LocalCluster

def sum_square(x,y,z):
    time.sleep(2)
    return {
            'input': [x,y,z],
            'result': x**2 + y**2+z**2,
            'host': socket.gethostname(),
            'pid': os.getpid(),
            'time': time.strftime("%H:%M:%S")}

if __name__ == '__main__':
    cluster = LocalCluster(processes=True ,n_workers=4)
    client = Client(cluster)

    print('client:', client)
    # In this example client.map() map the function to multiple input argument sequences. 
    for output in as_completed(client.map(sum_square, range(0,4), range(1,5), range(2,6))):
        result = output.result()
        pprint(result)
