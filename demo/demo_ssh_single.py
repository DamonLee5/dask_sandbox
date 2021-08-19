import time
import logging
import socket
import os
from pprint import pprint
# import dask.config
# import dask.distributed
from dask.distributed import Client, as_completed,SSHCluster

# dask.config.set({"distributed.admin.tick.limit":'3h'})
def slow_increment(x):
    time.sleep(2)
    return {'result': x + 1,
            'host': socket.gethostname(),
            'pid': os.getpid(),
            'time': time.strftime("%H:%M:%S")}

if __name__ == '__main__':
    # SSHCluster webpage: https://docs.dask.org/en/latest/setup/ssh.html
    # Reserve 4 nodes. Use one node(localhost) to run this script.
    # hosts: List of hostnames or addresses on which to launch our cluster. The first will be used for the scheduler and the rest for workers.
    # We have 12 workers here, for each worker, we assign 8 threads to it.   
    cluster = SSHCluster( 
    hosts=["localhost", "localhost", "localhost", "localhost"],
    connect_options={"username":'xxx', "password":"xxx","known_hosts": None},
    worker_options={"nthreads": 1},
    scheduler_options={"port": 0, "dashboard_address": ":8797"}
)
    client = Client(cluster)

    nb_workers = 0
    while True:
        nb_workers = len(client.scheduler_info()["workers"])
        print('Got {} workers'.format(nb_workers))
        if nb_workers >= 3:
            break
        time.sleep(1)

    # futures = client.map(slow_increment, range(0,1000,5))

    print('client:', client)

    for future in as_completed(client.map(slow_increment, range(0,20))): # FIX
        result = future.result()
        pprint(result)