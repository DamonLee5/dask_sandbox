import time
import logging
import socket
import os
from pprint import pprint
# import dask.config
# import dask.distributed
from dask.distributed import Client, as_completed

from dask_jobqueue import SLURMCluster

# dask.config.set({"distributed.admin.tick.limit":'3h'})
def slow_increment(x):
    time.sleep(5)
    return {'result': x + 1,
            'host': socket.gethostname(),
            'pid': os.getpid(),
            'time': int(time.time() % 100)}


cluster = SLURMCluster(project='bouman',processes=5 ,n_workers=20,walltime='01:00:00', memory='7 GB',
                       death_timeout=60, job_extra=['--nodes=20', '--ntasks-per-node=1'],
                       cores=20)

cluster.scale(jobs=2)
print(cluster.job_script())
client = Client(cluster)

nb_workers = 0
while True:
    nb_workers = len(client.scheduler_info()["workers"])
    print('Got {} workers'.format(nb_workers))
    if nb_workers >= 2:
        break
    time.sleep(1)

# futures = client.map(slow_increment, range(0,1000,5))

print('client:', client)

for future in as_completed(client.map(slow_increment, range(0,1000,5))): # FIX
    result = future.result()
    pprint(result)