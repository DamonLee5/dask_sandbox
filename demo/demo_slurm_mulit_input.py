import time
import logging
import socket
import os
from pprint import pprint
from dask.distributed import Client, as_completed
from dask_jobqueue import SLURMCluster

def sum_square(x,y,z):
    time.sleep(5)
    return {'result':[x,y,z,x**2 + y**2+z**2],
            'host': socket.gethostname(),
            'pid': os.getpid(),
            'time': time.strftime("%H:%M:%S")}

if __name__ == '__main__':
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


    print('client:', client)

    for future in as_completed(client.map(sum_square, range(0,100), tuple(i for i in range(100,200)), tuple(i for i in range(200,300)))): # FIX
        result = future.result()
        pprint(result)