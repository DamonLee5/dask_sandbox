import time
import numpy as np
from dask_jobqueue import SLURMCluster
from dask import delayed
from dask.distributed import Client, as_completed
import logging
import socket
import os
from pprint import pprint
# config in $HOME/.config/dask/jobqueue.yaml
cluster = SLURMCluster(project='bouman',processes=5 ,n_workers=20,walltime='01:00:00', memory='7 GB',
                       death_timeout=60, job_extra=['--nodes=20', '--ntasks-per-node=1'],
                       cores=20)
cluster.scale(jobs=2)
client = Client(cluster)

# each job takes 1s, and we have 4 cpus * 1 min * 60s/min = 240 cpu.s, let's ask for a little more tasks.
filenames = [f'img{num}.jpg' for num in range(4800)]

def features(num_fn):
    num, image_fn = num_fn
    time.sleep(1)  # takes about 1s to compute features on an image
    features = np.random.random(5)
    return {'num': num,
            'host': socket.gethostname(),
            'pid': os.getpid(),
            'features': features,
            'time': int(time.time())}

num_files = len(filenames)
num_features = 5 # FIX


for future in as_completed(client.map(features, list(enumerate(filenames)))): # FIX
    result = future.result()
    pprint(result)


