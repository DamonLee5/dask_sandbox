import time
import logging
import socket
import os
from pprint import pprint
from dask.distributed import Client, as_completed, LocalCluster

# Example of how to use Dask to run separate processes and combine results.

# Define function that returns dictionary containing host name, PID, and time.
def slow_increment(x):
    time.sleep(2)
    return {
            'input': x, # function input
            'result': x + 1, # function output
            'host': socket.gethostname(), # host name
            'pid': os.getpid(), # process ID
            'time': time.strftime("%H:%M:%S")} # time stamp

if __name__ == '__main__':
    # Set processes=True - Multiple independence processes with different process IDs will be used.
    # Reference: `https://docs.dask.org/en/latest/setup/single-distributed.html?highlight=LocalCluster#localcluster`
    # Set n_workers=4 - Use 4 processors. 
    # Set threads_per_worker=2: Number of threads per worker
    # Total number of jobs computed parallelly is n_workers*threads_per_worker
    # In this case you will see 8 jobs run parallelly at the same time.
    cluster = LocalCluster(processes=True ,n_workers=4, threads_per_worker=2) # This creates a “cluster” of a scheduler and workers running on the local machine.
    
    # reference: `https://distributed.dask.org/en/latest/api.html#distributed.Client`
    client = Client(cluster) # An interface that connect to and submit computation to cluster
    print('client:', client)

    # Call the function slow_increments with 16 different arguments and map to workers
     
    # client.map() map the function "slow_increment" to a sequence of input arguments.
    # reference: `https://distributed.dask.org/en/latest/api.html#distributed.Client.map`
    # The class object as_completed() returns outputs in the order in which they complete
    # reference: `https://distributed.dask.org/en/latest/api.html#distributed.as_completed`
    for output in as_completed(client.map(slow_increment, range(16))): 
        # Collect output of each job and combin into result
        result = output.result()
        # Print the result and time stamp of all 16 jobs
        #print("output object = ",output)
        pprint(result)
