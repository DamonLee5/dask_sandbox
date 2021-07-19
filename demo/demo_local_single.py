import time
import logging
import socket
import os
from pprint import pprint
from dask.distributed import Client, as_completed, LocalCluster

# Example of how to use Dask to run separate processes and combine results.

# Define function that returns dictionary containing host name, PID, and time.
def slow_increment(x):
    time.sleep(1)
    return {'result': x + 1,
            'host': socket.gethostname(),
            'pid': os.getpid(),
            'time': time.strftime("%H:%M:%S")}

if __name__ == '__main__':
    # Set processes=True - Multiple independence processes with different process IDs will be used.
    # Set n_workers=2 - Number of workers to start
    cluster = LocalCluster(processes=True ,n_workers=4)
    client = Client(cluster)

    print('client:', client)

    # Call the function slow_increments with 4 different arguments and map to workers
    for output in as_completed(client.map(slow_increment, range(4))): 
        # Collect output of each job and combin into result
        result = output.result()
        # Print the result of all 4 jobs
        pprint(output)
