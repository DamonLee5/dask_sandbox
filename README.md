# dask_sandbox
## Installation
1) Clone Repository and enter
```
git clone https://github.com/DamonLee5/dask_sandbox.git
cd dask_sandbox
```

2) Create conda environment
```
conda create -n dask_sandbox python=3.8
```
3) Activate conda environment
```
conda activate dask_sandbox
```
4) Install requirements
```
pip install -r requirements.txt
```

## Run demo
1) Run demo
```
python demo_local_single.py # Run on your local computer
python demo_slurm_single.py # Run on a slurm cluster.
```
