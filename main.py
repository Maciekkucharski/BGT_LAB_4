import dask.dataframe as dd
from dask.distributed import Client
from dask_cloudprovider.gcp import GCPCluster
import time

with GCPCluster(on_host_maintenance="STOP", env_vars=dict(EXTRA_PIP_PACKAGES="pyarrow gcsfs blosc lz4"),
                projectid="bgt_lab4",
                n_workers=3, machine_type="e2-small") as cluster:
    with Client(cluster) as client:
        df = dd.read_parquet(
            ["./data1/new_*.parquet", "./data2/new_*.parquet", "./data3/new_*.parquet", "./data4/new_*.parquet"],
            engine="pyarrow")
        start_time = time.time()
        df = df['repo_name'].explode()
        df = df.value_counts()
        print(df.compute())
        print(f"time: {time.time() - start_time}")
        print(client)

