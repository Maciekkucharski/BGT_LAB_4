import dask.dataframe as dd
from dask.distributed import Client
from dask_cloudprovider.gcp import GCPCluster
import time

with GCPCluster(on_host_maintenance="STOP", env_vars=dict(EXTRA_PIP_PACKAGES="pyarrow gcsfs blosc lz4"),
                projectid="bgt_lab4",
                n_workers=3, machine_type="e2-small") as cluster:
    with Client(cluster) as client:
        df = dd.read_parquet(
            ["gs://buc21474bgt/data/*", "gs://buc21474bgt/data2/*", "gs://buc21474bgt/data3/*",
             "gs://buc21474bgt/data4/*"], storage_options={'anon': True, 'use_ssl': False}, engine="pyarrow")
        start_time = time.time()
        df = df['repo_name'].explode()
        df = df.value_counts()
        print(df.compute())
        print(f"time: {time.time() - start_time}")
        print(client)

