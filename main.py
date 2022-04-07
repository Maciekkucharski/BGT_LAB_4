import dask.dataframe as dd
from dask.distributed import Client
import time


client = Client("<insert_ip_v4>:8786")

df = dd.read_parquet('data*/new_*.parquet', engine="pyarrow")

start_time = time.time()
df = df['repo_name'].explode()
df = df.value_counts(ascending=True)
print(df.compute())
print(f"time: {time.time() - start_time}")

print(client)

