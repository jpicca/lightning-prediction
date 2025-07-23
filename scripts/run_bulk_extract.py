import coiled
from dask.distributed import Client
import datetime
from lightning_features import extract_features

def main():
    cluster = coiled.Cluster(
        name="lightning-training-extract",
        software="lightning-pipeline",
        n_workers=8,
        region="us-east-1",
    )
    client = Client(cluster)

    # Define training date range
    start = datetime.datetime(2021, 1, 1, 0)
    end = datetime.datetime(2021, 1, 2, 0)

    time_cursor = start
    while time_cursor < end:
        next_time = time_cursor + datetime.timedelta(hours=1)
        time_range = (time_cursor, next_time)
        output_path = f"s3://your-bucket/features/{time_cursor:%Y%m%d%H}.zarr"
        print(f"Processing {time_cursor:%Y-%m-%d %H:%M}")
        extract_features(time_range, output_path)
        time_cursor = next_time

    client.close()
    cluster.close()

if __name__ == "__main__":
    main()