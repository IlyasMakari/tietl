import os
import fsspec

def get_fs():
    access_key = os.getenv('S3_ACCESS_KEY')
    secret_key = os.getenv('S3_SECRET_KEY')
    endpoint_url = os.getenv('S3_ENDPOINT')
    fs = fsspec.filesystem(
        "s3",
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": endpoint_url}
    )
    return fs