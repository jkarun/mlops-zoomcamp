import pandas as pd
from datetime import datetime
import os

import batch_q5

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

local_aws_url = 'http://localhost:4566'
# S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', local_aws_url)
S3_ENDPOINT_URL = local_aws_url

options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
}


data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2), dt(1, 10)),
    (1, 2, dt(2, 2), dt(2, 3)),
    (None, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (2, 3, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),     
]

columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
df_input = pd.DataFrame(data, columns=columns)

input_file = batch_q5.get_input_path(2023, 3)
print('input path: ', input_file)

df_input.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
)
