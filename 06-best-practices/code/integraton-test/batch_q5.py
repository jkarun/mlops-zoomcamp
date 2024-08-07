import pickle
import pandas as pd
import os
import sys

local_aws_url = 'http://localhost:4566'
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', local_aws_url)

options = {
            'client_kwargs': {
                'endpoint_url': S3_ENDPOINT_URL
            }
        }

DEFAULT_INPUT_FILE_PATTERN="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet" 
DEFAULT_OUTPUT_FILE_PATTERN="s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"

def prepare_data(df, categorical):  
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def read_data(filename, categorical):
    
    if S3_ENDPOINT_URL is not None:
        df = pd.read_parquet(filename, storage_options=options)
    else:
        df = pd.read_parquet(filename)
    
    return prepare_data(df, categorical)
    

def get_input_path(year, month):
    default_input_pattern = 's3://nyc-duration/trip-data/in/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', DEFAULT_INPUT_FILE_PATTERN)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration/trip-data/out/year={year:04d}/month={month:02d}/predictions.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', DEFAULT_OUTPUT_FILE_PATTERN)
    return output_pattern.format(year=year, month=month)


def main(year, month):
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    # Load the model and vectorizer
    try:
        with open('model.bin', 'rb') as f_in:
            dv, model = pickle.load(f_in)
    except FileNotFoundError:
        print("Error: The model file 'model.bin' was not found.")
        exit(1)

    categorical = ['PULocationID', 'DOLocationID']

    print("Reading data...")
    df = read_data(input_file, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    
    
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)

    print("Making predictions...")
    y_pred = model.predict(X_val)

    print(f"Predicted mean duration: {y_pred.mean():.2f}")

    # Prepare a DataFrame with the ride_id and predicted_duration
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    print("Saving results...")
    df_result.to_parquet(output_file, engine='pyarrow', index=False, storage_options=options)

    print("Process completed successfully.")

if __name__ == '__main__':
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year, month)
