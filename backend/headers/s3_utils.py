import pandas as pd
import boto3
import json
from io import StringIO, BytesIO
import os
from dotenv import load_dotenv # Import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize a global S3 client. Boto3 will automatically pick up AWS credentials
S3_REGION = os.environ.get('AWS_REGION', 'us-east-1') # Default to us-east-1 if not set
CONFIG = boto3.session.Config(
    connect_timeout=300,  # 5 minutes for connection establishment
    read_timeout=300,     # 5 minutes for reading data
    retries={'max_attempts': 10} # Increase max retry attempts
)
s3_client = boto3.client('s3', region_name=S3_REGION, config=CONFIG, verify=False)

def _get_s3_bucket_name(bucket_name: str = None) -> str:
    """
    Helper function to get the S3 bucket name, prioritizing the function argument,
    then an environment variable. Raises an error if neither is provided.
    """
    if bucket_name:
        return bucket_name
    
    env_bucket_name = os.environ.get('S3_BUCKET_NAME')
    if env_bucket_name:
        return env_bucket_name
    
    raise ValueError(
        "S3 bucket name not provided. Please pass it as an argument "
        "or set the 'S3_BUCKET_NAME' environment variable."
    )

def read_csv_from_s3(file_key: str, bucket_name: str = None) -> pd.DataFrame:
    """
    Reads a CSV file from an S3 bucket into a pandas DataFrame.

    Args:
        file_key (str): The full path to the CSV file within the S3 bucket
                        (e.g., 'data/my_data.csv').
        bucket_name (str, optional): The name of the S3 bucket. If not provided,
                                     it will try to use the 'S3_BUCKET_NAME' 
                                     environment variable.

    Returns:
        pd.DataFrame: The pandas DataFrame read from the CSV file.

    Raises:
        ValueError: If the S3 bucket name is not provided.
        FileNotFoundError: If the specified file_key does not exist in the bucket.
        Exception: For other S3 or pandas related errors during reading.
    """
    try:
        actual_bucket_name = _get_s3_bucket_name(bucket_name)
        print(f"Attempting to read s3://{actual_bucket_name}/{file_key}")
        
        # Get the S3 object
        obj = s3_client.get_object(Bucket=actual_bucket_name, Key=file_key)
        
        # Read the object's body (bytes) and decode to a UTF-8 string
        csv_string = obj['Body'].read().decode('utf-8')
        
        # Use StringIO to treat the string as a file-like object for pandas
        df = pd.read_csv(StringIO(csv_string))
        
        print(f"Successfully read s3://{actual_bucket_name}/{file_key}")
        return df
    except s3_client.exceptions.NoSuchKey:
        raise FileNotFoundError(
            f"File '{file_key}' not found in bucket '{actual_bucket_name}'."
        )
    except ValueError as ve:
        raise ve
    except Exception as e:
        print(f"Error reading s3://{actual_bucket_name}/{file_key}: {e}")
        raise

def write_df_to_csv_s3(df: pd.DataFrame, file_key: str, bucket_name: str = None):
    """
    Writes a pandas DataFrame to an S3 bucket as a CSV file.

    Args:
        df (pd.DataFrame): The pandas DataFrame to write.
        file_key (str): The full path for the CSV file within the S3 bucket
                        (e.g., 'output/processed_data.csv').
        bucket_name (str, optional): The name of the S3 bucket. If not provided,
                                     it will try to use the 'S3_BUCKET_NAME' 
                                     environment variable.

    Raises:
        ValueError: If the S3 bucket name is not provided.
        Exception: For other S3 or pandas related errors during writing.
    """
    try:
        actual_bucket_name = _get_s3_bucket_name(bucket_name)
        print(f"Attempting to write to s3://{actual_bucket_name}/{file_key}")

        # Convert DataFrame to CSV string in memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False) # index=False to avoid writing DataFrame index
        
        # Upload the CSV string to S3
        s3_client.put_object(
            Bucket=actual_bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue(), # Get the string value from the buffer
            ContentType='text/csv' # Important: Set the correct Content-Type
        )
        print(f"Successfully wrote DataFrame to s3://{actual_bucket_name}/{file_key}")
    except ValueError as ve:
        raise ve
    except Exception as e:
        print(f"Error writing DataFrame to s3://{actual_bucket_name}/{file_key}: {e}")
        raise

def read_json_from_s3(file_key: str, bucket_name: str = None):
    """
    Reads a JSON file from an S3 bucket into a Python object (e.g., dict, list).

    Args:
        file_key (str): The full path to the JSON file within the S3 bucket
                        (e.g., 'configs/my_config.json').
        bucket_name (str, optional): The name of the S3 bucket. If not provided,
                                     it will try to use the 'S3_BUCKET_NAME' 
                                     environment variable.

    Returns:
        object: The Python object (dict, list, etc.) parsed from the JSON file.

    Raises:
        ValueError: If the S3 bucket name is not provided.
        FileNotFoundError: If the specified file_key does not exist in the bucket.
        json.JSONDecodeError: If the content is not valid JSON.
        Exception: For other S3 related errors during reading.
    """
    try:
        actual_bucket_name = _get_s3_bucket_name(bucket_name)
        print(f"Attempting to read s3://{actual_bucket_name}/{file_key}")

        obj = s3_client.get_object(Bucket=actual_bucket_name, Key=file_key)
        json_bytes = obj['Body'].read()
        json_data = json.loads(json_bytes.decode('utf-8'))
        
        print(f"Successfully read s3://{actual_bucket_name}/{file_key}")
        return json_data
    except s3_client.exceptions.NoSuchKey:
        raise FileNotFoundError(
            f"File '{file_key}' not found in bucket '{actual_bucket_name}'."
        )
    except ValueError as ve:
        raise ve
    except json.JSONDecodeError as jde:
        print(f"Error decoding JSON from s3://{actual_bucket_name}/{file_key}: {jde}")
        raise
    except Exception as e:
        print(f"Error reading JSON from s3://{actual_bucket_name}/{file_key}: {e}")
        raise

def write_json_to_s3(data: object, file_key: str, bucket_name: str = None):
    """
    Writes a Python object as a JSON file to an S3 bucket.

    Args:
        data (object): The Python object (e.g., dict, list) to write as JSON.
        file_key (str): The full path for the JSON file within the S3 bucket
                        (e.g., 'configs/new_config.json').
        bucket_name (str, optional): The name of the S3 bucket. If not provided,
                                     it will try to use the 'S3_BUCKET_NAME' 
                                     environment variable.

    Raises:
        ValueError: If the S3 bucket name is not provided.
        TypeError: If the data cannot be serialized to JSON.
        Exception: For other S3 related errors during writing.
    """
    try:
        actual_bucket_name = _get_s3_bucket_name(bucket_name)
        print(f"Attempting to write to s3://{actual_bucket_name}/{file_key}")

        # Convert Python object to JSON string
        json_string = json.dumps(data, indent=4) # indent for readability
        json_bytes = json_string.encode('utf-8')
        
        # Upload the JSON bytes to S3
        s3_client.put_object(
            Bucket=actual_bucket_name,
            Key=file_key,
            Body=json_bytes,
            ContentType='application/json' # Set the correct Content-Type
        )
        print(f"Successfully wrote JSON to s3://{actual_bucket_name}/{file_key}")
    except ValueError as ve:
        raise ve
    except TypeError as te:
        print(f"Error serializing data to JSON for s3://{actual_bucket_name}/{file_key}: {te}")
        raise
    except Exception as e:
        print(f"Error writing JSON to s3://{actual_bucket_name}/{file_key}: {e}")
        raise
