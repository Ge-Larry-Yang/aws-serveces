import boto3
import json
import sys

BUCKET = 'tute-geek'
KEY = 'fifa19_kaggle.csv'
OUTPUT = 'fifa19_kaggle_output.csv'
GROUP = 'Club'
COLUMN = 'Overall'
CREDENTIALS = 'aws_credential.json'

# Use credentials from JSON file
# add something prove to be wrong
try:
    with open(CREDENTIALS) as json_file:
        credentials_json = json.load(json_file)
    print(credentials_json)
    assert credentials_json['access_key_id'], "Did not find 'access_key_id'"
    assert credentials_json['secret_access_key'], "Did not find 'secret_access_key'"
except:
    print('Error related to JSON Credentials')
    sys.exit()


def invoke_lambda(bucket, file_key, output_file, group, column):
    """Invoke Lambda function using boto3
    Parameters:
    - bucket: Name of your S3 bucket
    - file_key: Name of your CSV file on the S3 bucket
    - output_file: Name for the output CSV file
    - group: Column to use as group in groupby
    - column: Column to aggregate in groupby
    """
    global credentials_json

    # Set Lambda Client with credentials
    boto3.setup_default_session(region_name='us-east-2')
    client = boto3.client(
        'lambda',
        aws_access_key_id=credentials_json.get('access_key_id'),
        aws_secret_access_key=credentials_json.get('secret_access_key')
    )

    # Dictionary to be posted on the lambda event with information provided
    # by the user command line call
    payload = {
        "bucket": bucket,
        "file_key": file_key,
        "output_file": output_file,
        "group": group,
        "column": column
    }

    response = client.invoke(
        FunctionName='pythontrial',
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=json.dumps(payload)
    )
    return response


print(invoke_lambda(BUCKET, KEY, OUTPUT, GROUP, COLUMN)['Payload'].read())
