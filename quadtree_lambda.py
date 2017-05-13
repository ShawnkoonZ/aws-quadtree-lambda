'''
  For detailed information on API call, please checkout official boto3 website.
  => https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Bucket.copy

  Please checkout my github for more information.
  => https://github.com/shawnkoon/aws_python_practice

  @purpose: Receives an csv file from s3 bucket,
            parse & creates a json tree node object and uploads it into dynamodb.
  python v3.6
'''
import json
import boto3

# Get s3 client.
S3_CLIENT = boto3.client('s3')
DYNAMO_CLIENT = boto3.client('dynamodb')

def get_bucket_name(event):
    '''
    Get bucket name from incomming event.
    @param event : Received event.
    '''
    return event['Records'][0]['s3']['bucket']['name']

def get_key_name(event):
    '''
    Get key name from incomming event.
    @param event : Received event.
    '''
    return event['Records'][0]['s3']['object']['key']

def to_string(byte_object):
    '''
    Converts Byte object into String object.
    @param byte_object : Source Byte object.
    -> Returns String object.
    '''
    return byte_object.decode("utf-8")

def read_object(bucket_name, key_name):
    '''
    Reads object from given bucket with matching key.
    @param bucket_name : Source bucket name.
    @param key_name : Source key name.
    -> Returns s3 object.
    '''
    return S3_CLIENT.get_object(
        Bucket=bucket_name,
        Key=key_name
    )

def print_detail(event, context):
    '''
    Prints the detail of the event & context.
    @param event : received event.
    @param context : current context info.
    '''
    print('Received event: ' + json.dumps(event, indent=2))
    print('Current function: ' + context.function_name)

def launch_container(event, context):
    '''
    Lambda entry function that receives event & context.
    @param event : received event.
    @param context : current context info.
    '''
    print_detail(event, context)
    src_bucket = get_bucket_name(event)
    src_key = get_key_name(event)

    s3_object = read_object(src_bucket, src_key)
    s3_object_list = to_string(s3_object['Body'].read()).split('\n')
    print(s3_object_list)
