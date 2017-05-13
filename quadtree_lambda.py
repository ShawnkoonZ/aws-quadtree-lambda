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

def launch_container(event, context):
  '''
  Lambda entry function that receives event & context.
  '''

  # Print received object.
  print('Received event: ' + json.dumps(event, indent=2))
  print('context : ' + context.function_name)

  src_bucket = event['Records'][0]['s3']['bucket']['name']
  src_key = event['Records'][0]['s3']['object']['key']

  print('src_bucket : ' + src_bucket)
  print('src_key : ' + src_key)