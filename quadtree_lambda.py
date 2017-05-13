'''
  For detailed information on API call, please checkout official boto3 website.
  => https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Bucket.copy
  
  Please checkout my github for more information.
  => https://github.com/shawnkoon/aws_python_practice

  @purpose: Duplicate triggered object into specified output bucket.
  python v3.6
'''
import json
import boto3

# Get s3 client.
s3_client = boto3.client('s3')

def launch_container(event, context):
  # Print received object.
  print("Received event: " + json.dumps(event, indent=2))