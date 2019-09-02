#Boto is the Amazon Web Services (AWS) SDK for Python, 
#which allows Python developers to write software that makes use of Amazon services like S3, EC2, Glue. 

import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    client = boto3.client('glue')
    client.start_job_run(
               JobName = 'bank-transformer',
               Arguments = {} )
	print ("cloudnext.guru first lambda function")
    return {
        'statusCode': 200,
        'body': json.dumps('Glue Job triggered')
    }
