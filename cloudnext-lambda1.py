import json

def lambda_handler(event, context):
    # TODO implement
    print("Hello Cloudnext")
    return {
        'statusCode': 200,
        'body': json.dumps("Hello from CloudNext")
    }
