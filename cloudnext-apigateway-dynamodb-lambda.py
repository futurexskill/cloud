import json
import boto3

client = boto3.resource('dynamodb')
table = client.Table('cloudnextdb')

def lambda_handler(event, context):
    # TODO implement
    print("Hello Cloudnext")
    product_number = int(event["productno"])
    print(product_number)
    response = table.get_item(
    Key={
        'productID': product_number
    }
    )
    
    item = response['Item']
    availability = item['available']
    print("Product availability "+availability)
    return {
        'statusCode': 200,
        'body': json.dumps("product availability "+ availability)
    }

