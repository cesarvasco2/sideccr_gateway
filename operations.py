import boto3, decimal

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

import json

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

class Operations:

    def get(tabela, params):
        try:
            response = tabela.get_item(
                Key=params
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
            return False
        else:
            if 'Item' in response:
                return response['Item']
            else:
                return False

    def update(tabela, condition, expression, attr):
        try:
            response_update = tabela.update_item(
                Key=condition,
                UpdateExpression=expression,
                ExpressionAttributeValues=attr,
                ReturnValues="UPDATED_NEW"
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
            return False
        else:
            if 'Attributes' in response_update:
                return True
            else:
                return False

    def create(tabela, params):
        try:
            response = tabela.put_item(
                Item=params
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
            return False
        else:
            print("PutItem succeeded:")
            return json.dumps(response, indent=4, cls=DecimalEncoder)

    def remove(tabela, params, condition, attr):
        try:
            response = tabela.delete_item(
                Key=params,
                ConditionExpression=condition,
                ExpressionAttributeValues=attr
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
            return False
        else:
            print("DeleteItem succeeded:")
            return json.dumps(response, indent=4, cls=DecimalEncoder)


    def listAll(tabela, params, fields):
        try:
            if fields:
                response = tabela.query(
                    KeyConditionExpression=params,
                    ScanIndexForward=False,
                    ProjectionExpression=fields
                )
            else:
                response = tabela.query(
                    KeyConditionExpression=params,
                    ScanIndexForward=False
                )
        except ClientError as e:
            print(e.response['Error']['Message'])
            return False
        else:
            #print(response)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200 and response['Count'] > 0:
                return json.loads(json.dumps(response['Items'], indent=4, cls=DecimalEncoder))
            else:
                return False

    def scanFilter(tabela, expression, limit=0):
        try:
            response = tabela.query(
                KeyConditionExpression=expression,
                Limit=limit,
                ScanIndexForward=False
            )
        except ClientError as e:
            print(e.response['Error']['Message'])
            return False
        else:
            #print(response)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200 and response['Count'] > 0:
                return json.loads(json.dumps(response['Items'], indent=4, cls=DecimalEncoder))
            else:
                return False
