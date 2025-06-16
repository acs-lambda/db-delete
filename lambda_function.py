"""
Database Delete Lambda Function
==============================

This Lambda function provides secure deletion of DynamoDB records with account-based filtering.
It ensures that users can only delete records that have their account_id as the associated_account.

API Interface
------------
Endpoint: POST /db-delete
Authentication: Required (account_id and session cookie)

Request Payload:
{
    "table_name": string,      # Required: Name of the DynamoDB table to delete from
    "key_name": string,        # Required: Name of the key attribute to match
    "key_value": any,          # Required: Value to match against key_name
    "index_name": string,      # Required: Name of the GSI to use for querying
    "account_id": string       # Required: ID of the authenticated user
}

Response:
{
    "statusCode": number,      # HTTP status code
    "body": string            # JSON stringified response body
}

Status Codes:
- 200: Success - Records deleted successfully
- 400: Bad Request - Missing required parameters or invalid request format
- 401: Unauthorized - Invalid or expired session, or no session cookie provided
- 403: Forbidden - User not authorized to delete the specified records
- 404: Not Found - No matching records found for the given criteria
- 500: Internal Server Error - DynamoDB operation failed

Security:
- All requests must include valid account_id and session cookie
- Records are filtered to only allow deletion of those where associated_account matches account_id
- Authorization is performed using the authorize utility function
- Each item is verified to belong to the specified account before deletion

DynamoDB Behavior:
- Uses Global Secondary Indexes (GSI) for efficient querying
- Handles two query patterns based on index structure:
  1. When associated_account is part of the index name:
     - Uses associated_account as partition key
     - Filters results in memory for key_name match
  2. When associated_account is not part of the index:
     - Uses key_name as partition key
     - Filters by associated_account using FilterExpression
- Performs atomic delete operations using primary key

Error Handling:
- Validates all required parameters
- Handles DynamoDB errors with appropriate status codes
- Provides detailed error messages for debugging
- Logs all operations and errors for monitoring

Example Usage:
-------------
Request:
POST /db-delete
{
    "table_name": "Conversations",
    "key_name": "conversation_id",
    "key_value": "conv_123",
    "index_name": "conversation_id-index",
    "account_id": "acc_456"
}

Response (Success):
{
    "statusCode": 200,
    "body": "{\"message\": \"Successfully deleted 1 items\"}"
}

Response (Error):
{
    "statusCode": 404,
    "body": "{\"error\": \"No items found with conversation_id = conv_123 in index conversation_id-index for account acc_456\"}"
}
"""

import json
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from config import logger
from utils import create_response, LambdaError, parse_event, authorize

dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')

def delete_db_item(table_name, key_name, key_value, index_name, account_id):
    """
    Deletes items from DynamoDB that match a given key, after verifying ownership.
    """
    table = dynamodb.Table(table_name)
    
    try:
        if 'associated_account' in index_name.lower():
            response = table.query(
                IndexName=index_name,
                KeyConditionExpression=Key('associated_account').eq(account_id)
            )
            items = [item for item in response.get('Items', []) if item.get(key_name) == key_value]
        else:
            response = table.query(
                IndexName=index_name,
                KeyConditionExpression=Key(key_name).eq(key_value),
                FilterExpression='attribute_exists(associated_account) AND associated_account = :account_id',
                ExpressionAttributeValues={':account_id': account_id}
            )
            items = response.get('Items', [])

        if not items:
            raise LambdaError(404, f"No items found with {key_name} = {key_value} for the specified account.")

        table_description = dynamodb_client.describe_table(TableName=table_name)
        key_schema = table_description['Table']['KeySchema']
        
        deleted_count = 0
        with table.batch_writer() as batch:
            for item in items:
                if item.get('associated_account') != account_id:
                    logger.warning(f"Attempt to delete item not owned by account {account_id}.")
                    continue # Skip items not owned by the user

                delete_key = {key['AttributeName']: item[key['AttributeName']] for key in key_schema}
                batch.delete_item(Key=delete_key)
                deleted_count += 1
        
        if deleted_count == 0:
            raise LambdaError(403, "No items found that you are authorized to delete.")

        return f"Successfully deleted {deleted_count} items."

    except ClientError as e:
        logger.error(f"DynamoDB error during deletion: {e}")
        raise LambdaError(500, f"A database error occurred: {e.response['Error']['Message']}")
    except Exception as e:
        logger.error(f"Unexpected error during item deletion: {e}")
        raise LambdaError(500, "An unexpected error occurred during the delete operation.")

def lambda_handler(event, context):
    try:
        parsed_event = parse_event(event)
        body = parsed_event.get('body', {})
        cookies = parsed_event.get('cookies', [])
        
        session_cookie = next((cookie.split('=')[1] for cookie in cookies if cookie.startswith('session=')), None)
        if not session_cookie:
            raise LambdaError(401, "No session cookie provided.")

        required_fields = ['table_name', 'key_name', 'key_value', 'index_name', 'account_id']
        if any(field not in body for field in required_fields):
            raise LambdaError(400, "Missing one or more required fields.")
        
        authorize(body['account_id'], session_cookie)
        
        message = delete_db_item(
            body['table_name'],
            body['key_name'],
            body['key_value'],
            body['index_name'],
            body['account_id']
        )
        
        return create_response(200, {"message": message})

    except LambdaError as e:
        return create_response(e.status_code, {"error": e.message})
    except Exception as e:
        logger.error(f"Unhandled error in lambda_handler: {e}")
        return create_response(500, {"error": "An internal server error occurred."})
