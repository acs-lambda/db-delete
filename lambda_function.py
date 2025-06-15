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
import logging
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional, Tuple
from utils import parse_event, authorize, AuthorizationError
from boto3.dynamodb.conditions import Key

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')

def delete_item(table_name: str, key_name: str, key_value: Any, index_name: str, account_id: str) -> Tuple[bool, Optional[str]]:
    """
    Deletes all items from DynamoDB that match the given key value using a secondary index.
    
    Args:
        table_name (str): Name of the DynamoDB table
        key_name (str): Name of the key attribute
        key_value (Any): Value of the key attribute
        index_name (str): Name of the index to use
        account_id (str): The account ID to validate ownership
        
    Returns:
        Tuple[bool, Optional[str]]: (success status, error message if any)
    """
    table = dynamodb.Table(table_name)
    
    try:
        # Check if associated_account is part of the index name (indicating it's a GSI)
        if 'associated_account' in index_name.lower():
            logger.debug(f"Using associated_account as partition key in KeyConditionExpression and filtering for key_name")
            # Query by associated_account and then filter results for key_name match
            response = table.query(
                IndexName=index_name,
                KeyConditionExpression=Key('associated_account').eq(account_id)
            )
            # Filter the items to match key_name and key_value
            items = [
                item for item in response.get('Items', [])
                if item.get(key_name) == key_value
            ]
            logger.info(f"Query successful. Retrieved {len(items)} items after filtering")
        else:
            logger.debug(f"Using associated_account in FilterExpression")
            response = table.query(
                IndexName=index_name,
                KeyConditionExpression=Key(key_name).eq(key_value),
                FilterExpression='attribute_exists(associated_account) AND associated_account = :account_id',
                ExpressionAttributeValues={
                    ':account_id': account_id
                }
            )
            items = response.get('Items', [])
            logger.info(f"Query successful. Retrieved {len(items)} items")
        
        if not items:
            return False, f"No items found with {key_name} = {key_value} in index {index_name} for account {account_id}"
        
        # Get the primary key schema
        table_description = dynamodb_client.describe_table(TableName=table_name)
        key_schema = table_description['Table']['KeySchema']
        
        # Delete all matching items
        deleted_count = 0
        for item in items:
            # Verify account_id matches
            if item.get('associated_account') != account_id:
                return False, f"Item belongs to a different account"
                
            # Build the key for deletion
            delete_key = {}
            for key in key_schema:
                key_attr_name = key['AttributeName']
                if key_attr_name not in item:
                    return False, f"Found item missing required key attribute: {key_attr_name}"
                delete_key[key_attr_name] = item[key_attr_name]
            
            # Perform the delete using the primary key
            table.delete_item(Key=delete_key)
            deleted_count += 1
        
        return True, f"Successfully deleted {deleted_count} items"
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_msg = e.response['Error']['Message']
        logger.error(f"DynamoDB error: {error_code} - {error_msg}")
        return False, f"DynamoDB error: {error_msg}"
    except Exception as e:
        error_msg = f"Error deleting items from {table_name}: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function handler for database deletions.
    Expects a POST request with JSON body containing:
    {
        "table_name": "string",
        "key_name": "string",
        "key_value": "any",
        "index_name": "string",
        "account_id": "string"
    }
    Also expects a session cookie in the event for authorization.
    """
    try:
        # Parse the event to get body and cookies
        parsed_event = parse_event(event)
        body = parsed_event.get('body', {})
        cookies = parsed_event.get('cookies', {})
        
        # Get session cookie
        session_cookie = next((cookie for cookie in cookies if cookie.startswith('session=')), None)
        if not session_cookie:
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'No session cookie provided'})
            }
        
        # Extract session ID from cookie
        session_id = session_cookie.split('=')[1]
        
        # Validate required fields
        required_fields = ['table_name', 'key_name', 'key_value', 'index_name', 'account_id']
        missing_fields = [field for field in required_fields if field not in body]
        if missing_fields:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Missing required fields: {", ".join(missing_fields)}'
                })
            }
        
        try:
            # Authorize the user
            authorize(body['account_id'], session_id)
        except AuthorizationError as e:
            return {
                'statusCode': 401,
                'body': json.dumps({'error': str(e)})
            }
        
        # Perform the delete
        success, message = delete_item(
            body['table_name'],
            body['key_name'],
            body['key_value'],
            body['index_name'],
            body['account_id']
        )
        
        if not success:
            return {
                'statusCode': 404 if "No items found" in message else 500,
                'body': json.dumps({'error': message})
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': message
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Internal server error: {str(e)}'})
        }
