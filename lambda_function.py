import json
import boto3
import logging
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional, Tuple

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')

def delete_item(table_name: str, key_name: str, key_value: Any, index_name: str) -> Tuple[bool, Optional[str]]:
    """
    Deletes all items from DynamoDB that match the given key value using a secondary index.
    
    Args:
        table_name (str): Name of the DynamoDB table
        key_name (str): Name of the key attribute
        key_value (Any): Value of the key attribute
        index_name (str): Name of the index to use
        
    Returns:
        Tuple[bool, Optional[str]]: (success status, error message if any)
    """
    table = dynamodb.Table(table_name)
    
    try:
        # Query the index to get all matching items
        response = table.query(
            IndexName=index_name,
            KeyConditionExpression=f"#{key_name} = :value",
            ExpressionAttributeNames={
                f"#{key_name}": key_name
            },
            ExpressionAttributeValues={
                ":value": key_value
            }
        )
        
        items = response.get('Items', [])
        if not items:
            return False, f"No items found with {key_name} = {key_value} in index {index_name}"
        
        # Get the primary key schema
        table_description = dynamodb_client.describe_table(TableName=table_name)
        key_schema = table_description['Table']['KeySchema']
        
        # Delete all matching items
        deleted_count = 0
        for item in items:
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
        "index_name": "string"
    }
    """
    try:
        # Parse request body
        if not event['body']:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Request body is required'})
            }
        
        try:
            body = json.loads(event['body'])
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid JSON in request body'})
            }
        
        # Validate required fields
        required_fields = ['table_name', 'key_name', 'key_value', 'index_name']
        missing_fields = [field for field in required_fields if field not in body]
        if missing_fields:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Missing required fields: {", ".join(missing_fields)}'
                })
            }
        
        # Perform the delete
        success, message = delete_item(
            body['table_name'],
            body['key_name'],
            body['key_value'],
            body['index_name']
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
