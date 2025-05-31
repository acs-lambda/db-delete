import json
import boto3
import logging
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional, List, Tuple

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')

def find_item_by_attribute(table_name: str, attribute_name: str, attribute_value: Any) -> Optional[Dict[str, Any]]:
    """
    Finds an item in DynamoDB using a non-primary key attribute.
    Uses a scan operation since we don't know the index.
    
    Args:
        table_name (str): Name of the DynamoDB table
        attribute_name (str): Name of the attribute to search for
        attribute_value (Any): Value of the attribute to match
        
    Returns:
        Optional[Dict[str, Any]]: The found item or None if not found
    """
    table = dynamodb.Table(table_name)
    try:
        # Use scan with filter expression
        response = table.scan(
            FilterExpression=f"#{attribute_name} = :value",
            ExpressionAttributeNames={
                f"#{attribute_name}": attribute_name
            },
            ExpressionAttributeValues={
                ":value": attribute_value
            }
        )
        
        items = response.get('Items', [])
        if items:
            return items[0]  # Return first matching item
        return None
        
    except Exception as e:
        logger.error(f"Error scanning table {table_name}: {str(e)}")
        return None

def db_delete(table_name: str, attribute_name: str, attribute_value: Any, is_primary_key: bool = False) -> Tuple[bool, Optional[str]]:
    """
    Deletes an item from the specified DynamoDB table.
    Can delete using either primary key or any other attribute.
    
    Args:
        table_name (str): Name of the DynamoDB table
        attribute_name (str): Name of the attribute to match
        attribute_value (Any): Value of the attribute to match
        is_primary_key (bool): Whether the attribute is a primary key
        
    Returns:
        Tuple[bool, Optional[str]]: (success status, error message if any)
    """
    table = dynamodb.Table(table_name)
    try:
        if is_primary_key:
            # Direct delete using primary key
            table.delete_item(
                Key={attribute_name: attribute_value}
            )
            return True, None
        else:
            # Find the item first using the attribute
            item = find_item_by_attribute(table_name, attribute_name, attribute_value)
            if not item:
                return False, f"No item found with {attribute_name} = {attribute_value}"
            
            # Get the primary key from the found item
            # We need to get the table's key schema to know which attributes are keys
            table_description = dynamodb_client.describe_table(TableName=table_name)
            key_schema = table_description['Table']['KeySchema']
            
            # Build the key for deletion
            delete_key = {}
            for key in key_schema:
                key_name = key['AttributeName']
                if key_name not in item:
                    return False, f"Found item missing required key attribute: {key_name}"
                delete_key[key_name] = item[key_name]
            
            # Perform the delete using the primary key
            table.delete_item(Key=delete_key)
            return True, None
            
    except Exception as e:
        error_msg = f"Error deleting item from {table_name}: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function handler for database deletions.
    Expects a POST request with JSON body containing:
    {
        "table_name": "string",
        "attribute_name": "string",
        "attribute_value": "any",
        "is_primary_key": boolean (optional, defaults to false)
    }
    """
    try:
        # Parse request body
        if not event.get('body'):
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
        required_fields = ['table_name', 'attribute_name', 'attribute_value']
        missing_fields = [field for field in required_fields if field not in body]
        if missing_fields:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Missing required fields: {", ".join(missing_fields)}'
                })
            }
        
        # Perform the delete
        is_primary_key = body.get('is_primary_key', False)
        success, error_msg = db_delete(
            body['table_name'],
            body['attribute_name'],
            body['attribute_value'],
            is_primary_key
        )
        
        if not success:
            return {
                'statusCode': 404 if "No item found" in error_msg else 500,
                'body': json.dumps({'error': error_msg})
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Item deleted successfully'
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Internal server error: {str(e)}'})
        }
