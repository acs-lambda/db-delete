import json
import boto3
from typing import Dict, Any, Union, Optional
from botocore.exceptions import ClientError
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
lambda_client = boto3.client('lambda')
dynamodb = boto3.resource('dynamodb')
sessions_table = dynamodb.Table('Sessions')

class AuthorizationError(Exception):
    """Custom exception for authorization failures"""
    pass

def invoke(function_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Invoke a Lambda function by name with the given payload
    
    Args:
        function_name (str): Name of the Lambda function to invoke
        payload (Dict[str, Any]): Payload to send to the Lambda function
        
    Returns:
        Dict[str, Any]: Response from the Lambda function
        
    Raises:
        ClientError: If Lambda invocation fails
    """
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        # Parse the response payload
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        
        # If the Lambda function returned an error
        if 'FunctionError' in response:
            logger.error(f"Lambda function {function_name} returned an error: {response_payload}")
            raise ClientError(
                error_response={'Error': {'Message': response_payload.get('errorMessage', 'Unknown error')}},
                operation_name='InvokeLambda'
            )
            
        return response_payload
        
    except ClientError as e:
        logger.error(f"Failed to invoke Lambda function {function_name}: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Lambda response for {function_name}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error invoking Lambda function {function_name}: {str(e)}")
        raise

def parse_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse an event from either API Gateway or direct Lambda invocation by invoking the parse-event Lambda function
    
    Args:
        event (Dict[str, Any]): The event to parse, either from API Gateway or direct Lambda
        
    Returns:
        Dict[str, Any]: Parsed event data including body and cookies if present
        
    Raises:
        ClientError: If Lambda invocation fails
        Exception: If parsing fails
    """
    try:
        # Invoke the parse-event Lambda function
        response = invoke('ParseEvent', event)
        
        # Check if the parsing was successful
        if response['statusCode'] != 200:
            logger.error(f"Failed to parse event: {response['body']}")
            raise Exception(f"Failed to parse event: {response['body'].get('message', 'Unknown error')}")
            
        return response['body']
        
    except Exception as e:
        logger.error(f"Error parsing event: {str(e)}")
        raise

def authorize(user_id: str, session_id: str) -> None:
    """
    Authorize a user by invoking the authorize Lambda function
    
    Args:
        user_id (str): The user ID to validate
        session_id (str): The session ID to validate
        
    Returns:
        None
        
    Raises:
        AuthorizationError: If authorization fails
    """
    try:
        # Invoke the authorize Lambda function
        response = invoke('Authorize', {
            'user_id': user_id,
            'session_id': session_id
        })
        
        # Check if authorization was successful
        if response['statusCode'] != 200 or not response['body'].get('authorized', False):
            raise AuthorizationError(response['body'].get('message', 'ACS: Unauthorized'))
            
    except ClientError as e:
        logger.error(f"Lambda invocation error during authorization: {str(e)}")
        raise AuthorizationError("ACS: Unauthorized")
    except Exception as e:
        logger.error(f"Unexpected error during authorization: {str(e)}")
        raise AuthorizationError("ACS: Unauthorized") 