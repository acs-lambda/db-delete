import json
import boto3
from typing import Dict, Any, Union, Optional
from botocore.exceptions import ClientError
import logging
from config import logger, AWS_REGION

# Configure logging
logger.setLevel(logging.INFO)

# Initialize AWS clients
lambda_client = boto3.client('lambda', region_name=AWS_REGION)
dynamodb = boto3.resource('dynamodb')
sessions_table = dynamodb.Table('Sessions')

class LambdaError(Exception):
    """Custom exception for Lambda function errors."""
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(f"[{status_code}] {message}")

def create_response(status_code, body):
    """Creates a standard API Gateway response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }

def invoke_lambda(function_name, payload, invocation_type="RequestResponse"):
    """
    Invokes another Lambda function and returns the entire response payload.
    """
    try:
        logger.info(f"Invoking {function_name}...")
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType=invocation_type,
            Payload=json.dumps(payload),
        )
        response_payload = response["Payload"].read().decode("utf-8")
        if not response_payload:
            return {}
        return json.loads(response_payload)
    except ClientError as e:
        raise LambdaError(500, f"Failed to invoke {function_name}: {e.response['Error']['Message']}")
    except Exception as e:
        raise LambdaError(500, f"An unexpected error occurred invoking {function_name}: {e}")

def parse_event(event):
    """
    Parse an event by invoking the ParseEvent Lambda function.
    """
    response = invoke_lambda('ParseEvent', event)
    if response.get('statusCode') != 200:
        raise LambdaError(response.get('statusCode', 500), "Failed to parse event.")
    return json.loads(response.get('body', '{}'))

def authorize(user_id, session_id):
    """
    Authorize a user by invoking the Authorize Lambda function.
    """
    response = invoke_lambda('Authorize', {'user_id': user_id, 'session_id': session_id})
    body = json.loads(response.get('body', '{}'))
    if response.get('statusCode') != 200 or not body.get('authorized'):
        raise LambdaError(response.get('statusCode', 401), body.get('message', 'ACS: Unauthorized'))

class AuthorizationError(Exception):
    """Custom exception for authorization failures"""
    pass 