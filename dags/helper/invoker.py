import os
from subprocess import PIPE, Popen
import requests
import logging
import google.auth.transport.requests
import google.oauth2.id_token
from airflow.models import Variable
from dags.python_scripts.requests_ext import TCPKeepAliveHttpAdapter

import json
import logging
import boto3

# Cloud Function Handler
def invoke_cloud_functions(**context):
    data = context.get("data")
    url = context.get("url")
    attempt = int(context.get("task_instance").try_number)
    max_tries = int(context.get("task_instance").max_tries)

    logging.info("attempt: ", attempt)
    logging.info("max_tries: ", max_tries)

    audience = url
    auth_req = google.auth.transport.requests.Request()
    token = google.oauth2.id_token.fetch_id_token(auth_req, audience)

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {0}".format(token),
    }

    logging.info("data: ", data)
    with requests.Session() as session:
        adapter = TCPKeepAliveHttpAdapter()
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        x = session.post(url, headers=headers, json=data)

        if int(x.status_code) != 200 and str(x.text).strip() != "Done!":
            raise Exception(x.text)
        else:
            return "Done!"

# Lambda Function Handler
def invoke_lambda_function(**context):
    data = context.get("data")
    function_name = context.get("function_name")
    attempt = int(context.get("task_instance").get("try_number"))
    max_tries = int(context.get("task_instance").get("max_tries"))

    logging.info("attempt: %d", attempt)
    logging.info("max_tries: %d", max_tries)
    logging.info("data: %s", json.dumps(data))

    lambda_client = boto3.client('lambda')

    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(data)
        )

        response_payload = json.loads(response['Payload'].read())

        if response['StatusCode'] != 200 or str(response_payload).strip() != "Done!":
            raise Exception(response_payload)
        else:
            return "Done!"

    except Exception as e:
        logging.error("Error invoking Lambda function: %s", e)
        raise