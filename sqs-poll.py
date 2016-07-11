#!/usr/bin/python
import boto3
import time
import json
import sys
import logging

log = logging.RotatingFileHandler("/tmp/poller.log", "a", 1024*1024, 5)
log.setLevel(logging.INFO)

def get_sqs_url(region, account_id, stage):
  return 'https://sqs.{0}.amazonaws.com/{1}/{2}_deferred_execution_main_queue'.format(region, account_id, stage)

def process_message(lambda_client, message):
  message_body = json.loads(message['Body'])
  body_params = message_body['body_params']
  TargetLambda = body_params['TargetLambda']
  UndoLambda = body_params['UndoLambda']
  FailureLambda = body_params['FailureLambda']
  MaxRetries = body_params['MaxRetries']
  SQSQueueUrl = body_params['SQSQueueUrl']
  
  approximateRetryCount = int(message.get('Attributes',{}).get('ApproximateReceiveCount', MaxRetries))
  receipt = message['ReceiptHandle']
  message_body['body_params']['ReceiptHandle'] = receipt
  
  log.info('URL: {0}, TargetLambda: {1}, PollLambda: {2}, Retries: {3}, MaxRetries: {4}, Modified Body: {5}'
          .format(SQSQueueUrl, TargetLambda, PollLambda, approximateRetryCount, MaxRetries, message_body))
  
  if approximateRetryCount < MaxRetries:
      log.info('trying to take the requested action')
      lambda_client.invoke_async(FunctionName=TargetLambda, InvokeArgs=json.dumps(message_body))
  elif UndoLambda and (approximateRetryCount < (MaxRetries * 2)):
      log.info('trying to undo the requested action')
      lambda_client.invoke_async(FunctionName=UndoLambda, InvokeArgs=json.dumps(message_body))
  else:
      log.info('Both the attempt and the subsequent cleanup attempt failed, giving up')
      lambda_client.invoke_async(FunctionName=FailureLambda, InvokeArgs=json.dumps(message_body))

def process_queue(sqs_client, sqs_url, lambda_client):

  try:
    response = sqs_client.receive_message(QueueUrl=sqs_queue_url,
                                   AttributeNames=['SentTimestamp', 'ApproximateReceiveCount'],
                                   MaxNumberOfMessages=10,
                                   WaitTimeSeconds=30)
    for message in response.get('Messages', []):
      process_message(lambda_client, message)
  except Exception as e:
    logging.info('SQS receive_message failed, {0}'.format(e))
    pass

def main():
  stage_label = sys.argv[1]
  lambda_name = sys.argv[2]
  sqs_region = sys.argv[3]
  account_id = sys.argv[4]

  sqs_client = boto3.client('sqs', region_name=sqs_region)
  lambda_client = boto3.client('lambda', region_name=sqs_region)
  sqs_queue_url = get_sqs_url(sqs_region, account_id, stage_label)
  
  while(True):
    try:
      process_queue(sqs_client, sqs_queue_url, lambda_client)
    except Exception as e:
      log.error(e)

if __name__ == '__main__':
  sys.exit(main())
  

