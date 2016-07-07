#!/usr/bin/python
import boto3
import time
import json

l = boto3.client('lambda', region_name='us-west-2')
print("starting endless loop...")

while(True):
  time.sleep(10)
  res = l.invoke_async(FunctionName='echoTest', InvokeArgs=json.dumps({"ping":"pong"}))
  print(res)

