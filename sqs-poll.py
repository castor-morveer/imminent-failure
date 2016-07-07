#!/usr/bin/python
import boto3
import time
import json
import sys

def main():
  stage_label = sys.argv[1]
  l = boto3.client('lambda', region_name='us-west-2')
  print("starting endless loop...")

  while(True):
    time.sleep(10)
    res = l.invoke_async(FunctionName='{0}_echoTest'.format(stage_label), InvokeArgs=json.dumps({"ping":"pong"}))
    print(res)
    
if __name__ == '__main__':
  sys.exit(main())
  

