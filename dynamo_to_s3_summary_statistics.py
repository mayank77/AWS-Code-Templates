# -*- coding: utf-8 -*-
"""
Created on Fri Aug 10 12:36:24 2018

@author: mayank.khandelwal
"""

'''
This code pushes data summary statistics from DynamoDB to S3 Bucket 
'''

import boto3
from boto3.dynamodb.conditions import Key, Attr
import time
import datetime as dt
import pandas as pd
from io import StringIO

session = boto3.Session(
    aws_access_key_id='AMAZON_KEY_ID',
    aws_secret_access_key='AMAZON_SECRET_ACCESS_KEY'
)

dynamodb = session.resource('dynamodb', region_name='eu-central-1')
table = dynamodb.Table('data-collection-requests')

start_date = int(dt.datetime.strptime(str('20180813000000'), "%Y%m%d%H%M%S").timestamp()) #Convert to epoch
end_date = start_date + (60*60*24) #add 24 hours to the start_date to obtain end_date

response = table.scan(
    ExpressionAttributeNames= {"#timestamp": "timestamp"},
    ProjectionExpression="#timestamp , col1, col2, col3, col4",
    FilterExpression=(Attr('timestamp').gte(int(start_date)) and  Attr('timestamp').lt(int(end_date)))
)

current_df = (pd.DataFrame(response['Items'])).drop(['timestamp'],axis=1) #Remove Timestamp from the dataframe
current_df.rename(columns={'col3': 'col5'}, inplace=True) #Rename "col3" column to "col5"
current_df = current_df[['col1', 'col2', 'col5', 'col4']] #Reorder the columns to ["col1", "col2", "col5", "col4"]
description = (current_df.describe(include = 'all')) #summary statistics

'''
STRING WAY TO PUSH TO s3
s = StringIO()
description_csv_format =  description.to_csv(s)
s3.Object(BUCKET_NAME, 'summary_statistics/'+(time.strftime('%Y-%m-%d', time.localtime(start_date)))+'.csv').put(Body=s.getvalue())
'''

s3 = boto3.resource('s3')
BUCKET_NAME = 'AMAZON_BUCKET_NAME'

buffer=description.to_csv(index=False).encode("utf-8")
s3.Bucket(BUCKET_NAME).put_object(Key='summary_statistics/'+(time.strftime('%Y-%m-%d', time.localtime(start_date)))+'.csv', Body=buffer)
