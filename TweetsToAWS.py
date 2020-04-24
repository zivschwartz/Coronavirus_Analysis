import boto3
import pandas as pd
import numpy as numpy
import pickle
import os

dldir = 'HydratedTweets/'
S3 = boto3.resource('s3')
BUCKET = 'coronavirus-analysis'

pickles = [f for f in os.listdir(dldir)]

c = 1
for fn in pickles:
    KEY='TweetPickles/'+fn
    print(f'Uploading {c}/{len(pickles)} to S3...', end='\r')
    data = open(dldir+fn,'rb')
    df = pickle.load(data)
    df.to_pickle(fn)
    S3.Object(BUCKET,KEY).put(Body=open(fn,'rb'))
    c += 1

print('Uploads complete!')