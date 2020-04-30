import boto3
import pandas as pd
import numpy as numpy
import pickle
import os
from time import time
from multiprocessing import cpu_count
from functools import partial
from multiprocessing.pool import Pool
from boto.s3.connection import S3Connection
import threading
print("\nNumber of CPU cores:", cpu_count())

dldir = 'HydratedTweets/'  ## directory of stored pickles
BUCKET = 'coronavirus-analysis'
cores = cpu_count()

def OneUpload(localdir, f, bucket):
    S3 = boto3.resource('s3')
    KEY='TweetPickles/'+f
    data = open(localdir+f,'rb')
    df = pickle.load(data)
    df.to_pickle(f)
    S3.Object(BUCKET,KEY).put(Body=open(f,'rb'))

def TweetsToAWS(localdir, bucket):
    print('\nNon-threading upload function...\n')
    ts = time()
    pickles = [f for f in os.listdir(localdir)]
    c = 1  ## counter for print statements

    for fn in pickles:
        print(f'Uploading {c}/{len(pickles)} to S3...', end='\r')
        OneUpload(localdir, fn, bucket)
        c += 1

    print('\nUploads complete!')
    print('Non-threading took {} s'.format(time() - ts))
    
def ThreadTweetsToAWS(localdir, bucket):
    print('\nThreading upload function...\n')
    ts = time()
    pickles = [f for f in os.listdir(localdir)]
    c = 1  ## counter for print statements

    print(f'Threading {len(pickles)} pickle files...')
    for fn in pickles:
        t = threading.Thread(target = OneUpload, args=(localdir, fn, bucket, )).start()
        c += 1

    print('Uploads complete!')
    print('Threading took {} s'.format(time() - ts))
  
TweetsToAWS(dldir,BUCKET)
ThreadTweetsToAWS(dldir,BUCKET)