import os
import time
import numpy as np
import pandas as pd
import gzip
import json
import pickle
import glob

def get_tweets_json(filename):
    f = gzip.open(filename, 'rb')
    response = f.read().decode('utf-8')
    f.close()
    return response.split('\n')

def metadata_df(tweets, RT=False):

    #create dataframe to store tweet metadata
    df = pd.DataFrame(columns=['tweet_id',
                                        'created_at',
                                        'user_id',
                                        'user_name',
                                        'location',
                                        'country',
                                        'latitude',
                                        'longitude',
                                        'text'])

    for i in range(len(tweets)):
        try:
            tweet = json.loads(tweets[i])
        except:
            #already formatted in JSON
            if tweets[i] != '': #if tweet is not empty
                tweet = tweets[i]

        #NOTE: this line filters out retweets
        if not RT: #if we don't want retweets
            if 'RT' in tweet['full_text']: continue
            
        #first, get location (city) and country depending on what info is available
        if tweet['place'] != None:
            location = tweet['place']['full_name']
            country = tweet['place']['country_code']
            
        else:
            location = tweet['user']['location'] #if actual city doesn't exist, get location provided by the user
            country = np.nan
            
        #get latitude and longitude depending on whether an exact location or bounding box is provided
        if tweet['coordinates'] != None: #exact location
            longitude, latitude = tweet['coordinates']['coordinates']
            
        elif tweet['place'] != None: #bounding box --> take center point (average) of box
            bounding_box = np.array(tweet['place']['bounding_box']['coordinates'][0])
            longitude, latitude = np.mean(bounding_box, axis=0)
            
        else: #no location provided --> make nan
            longitude = np.nan
            latitude = np.nan

        #append this tweet to the dataframe
        df = df.append({'tweet_id':tweet['id'],
                        'created_at':tweet['created_at'],
                        'user_id':tweet['user']['id'],
                        'user_name':tweet['user']['screen_name'],
                        'location':location,
                        'country':country,
                        'latitude':latitude,
                        'longitude':longitude,
                        'text':tweet['full_text'],
                        }, ignore_index=True)

    return df

# '2020-01-21' <---- must be a string of this format
def daily_tweets(day):
    daily_df = pd.DataFrame(columns=['tweet_id',
                                    'created_at',
                                    'user_id',
                                    'user_name',
                                    'location',
                                    'country',
                                    'latitude',
                                    'longitude',
                                    'text'])
    month_dir = day[:7]
    hours = ["%02d" % n for n in range(24)]

    for hour in hours:
        filename = './'+month_dir+'/coronavirus-tweet-id-'+ \
                    day+'-'+hour+'.jsonl.gz'

        if not os.path.exists(filename): #if file doesn't exist
            continue
        else:
            print("\rReading tweets for:",day+'-'+hour, end='')
            tweets = get_tweets_json(filename)
            hourly_df = metadata_df(tweets) #RT = False
            daily_df = pd.concat([daily_df,hourly_df])

    return daily_df

#year_month must be in format '2020-01'
#folder is '2020-01-daily-dfs'
def save_monthly_tweets(year_month,folder):
    days = [year_month+'-%02d' % n for n in range(1,32)]
    for day in days:
        #if no tweet data for that day
        if not [f for f in os.listdir('./'+year_month) if f.startswith("coronavirus-tweet-id-"+day)]:
            continue
        else: #get the daily data frame and pickle it
            daily_df = daily_tweets(day)
            pickle_filename = './'+folder+'/'+day+'-tweets.pkl'
            daily_df.to_pickle(pickle_filename)
    return