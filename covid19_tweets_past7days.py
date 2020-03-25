import os
import tweepy as tw #be sure to "pip install tweepy"
import numpy as np
import pandas as pd
import time
from IPython import display
import pickle
import datetime

#import necessary keys from Twitter Developer API
CONSUMER_KEY =
CONSUMER_SECRET = 
ACCESS_TOKEN = 
ACCESS_TOKEN_SECRET =

def metadata_df(search_api,hashtags,since,until, filename, lat_long_only=True):
    print('---------- COLLECTING TWEETS OF THE PREVIOUS WEEK FROM BEFORE '+until+' ----------')
    #keep track of how many tweets with valid latitude and longitude are preserved
    total_tweets = 0
    latlong_tweets = 0
    
    #open current pickle if it exists to keep appending to it
    if os.path.exists(filename):
        df = pickle.load(open(filename, 'rb'))
    #otherwise, initialize an empty dataframe with the desired columns
    else:
        df = pd.DataFrame(columns=['tweet_id',
                                   'created_at',
                                   'user_id',
                                   'user_name',
                                   'location',
                                   'country',
                                   'latitude',
                                   'longitude',
                                   'text'])
    
    # loop through tweet objects
    for tweet in tw.Cursor(api.search,
                            q=hashtags,
                            #lang="en",
                            since=since,
                            until=until,
                            tweet_mode='extended',  
                            count = 2000).items():
        
        #this just prints the tweets so we know the progress of pulling
        total_tweets += 1
        #print("Total tweets pulled: ",total_tweets,end='\r')
        print("Tweets with valid latitude/longitude: ",latlong_tweets,end='\r')
        #display.clear_output(wait=True)
        
        #NOTE: this line ensures that we only return tweets with valid locations/latitudes + longitudes
        if lat_long_only:
            if not tweet.place: continue
            
        #first, get location (city) and country depending on what info is available
        if tweet.place != None:
            location = tweet.place.full_name
            country = tweet.place.country_code
            
        else:
            location = tweet.user.location #if actual city doesn't exist, get location provided by the user
            country = np.nan
            
        #get latitude and longitude depending on whether an exact location or bounding box is provided
        if tweet.coordinates != None: #exact location
            longitude, latitude = tweet.coordinates['coordinates']
            
        elif tweet.place != None: #bounding box --> take center point (average) of box
            bounding_box = np.array(tweet.place.bounding_box.coordinates[0])
            longitude, latitude = np.mean(bounding_box, axis=0)
            
        else: #no location provided --> make nan
            longitude = np.nan
            latitude = np.nan
        
        latlong_tweets += 1
        #print("Total tweets pulled: ",total_tweets,end='\r')
        print("Tweets with valid latitude/longitude: ",latlong_tweets,end='\r')

        # create and/or append this tweet to the dataframe
        df = df.append({'tweet_id':tweet.id,
                       'created_at':tweet.created_at,
                       'user_id':tweet.user.id,
                       'user_name':tweet.user.screen_name,
                       'location':location,
                       'country':country,
                       'latitude':latitude,
                       'longitude':longitude,
                       'text':tweet.full_text,
                      }, ignore_index=True)
        
        #pickle the intermediate result with the date
        df.to_pickle(filename)
        
    print("Total tweets pulled: ",total_tweets)
    print("Tweets with valid latitude/longitude: ",latlong_tweets) 
    
    return df

if __name__ == "__main__":
    #connect to Twitter API
    auth = tw.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    #automatically wait 15ish minutes for the rate limit to refresh + notify us that this is happening
    api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    #collect tweets, results saved in pickle
    hashtags = '#coronavirus OR #covid19 -filter:retweets'
    #to get all the tweets in one day, since=today, until=tomorrow (even if in the future)
    #TODAY IS MARCH 25 ==> 7 "days" before is since='2020-03-17', until '2020-03-25'
    since = '2020-03-17'
    until = '2020-03-25' #EXCLUSIVE, will get up to 2020-03-24's tweets
    pickle_name = 'past7days_before_'+until+'_tweets.pkl'
    metadata_df(api.search, hashtags, since=since,until=until, filename=pickle_name,lat_long_only=True)



