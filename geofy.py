from geopy.geocoders import Nominatim
import pickle
import pandas as pd
import sys
import time

## load existing "master" list of location mappings
masterdf = pd.read_csv('GeopyCleanedTweets/geopy-cleaned-tweets.csv')
masterlist = set(list(masterdf['unclean'].unique()))

date = str(sys.argv[-1])  ## take single input date file to clean
GEOLOCATOR = Nominatim(user_agent='coronavirus-analysis')  ## arbitrary user_agent to mollify warning
wait = 1.25 ## define wait time between api calls >1s
d = []  ## empty list to append geopy mappings

def geofy(lst):
    c = 1  ## counter for print statements
    for el in lst:
        print(f'Geofying {c}/{len(lst)}', end='\r')
        time.sleep(wait)
        try: d.append([el,GEOLOCATOR.geocode(el, addressdetails=True).raw['address']['country'].lower()])
        except: d.append([el,''])
        c += 1

## read file corresponding to input date and subset rows that haven't yet been incorporated into master
fn = f'DirtyTweets/{date}-dirty-tweets.pkl'
df = pd.read_pickle(fn)
print(f'Length of dirty df: {len(df)}')
togeofy = (set(list(df['location'].unique()))).difference(masterlist)
print(f'Length of set after consulting master: {len(togeofy)}')

## call geofy on remaining set
geofy(togeofy)
df = pd.DataFrame(d, columns=['unclean','clean'])
df.to_csv(f'GeopyCleanedTweets/{date}-geopy-cleaned-tweets.csv', index=False)
# df.to_csv(f'GeopyCleanedTweets/geopy-cleaned-tweets.csv', mode='a', index=False, header=True)