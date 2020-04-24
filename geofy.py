from geopy.geocoders import Nominatim
import pickle
import pandas as pd
import sys
import time

date = str(sys.argv[-1])
GEOLOCATOR = Nominatim(user_agent='coronavirus-analysis')
d = []

def geofy(lst):
    for el in lst:
        time.sleep(1.25)
        try:
            d.append([el,GEOLOCATOR.geocode(el, addressdetails=True).raw['address']['country'].lower()])
        except:
            d.append([el,''])
            
fn = f'DirtyTweets/{date}-dirty-tweets.pkl'
testdf = pd.read_pickle(fn)
togeofy = list(testdf['location'].unique())
geofy(togeofy)
df = pd.DataFrame(d, columns=['unclean','clean'])
df.to_csv(f'GeopyCleanedTweets/{date}-geopy-cleaned-tweets.csv',index=False)