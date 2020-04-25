from geopy.geocoders import Nominatim
import pickle
import pandas as pd
import sys
import time

masterdf = pd.read_csv('GeopyCleanedTweets/geopy-cleaned-tweets.csv')
# masterdf = masterdf[masterdf['clean']!='']
masterl = set(list(masterdf['unclean'].unique()))

date = str(sys.argv[-1])
GEOLOCATOR = Nominatim(user_agent='coronavirus-analysis')
d = []

def geofy(lst):
    c = 1
    for el in lst:
        print(f'Geofying {c}/{len(lst)}', end='\r')
        time.sleep(1.25)
        try:
            d.append([el,GEOLOCATOR.geocode(el, addressdetails=True).raw['address']['country'].lower()])
        except:
            d.append([el,''])
        c += 1
            
fn = f'DirtyTweets/{date}-dirty-tweets.pkl'
testdf = pd.read_pickle(fn)
print(f'Length of dirty df: {len(testdf)}')
togeofy = (set(list(testdf['location'].unique()))).difference(masterl)
print(f'Length of set after consulting master: {len(togeofy)}')
geofy(togeofy)
df = pd.DataFrame(d, columns=['unclean','clean'])
df.to_csv(f'GeopyCleanedTweets/{date}-geopy-cleaned-tweets.csv', index=False)
# df.to_csv(f'GeopyCleanedTweets/geopy-cleaned-tweets.csv', mode='a', index=False, header=True)