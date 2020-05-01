import pickle
import boto3
import numpy as np
import pandas as pd
from time import time
from geopy.geocoders import Nominatim
from country_list import countries_for_language
pd.set_option('display.max_rows',100)

master = pd.read_csv('GeopyCleanedTweets/geopy-cleaned-tweets.csv')
masterd = dict(zip(master['unclean'],master['clean']))
countrycodes = pd.read_csv('countrycodes.csv')
countrycodes = dict(zip(countrycodes['Code'],countrycodes['Country']))
countrycodes['NA'] = 'Namibia'
del countrycodes[np.nan]
countrycodes = {k.lower(): v.lower() for k,v in countrycodes.items()}

pd.DataFrame(columns=['date','country','count']).to_csv('tweetcounts.csv',index=False)
S3 = boto3.resource('s3')
BUCKET = 'coronavirus-analysis'
conn = S3.Bucket(BUCKET)
fns = [object_summary.key for object_summary in conn.objects.filter(Prefix="TweetPickles/")]

imported_countries = dict(countries_for_language('en'))
countries = [x.lower() for x in list(imported_countries.values())]
def countrycheck(row):
    if not any(country in row for country in countries):
        return row
    else:
        for country in countries:
            if country in row:
                idx = row.find(country)
                nextchar = row[idx:idx+len(country)+1]
                if len(row)>idx+len(country)+1:
                    continue
                else:
                    return country
                    break

states = [s.lower() for s in ["Alabama","Alaska","Arizona","Arkansas","California","Colorado",
          "Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho","Illinois",
          "Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland",
          "Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana",
          "Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York",
          "North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania",
          "Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah",
          "Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming"]]

def statecheck(row):
    for state in states:
        if state in row:
            return 'united states'
    return row

c = 1
dropcols = ['created_at','user_id','user_name','latitude','longitude','text']
for fn in fns:
    print(f'Working with {c}/{len(fns)}',end='\r')
    date = fn.replace('TweetPickles/','').replace('-tweets.pkl','')
    df = pickle.loads(S3.Bucket(BUCKET).Object(fn).get()['Body'].read()).drop(columns=dropcols).fillna('')
    df['location'] = df['location'].str.strip()
    df['country'] = df['country'].str.strip()
    df = df[(df['location']!='')|(df['country']!='')].apply(lambda x: x.astype(str).str.lower())
    df['country'] = df['country'].map(countrycodes).fillna('')
    df['location'] = np.where(df['country']!='', df['country'], df['location'])
    del df['country']
    df['location'] = df['location'].apply(lambda row: statecheck(row))  
    df['location'] = df['location'].apply(lambda row: countrycheck(row))    
    counts = []
    cleaned = df[df['location'].isin(countries)]
    dirty = df[~df['location'].isin(countries)]
    dirty['location'] = dirty['location'].map(masterd).fillna('')
    dirty = dirty[dirty['location']!='']
    final = cleaned.append(dirty)
    final['location'] = final['location'].replace('united states of america','united states')
#     dirty.to_pickle(f'./DirtyTweets/{date}-dirty-tweets.pkl')
    items = list(map(list, dict(final['location'].value_counts()).items()))
    dfcount = []
    for i in items:
        dated = [date]+[j for j in i]
        dfcount.append(dated)
    counts.append(dfcount)
    counts = [x for y in counts for x in y]
    countdf = pd.DataFrame(counts, columns=['date','country','count'])
    countdf.to_csv('tweetcounts.csv', mode='a', header=False)
    c += 1