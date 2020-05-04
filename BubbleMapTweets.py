import pickle
import boto3
import numpy as np
import pandas as pd
from geopy.geocoders import Nominatim
from country_list import countries_for_language
pd.set_option('display.max_rows',100)
import country_converter as coco
import pycountry
import pycountry_convert
import time
import plotly.express as px
import plotly.graph_objects as go
from geopy.geocoders import Nominatim
import gmplot

## read in tweet counts and subset appropriate date range
counts = pd.read_csv('tweetcounts.csv')
date_range = counts[(counts.date >='2020-01-21')&(counts.date <='2020-02-29')]

## instantiate world map and plot counts as bubbles over time
gmap = gmplot.GoogleMapPlotter(30, 0, 3)
fig = px.scatter_geo(date_range, 
                     color="count", 
                     locations='country', 
                     locationmode='country names',
                     hover_name="country", 
                     size="count",
                     size_max=50,
                     animation_frame="date", 
                     center={'lat': 34, 'lon': 9},
                     height=600)
fig.show()