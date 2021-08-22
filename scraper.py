import requests
import json
from datetime import datetime, timedelta
from time import sleep
import pandas as pd
from pmaw import PushShiftAPI, Response


api = PushshiftAPI()

before = int(datetime(2021, 8, 1, 0, 0).timestamp())
after = int(datetime(2021, 7, 1, 0, 0).timestamp())

p = {'subreddit': 'gameideas', 'before': before, 'after': after, 'limit': 1000, 
      'fields': ['id', 'title', 'selftext']}
      
stop = int(datetime(2015, 1, 1, 0, 0).timestamp())
interval = 5184000          # 2 months

class Dataset:
    def __init__(self, data=None):
        self.collection = []
        if data:
            self.add(data)
            
    def add(self, gen):
        """ Adds data to collection """
        if isinstance(gen, Response):
            self.collection += gen.responses
        else:
            raise TypeError("Excepted object of praw.Response not %s" % type(gen))
    
    def clear(self):
        """ Resets, or clears collection to empty list """
        self.collection.clear()
    
    def upload(self, df):
        """ Append data to dataframe """
        df = df.append(self.collection, ignore_index=True)
        self.clear()
        return df

    def __len__(self): 
        return len(self.collection)

collect = Dataset()
ec = 0

while p['before'] > stop:
    try:
        comments = api.search_submissions(**p)
        collect.add(comments)

        if len(collect) >= 1500:
            comdf = collect.upload(comdf)

        p['before'] = p['after']
        p['after'] -= interval

    except Exception as e:
        # print('\u001b[36m', type(e).__name__, e)
        print('\u001b[36m', type(e).__name__, '\u001b[37m', e)
        ec += 1
        sleep(0.5 * ec)
        continue

    
comdf = collect.upload(comdf)
comdf.head(5)
