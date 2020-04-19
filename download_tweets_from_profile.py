from __future__ import unicode_literals
from twitter import *
from twitter.stream import Timeout, HeartbeatTimeout, Hangup
import sys
import pandas as pd
import json
import os
import time
sys.path.append(".")

#
# This function can be used to iteratively flatten the json file level specific the number of levels to flatten
#
def flatten_json(y):
    out = {}
    def flatten(x, name='', level = 0):
        level += 1
        if(level == 1):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_', level)
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '_', level)
                    i += 1
            else:
                out[name[:-1]] = x
        else:
            out[name[:-1]] = x
    flatten(y)
    return out

# Replace with your own key/secret
consumer_key = ''
consumer_secret = ''
resource_owner_key = ''
resource_owner_secret = ''

# details regarding twitter handles to track
twitter_handle = '@HouseGOP'
rep_politicians, rep_celebs, rep_media = [],[],[]
demo_politicians, dem_celebs, dem_media = [],[],[]

# populate lists with handles from
handle_df = pd.read_csv("./handles_list_all.csv")

# create dataframes for each category
reps = handle_df.loc[handle_df["party"] == 0]
dems = handle_df.loc[handle_df["party"] == 1]
rep_politicians = reps.loc[reps["category"] == 0]
rep_celebs = reps.loc[reps["category"] == 1]
rep_media = reps.loc[reps["category"] == 2]
dem_politicians = dems.loc[dems["category"] == 0]
dem_celebs = dems.loc[dems["category"] == 1]
dem_media = dems.loc[dems["category"] == 2]

# create lists
rep_politicians = list(rep_politicians["handle"])
rep_celebs = list(rep_celebs["handle"])
rep_media = list(rep_media["handle"])
dem_politicians = list(dem_politicians["handle"])
dem_celebs = list(dem_celebs["handle"])
dem_media = list(dem_media["handle"])

# create twitter API access
twitter = Twitter(auth=OAuth(resource_owner_key,
                  resource_owner_secret,
                  consumer_key,
                  consumer_secret))


#
# Function to save tweets in the correct format to the CSV file
# @inputs results - results retrieved from twitter API for a specific user, party - party affiliation of the profile, type - whether they are media, celeb or politicians
#
def save_tweets_for_results(results, party, type):
    # data will be collected to this array
    rows_list = []

    # column names for the dataframe
    columns = ["TEXT", "RETWEET_COUNT", "FAVORITE_COUNT", "TWEET_ID", "TWEET_BY",
               "TWEET_BY_ID", "DATETIME", "NUM_OF_URLS", "RETWEETED", "RETWEETED_TWEET_ID",
               "RETWEETED_TWEET_BY", "RETWEETED_TWEET_BY_ID", "RETWEETED_TEXT",
               "RETWEETED_URLS","RETWEETED_MEDIA", "PARTY", "TYPE"]

    # directory to write the file to
    write_directory = "./tweet_dataset.csv"

    # iterate through the tweets
    for tweet in results:
        if tweet is None:
            print("-- None --")
        elif tweet is Timeout:
            print("-- Timeout --")
        elif tweet is HeartbeatTimeout:
            print("-- Heartbeat Timeout --")
        elif tweet is Hangup:
            print("-- Hangup --")
        else:
            # pro-process the tweet by flattening
            twt = flatten_json(json.loads(json.dumps(tweet, ensure_ascii=False, indent=4)))
            entities = flatten_json(twt['entities'])
            user = flatten_json(twt['user'])

            # data to save in the CSV
            TEXT = twt['full_text']
            RETWEET_COUNT = twt['retweet_count']
            FAVORITE_COUNT = twt['favorite_count']
            TWEET_ID = "#" + str(twt['id_str'])
            TWEET_BY = user['screen_name']
            TWEET_BY_ID = "#" + str(user['id_str'])
            DATETIME = twt["created_at"]
            NUM_OF_URLS = len(entities['urls'])
            PARTY = party
            TYPE = type
            try:
                RETWEETED = twt['retweeted_status']
                content = flatten_json(RETWEETED)
                retweeted_user = content["user"]
                rt_entities = flatten_json(content["entities"])
                RETWEETED = True
                RETWEETED_TWEET_ID = "#" + str(content["id_str"])
                RETWEETED_TWEET_BY = retweeted_user["screen_name"]
                RETWEETED_TWEET_BY_ID = "#" + str(retweeted_user["id_str"])
                RETWEETED_TEXT = content["full_text"]
                RETWEETED_URLS = len(rt_entities["urls"])
                RETWEETED_MEDIA = len(rt_entities["media"])
            except:
                RETWEETED = False
                RETWEETED_TWEET_ID = None
                RETWEETED_TWEET_BY = None
                RETWEETED_TWEET_BY_ID = None
                RETWEETED_TEXT = None
                RETWEETED_URLS = None
                RETWEETED_MEDIA = None

            # append data to be saved in the CSV
            rows_list.append([TEXT,RETWEET_COUNT,FAVORITE_COUNT,TWEET_ID,TWEET_BY,TWEET_BY_ID,
                                   DATETIME,NUM_OF_URLS,RETWEETED,RETWEETED_TWEET_ID,
                                   RETWEETED_TWEET_BY,RETWEETED_TWEET_BY_ID,RETWEETED_TEXT,
                                    RETWEETED_URLS, RETWEETED_MEDIA, PARTY, TYPE])

    # create a dataframe with collected data and rename columns
    dataframe = pd.DataFrame(rows_list)
    dataframe.columns = columns

    # if file exists - append, else create
    if os.path.exists(write_directory): export_csv = dataframe.to_csv(write_directory, index=None, header=False, mode='a')
    else: export_csv = dataframe.to_csv(write_directory, index=None, header=True, mode='w')

# save republican politicians
print("Getting Data for Republican Politicians")
for twitter_handle in rep_politicians:
    results = twitter.statuses.user_timeline(screen_name = twitter_handle, count = 200, exclude_replies = True, tweet_mode = "extended")
    print(str(len(results)) + " tweets saved for screen name: " + str(twitter_handle))
    save_tweets_for_results(results, "Rep", "Politician")
    time.sleep(2)

# save republican celebrities
print("Getting Data for Republican Celebrities")
for twitter_handle in rep_celebs:
    results = twitter.statuses.user_timeline(screen_name = twitter_handle, count = 200, exclude_replies = True, tweet_mode = "extended")
    print(str(len(results)) + " tweets saved for screen name: " + str(twitter_handle))
    save_tweets_for_results(results, "Rep", "Celebrity")
    time.sleep(2)

# save republican media
print("Getting Data for Republican Media")
for twitter_handle in rep_media:
    results = twitter.statuses.user_timeline(screen_name = twitter_handle, count = 200, exclude_replies = True, tweet_mode = "extended")
    print(str(len(results)) + " tweets saved for screen name: " + str(twitter_handle))
    save_tweets_for_results(results, "Rep", "Media")
    time.sleep(2)

# save republican politicians
print("Getting Data for Democratic Politicians")
for twitter_handle in dem_politicians:
    results = twitter.statuses.user_timeline(screen_name = twitter_handle, count = 200, exclude_replies = True, tweet_mode = "extended")
    print(str(len(results)) + " tweets saved for screen name: " + str(twitter_handle))
    save_tweets_for_results(results, "Dem", "Politician")
    time.sleep(2)

# save republican celebrities
print("Getting Data for Democratic Celebrities")
for twitter_handle in dem_celebs:
    results = twitter.statuses.user_timeline(screen_name = twitter_handle, count = 200, exclude_replies = True, tweet_mode = "extended")
    print(str(len(results)) + " tweets saved for screen name: " + str(twitter_handle))
    save_tweets_for_results(results, "Dem", "Celebrity")
    time.sleep(2)

# save republican media
print("Getting Data for Democratic Media")
for twitter_handle in dem_media:
    results = twitter.statuses.user_timeline(screen_name = twitter_handle, count = 200, exclude_replies = True, tweet_mode = "extended")
    print(str(len(results)) + " tweets saved for screen name: " + str(twitter_handle))
    save_tweets_for_results(results, "Dem", "Media")
    time.sleep(2)
