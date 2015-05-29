from utils import *
import tweepy
import pymongo

conn = pymongo.MongoClient()
db = conn['db']
coll = db['berlin_by_id']

auths = get_login("loginfile_test~")
auth = tweepy.OAuthHandler(auths[0], auths[1])
auth.set_access_token(auths[2], auths[3])
api = tweepy.API(auth)

ids = read_ids("/Users/quique/data/tweet_ids/berlin.id")

for i in ids:
    cursor = api.user_timeline(i, count=4000)
    for tweet in cursor:
        if tweet._json["coordinates"]:
            x, y = tweet._json["coordinates"]["coordinates"]
            in_city = in_rect(x, y, *boxes["berlin"])
            my_tweet = handle_tweet(tweet._json, tweet_keys, verbose=False)
            my_tweet['city'] = "berlin" if in_city else "unknown"
            print "inserted tweet for id [%d]" % i
            coll.insert(my_tweet)
