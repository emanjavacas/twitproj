from utils import *
import tweepy
import json
import pymongo
from time import sleep
import sys


@classmethod
def parse(cls, api, raw):
    status = cls.first_parse(api, raw)
    setattr(status, 'json', json.dumps(raw))
    return status

tweepy.models.Status.first_parse = tweepy.models.Status.parse
tweepy.models.Status.parse = parse


def find_idx(lst, fn):
    i = 0
    while not fn(lst[i]):
        i += 1
    return i

usage = """
Usage: $ python (0) twitter_mining.py (1) dbname (2) coll (3) idfile (4)
                 authfile (5) city (6) secs (opt 7) startid
"""
if len(sys.argv) not in [7, 8]:
    print usage
    raise SystemExit

conn = pymongo.MongoClient()
db = conn[sys.argv[1]]
coll = db[sys.argv[2]]
ids = read_ids(sys.argv[3])
auths = get_login(sys.argv[4])
city = sys.argv[5]
secs = int(sys.argv[6])

auth = tweepy.OAuthHandler(auths[0], auths[1])
auth.set_access_token(auths[2], auths[3])
api = tweepy.API(auth)

if len(sys.argv) == 8:
    start = find_idx(ids, lambda x: x[0] == sys.argv[7])
else:
    start = 0

total = 0
for i, _ in ids[start:]:
    if total > 250:
        total = 0
        continue
    cursor = tweepy.Cursor(api.user_timeline, id=i)
    total = 0
    print "*** retrieving id [%s]" % i
    for page in cursor.pages():
        print "*** *** retrieved %d tweets from page" % len(page)
        sleep(secs)
        for tweet in page:
            json_tweet = json.loads(tweet.json)
            if json_tweet["coordinates"]:
                x, y = json_tweet["coordinates"]["coordinates"]
                in_city = in_rect(x, y, *boxes[city])
                my_tweet = handle_tweet(json_tweet, tweet_keys, verbose=False)
                my_tweet['city'] = city if in_city else "unknown"
                try:
                    coll.insert(my_tweet)
                    total += 1
                    print "*** *** *** inserted tweet for id [%s] in city [%s]" \
                        % (i, my_tweet['city'])
                except pymongo.errors.DuplicateKeyError:
                    print "post already in the database"
