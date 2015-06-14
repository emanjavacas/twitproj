from utils import *
import tweepy
import json
import pymongo
from time import sleep
from datetime import datetime


@classmethod
def parse(cls, api, raw):
	status = cls.first_parse(api, raw)
	setattr(status, 'json', json.dumps(raw))
	return status

tweepy.models.Status.first_parse = tweepy.models.Status.parse
tweepy.models.Status.parse = parse


def test_rate_limit(api, wait=True, buffer=.1):
    """
    http://stackoverflow.com/a/25141354/2420152
    Tests whether the rate limit of the last request has been reached.
    :param api: The `tweepy` api instance.
    :param wait: A flag indicating whether to wait for the rate limit reset
                 if the rate limit has been reached.
    :param buffer: A buffer time in seconds that is added on to the waiting
                   time as an extra safety margin.
    :return: True if it is ok to proceed with the next request. False otherwise.
    """
    #Get the number of remaining requests
    # remaining = int(api.last_response.getheader('x-rate-limit-remaining'))
    data = api.rate_limit_status()['resources']['statuses']['/statuses/user_timeline']
    remaining = data['remaining'] - 2
    print "remaining [%d] requests" % remaining
    #Check if we have reached the limit
    if remaining == 0:
        limit = data['limit']
        reset = data['reset']
        # limit = int(api.last_response.getheader('x-rate-limit-limit'))
        # reset = int(api.last_response.getheader('x-rate-limit-reset'))
        #Parse the UTC time
        reset = datetime.fromtimestamp(reset)
        #Let the user know we have reached the rate limit
        print "0 of {} requests remaining until {}.".format(limit, reset)

        if wait:
            #Determine the delay and sleep
            delay = (reset - datetime.now()).total_seconds() + buffer
            print "Sleeping for {}s...".format(delay)
            sleep(abs(delay))
            #We have waited for the rate limit reset. OK to proceed.
            return True
        else:
            #We have reached the rate limit.
            return False 

    #We have not reached the rate limit
    return True


def find_idx(lst, fn):
    i = 0
    while not fn(lst[i]):
        i+=1
    return i


conn = pymongo.MongoClient()
db = conn['twitdb']
coll = db['berlin_by_id']

auths = get_login("loginfile_test~")
auth = tweepy.OAuthHandler(auths[0], auths[1])
auth.set_access_token(auths[2], auths[3])
api = tweepy.API(auth)

ids = read_ids("/home/manjavacas/berlin.id")
# cursor = api.user_timeline(ids[100][0], count=1000)


start = find_idx(ids, lambda x: x[0] == "40902933")
for i, _ in ids[start:]:
    sleep(5)
    try:
        last = db.berlin_by_id.find({"user.id" : int(i)}).sort("id",1).next()
        max_id = last["id"]
    except StopIteration:
        continue
    try:
        cursor = api.user_timeline(i, count=4000, max_id=max_id)
    except tweepy.error.TweepError as e:
        print "exception " + str(e)
        continue
    print "retrieved %d" % len(cursor)
    for tweet in cursor:
        json_tweet = json.loads(tweet.json)
        if json_tweet["coordinates"]:
            x, y = json_tweet["coordinates"]["coordinates"]
            in_city = in_rect(x, y, *boxes["berlin"])
            my_tweet = handle_tweet(json_tweet, tweet_keys, verbose=False)
            my_tweet['city'] = "berlin" if in_city else "unknown"
            try:
                coll.insert(my_tweet)
                print "inserted tweet for id [%s]" % i
            except pymongo.errors.DuplicateKeyError:
                print "post already in the database"
