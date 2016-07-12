from utils import *
import tweepy
import argparse
import json
from time import sleep
import pymongo

@classmethod
def parse(cls, api, raw):
    status = cls.first_parse(api, raw)
    setattr(status, 'json', json.dumps(raw))
    return status

tweepy.models.Status.first_parse = tweepy.models.Status.parse
tweepy.models.Status.parse = parse


def insert_tweet(tweet, conn, city):
    my_tweet = handle_tweet(tweet, verbose=False)
    my_tweet['city'] = city
    try:
        conn.insert(my_tweet)
    except pymongo.errors.DuplicateKeyError:
        print "post already in the database"


def main(**kwargs):
    # set args:
    print "setting DB connection"
    conn = pymongo.MongoClient()
    db = conn[kwargs.pop('db')]
    coll = db[kwargs.pop('coll')]
    secs = kwargs.pop('secs')
    radius = kwargs.pop('r')
    loginfile = kwargs.pop('loginfile')
    city = kwargs.pop('fileprefix')
    center = centers[kwargs['locations']]
#    max_id = 0
    # authenticate
    print "authenticating"
    auth = get_login(loginfile)
    api = tweepy.API(auth)
    # set cursor
    geocode = ",".join([str(i) for i in center]) + "," + str(radius) + "km"    
    while True:
        cursor = tweepy.Cursor(api.search, geocode=geocode)
#                               , since_id=max_id)
        for page in cursor.pages():
            print "got [%d] tweets" % len(page)
            sleep(secs)
            for tweet in page:
#                max_id = tweet.id if tweet.id > max_id else max_id
                if tweet.coordinates:
                    insert_tweet(json.loads(tweet.json), coll, city)

if __name__  == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('loginfile')
    parser.add_argument('db')
    parser.add_argument('coll')
    parser.add_argument('fileprefix')
    parser.add_argument('r', help="Radius in km")
    parser.add_argument('-l', '--locations', type=str,
                        help="Available cities: [%s]" %
                        reduce(lambda x, y: x+","+y, centers.keys()))
    parser.add_argument('-t', '--track', nargs='+', type=str, default=None,
                        help="A series of keywords to filter for")
    parser.add_argument('-L', '--languages', nargs='+', type=str,
                        help="A series of language-iso codes to filter for")
    parser.add_argument('-s', '--secs', type=int,
                        default=60, help="Wait time between connections")
    args = vars(parser.parse_args())
    main(**args)
