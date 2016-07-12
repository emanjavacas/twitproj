import json
import cld, langid
from ldig import ldig
import pymongo
import os
import codecs

det = ldig.ldig('ldig/models/model.latin')
test = '/home/manjavacas/python/twitproj/test_051214_213456.json'
in_dir = '/home/manjavacas/python/twitproj/streaming_data/'
files = os.listdir(in_dir)
conn = pymongo.MongoClient()
db = conn['twitdb']
coll = db['tweets']

def read_tweets(fname):
    """generator of tweets from file"""
    with codecs.open(fname, 'r', 'utf-8') as f:
        for l in f:
            tweet = json.loads(l.strip())
            yield tweet


def get_city(fname):
    return os.path.basename(fname).split('_')[0]

def handle_tweet(tweet):
    if 'langid_guess' not in tweet:
        tweet['langid_guess'] = langid.classify(tweet['text'])[0]
    if 'ldig_guess' not in tweet:
        tweet['ldig_guess'] = det.detect('model.latin', tweet['text'])[1]
    cld_guess = cld.detect(tweet['text'].encode('utf-8'))[1]
    guesses = {'langid_guess' : tweet['langid_guess'],
               'ldig_guess' : tweet['ldig_guess'],
               'twitter_guess' : tweet['lang'],
               'cld_guess' : cld_guess}
    del tweet['langid_guess']
    del tweet['ldig_guess']
    del tweet['lang']
    tweet['langs'] = guesses
    return tweet

for fname in files:
    city = get_city(fname)
    for t in read_tweets(in_dir + fname):
        parsed_t = handle_tweet(t)
        parsed_t['city'] = city
        try:
            coll.insert(parsed_t)
            print "indexed"
        except pymongo.errors.DuplicateKeyError:
            print "post already in the database"
