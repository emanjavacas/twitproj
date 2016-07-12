from data import *
from collections import defaultdict
from ldig import ldig
import logging
import langdetect
import langid
import cld
import re
import codecs
import json
import tweepy
import os

main_dir = "/home/manjavacas/python/twitproj/"
det = ldig.ldig(os.path.join(main_dir, 'ldig/models/model.latin'))


def langDetect(tweet):
    try:
        return langdetect.detect(tweet)
    except langdetect.lang_detect_exception.LangDetectException:
        return "unk"

detectors = {
    'langid_guess': lambda x: langid.classify(x)[0],
    'ldig_guess': lambda x:  det.detect('model.latin', x)[1],
    'cld_guess': lambda x: cld.detect(x.encode('utf-8'))[1],
    'langdetect_guess': lambda x: langDetect(x)
    }


class Logger(object):
    def __init__(self, name, filename):
        self.logger = self._getLogger(name, filename)

    def _getLogger(self, name, filename):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        h = logging.StreamHandler()
        f = logging.FileHandler(filename)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        h.setFormatter(formatter)
        f.setFormatter(formatter)
        logger.addHandler(h)
        logger.addHandler(f)
        return logger


def preprocess(tweet, rem_smilies=False):
    for r in regexes:
        tweet = re.sub(r, "", tweet)
    if rem_smilies:
        tweet = re.sub(smilies, "", tweet)
    return tweet.strip()


def in_rect(x, y, x1, y1, x2, y2):
    return x >= x1 and x <= x2 and y >= y1 and y <= y2

def getAuth(loginfile):
    with open(loginfile, 'r') as f:
        auths = [l.split(' ')[0] for l in f.readlines()]
    auth = tweepy.OAuthHandler(auths[0], auths[1])
    auth.set_access_token(auths[2], auths[3])
    return auth

def loginApp(loginfile):
    with open(loginfile, 'r') as f:
        auths = [l.split(' ')[0] for l in f.readlines()]
    auth = tweepy.AppAuthHandler(auths[0], auths[1])
    api = tweepy.API(auth, wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)
    return api


def loginOAuth(loginfile):
    with open(loginfile, 'r') as f:
        auths = [l.split(' ')[0] for l in f.readlines()]
    auth = tweepy.OAuthHandler(auths[0], auths[1])
    auth.set_access_token(auths[2], auths[3])
    api = tweepy.API(auth)
    return api


def filter_json(keys, json_obj):
    '''
    closure on _filter_json applying SListener.keys
    '''
    acc = defaultdict(defaultdict)
    _filter_json(keys, json_obj, acc)
    return json.loads(json.dumps(acc))


def _filter_json(keys, json_obj, acc):
    ''' filter a json object according to a python dictionary
    :params keys: python dict, leaves are either None or a list'''
    for k, v in keys.items():
        if type(v) == list:
            for i in v:
                acc[k][i] = json_obj[k][i]
        elif not v:
            acc[k] = json_obj[k]
        else:
            _filter_json(v, json_obj[k], acc[k])


def handle_lang(tweet, rem_smilies=False):
    ''' return language guesses according the detectors dictionary '''
    tweet = preprocess(tweet, rem_smilies)
    return {k: v(tweet) for k, v in detectors.items()}


def _log_tweet(tweet, verbose=True):
    if not verbose:
        return
    print "%s\n%s [%s], %s [%s], %s [%s], %s [%s], %s [%s]"  \
        % tuple([tweet['text'].encode('utf-8')] +
                [i.encode('utf-8') for tpl in tweet['langs'].items()
                 for i in tpl])
    print tweet['coordinates']
    print "***"


def handle_tweet(tweet, tweet_keys=tweet_keys,
                 verbose=True, rem_smilies=False):
    ''' filters an incoming tweet in json form according to
    a specified tweet_key and adds language guesses  '''
    my_tweet = filter_json(tweet_keys, tweet)
    guesses = handle_lang(tweet['text'])
    guesses['twitter_guess'] = my_tweet['lang']
    my_tweet['langs'] = {}
    for k, v in guesses.items():
        my_tweet['langs'][k] = v
    my_tweet['lang'] = tweet_to_lang(guesses)
    _log_tweet(my_tweet, verbose)
    return my_tweet


def read_ids(infn):
    ids = []
    with open(infn, "r") as f:
        next(f)
        for i in f:
            user_id, count = i.strip().split(",")
            if user_id.startswith('"'):
                user_id = user_id[1:-1]
            ids.append((user_id, int(float(count))))
    return ids


def stream_tweets(fname):
    with codecs.open(fname, 'r', 'utf-8') as f:
        for l in f:
            line = l.strip()
            yield json.loads(line)


def tweet_to_lang(langs):
    my_langs = ['langdetect_guess', 'langid_guess', 'cld_guess']
    result = defaultdict(int)
    for k, v in langs.items():
        if k in my_langs:
            result[v] += 1
    for k, v in result.items():
        if v >= (len(my_langs) / 2) + 1:
            return k
    return 'unknown'


def write_by_lang(infn, outfn, *fields):
    "filters a json tweet file for lang, coordinates and optional fields"
    tweets = stream_tweets(infn)
    with open(outfn, 'a+') as f:
        for tweet in tweets:
            lang = tweet_to_lang(tweet['langs'])
            x, y = tweet['coordinates']['coordinates']
            user_id = str(tweet["user"]["id"])
            output = [lang, str(x), str(y), user_id]
            for field in fields:
                try:
                    output.append(str(tweet[field]))
                except KeyError:
                    print "Field [%s] not found at tweet [%s]" \
                        % (str(f), str(tweet['id']))
                    continue
            if tweet['user']['id'] not in bots['berlin'] and lang:
                    f.write(",".join(output) + "\n")
            elif lang:
                f.write(",".join(output) + "\n")

