from collections import defaultdict
from ldig import ldig
import langdetect
import langid
import cld
import re
import codecs
import json
import tweepy
from time import time
import os

main_dir = '/Users/quique/code/python/twitproj/'


# http://boundingbox.klokantech.com
boxes = {
    'amsterdam': [4.789967, 52.327927, 4.976362, 52.426971],
    'berlin':    [13.089155, 52.33963, 13.761118, 52.675454],
    'antwerp':   [4.245066, 51.113175, 4.611823, 51.323395],
    'madrid':    [-3.834162, 40.312064, -3.524912, 40.56359],
    'brussels':  [4.208164, 50.745668, 4.562496, 50.942552],
    'hamburg':   [9.720519, 53.393829, 10.33352, 53.743495]
}


centers = {
    "berlin": [52.516042, 13.390245],
    "amsterdam": [52.370292, 4.900077],
    "antwerp": [51.220763, 4.401598],
    "brussels": [50.844625, 4.352359],
    "hamburg": [53.561427, 10.032878]
}


with codecs.open(os.path.join(main_dir, 'smilies.txt'), 'r', 'utf-8') as f:
    smilies = "|".join(map(re.escape,
                           [s for s in f.read().strip().split('\t')]))


bots = {'berlin':
        [112169930, 1212442812, 1336218432, 1597521211,
         160874621, 161262801, 186899860, 288715859,
         71528370, 81237494, 2309807226, 343197788,
         352734759, 422055979, 436016601, 456864067]}


tweet_keys = {'created_at': None,
              'text': None,
              'id': None,
              'coordinates': None,
              'user': ['id', 'location', 'followers_count',
                       'friends_count', 'statuses_count',
                       'created_at', 'time_zone'],
              'lang': None,
              'retweeted': None}


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

regexes = [
    re.compile('\(@[^)]+\)'),
    re.compile('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|'
               '[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'),
    re.compile('#[^ ]+'),
    re.compile('@[^ ]+')
]


TIME = time()


def sleepy_time(lim=180):
    global TIME
    process_time = time() - TIME
    TIME = time()
    cooldown = float(15 * 60) / float(lim) - process_time
    return cooldown if cooldown > 0 else 0


def preprocess(tweet, rem_smilies=False):
    for r in regexes:
        tweet = re.sub(r, "", tweet)
    if rem_smilies:
        tweet = re.sub(smilies, "", tweet)
    return tweet.strip()


def in_rect(x, y, x1, y1, x2, y2):
    return x >= x1 and x <= x2 and y >= y1 and y <= y2


def get_login(loginfile):
    with open(loginfile, 'r') as f:
        auths = [l.split(' ')[0] for l in f.readlines()]
    auth = tweepy.OAuthHandler(auths[0], auths[1])
    auth.set_access_token(auths[2], auths[3])
    return auth


def filter_json(keys, json_obj):
    '''
    closure on _filter_json applying SListener.keys
    '''
    acc = defaultdict(defaultdict)
    _filter_json(keys, json_obj, acc)
    return acc


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


def handle_tweet(tweet, tweet_keys=tweet_keys, verbose=True, rem_smilies=False):
    '''
    filters an incoming tweet in json form according to
    a specified tweet_key and adds language guesses
    '''
    # filter tweet
    my_tweet = filter_json(tweet_keys, tweet)
    # add lang guesses
    guesses = handle_lang(tweet['text'])
    guesses['twitter_guess'] = my_tweet['lang']
    del my_tweet['lang']
    my_tweet['langs'] = {}
    for k, v in guesses.items():
        my_tweet['langs'][k] = v
        # process tweet (not necessary)
        # my_tweet['text'] = unicode(my_tweet['text']).encode('utf-8')
        # monitoring
    if verbose:
        print "%s\n%s [%s], %s [%s], %s [%s], %s [%s], %s [%s]"  \
            % tuple([my_tweet['text'].encode('utf-8')] +
                    [i.encode('utf-8') for tpl in my_tweet['langs'].items() for i in tpl])
        print my_tweet['coordinates']
        print "***"
    # return json.dumps(my_tweet)
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
