from collections import defaultdict
from ldig import ldig
import langdetect 
import langid
import cld
import re
import codecs

# http://boundingbox.klokantech.com
boxes = {
    'amsterdam': [4.789967, 52.327927, 4.976362, 52.426971],
    'berlin':    [13.089155, 52.33963, 13.761118, 52.675454],
    'antwerp':   [4.245066, 51.113175, 4.611823, 51.323395],
    'madrid':    [-3.834162, 40.312064, -3.524912, 40.56359],
    'brussels':  [4.208164, 50.745668, 4.562496, 50.942552]
}


tweet_keys = {'created_at': None,
              'text': None,
              'id': None,
              'coordinates': None,
              'user': ['id', 'location', 'followers_count',
                       'friends_count', 'statuses_count',
                       'created_at', 'time_zone'],
              'lang': None,
              'retweeted': None}


det = ldig.ldig('ldig/models/model.latin')
def langDetect(tweet):
    try:
        return langdetect.detect(tweet)
    except langdetect.lang_detect_exception.LangDetectException:
        return "unk"

detectors = {
    'langid_guess': lambda x: langid.classify(x)[0],
    'ldig_guess': lambda x:  det.detect('model.latin', x)[1],
    'cld_guess': lambda x: cld.detect(x.encode('utf-8'))[1],
    'langdetect_guess' : lambda x: langDetect(x)
    }

regexes = [
    r'\(@[^)]+\)',
    r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
    r'#[^ ]+',
    r'@[^ ]+'
]
with codecs.open('smilies.txt', 'r', 'utf-8') as f:
    smilies = "|".join(map(re.escape, [s for s in f.read().strip().split('\t')]))

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
    return auths


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


def handle_tweet(tweet, tweet_keys, verbose=True, rem_smilies=False):
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
            ids.append((user_id, int(float(count))))
    return ids
