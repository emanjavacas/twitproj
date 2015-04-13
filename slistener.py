from tweepy import StreamListener
from collections import defaultdict
import time, json, codecs
from ldig import ldig
import langid, cld
import pymongo

conn = pymongo.MongoClient()
db = conn['twitdb']
coll = db['tweets']

# create a subclass of StreamListener with desired settings
class SListener(StreamListener):
    ''' creates a stream '''

    def __init__(self, api=None, fileprefix='streamer'):
        self.api = api or API()
        self.counter = 0
        self.fileprefix = fileprefix
        self.deletefile = 'delete.txt'
        self.update_outputfile()
        self.tweet_keys = {'created_at' : None,
                           'text' : None,
                           'id' : None,
                           'coordinates' : None,
                           'user' : ['id', 'location', 'followers_count',
                                     'friends_count', 'statuses_count',
                                     'created_at', 'time_zone'],
                           'lang' : None,
                           'retweeted' : None}

    def on_data(self, raw_data):
        data = json.loads(raw_data)
        if 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print warning['message']
            return False
        else:
            if data['coordinates']:
                self.on_status(data)

    def on_delete(self, status_id, user_id):
        with open(self.deletefile, 'a') as f:
            f.write(str(status_id) + '\n')
        return
    
    # def on_status(self, status):
    #     with codecs.open(self.output, 'a', 'utf-8') as f:
    #         f.write(self.handle_tweet(status) + '\n')
    #     print self.handle_tweet(status).encode('utf-8')
    #     self.counter += 1
    #     if self.counter >= 5000:
    #         self.update_outputfile()
    #         self.counter = 0
    #         return False
    #     return

    def on_status(self, status):
        coll.insert(self.handle_tweet(status))
        return

    def update_outputfile(self):
        self.output = './streaming_data/' + self.fileprefix + '_' + \
                      time.strftime('%d%m%y_%H%M%S') + '.json'

    def handle_tweet(self, tweet):
        '''
        filters an incoming tweet in json form according to
        a specified tweet_key and adds language guesses
        '''
        # filter tweet
        my_tweet = filter_json(self.tweet_keys, tweet)
        # add lang guesses
        guesses = handle_lang(tweet['text'])
        guesses['twitter_guess'] = my_tweet['lang']
        del my_tweet['lang']
        my_tweet['langs'] = {}
        for k, v in guesses.items():
            my_tweet['langs'][k] = v
        # process tweet (not necessary)
        # my_tweet['text'] = unicode(my_tweet['text']).encode('utf-8')
        my_tweet['city'] = self.fileprefix
        # monitoring
        print "%s\n%s: [%s], %s: [%s], %s: [%s], %s: [%s]" \
            % tuple([my_tweet['text'].encode('utf-8')] +
                    [i.encode('utf-8') for tpl in my_tweet['langs'].items() for i in tpl])
        print my_tweet['coordinates']
        print "***"
        # return json.dumps(my_tweet)
        return my_tweet

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
        else: #dict
            _filter_json(v, json_obj[k], acc[k])

det = ldig.ldig('ldig/models/model.latin')
detectors = {
    'langid_guess' : lambda x: langid.classify(x)[0],
    'ldig_guess' : lambda x:
        det.detect('model.latin', x)[1],
    'cld_guess' : lambda x: cld.detect(x.encode('utf-8'))[1]
    }
    
def handle_lang(tweet):
    ''' return language guesses according the detectors dictionary '''
    return {k : v(tweet) for k, v in detectors.items()}
