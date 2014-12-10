from tweepy import StreamListener
from collections import defaultdict
import time, json, codecs
from ldig import ldig
import langid

# create a subclass of StreamListener with desired settings
class SListener(StreamListener):
    ''' creates a stream '''
    __detector = ldig.ldig('ldig/models/model.latin')

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
        # if 'in_reply_to_status_id' in data:
        #     self.on_status(data)
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
    
    def on_status(self, status):
        with codecs.open(self.output, 'a', 'utf-8') as f:
            f.write(self.handle_tweet(status) + '\n')
        self.counter += 1
        if self.counter >= 5000:
            self.update_outputfile()
            self.counter = 0
            return False
        return

    def update_outputfile(self):
        self.output = './streaming_data/' + self.fileprefix + '_' + \
                      time.strftime('%d%m%y_%H%M%S') + '.json'


    def handle_tweet(self, tweet):
        '''
        filters an incoming tweet in json form according to
        a specified tweet_key and adds language guesses
        '''
        my_tweet = filter_json(self.tweet_keys, tweet)
        ldig_lang, langid_lang = handle_lang(SListener.__detector, tweet)
        my_tweet['ldig_lang'] = ldig_lang
        my_tweet['langid_lang'] = langid_lang
        my_tweet['text'] = unicode(my_tweet['text']).encode('utf-8')
        return json.dumps(my_tweet)

def filter_json(keys, json_obj):
    e = defaultdict(defaultdict)
    _filter_json(keys, json_obj, e)
    return e

def _filter_json(d, json_obj, acc):
    for k, v in d.items():
        if type(v) == list:
            for i in v:
                acc[k][i] = json_obj[k][i]
        elif not v:
            acc[k] = json_obj[k]
        else: #dict
            _filter_json(v, json_obj[k], acc[k])

def handle_lang(detector, tweet):
    '''
    return language guesses according to the packages:
    ldig
    lang_id
    '''
    ldig_lang = detector.detect('model.latin', tweet['text'])[1]
    langid_lang = langid.classify(tweet['text'])[0]
    # monitoring
    print "%s\nTwitter: [%s], LDIG: [%s], langid: [%s]" \
        % (unicode(tweet['text']), tweet['lang'],
        ldig_lang, langid_lang)
    print tweet['coordinates']
    print "***"
    # return
    return ldig_lang, langid_lang

