from utils import *
from tweepy import StreamListener, API
import time
import json


# create a subclass of StreamListener with desired settings
class SListener(StreamListener):
    ''' creates a stream '''

    def __init__(self, coll, api=None, fileprefix='streamer'):
        self.coll = coll
        self.api = api or API()
        self.counter = 0
        self.fileprefix = fileprefix
        self.deletefile = 'delete.txt'
        # self.update_outputfile()
        self.tweet_keys = tweet_keys

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
        tweet = handle_tweet(status, self.tweet_keys)
        tweet["city"] = self.fileprefix
        self.coll.insert(tweet)
        return

    def update_outputfile(self):
        self.output = './streaming_data/' + self.fileprefix + '_' + \
                      time.strftime('%d%m%y_%H%M%S') + '.json'

