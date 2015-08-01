from utils import *
from tweepy import StreamListener, API
import json
from Queue import Queue
from threading import Thread


class SListener(StreamListener):

    def __init__(self, coll, api=None, city='unknown', workers=5):
        self.coll = coll
        self.api = api or API()
        self.city = city
        self.deletefile = 'delete.txt'
        self.queue = Queue()
        for _ in range(workers):
            worker = Thread(target=self.insert_tweet, args=())
            worker.setDaemon(True)
            worker.start()

    def insert_tweet(self):
        while True:
            status = self.queue.get()
            tweet = handle_tweet(status)
            tweet["city"] = self.city
            self.coll.insert(tweet)
            self.queue.task_done()

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

    def on_status(self, status):
        self.queue.put(status)
        return
