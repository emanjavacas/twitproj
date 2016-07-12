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
            # self.coll.insert(tweet)
            self.queue.task_done()

    def on_data(self, raw_data):
        print "got data"
        data = json.loads(raw_data)
        if 'delete' in data:
            delete = data['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(data['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = data['warnings']
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

    def on_error(self, status_code):
        print "Received error with status code: " + str(status_code)
        return True

# from utils import *
# from tweepy import StreamListener, API
# import time, json
# # from Queue import Queue
# # from threading import Thread

# # create a subclass of StreamListener with desired settings
# class SListener(StreamListener):
#     ''' creates a stream '''

#     def __init__(self, api=None, fileprefix='streamer', workers=5):
#         # self.coll = coll
#         self.api = api or API()
#         self.fileprefix = fileprefix
#         self.deletefile = 'delete.txt'        
#         self.tweet_keys = tweet_keys
#         # self.queue = Queue()
#     #     for _ in range(workers):
#     #         print "init worker [%d]" % (_+1)
#     #         worker = Thread(target=self.insert_tweet, args=())
#     #         worker.setDaemon(True)
#     #         worker.start()
            
#     # def insert_tweet(self):
#     #     while True:
#     #         status = self.queue.get()
#     #         tweet = handle_tweet(status, self.tweet_keys)
#     #         tweet["city"] = self.fileprefix
#     #         print " inserted tweet "
#     #         self.coll.insert(tweet)
#     #         self.queue.task_done()

#     def on_data(self, raw_data):
#         return raw_data
#         print "got data"
#         data = json.loads(raw_data)
#         if 'delete' in data:
#             delete = data['delete']['status']
#             if self.on_delete(delete['id'], delete['user_id']) is False:
#                 return False
#         elif 'limit' in data:
#             if self.on_limit(data['limit']['track']) is False:
#                 return False
#         elif 'warning' in data:
#             warning = data['warnings']
#             print warning['message']
#             return False
#         else:
#             if data['coordinates']:
#                 self.on_status(data)
                
#     def on_error(self, status_code):
#         print "Received error with status code: " + str(status_code)
#         return True

#     def on_delete(self, status_id, user_id):
#         with open(self.deletefile, 'a') as f:
#             f.write(str(status_id) + '\n')
#         return

#     def on_status(self, status):
# #        self.queue.put(status)
#         _log_tweet(status)
#         return status
