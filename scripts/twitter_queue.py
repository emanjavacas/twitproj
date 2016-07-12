import sys
sys.path.append('../')
from utils import *
from Queue import Queue
from threading import Thread


class TwitterQueue():
    def __init__(self, on_status, workers=5, **kwargs):
        """
        on_status: function defining the workload for the threads
        when getting data from the queue
        """
        self.queue = Queue(**kwargs)
        self.on_status = on_status
        for _ in range(workers):
            worker = Thread(target=self.target, args=())
            worker.setDaemon(True)
            worker.start()

    def target(self):
        """target skeleton. Uses self.on_status as a target"""
        while True:
            it = self.queue.get()
            self.on_status(it)
            self.queue.task_done()

    def put(self, tweet):
        self.queue.put(tweet)

# auth = get_login("../loginfile_test~")
# api = tweepy.API(auth)
# api.Cursor(api.friends, 392898773, count=100)
# a_id = 392898773
# friends = api.friends_ids(a_id)
# user = api.get_user(a_id)
