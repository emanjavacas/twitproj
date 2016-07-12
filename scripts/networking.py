# import sys
# sys.path.append('../')
from utils import *
from pymongo import Connection, DESCENDING
import logging
from time import sleep


class Logger(object):
    def __init__(self, name):
        self.logger = self._getLogger(name)

    def _getLogger(self, name):
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        h = logging.StreamHandler()
        f = logging.FileHandler('network.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        h.setFormatter(formatter)
        f.setFormatter(formatter)
        logger.addHandler(h)
        logger.addHandler(f)
        return logger


class Mongo(object):
    def __init__(self, db="twitdb", coll="turkish_network", api=None):
        self.db = db
        self.coll = coll
        self.conn = eval("Connection()." + db)
        self.docs = eval("self.conn." + coll)
        self.api = api
        self.log = Logger(__name__).logger

    def _check_collection(self):
        if self.coll in self.conn.collection_names():
            raise ValueError("Collection already exits!")
        else:
            self.docs.create_index(
                [("id", DESCENDING),
                 ("tweets.id", DESCENDING)],
                unique=True)

    def _check_api(self, api):
        if not self.api and not api:
            raise ValueError(
                """
                This method requires an API and
                instance was intialized without one.
                """
            )
        else:
            return api or self.api

    # Utils
    def _init_db(self, cursor):
        "call only once on a cursor of tweets by target users"
        self.log.info(
            "started indexing database with %d docs" % cursor.count()
        )
        self._check_collection()
        for doc in cursor:
            try:
                self.insert_tweet(doc, from_target=True)
            except Exception as e:
                self.log.error("Exception [%s] when indexing tweet %d" %
                               (e, doc["id"]))
            except pymongo.errors.DuplicateKeyError:
                self.log.error("DuplicateKeyError on doc [%d]" % doc["id"])
                continue

    def _handle_tweet(self, tweet):
        if "langs" not in tweet:
            tweet = handle_tweet(tweet, verbose=False) 
        tweet['lang'] = tweet_to_lang(tweet["langs"])
        return tweet

    # Tweepy Interactions
    def _wrap_cursor(self, cursor):
        iterator = cursor.pages()
        try:
            for page in iterator:
                yield page
        except error.TweepError as e:
            self.log.error(e)
            raise StopIteration

    def get_from_tweepy(self, user_id, method, api=None):
        "method (str): followers_ids, friends_id"
        api = self._check_api(api)
        try:
            return eval("api.%s(user_id)" % method)
        except error.TweepError as e:
            self.log.error("TweepError [%s] at user %d" % (e, user_id))

    def get_tweets_from_user(self, user_id, api=None):
        api = self._check_api(api)
        cursor = Cursor(api.user_timeline, id=user_id)
        for page in self._wrap_cursor(cursor):
            for tweet in page:
                yield tweet
            sleep(10)

    # Insertions and Updates
    def _update_count_in_db(self, target_user, *fields):
        "fields: [statuses_in_db, friends_in_db, followers_in_db]"

        def aux(field):
            items = self.retrieve_from_user(target_user, field.split("_")[0])
            return self.docs.find({"id": {"$in": items}}).count()

        self.docs.update(
            {"id": target_user},
            {"$set": {field: aux(field) for field in fields}}
        )

    def insert_tweet(self, tweet, from_target=False):
        user = self.docs.find({"id": tweet["user"]["id"]})
        if user.count() > 0:
            assert user.count() == 1  # top level docs are users
            self.add_tweet_to_user(user.next(), tweet)
        else:
            self.log.info("Adding new user [%d]" % tweet["user"]["id"])
            # print "Sleeping 10 sec"
            # sleep(60)
            self.create_user(tweet, from_target)

    def create_user(self, tweet, target):
        friends = self.get_from_tweepy(tweet["user"]["id"],
                                       "friends_ids", api=self.api)
        followers = self.get_from_tweepy(tweet["user"]["id"],
                                         "followers_ids", api=self.api)
        user = {
            "id": tweet["user"]["id"],
            "target": int(target),
            "location": tweet["user"]["location"],
            "time_zone": tweet["user"]["time_zone"],
            "created_at": tweet["user"]["created_at"],

            "statuses_count": tweet["user"]["statuses_count"],
            "friends_count": tweet["user"]["friends_count"],
            "followers_count": tweet["user"]["followers_count"],

            "statuses_in_db": 1,
            "friends_in_db": None,
            "followers_in_db": None,

            "friends": friends,
            "followers": followers,
            "tweets": [self._handle_tweet(tweet)]
        }
        self.docs.insert(user)
        self.log.info("Inserted user [%d]" % user["id"])

    def add_tweet_to_user(self, user, tweet):
        assert user
        self.docs.update(
            {"id": user["id"]},  # query
            {"$inc": {"statuses_in_db": 1},
             "$push": {"tweets": self._handle_tweet(tweet)},
             "$max":
             {"followers_count": tweet["user"]["followers_count"],
              "statuses_count": tweet["user"]["statuses_count"],
              "friends_count": tweet["user"]["friends_count"]}}
        )

    # Queries
    def user_in_db(self, user_id):
        return self.docs.find({"id": user_id}).count() != 0

    def retrieve_from_user(self, user_id, field):
        return self.docs.aggregate([
            {"$match": {"id": user_id}},
            {"$project": {field: 1}}
        ])['result'][0][field]

    def retrieve_all_tweets(self):
        tweets = self.docs.aggregate([
            {"$unwind": "$tweets"},
            {"$project": {"_id": 0, "tweets": 1}}
        ], cursor={})
        for doc in tweets:
            yield doc["tweets"]

    def retrieve_ffs_from_user(self, user_id):
        "return two lists of friends and followers still not in db"
        friends = self.retrieve_from_user(user_id, "friends")
        followers = self.retrieve_from_user(user_id, "followers")

        def sampled_user(user_id):
            status_in_db = self.retrieve_from_user(user_id, "statuses_in_db")
            status_count = self.retrieve_from_user(user_id, "statuses_count")
            return not self.user_in_db(user_id) or \
                status_in_db < min_count or status_in_db < status_count
        return (filter(sampled_user, friends),
                filter(sampled_user, followers))


# conn = Connection().twitdb.berlin_merged
# ids = read_ids("/home/manjavacas/python/twitproj/turks_more")
# ids = [int(i) for (i, j) in filter(lambda (x, y): y >= 20, ids) if i != 'null']
# cursor = conn.find({"user.id": {"$in": ids}, "city": "berlin"})
# db = Mongo()
# db._init_db(cursor)
# self.log.info("Remaining [%d] calls" % self.api.rate_limit_status()['resources']['friends']['/friends/ids']['remaining'])
# self.log.info("Remaining [%d] calls" % self.api.rate_limit_status()['resources']['followers']['/followers/ids']['remaining'])        
