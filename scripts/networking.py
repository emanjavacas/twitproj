from utils import Logger
from warnings import warn
import pymongo
from random import sample

class UserNotInDbException(Exception):
    pass

class Mongo(object):
    def __init__(self, db="twitdb", api=None,
                 users="turkish_network",
                 tweets="turkish_tweets"):
        if not api:
            warn("""
            Mongo was instantiated without TweepyApi.
            create_user is disabled.
            """)
        self.api = api
        self.db = db
        self.users_collname = users
        self.tweets_collname = tweets
        self.conn = eval("pymongo.Connection('localhost', 27017, safe=True)." + db)
        self.users = eval("self.conn." + users)
        self.tweets = eval("self.conn." + tweets)
        self.log = Logger(__name__, 'network.log').logger

    # Utils
    def _check_collections(self):
        """fast check on collections to be used on first setup"""
        if self.users_collname in self.conn.collection_names() or \
           self.tweets_collname in self.conn.collection_names():
            raise ValueError("Collection already exits!")
        else:
            self.users.create_index(
                [("id", pymongo.DESCENDING),
                 ("tweets.id", pymongo.DESCENDING)],
                unique=True
            )
            self.tweets.create_index(
                [("id", pymongo.DESCENDING)],
                unique=True
            )

    def _init_db(self, cursor, from_target):
        """call only once on a cursor of tweets by target users
        from_target must be a string denoting the ppal language"""
        self.log.info(
            "started indexing database with %d docs" % cursor.count()
        )
        self._check_collection()
        for tweet in cursor:
            self.insert_tweet(tweet, from_target=from_target)

    # Insertions and Updates
    def _update_count_in_db(self, target_user, *fields):
        "fields: [friends_in_db, followers_in_db]"

        def aux(field):
            items = self.retrieve_from_user(target_user, field.split("_")[0])
            return self.users.find({"id": {"$in": items}}).count()

        self.users.update(
            {"id": target_user},
            {"$set": {field: aux(field) for field in fields}}
        )

    def _tweet_to_db(self, tweet):
        try:
            self.tweets.insert(tweet)
            self.log.info("Inserted tweet [%d] to user [%d]" % (tweet["id"], tweet["user"]["id"]))
        except Exception as e:
            self.log.error("Exception [%s] when indexing tweet %d" %
                           (e, tweet["id"]))
        except pymongo.errors.DuplicateKeyError:
            self.log.error("DuplicateKeyError on tweet [%d]" % tweet["id"])

    def insert_tweet(self, tweet, from_target=False):
        user_id = tweet["user"]["id"]
        if self.user_in_db(user_id):
            self._tweet_to_db(tweet)
        else:
            raise UserNotInDbException("Tweet from unknown user [%d]" % user_id)
            #self.create_user(tweet, from_target)

    # def add_tweet_to_user(self, user_id, tweet):
    #     assert user_id
    #     self.log.info("Statuses in db:" + str(self.retrieve_from_user(user["id"], "statuses_in_db")[0]))
    #     self.users.update(
    #         {"id": user_id},  # query
    #         {"$inc": {"statuses_in_db": 1},
    #          "$max":
    #          {"followers_count": tweet["user"]["followers_count"],
    #           "statuses_count": tweet["user"]["statuses_count"],
    #           "friends_count": tweet["user"]["friends_count"]}}
    #     )
    #     self._tweet_to_db(tweet)

    def create_user(self, tweet, is_target):
        if not self.api:
            raise ValueError("Creating a user requires Tweepy API")
        if type(tweet) != dict:
            user = self.api.get_user(tweet)
        else:
            user = tweet["user"]
            self._tweet_to_db(tweet)
        friends = self.api.get_friends(tweet["user"]["id"])
        followers = self.api.get_followers(tweet["user"]["id"])
        user = {
            "id": user["id"],
            "screen_name": user["screen_name"],
            "target": is_target,
            "location": user["location"],
            "time_zone": user["time_zone"],
            "created_at": user["created_at"],

            "statuses_count": user["statuses_count"],
            "friends_count": user["friends_count"],
            "followers_count": user["followers_count"],

            "statuses_in_db": 1,
            "friends_in_db": None,
            "followers_in_db": None,

            "friends": friends,
            "followers": followers,
        }
        self.users.insert(user)
        self.log.info("Inserted user [%d]" % user["id"])

    # Queries
    def user_in_db(self, user_id, min_count=0):
        tweets_count = len(list(self.retrieve_tweets_from_user(user_id)))
        statuses_count = self.retrieve_from_user(user_id, "statuses_count")
        return self.users.find({"id": user_id}).count() != 0 and \
                    (tweets_count > min_count or tweets_count == statuses_count[0])

    def retrieve_user(self, user_id):
        return self.users.find({"id": user_id}, timeout=False)

    def retrieve_users(self, **kwargs):
        return self.users.find(kwargs, timeout=False)

    def retrieve_from_user(self, user_id, *fields):
        query = self.users.aggregate([
            {"$match": {"id": user_id}},
            {"$project": {field: 1 for field in fields}}
        ])['result']
        return [query[0][field] for field in fields] if query else [[] for i in range(len(fields))]

    def retrieve_from_users(self, field, **kwargs):
        result = self.users.aggregate([
            {"$match": kwargs},
            {"$project": {field: 1}}
        ], cursor={})
        for doc in result:
            try:
                yield doc[field]
            except KeyError:
                continue

    def retrieve_ffs_from_user(self, user_id, min_count=float("inf")):
        """return two lists of friends and followers still not in db,
        accepts a min_count of tweets for a user to be considered in_db"""
        friends = self.retrieve_from_user(user_id, "friends")
        followers = self.retrieve_from_user(user_id, "followers")

        def sample_user(user_id):
            in_db = len(list(self.retrieve_tweets_from_user(user_id)))
            count = self.retrieve_from_user(user_id, "statuses_count")
            return in_db <= min_count and in_db != count

        sampled_friends = filter(sample_user, friends)
        sampled_followers = filter(sample_user, followers)
        return (sampled_friends, sampled_followers)                

    def _retrieve_tweets_from_user(self, user_id):
        return self.tweets.find({"user.id": user_id}, timeout=False)

    def retrieve_tweets_from_user(self, user_id, *fields):
        tweets = self._retrieve_tweets_from_user(user_id)
        for tweet in tweets:
            yield {f: tweet[f] for f in fields}

    def retrieve_tweets_from_users(self, **kwargs):
        ids = list(self.retrieve_from_users("id", kwargs))
        return self.tweets.find({"user.id": {"$in": ids}}, timeout=False)

    def retrieve_ffs_in_db(self, user_id, min_count=0):
        friends, followers = self.retrieve_from_user(user_id, "friends", "followers")
        def sampled_user(user_id):
            if self.user_in_db(user_id):
                in_db = len(list(self.retrieve_tweets_from_user(user_id)))
                return in_db >= min_count or in_db == self.retrieve_from_user(user_id, "statuses_count")
            return False
        return (filter(sampled_user, friends) if friends else [],
                filter(sampled_user, followers) if followers else [])

    def sample_ffs(self, user_id, n, min_count):
        """min_count = min tweets count, returns a list of m ids
        needed to cover n ffs in db"""
        # friends_in_db, followers_in_db = self.retrieve_ffs_in_db(user_id, min_count)
        # friends, followers = self.retrieve_from_user(user_id, "friends", "followers")
        # todo_friends = None 
        # todo_followers = None
        # if len(list(friends_in_db)) <= n and len(list(friends_in_db)) != len(friends):
        #     not_in_db = [i for i in friends if i not in friends_in_db]
        #     todo_friends = sample(not_in_db, min(n, len(not_in_db)))
        # if len(list(followers_in_db)) <= n and len(list(followers_in_db)) != len(followers):
        #     not_in_db = [i for i in followers if i not in followers_in_db]
        #     todo_followers = sample(not_in_db, min(n, len(not_in_db)))            
        # return (todo_friends or [], todo_followers or [])
        fr, fl = self.retrieve_from_user(user_id, "friends", "followers")
        if not fr and not fl:
            return ([], [])
        fl_in = filter(lambda x: self.user_in_db(x, min_count=min_count), fl)
        fr_in = filter(lambda x: self.user_in_db(x, min_count=min_count), fr)
        fl_not = [i for i in fl if i not in fl_in]
        fr_not = [i for i in fr if i not in fr_in]
        return (sample(fr_not, min(max(0, n-len(fr_in)), len(fr_not))),
                sample(fl_not, min(max(0, n-len(fl_in)), len(fl_not))))
