import sys
sys.path.append('../')
from utils import *
import argparse
import tweepy
import json
import pymongo
from time import sleep


@classmethod
def parse(cls, api, raw):
        status = cls.first_parse(api, raw)
        setattr(status, 'json', json.dumps(raw))
        return status

tweepy.models.Status.first_parse = tweepy.models.Status.parse
tweepy.models.Status.parse = parse


def find_idx(lst, fn):
    i = 0
    while not fn(lst[i]):
        i += 1
    return i


# api = tweepy.API(get_login("../loginfile_test~"))
# cursor = tweepy.Cursor(api.user_timeline, id="170255403")
id_total = 0

class IdsInCity():
    def __init__(self, api, ids, city, throttle=10, total=float("inf")):
        self.api = api
        self.ids = ids
        self.city = city
        self.throttle = int(throttle)
        self.total = total

    def put(self, tweet, coll):
        try:
            coll.insert(tweet)
            global id_total
            id_total += 1
            print "inserted tweet from city [%s]; tweet count = [%d]" \
                    % (tweet.get('city', 'unknown'), id_total)
        except pymongo.errors.DuplicateKeyError:
            print "post already in the database"

    def wrap_cursor(self, cursor):
        iterator = cursor.pages()
        try:
            for page in iterator:
                global id_total                
                if id_total >= self.total:
                    raise StopIteration
                yield page
        except tweepy.error.TweepError as e:
            print e
            raise StopIteration

    def handle_tweet(self, tweet):
        json_tweet = json.loads(tweet.json)
        if json_tweet["coordinates"]:
            x, y = json_tweet["coordinates"]["coordinates"]
            in_city = in_rect(x, y, *boxes[self.city])
            my_tweet = handle_tweet(json_tweet, tweet_keys, verbose=False)
            my_tweet['city'] = self.city if in_city else 'unknown'
            return my_tweet

    def run(self, coll, start=None):
        start = find_idx(self.ids, lambda x: x[0] == start) if start else 0
        for i, _ in self.ids[start:]:
            print "*** %s ***" % (i)
            global id_total
            id_total = 0
            cursor = tweepy.Cursor(self.api.user_timeline, id=i)
            for page in self.wrap_cursor(cursor):
                sleep(self.throttle)
                print "recieved page of length [%d] from id [%s]" % (len(page), i)
                for tweet in page:
                    my_tweet = self.handle_tweet(tweet)
                    if my_tweet:
                        self.put(my_tweet, coll)
                    else:
                        continue


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Input file with ids")
    parser.add_argument("-d", "--db", help="database name", required=True)
    parser.add_argument("-e", "--coll", help="output coll", required=True)
    parser.add_argument("-l", "--login", help="login file", required=True)
    parser.add_argument("-c", "--city", help="city to check tweets in",
                        required=True)
    parser.add_argument("-t", "--throttle", help="throttle time", default=10)
    parser.add_argument("-s", "--start", help="start id")

    args = vars(parser.parse_args())
    auth = get_login(args['login'])
    api = tweepy.API(auth)
    coll = pymongo.MongoClient()[args['db']][args['coll']]
    main = IdsInCity(api,
                     read_ids(args['input']),
                     args['city'],
                     args['throttle'],
                     total=150)
    main.run(coll, start=args['start'])
