from utils import *
import tweepy
from time import sleep
import json

@classmethod
def parse(cls, api, raw):
    status = cls.first_parse(api, raw)
    setattr(status, 'json', json.dumps(raw))
    return status

tweepy.models.Status.first_parse = tweepy.models.Status.parse
tweepy.models.Status.parse = parse

auth = get_login("loginfile_test~")
api = tweepy.API(auth)

while True:
    cursor = tweepy.Cursor(api.search)
    for page in cursor.pages():
        sleep(5)
        for tweet in page:
            t = json.loads(tweet.json)
            print t['coordinates']


class TwitterQueue():
