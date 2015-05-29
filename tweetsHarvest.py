from utils import *
from slistener import SListener
import tweepy
import argparse
from time import sleep
import pymongo


def main(**kwargs):
    # set args:
    conn = pymongo.MongoClient()
    db = conn[kwargs.pop('db')]
    coll = db[kwargs.pop('coll')]
    fileprefix = kwargs.pop('fileprefix')
    secs = kwargs.pop('secs')
    loginfile = kwargs.pop('loginfile')
    kwargs['locations'] = boxes[kwargs['locations']]
    auths = get_login(loginfile)
    # authenticate
    auth = tweepy.OAuthHandler(auths[0], auths[1])
    auth.set_access_token(auths[2], auths[3])
    api = tweepy.API(auth)
    # set connection
    stream = tweepy.Stream(auth=auth,
                           listener=SListener(
                               coll,
                               api=api,
                               fileprefix=fileprefix))
    print kwargs
    while True:
        try:
            stream.filter(**kwargs)
        except Exception as e:
            print "Error! [%s]" % e
            stream.disconnect()
            print "Waiting [%d] sec until reconnecting" % secs
            sleep(secs)

if __name__  == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('loginfile')
    parser.add_argument('fileprefix')
    parser.add_argument('coll')
    parser.add_argument('db')
    parser.add_argument('-l', '--locations', type=str,
                        help="Available cities: [%s]" %
                        reduce(lambda x, y: x+","+y, boxes.keys()))
    parser.add_argument('-t', '--track', nargs='+', type=str, default=None,
                        help="A series of keywords to filter for")
    parser.add_argument('-L', '--languages', nargs='+', type=str,
                        help="A series of language-iso codes to filter for")
    parser.add_argument('-s', '--secs', type=int,
                        default=60, help="Wait time between connections")
    args = vars(parser.parse_args())
    main(**args)





