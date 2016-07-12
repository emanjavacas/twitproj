from utils import *
from slistener import SListener
import tweepy
import argparse
from time import sleep
import pymongo


def main(**kwargs):
    # set args:
    # conn = pymongo.MongoClient()
    # db = conn[kwargs.pop('db')]
    # coll = db[kwargs.pop('coll')]
    db = kwargs.pop('db')
    coll = kwargs.pop('coll')
    secs = kwargs.pop('secs')
    loginfile = kwargs.pop('loginfile')
    city = kwargs.pop('city')
    kwargs['locations'] = boxes[kwargs['locations']]
    # authenticate
    with open(loginfile, 'r') as f:
        auths = [l.split(' ')[0] for l in f.readlines()]
    auth = tweepy.OAuthHandler(auths[0], auths[1])
    auth.set_access_token(auths[2], auths[3])
    api = tweepy.API(auth)
    # set connection
    stream = tweepy.Stream(auth=auth,
                           listener=SListener(
                               "coll",
                               api=api,
                               city=city))
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
    parser.add_argument('city')
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


# from utils import *
# from slistener import SListener
# import tweepy
# import argparse
# from time import sleep
# import pymongo


# def main(**kwargs):
#     # set args:
#     conn = pymongo.MongoClient()
#     db = conn[kwargs.pop('db')]
#     coll = db[kwargs.pop('coll')]
#     print "connected to " + str(coll)
#     fileprefix = kwargs.pop('fileprefix')
#     locations = boxes.get(kwargs['locations'], None)
#     secs = kwargs.pop('secs')
#     loginfile = kwargs.pop('loginfile')
#     with open(loginfile, 'r') as f:
#         auths = [l.split(' ')[0] for l in f.readlines()]
#     auth = tweepy.OAuthHandler(auths[0], auths[1])
#     auth.set_access_token(auths[2], auths[3])
#     auth.secure=True
#     api = tweepy.API(auth)
#     # set connection
#     stream = tweepy.Stream(auth=auth,
#                            listener=SListener(
# #                               coll,
#                                api=api,
#                                fileprefix=fileprefix))
#     print str(locations)
#     while True:
#         try:
#             stream.filter()
#         except Exception as e:
#             print "Error! [%s]" % e
#             stream.disconnect()
#             print "Waiting [%d] sec until reconnecting" % secs
#             sleep(secs)

# if __name__  == '__main__':
#     parser = argparse.ArgumentParser()
#     parser.add_argument('loginfile')
#     parser.add_argument('fileprefix')
#     parser.add_argument('coll')
#     parser.add_argument('db')
#     parser.add_argument('-l', '--locations', type=str,
#                         help="Available cities: [%s]" %
#                         reduce(lambda x, y: x+","+y, boxes.keys()))
#     parser.add_argument('-t', '--track', nargs='+', type=str, default=None,
#                         help="A series of keywords to filter for")
#     parser.add_argument('-s', '--secs', type=int,
#                         default=60, help="Wait time between connections")
#     args = vars(parser.parse_args())
#     main(**args)
