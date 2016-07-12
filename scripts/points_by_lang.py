import json


def write_by_lang(infn, outfn, *fields):
    tweets=stream_tweets(infn)
    with open(outfn, 'a+') as f:
        for tweet in tweets:
            lang = tweet_to_lang(tweet['langs'])
            x, y = tweet['coordinates']['coordinates']
            if tweet['user']['id'] not in bots['berlin'] and lang:
                f.write(",".join([lang, str(x), str(y)]) + "\n")
