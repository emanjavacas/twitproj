from tweepy import models, Cursor, API, AppAuthHandler, error
import json


@classmethod
def parse(cls, api, raw):
        status = cls.first_parse(api, raw)
        setattr(status, 'json', json.dumps(raw))
        return status

models.Status.first_parse = models.Status.parse
models.Status.parse = parse


def loginApi(loginfile):
    with open(loginfile, 'r') as f:
        auths = [l.split(' ')[0] for l in f.readlines()]
    auth = AppAuthHandler(auths[0], auths[1])
    api = API(auth, wait_on_rate_limit=True,
              wait_on_rate_limit_notify=True)
    return api
