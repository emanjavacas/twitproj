import pymongo
from ..utils import handle_lang
import sys

print sys.argv[1]
conn = pymongo.MongoClient()
coll = conn.twitdb[sys.argv[1]]

cursor = coll.find(timeout=False)
errors = 0
for doc in cursor:
    doc_id = doc["_id"]
    langs = handle_lang(doc["text"])
    coll.update({'_id':doc_id}, {"$set":{"langs":langs}})
cursor.close()
# for i in cursor:
#     langs = handle_lang(i["text"])
#     ls = i["langs"]
#     for l in ["langid_guess", "cld_guess"]:
#         if langs[l] != ls[l]:
#             print i["text"].encode("utf-8")
#             print preprocess(i["text"]).encode("utf-8")
#             print i["langs"]
#             print langs
#             break
