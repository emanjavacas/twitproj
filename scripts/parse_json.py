import json
import sys

in_fn = sys.argv[1]
out_fn = sys.argv[2]

with open(in_fn, "r") as f:
    poly = json.load(f)

for p in poly['features']:
    new_geo = [[j, i] for (i, j) in p['geometry']['coordinates'][0]]
    del p['geometry']['coordinates']
    p['geometry']['coordinates'] = [new_geo]

with open(out_fn, "w") as f:
    json.dump(poly, f)
