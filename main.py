#!/usr/bin/env python
from contextlib import redirect_stdout
import os
import io
import sys
import json
from vastai.vast import server_url_default, api_key_file, show__instances, search__offers

MAX_BID = 0.06

if os.path.exists(api_key_file):
    with open(api_key_file, "r") as reader:
        api_key = reader.read().strip()
else:
    print("You need to set an API key first: vastai set api-key xxx-xxxxx")
    exit(1)

class Args:
    def __init__(self, **kwargs):
        self.api_key = api_key
        self.url = server_url_default
        self.explain = False
        self.retry = 3
        self.raw = True
        self.no_default = False
        self.order = '-score'
        self.disable_bundling = False
        for k,v in kwargs.items():
            setattr(self, k, v)

def capture_json(func):
    def wrapper(**kwargs):
        with redirect_stdout(io.StringIO()) as f:
            func(Args(**kwargs))
        return json.loads(f.getvalue())
    return wrapper

show_instances = capture_json(show__instances)
search_offers = capture_json(search__offers)
# instances = show_instances()
# for inst in instances:
#     print(inst['cur_state'], inst['min_bid'], inst['machine_id'])
#     # if inst['actual_status'] != 'running':
#     #     print(json.dumps(inst))
#     #     exit(0)

# type = bid|on-demand
offers = search_offers(type="bid", query=f"rentable=true reliability > 0.9 min_bid < {MAX_BID}")
print(json.dumps(offers[0]))

