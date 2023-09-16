#!/usr/bin/env python
from contextlib import redirect_stdout
import os
import io
import time
import sys
import json
import argparse
import logging
from vastai.vast import server_url_default, api_key_file, show__instances, \
search__offers, create__instance, change__bid, destroy__instance

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


parser = argparse.ArgumentParser(description='Snipe vast.ai instances')
parser.add_argument('--max-bid',
    type=float,
    default=0.06,
    help='Maximum hourly bid. Default: %(default)s')
parser.add_argument('--bid-type',
    type=str,
    choices=("bid", "on-demand"),
    default="bid",
    help='Bid type. Default: %(default)s')
parser.add_argument('--query',
    type=str,
    default="reliability > 0.9 num_gpus=1",
    help='Query for offers endpoint. See https://vast.ai/docs/cli/commands#search-offers. Default: %(default)s')
parser.add_argument('--max-instances',
    type=int,
    default=1,
    help='Number of instances to run. Default: %(default)s')
parser.add_argument('--image',
    type=str,
    default="libretranslate/nllu",
    help='Docker image to run. Default: %(default)s')
parser.add_argument('--args',
    default="--server http://5.9.17.93:5555 --dataset paracrawl-en-15e6 --batch-size 1 --split --target-lang nl",
    help='Arguments passed to container entrypoint. Default: %(default)s')
parser.add_argument('--disk-space',
    type=float,
    default=10.0,
    help='Disk space to allocate in GB. Default: %(default)s')
args = parser.parse_args()

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
        self.onstart = None
        self.jupyter_dir = None
        self.jupyter_lab = None
        self.jupyter = None
        self.ssh = None
        self.env = ""
        self.label = ""
        self.extra = ""
        self.login = ""
        self.onstart_cmd = ""
        self.python_utf8 = True
        self.lang_utf8 = True
        self.create_from = None
        self.force = False
        for k,v in kwargs.items():
            setattr(self, k, v)

def capture_json(func):
    def wrapper(**kwargs):
        with redirect_stdout(io.StringIO()) as f:
            func(Args(**kwargs))
        res = f.getvalue()
        try:
            return json.loads(res)
        except:
            return {}
    return wrapper

show_instances = capture_json(show__instances)
search_offers = capture_json(search__offers)
create_instance = capture_json(create__instance)
change_bid = capture_json(change__bid)
destroy_instance = capture_json(destroy__instance

)
while True:
    my_instances = show_instances()

    run_instances = [i for i in my_instances if i['actual_status'] == 'running' or i['actual_status'] is None]
    instance_count = len(run_instances)

    if instance_count < args.max_instances:
        offers = search_offers(type="bid", query=f"rentable=true rented=true disk_space >= {args.disk_space} min_bid < {args.max_bid}  {args.query}")
        for o in offers:
            # Compute bid
            o['bid'] = o['min_bid'] + (args.disk_space * o['storage_cost'] / 30 / 24)
            
        offers.sort(key=lambda o: o['bid'])
        
        created_count = 0
        created_target = args.max_instances - instance_count
        for offer in offers:
            if offer['bid'] > args.max_bid:
                continue
            if created_count >= created_target:
                break

            found = False
            for inst in my_instances:
                if inst['machine_id'] == offer['machine_id']:
                    found = True
                    if inst['actual_status'] is None:
                        # Wait, might be launching
                        pass
                    elif inst['actual_status'] != 'running':
                        # Update bid
                        logging.info(f"Updating bid (m: {offer['machine_id']} c: {offer['gpu_name']} g: {offer['geolocation']} bid: ${offer['bid']})")
                        change_bid(id=inst['id'], price=offer['bid'])
                        time.sleep(2)
                    # TODO: update bid to lower cost?
            if not found:
                # Create
                logging.info(f"Creating instance {offer['id']} (m: {offer['machine_id']} c: {offer['gpu_name']} g: {offer['geolocation']} bid: ${offer['bid']})")

                create_instance(id=offer['id'], price=offer['bid'], disk=args.disk_space, image=args.image, args=args.args.split(" "))
                created_count += 1
                time.sleep(2)
    elif instance_count > args.max_instances:
        my_instances.sort(key=lambda i: i['bid'])
        delete_count = instance_count - args.max_instances
        for _ in range(delete_count):
            inst = my_instances.pop()
            logging.info(f"Destroying instance {inst['id']} (m: {inst['machine_id']} c: {inst['gpu_name']} g: {inst['geolocation']})")
            destroy_instance(id=inst['id'])
            time.sleep(2)
    else:
        logging.info(f'{instance_count} instances running (max is {args.max_instances}), waiting...')
    
    # Check for actual_status['exited'] and destroy those instances

    time.sleep(60)

