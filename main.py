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
parser.add_argument('--bid-spread',
    type=float,
    default=0.00,
    help='Bid this amount over minimum. Default: %(default)s')
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
parser.add_argument('--gpu-blacklist',
    type=str,
    default="",
    help='Blacklist of GPUs (comma separated). Default: %(default)s')

args = parser.parse_args()
gpu_blacklist = [n.lower() for n in args.gpu_blacklist.split(",")]

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
        except Exception as e:
            return {}
    return wrapper

show_instances = capture_json(show__instances)
search_offers = capture_json(search__offers)
create_instance = capture_json(create__instance)
change_bid = capture_json(change__bid)
destroy_instance = capture_json(destroy__instance)


def compute_bid(offers):
    for o in offers:
        # Compute bid
        o['bid'] = o['min_bid'] + (args.disk_space * o['storage_cost'] / 30 / 24) + args.bid_spread
    return offers

def filter_instances(offers):
    machine_ids = {}
    host_ids = {}
    tmp = []

    # Filter duplicates, expensive, bogus instances
    for o in offers:
        if o['bid'] > args.max_bid:
            continue
        if o['machine_id'] in machine_ids:
            continue
        if o['host_id'] in host_ids:
            continue
        if o['bid'] < 0.01:
            continue
        
        blacklisted = False
        for bg in gpu_blacklist:
            if bg in o['gpu_name'].lower():
                blacklisted = True
                break
        
        if blacklisted:
            continue

        machine_ids[o['machine_id']] = True
        host_ids[o['host_id']] = True
        
        tmp.append(o)

    return tmp

def inst_info(inst):
    return f"{inst['id']} (m: {inst['machine_id']} c: {inst['gpu_name']} g: {inst['geolocation']} bid: ${inst.get('bid', 'N/A')})"


def run_instances(instances):
    return [i for i in instances if i['actual_status'] == 'running' or i['actual_status'] is None]
        
while True:
    try:
        my_instances = compute_bid(show_instances())
        my_machine_ids = [i['machine_id'] for i in my_instances]
        my_host_ids = [i['host_id'] for i in my_instances]
        instance_count = len(run_instances(my_instances))

        offers = compute_bid(search_offers(type="bid", query=f"rentable=true rented=true disk_space >= {args.disk_space} min_bid <= {args.max_bid}  {args.query}"))
        offers.sort(key=lambda o: o['bid'])
        offers = filter_instances(offers)
        
        if instance_count < args.max_instances:
            created_count = 0
            created_target = args.max_instances - instance_count
            for offer in offers:
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
                            logging.info(f"Updating bid {inst_info(offer)})")
                            change_bid(id=inst['id'], price=offer['bid'])
                            time.sleep(2)
                if not found:
                    # Create
                    logging.info(f"Creating instance {inst_info(offer)})")

                    create_instance(id=offer['id'], price=offer['bid'], disk=args.disk_space, image=args.image, args=args.args.split(" "))
                    created_count += 1
                    time.sleep(2)
        elif instance_count > args.max_instances:
            my_instances.sort(key=lambda i: i['bid'])
            delete_count = instance_count - args.max_instances
            for _ in range(delete_count):
                inst = my_instances.pop()
                logging.info(f"Destroying instance {inst_info(inst)}")
                destroy_instance(id=inst['id'])
                my_instances = [i for i in my_instances if i['id'] != inst['id']]
                time.sleep(2)
        else:
            # Revise bids
            for inst in run_instances(my_instances):
                for offer in offers:
                    if offer['machine_id'] == inst['machine_id']:
                        if offer['bid'] + 0.01 < inst['dph_total']:
                            logging.info(f"Updating bid {inst_info(offer)}")
                            change_bid(id=inst['id'], price=offer['bid'])
                            time.sleep(2)
            
            # Check for cheaper machines
            # Find most expensive instance
            ri = run_instances(my_instances)
            ri.sort(key=lambda i: i['dph_base'], reverse=True)
            most_expensive = None
            if len(ri) > 0:
                most_expensive = ri[0]
            
            swapped = False
            if most_expensive is not None:
                for offer in offers:
                    if offer['machine_id'] in my_machine_ids:
                        continue
                    if offer['host_id'] in my_host_ids:
                        continue
                    if offer['bid'] < most_expensive['dph_base']:
                        # Swap
                        swapped = True
                        logging.info(f"Swapping out {inst_info(most_expensive)}")
                        destroy_instance(id=most_expensive['id'])
                        my_instances = [i for i in my_instances if i['id'] != most_expensive['id']]
                        time.sleep(2)
                        logging.info(f"For instance {inst_info(offer)})")
                        create_instance(id=offer['id'], price=offer['bid'], disk=args.disk_space, image=args.image, args=args.args.split(" "))
            
            if not swapped:
                # Check for duplicates
                machine_ids = {}
                host_ids = {}
    
                for inst in my_instances:
                    if inst['machine_id'] in machine_ids or inst['host_id'] in host_ids:
                        # Destroy
                        logging.info(f"Destroying duplicate {inst_info(inst)}")
                        destroy_instance(id=inst['id'])
                        time.sleep(2)
                    machine_ids[inst['machine_id']] = True
                    host_ids[inst['host_id']] = True
        # TODO: Check for actual_status['exited'] and destroy those instances
    except Exception as e:
        logging.error(str(e))
        
    time.sleep(10)

