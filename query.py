#!/usr/bin/env python3
##############################################################################
# Based on:
#   https://github.com/RedisJSON/redisjson-py
##############################################################################

import argparse
import redis

from r2r_offer_utils.cli_utils import IntRange


if __name__ == '__main__':
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379

    parser = argparse.ArgumentParser()
    parser.add_argument('request_ids',
                        metavar='<request_id>',
                        nargs='+',
                        help='Request ids to query.')
    parser.add_argument('-H', '--host',
                        default=REDIS_HOST,
                        dest='redis_host',
                        help=f'Redis hostname [default: {REDIS_HOST}].')
    parser.add_argument('--port',
                        default=REDIS_PORT,
                        dest='redis_port',
                        type=IntRange(1, 65536),
                        help=f'Redis port [default: {REDIS_PORT}].')

    args = parser.parse_args()

    redis = redis.Redis(host=args.redis_host, port=args.redis_port)

    # getting a trip back
    for reqid in args.request_ids:
        offers = redis.lrange(f'{reqid}:offers', 0, -1)
        print(f"* request id: {reqid}")
        for offer in offers:
            print("    - offer id: {}".format(offer.decode('utf-8')))
        print("---")

    exit(0)
