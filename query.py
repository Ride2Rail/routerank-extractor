#!/usr/bin/env python3
##############################################################################
# Based on:
#   https://github.com/RedisJSON/redisjson-py
##############################################################################

import argparse
import redis


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('request_ids',
                        metavar='<request_id>',
                        nargs='+',
                        help='Request ids to query.')
    args = parser.parse_args()

    redis = redis.Redis(host='localhost', port=6379)

    # getting a trip back
    for reqid in args.request_ids:
        offers = redis.lrange('{}:offers'.format(reqid), 0, -1)
        print("* request id: {}".format(reqid))
        for offer in offers:
            print("    - offer id: {}".format(offer.decode('utf-8')))
        print("---")

    exit(0)
