#!/usr/bin/env python3
##############################################################################
# Based on:
#   https://github.com/RedisJSON/redisjson-py
##############################################################################

import argparse
from pprint import pprint
import rejson as rj


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('request_ids',
                        metavar='<request_id>',
                        nargs='+',
                        help='Request ids to query.')
    args = parser.parse_args()

    redis = rj.Client(host='localhost', port=6379, decode_responses=True)

    # getting a trip back
    for reqid in args.request_ids:
        print("Request id: {}".format(reqid))
        example = redis.jsonget('{}:offers'.format(reqid),
                                rj.Path.rootPath()
                                )
        pprint(example)

    exit(0)
