#!/usr/bin/env python3
##############################################################################
# Based on:
#   https://github.com/RedisJSON/redisjson-py
##############################################################################

from pprint import pprint
import rejson as rj


if __name__ == '__main__':
    redis = rj.Client(host='localhost', port=6379, decode_responses=True)

    # getting a trip back
    example = redis.jsonget('#23:17780:offers', rj.Path.rootPath())

    # import ipdb; ipdb.set_trace()

    pprint(example)

    exit(0)
