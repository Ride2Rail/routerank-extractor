#!/usr/bin/env python3
##############################################################################
# Based on:
#   https://github.com/RedisJSON/redisjson-py
##############################################################################

import argparse
import json
import itertools
import rejson as rj
from typing import IO, Union

import compressed_stream as cs
from cache_format import transform_trip


NPRINT = 1000


def open_jsonobjects_file(path: Union[str, IO]):
    """Open a file of JSON object, one per line,
       decompressing it if necessary."""
    f = cs.functions.open_file(
        cs.functions.file(path)
    )

    return (json.loads(line) for line in f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_files',
                        metavar='<json_file>',
                        nargs='+',
                        help='JSON file to load')
    args = parser.parse_args()

    json_files = []
    for json_file in args.input_files:
        json_files.append(open_jsonobjects_file(json_file))
    json_files = itertools.chain.from_iterable(json_files)

    redis = rj.Client(host='localhost', port=6379, decode_responses=True)

    i = 1
    for json_file in json_files:
        for trip in json_file:
            offer = transform_trip(trip)
            if i % NPRINT == 0:
                print('insert #{} (tripId: {})'.format(i, trip['legId']))
            redis.jsonset(trip['legId'], rj.Path.rootPath(), offer[trip['legId']])
            i = i + 1


    # getting a trip backs
    #redis.jsonget('#24:28250', rj.Path.rootPath())
    # import ipdb; ipdb.set_trace()

    exit(0)
