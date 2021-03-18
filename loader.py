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
            if len(trip['alternatives']) != 0:
                offer = transform_trip(trip)

                # request-level information
                request_id = trip['legId']
                redis.jsonset(request_id + ':offers', rj.Path.rootPath(), offer[request_id]['offers'])

                if i % NPRINT == 0:
                    print('insert #{} (request_id: {})'.format(i, request_id))

                # offer-level information
                for alternative in trip['alternatives']:
                    offer_id = alternative['id']
                    # start_time
                    redis.jsonset(request_id + ':' + offer_id + ':start_time', rj.Path.rootPath(),
                                  offer[request_id][offer_id]['start_time'])
                    # end_time
                    redis.jsonset(request_id + ':' + offer_id + ':end_time', rj.Path.rootPath(),
                                  offer[request_id][offer_id]['end_time'])
                    # duration
                    redis.jsonset(request_id + ':' + offer_id + ':duration', rj.Path.rootPath(),
                                  offer[request_id][offer_id]['duration'])
                    # num_interchanges
                    redis.jsonset(request_id + ':' + offer_id + ':num_interchanges', rj.Path.rootPath(),
                                  offer[request_id][offer_id]['num_interchanges'])
                    # legs
                    redis.jsonset(request_id + ':' + offer_id + ':legs', rj.Path.rootPath(),
                                  offer[request_id][offer_id]['legs'])
                    # offer_items
                    redis.jsonset(request_id + ':' + offer_id + ':offer_items', rj.Path.rootPath(),
                                  offer[request_id][offer_id]['offer_items'])
                    # bookable_total
                    redis.jsonset(request_id + ':' + offer_id + ':bookable_total', rj.Path.rootPath(),
                                  offer[request_id][offer_id]['bookable_total'])
                    # complete_total
                    redis.jsonset(request_id + ':' + offer_id + ':complete_total', rj.Path.rootPath(),
                                  offer[request_id][offer_id]['complete_total'])
                    # currency
                    redis.jsonset(request_id + ':' + offer_id + ':currency', rj.Path.rootPath(),
                                  offer[request_id][offer_id]['currency'])

                    # leg-level information
                    for segment in alternative['segments']:
                        for leg in segment['legs']:
                            leg_id = leg['id']
                            # leg_type
                            redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'leg_type',
                                          rj.Path.rootPath(), offer[request_id][offer_id][leg_id]['leg_type'])
                            # start_time
                            redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'start_time',
                                          rj.Path.rootPath(), offer[request_id][offer_id][leg_id]['start_time'])
                            # end_time
                            redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'end_time',
                                          rj.Path.rootPath(), offer[request_id][offer_id][leg_id]['end_time'])
                            # duration
                            redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'duration',
                                          rj.Path.rootPath(), offer[request_id][offer_id][leg_id]['duration'])
                            # transportation_mode
                            redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'transportation_mode',
                                          rj.Path.rootPath(),
                                          offer[request_id][offer_id][leg_id]['transportation_mode'])
                            # leg_stops
                            redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'leg_stops',
                                          rj.Path.rootPath(), offer[request_id][offer_id][leg_id]['leg_stops'])
                            # leg_track
                            redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'leg_track',
                                          rj.Path.rootPath(), offer[request_id][offer_id][leg_id]['leg_track'])
                            # travel_expert
                            redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'travel_expert',
                                          rj.Path.rootPath(), offer[request_id][offer_id][leg_id]['travel_expert'])
                            if offer[request_id][offer_id][leg_id]['leg_type'] == 'timed':
                                # line
                                redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'line',
                                              rj.Path.rootPath(), offer[request_id][offer_id][leg_id]['line'])
                                # journey
                                redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'journey',
                                              rj.Path.rootPath(), offer[request_id][offer_id][leg_id]['journey'])
                            elif offer[request_id][offer_id][leg_id]['leg_type'] == 'ridesharing':
                                # driver
                                redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'driver',
                                              rj.Path.rootPath(), offer[request_id][offer_id][leg_id]['driver'])
                                # passenger
                                redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'passenger',
                                              rj.Path.rootPath(), offer[request_id][offer_id][leg_id]['passenger'])
                                # vehicle
                                redis.jsonset(request_id + ':' + offer_id + ':' + leg_id + ':' + 'vehicle',
                                              rj.Path.rootPath(), offer[request_id][offer_id][leg_id]['vehicle'])

                i = i + 1

    exit(0)
