#!/usr/bin/env python3
##############################################################################
# Based on:
#   https://github.com/RedisJSON/redisjson-py
##############################################################################

import sys
import json
import geojson
import argparse
import itertools
import redis
from typing import IO, Union

import compressed_stream as cs
from r2r_offer_utils.cli_utils import IntRange

from loader.cache_format import transform_trip

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
    parser.add_argument('-H', '--host',
                        default='localhost',
                        help='Redis hostname [default: localhost].')
    parser.add_argument('-p', '--port',
                        default=6379,
                        type=IntRange(1, 65536),
                        help='Redis port [default: 6379].')

    args = parser.parse_args()

    print("Reading input...", file=sys.stderr, flush=True)
    json_files = []
    for json_file in args.input_files:
        json_files.append(open_jsonobjects_file(json_file))
    json_files = itertools.chain.from_iterable(json_files)

    print("Connecting to Redis instance on {host}:{port}..."
          .format(host=args.host, port=args.port),
          file=sys.stderr, flush=True)
    redis = redis.Redis(host=args.host, port=args.port)

    trip_count = 1
    insert_count = 1
    for json_file in json_files:
        for trip in json_file:

            if trip_count % NPRINT == 0:
                print('{} trips read (trip_id: {})'
                      .format(trip_count, trip['tripId']),
                      file=sys.stderr, flush=True)

            trip_count = trip_count + 1

            if len(trip['alternatives']) > 0:
                # get mobility request data
                mreq = transform_trip(trip)

                # request-level information
                request_id = '{tripId}-{legId}'.format(tripId=trip['tripId'], legId=trip['legId'])

                # insert a list
                #   https://stackoverflow.com/a/39930371/2377454
                redis.lpush('{request_id}:offers'.format(request_id=request_id),
                            *mreq[request_id]['offers']
                            )

                # offer-level information
                for alternative in trip['alternatives']:
                    offer_id = alternative['id']
                    prefix = ('{request_id}:{offer_id}'
                              .format(request_id=request_id,
                                      offer_id=offer_id)
                              )

                    # start_time
                    redis.set('{p}:start_time'.format(p=prefix),
                              mreq[request_id][offer_id]['start_time']
                              )
                    # end_time
                    redis.set('{p}:end_time'.format(p=prefix),
                              mreq[request_id][offer_id]['end_time']
                              )
                    # duration
                    redis.set('{p}:duration'.format(p=prefix),
                              mreq[request_id][offer_id]['duration']
                              )
                    # num_interchanges
                    redis.set('{p}:num_interchanges'.format(p=prefix),
                              mreq[request_id][offer_id]['num_interchanges']
                              )

                    # legs
                    if mreq[request_id][offer_id]['legs']:
                        redis.lpush('{p}:legs'.format(p=prefix),
                                    *mreq[request_id][offer_id]['legs']
                                    )

                    # offer_items
                    if mreq[request_id][offer_id]['offer_items']:
                        redis.lpush('{p}:offer_items'.format(p=prefix),
                                    *mreq[request_id][offer_id]['offer_items']
                                    )

                    # bookable_total
                    if mreq[request_id][offer_id]['bookable_total']:
                        redis.set('{p}:bookable_total'.format(p=prefix),
                                  mreq[request_id][offer_id]['bookable_total']
                                  )

                    # complete_total
                    # Switch from hmset() to hset() in Redis
                    #   https://stackoverflow.com/a/61826471/2377454
                    ct_dict = mreq[request_id][offer_id]['complete_total']
                    for k, v in ct_dict.items():
                        field = ('{p}:complete_total:{k}'
                                 .format(p=prefix, k=k))
                        redis.hset(field, k, v)
                    del ct_dict, field

                    # leg-level information
                    for segment in alternative['segments']:
                        for leg in segment['legs']:
                            leg_id = leg['id']
                            # leg_type
                            prefix_leg = ('{request_id}:{offer_id}:{leg_id}'
                                          .format(request_id=request_id,
                                                  offer_id=offer_id,
                                                  leg_id=leg_id)
                                          )

                            leg = mreq[request_id][offer_id][leg_id]
                            redis.set('{pl}:leg_type'.format(pl=prefix_leg),
                                      leg['leg_type']
                                      )
                            # start_time
                            redis.set('{pl}:start_time'.format(pl=prefix_leg),
                                      leg['start_time']
                                      )
                            # end_time
                            redis.set('{pl}:end_time'.format(pl=prefix_leg),
                                      leg['end_time']
                                      )
                            # duration
                            redis.set('{pl}:duration'.format(pl=prefix_leg),
                                      leg['duration']
                                      )
                            # transportation_mode
                            redis.set('{pl}:transportation_mode'.format(pl=prefix_leg),
                                      leg['transportation_mode']
                                      )
                            # leg_stops
                            redis.set('{pl}:leg_stops'.format(pl=prefix_leg),
                                      geojson.dumps(leg['leg_stops'])
                                      )
                            # leg_track
                            if leg['leg_track']:
                                redis.set('{pl}:leg_track'.format(pl=prefix_leg),
                                          leg['leg_track']
                                          )

                            # travel_expert
                            if leg['travel_expert']:
                                redis.set('{pl}:travel_expert'.format(pl=prefix_leg),
                                          leg['travel_expert']
                                          )

                            if leg['leg_type'] == 'timed':
                                # line
                                if leg['line']:
                                    redis.set('{pl}:line'.format(pl=prefix_leg),
                                              leg['line']
                                              )
                                # journey
                                if leg['journey']:
                                    redis.set('{pl}:journey'.format(pl=prefix_leg),
                                              leg['journey']
                                              )

                            elif leg['leg_type'] == 'ridesharing':
                                # driver
                                if leg['driver']:
                                    redis.set('{pl}:driver'.format(pl=prefix_leg),
                                              leg['driver']
                                              )
                                # passenger
                                if leg['passenger']:
                                    redis.set('{pl}:passenger'.format(pl=prefix_leg),
                                              leg['passenger']
                                              )
                                # vehicle
                                if leg['vehicle']:
                                    redis.set('{pl}:vehicle'.format(pl=prefix_leg),
                                              leg['vehicle']
                                              )

                if insert_count % NPRINT == 0:
                    print('--> insert no. {} (request_id: {})'
                          .format(insert_count, request_id),
                          file=sys.stderr, flush=True)

                insert_count = insert_count + 1

    print('All data loaded!', file=sys.stderr)
    exit(0)
