#!/usr/bin/env python
# coding: utf-8


# import modules
from datetime import datetime, timedelta
from geojson import LineString


def leg_type(mode):
    continuous_leg = ['walking', 'bike', 'car']
    timed_leg = ['train', 'taxi', 'change', 'bus', 'subway', 'tram', 'genericpubtrans', 'boat', 'funicular']
    ridesharing_leg = ['carsharing', 'bikesharing']
    if mode in continuous_leg:
        return 'continuous'
    if mode in timed_leg:
        return 'timed'
    if mode in ridesharing_leg:
        return 'ridesharing'


def mapping_transport_mode(t_mode):
    transports_to_map = {'bike': 'cycle',
                         'boat': 'water',
                         'car': 'self-drive-car',
                         'carsharing': 'others-drive-car',
                         'walking': 'walk',
                         'train': 'rail',
                         'subway': 'metro',
                         'bikesharing': 'cycle',
                         'genericpublictrans': 'rail'}
    if t_mode in transports_to_map:
        return transports_to_map[t_mode]
    else:
        return t_mode


def get_coordinates(offer, place):
    lat, lon = offer['places'][place]['latitude'], offer['places'][place]['longitude']
    return float(lat), float(lon)


def get_time_format(dur):
    sec = timedelta(seconds=dur)
    d = datetime(10, 10, 10) + sec
    return 'P%dY%dM%dDT%dH%dM%dS' % (d.year - 10, d.month - 10, d.day - 10, d.hour, d.minute, d.second)


def transform_trip(trip):
    request_id = '{tripId}-{legId}'.format(tripId=trip['tripId'], legId=trip['legId'])
    example_new_format = dict()
    example_new_format.setdefault(request_id, {})
    fmt = '%H:%M'
    offer_list = []
    for alternative in trip['alternatives']:
        # offer level information
        # get the offer ID and store it
        offer_id = alternative['id']
        offer_list.append(offer_id)
        example_new_format[request_id].setdefault(offer_id, {})

        # get the starting time and store it
        start_time = alternative['segments'][0]['departureTime']
        start_date = alternative['segments'][0]['departureDate']
        t = start_date + ' ' + start_time
        starttime = datetime.fromisoformat(t).isoformat()
        example_new_format[request_id][offer_id].setdefault('start_time', starttime)

        # get the ending time and store it
        end_time = alternative['segments'][len(alternative['segments']) - 1]['arrivalTime']
        end_date = alternative['segments'][len(alternative['segments']) - 1]['arrivalDate']
        t = end_date + ' ' + end_time
        endtime = datetime.fromisoformat(t).isoformat()
        example_new_format[request_id][offer_id].setdefault('end_time', endtime)

        # get duration and store it
        tdelta = datetime.strptime(end_time, fmt) - datetime.strptime(start_time, fmt)
        example_new_format[request_id][offer_id].setdefault('duration', get_time_format(tdelta.total_seconds()))
        # num_legs = 0
        leg_ids = []
        for segment in alternative['segments']:
            # num_legs += len(segment['legs'])
            for leg in segment['legs']:
                leg_ids.append(leg['id'])
        example_new_format[request_id][offer_id].setdefault('num_interchanges', len(leg_ids) - 1)
        example_new_format[request_id][offer_id].setdefault('legs', leg_ids)
        # offer_items
        example_new_format[request_id][offer_id].setdefault('offer_items', )
        # bookable_total
        example_new_format[request_id][offer_id].setdefault('bookable_total', )
        # get the price in the correct format (last two digits are decimal)
        price = int(float(alternative['totals']['price']) * 100)
        price_dict = {'value': price, 'currency': 'EUR'}
        example_new_format[request_id][offer_id].setdefault('complete_total', price_dict)
        # leg level information
        for segment in alternative['segments']:
            for leg in segment['legs']:
                leg_id = leg['id']
                # leg type
                example_new_format[request_id][offer_id].setdefault(leg_id, {})
                legtype = leg_type(leg['transport'])
                example_new_format[request_id][offer_id][leg_id].setdefault('leg_type', legtype)

                # departure time
                start_time = leg['departureTime']
                start_date = leg['departureDate']
                t = start_date + ' ' + start_time
                starttime = datetime.fromisoformat(t).isoformat()
                example_new_format[request_id][offer_id][leg_id].setdefault('start_time', starttime)

                # arrival time
                end_time = leg['arrivalTime']
                end_date = leg['arrivalDate']
                t = end_date + ' ' + end_time
                endtime = datetime.fromisoformat(t).isoformat()
                example_new_format[request_id][offer_id][leg_id].setdefault('end_time', endtime)

                # duration
                tdelta = datetime.strptime(end_time, fmt) - datetime.strptime(start_time, fmt)
                # print(tdelta.total_seconds())
                example_new_format[request_id][offer_id][leg_id].setdefault('duration',
                                                                            get_time_format(tdelta.total_seconds()))
                # transportation mode
                example_new_format[request_id][offer_id][leg_id].setdefault('transportation_mode',
                                                                            mapping_transport_mode(leg['transport']))

                # leg stops
                coord_ini = get_coordinates(trip, leg['from'])
                coord_fin = get_coordinates(trip, leg['to'])
                # example_new_format[request_id][offer_id][leg_id].setdefault('leg_stops',[coord_ini,coord_fin])
                example_new_format[request_id][offer_id][leg_id].setdefault('leg_stops',
                                                                            LineString([coord_ini, coord_fin]))

                # leg track
                example_new_format[request_id][offer_id][leg_id].setdefault('leg_track', )

                # travel_expert
                example_new_format[request_id][offer_id][leg_id].setdefault('travel_expert', )

                # line and journey
                if legtype == 'timed':
                    example_new_format[request_id][offer_id][leg_id].setdefault('line', )
                    example_new_format[request_id][offer_id][leg_id].setdefault('journey', )
                if legtype == 'ridesharing':
                    example_new_format[request_id][offer_id][leg_id].setdefault('driver', )
                    example_new_format[request_id][offer_id][leg_id].setdefault('passenger', )
                    example_new_format[request_id][offer_id][leg_id].setdefault('vehicle', )
        example_new_format[request_id].setdefault('offers', offer_list)
    return example_new_format
