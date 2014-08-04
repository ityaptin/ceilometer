
from __future__ import print_function

import argparse
import json
import datetime
import sys

import make_test_data

from ceilometer import agent
from ceilometer import messaging
from ceilometer import service
import time
import datetime


from oslo.config import cfg
from ceilometer.publisher import utils


def send_batch(rabbit_topic, batch, rpc_client):
    rpc_client.prepare(topic=rabbit_topic).cast(agent.context.RequestContext(),
                                                'record_metering_data', data=batch)


def get_rpc_client(config_file):
    print(config_file)
    service.prepare_service(argv=['/', '--config-file', config_file])
    transport = messaging.get_transport()
    rpc_client = messaging.get_rpc_client(transport, version='1.0')
    return rpc_client

def get_parser(parser=None):
    if not parser:
        parser = make_test_data.get_parser()
    parser.add_argument(
        '--sample-batch-size',
        default=1,
        type=int,
        help='Size of samples packet which is sent by rabbit',
    )
    parser.add_argument(
        '--config-file',
        help='Config file for ceilometer services',
    )
    parser.add_argument(
        '--rabbit-topic',
        default='metering',
        help='Rabbit topic for samples',
    )
    return parser


def send_test_data(args):
    batch_size = args.sample_batch_size

    start = datetime.datetime.utcnow() - datetime.timedelta(days=int(args.start))
    end = datetime.datetime.utcnow() + datetime.timedelta(days=int(args.end))

    rpc_client = get_rpc_client(args.config_file)
    batch = []
    start_time = time.time()
    batch_count = 0
    for sample in make_test_data.make_samples(name=args.counter,
                                              meter_type=args.type,
                                              unit=args.unit,
                                              volume=args.volume,
                                              random_min=args.random_min,
                                              random_max=args.random_max,
                                              user_id=args.user,
                                              project_id=args.project,
                                              resource_id=args.resource,
                                              start=start,
                                              end=end,
                                              interval=args.interval,
                                              resource_metadata={},
                                              source='artificial'):
        sample.timestamp = str(sample.timestamp)
        data = utils.meter_message_from_counter(sample, cfg.CONF.publisher.metering_secret)
        batch.append(data)
        if len(batch) == batch_size:
            send_batch(args.rabbit_topic, batch, rpc_client)
            batch = []
        batch_count += 1
        rate = batch_count / (time.time() - start_time)
        print("Rate is %.2f" % rate)


def main(argv=None):
    argv = argv or sys.argv
    cfg.CONF([], project='ceilometer')
    parser = make_test_data.get_parser()
    try:
        args = parser.parse_args(argv)
    except Exception:
        args = parser.parse_args(argv[1:])
    send_test_data(args)

if __name__ == '__main__':
    main()
