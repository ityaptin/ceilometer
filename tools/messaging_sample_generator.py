from __future__ import print_function

import random
import sys
import uuid

import make_test_data

from ceilometer import agent
from ceilometer import messaging
from ceilometer.openstack.common import jsonutils
from ceilometer import service
import time
import datetime


from oslo.config import cfg
from ceilometer.publisher import utils


def send_batch(rpc_client, rabbit_topic, batch):
    rpc_client.prepare(topic=rabbit_topic).cast(agent.context.RequestContext(),
                                                'record_metering_data', data=batch)


def get_rpc_client(config_file):
    service.prepare_service(argv=['/', '--config-file', config_file])
    transport = messaging.get_transport()
    rpc_client = messaging.get_rpc_client(transport, version='1.0')
    return rpc_client


def get_parser(parser=None):
    if not parser:
        parser = make_test_data.get_parser()
    parser.add_argument(
        '--count',
        default=10000,
        type=int,
        help='Count of samples',
    )
    parser.add_argument(
        '--sample-batch-size',
        default=20,
        type=int,
        help='Size of samples packet which is sent by rabbit',
    )
    parser.add_argument(
        '--config-file',
        help='Config file for ceilometer services',
    )
    parser.add_argument(
        '--rabbit-topic',
        default='perfmetering',
        help='Rabbit topic for samples',
    )
    parser.add_argument(
        '--rate',
        default=1000,
        help='Rate of samples per seconds',
    )
    parser.add_argument(
        '--resource-count',
        default=100,
    )
    parser.add_argument('--counters',
                        nargs='+',
                        default=['instance'],
                        help='')
    return parser


def generate_resources(count):
    resources = []
    for i in xrange(count):
        resources.append(uuid.uuid4().hex)
    return resources


def send_test_data(args, producer_log_file=None):
    batch_size = args.sample_batch_size
    start = datetime.datetime.utcnow() - datetime.timedelta(minutes=int(args.count))
    rpc_client = get_rpc_client(args.config_file)
    sample_count = 0
    resources_by_counter = {}
    counter_len = len(args.counters)
    output = None
    try:

        if producer_log_file:
            output = open(producer_log_file, 'w')

        while sample_count < args.count:
            start_time = time.time()
            counter_name = args.counters[random.randint(0, counter_len - 1)]
            if counter_name not in resources_by_counter:
                resources_by_counter[counter_name] = \
                    generate_resources(args.resource_count)
            resource_id = (resources_by_counter[counter_name]
                           [random.randint(0, args.resource_count - 1)])

            end = start + datetime.timedelta(minutes=batch_size)
            gen = make_test_data.make_samples(name=counter_name,
                                              meter_type=args.type,
                                              unit=args.unit,
                                              volume=args.volume,
                                              random_min=args.random_min,
                                              random_max=args.random_max,
                                              user_id=args.user,
                                              project_id='openstack',
                                              resource_id=resource_id,
                                              start=start,
                                              end=end,
                                              interval=1)
            batch = []
            for sample in gen:
                sample.timestamp = jsonutils.to_primitive(sample.timestamp)
                data = utils.meter_message_from_counter(
                    sample,
                    cfg.CONF.publisher.metering_secret)
                batch.append(data)

            sample_count += batch_size
            if sample_count > args.count:
                batch = batch[:-(sample_count - args.count) - 1]
            start = start + datetime.timedelta(minutes=batch_size + 1)
            send_batch(rpc_client, args.rabbit_topic, batch)
            delta_time = time.time() - start_time
            rate = batch_size / delta_time
            if output:
                output.write("%s\t%s\n" % (time.time(), rate))
    except Exception as e:
        raise
    finally:
        if output:
            output.close()

def main(argv=None):
    argv = argv or sys.argv
    cfg.CONF([], project='ceilometer')
    parser = get_parser()
    try:
        args = parser.parse_args(argv[1:])
    except Exception as e:
        raise
    print(args)
    send_test_data(args, "/tmp/default_messaging_rate")

if __name__ == '__main__':
    main()
