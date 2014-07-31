import datetime
import json
import logging
import random as r
import sys
import time
import uuid

from oslo.messaging._drivers import common as rc
import pika
import optparse

from ceilometer import sample as s
from ceilometer.publisher import utils

LOG_FORMAT = ('%(levelname) -10s %(asctime)s  %(funcName) '
              '-10s: %(message)s')
LOGGER = logging.getLogger(__name__)

parser = optparse.OptionParser()
parser.add_option("-r", "--rate", dest="rate", default=10 ** 2,
                  type="int", help="Rate of messages  generating, m/s")
parser.add_option("-m", "--max-count", dest="max_count", default=10 ** 2,
                  type="int", help="Max count of published messages")
parser.add_option("--rate-step", dest="rate_step", default=1,
                  type="int")
parser.add_option("--resource-count", dest="resource_count", default=100,
                  type="int")


class SamplesPublisher(object):
    MESSAGE_SECRET = 'secret'
    EXCHANGE = 'metering'
    EXCHANGE_TYPE = 'topic'
    QUEUE = 'metering'
    ROUTING_KEY = '/'
    sample = s.Sample("instance", "gauge", "instance", 1,
                      'c052abd141ea4ea09be30d6953ee0272',
                      '6e95f8ac361d43698b8afa78f576a10e',
                      'b5dea434-e6d7-4126-9016-c5a1dde7e131',
                      '2014-03-26T08:55:28Z',
                      {'ephemeral_gb': 0, 'display_name': u'ai',
                       'name': u'instance-00000016',
                       'disk_gb': 40, 'kernel_id': None,
                       'image': {
                           u'id': u'c2cf308a-75d3-4a72-be4d-f20cc8c3194e',
                           u'links': [{
                                          u'rel': u'bookmark'}],
                           'name': u'ubuntu-12.04'},
                       'ramdisk_id': None, 'vcpus': 2,
                       'memory_mb': 4096, 'instance_type': u'3',
                       'root_gb': 40,
                       'image_ref': u'c2cf308a-75d3-4a72-be4d-f20cc8c3194e',
                       'flavor': {'name': u'm1.medium',
                                  u'links': [{u'rel': u'bookmark'}],
                                  'ram': 4096, 'ephemeral': 0,
                                  'vcpus': 2, 'disk': 40,
                                  u'id': u'3'},
                       'OS-EXT-AZ:availability_zone': u'nova',
                       },
                      'openstack')

    def __init__(self, url, interval=0.02, rate_step=1, max_count=10 ** 5,
                 resource_count=100, exchange='metering'):
        self.EXCHANGE = exchange
        self.QUEUE = exchange
        self.connection = None
        self.channel = None
        self.message_number = 0
        self.stopping = False
        self.closing = False
        self.url = url
        self.props = pika.BasicProperties("application/json",
                                          "utf-8", [], 2, 0)
        self.first_interval = interval
        self.interval = interval
        self.interval_delimeter = 1
        self.rate_step = rate_step
        self.ids = []
        for i in range(0, resource_count):
            self.ids.append(uuid.uuid4())
        self.message_max_count = max_count

    def connect(self):
        LOGGER.info('Connecting to %s', self.url)
        return pika.SelectConnection(pika.URLParameters(self.url),
                                     self.on_connection_open,
                                     stop_ioloop_on_close=False)

    def close_connection(self):
        LOGGER.info('Closing connection')
        self.closing = True
        self.connection.close()

    def on_connection_open(self, unused_connection):
        LOGGER.info('Connection opened')
        self.open_channel()

    def reconnect(self):
        self.connection.ioloop.stop()
        self.connection = self.connect()
        self.connection.ioloop.start()

    def on_channel_open(self, channel):
        LOGGER.info('Channel opened')
        self.channel = channel
        self.setup_exchange(self.EXCHANGE)

    def setup_exchange(self, exchange_name):
        LOGGER.info('Declaring exchange %s', exchange_name)
        self.channel.exchange_declare(self.on_exchange_declareok,
                                      exchange_name,
                                      self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        LOGGER.info('Exchange declared')
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        LOGGER.info('Declaring queue %s', queue_name)
        self.channel.queue_declare(self.on_queue_declareok, queue_name)

    def on_queue_declareok(self, method_frame):
        LOGGER.info('Binding %s to %s with %s',
                    self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
        self.channel.queue_bind(self.on_bindok, self.QUEUE,
                                self.EXCHANGE, self.ROUTING_KEY)

    def get_timestamp(self, dt):
        """Convert datetime to seconds' count"""
        return time.mktime(dt.timetuple())

    def publish_message(self):
        start_time = self.get_timestamp(datetime.datetime.utcnow())
        rate_message_count = 0
        while not self.stopping:
            # Random resource id from pre generated list
            self.message_number += 1
            delta_time = \
                (self.get_timestamp(datetime.datetime.utcnow()) -
                 start_time)
            rate = rate_message_count / (delta_time + 1)

            self.sample.resource_id = str(self.ids[
                r.randint(0, len(self.ids) - 1)])

            # Timestamp of message creating. Uses as SENT in logs
            self.sample.timestamp = str(datetime.datetime.utcnow())
            self.sample.resource_metadata.update({'rate': rate})

            #Prepare message
            msg = utils.meter_message_from_counter(self.sample,
                                                   self.MESSAGE_SECRET)
            rabbit_msg = {
                'method': 'record_metering_data',
                'version': '1.0',
                'args': {'data': msg},
                }
            rabbit_msg = json.dumps(rc.serialize_msg(rabbit_msg))

            self.channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
                                       rabbit_msg, self.props)

            LOGGER.info('Published message # %i. '
                        'Delta time from last speeding up %.0f s. '
                        'Rate %.0f m/s',
                        self.message_number, delta_time,
                        rate)
            rate_message_count += 1

            #Improve rate of generating every 5000 messages
            if self.message_number % 5000 == 0:
                self.interval_delimeter += self.rate_step
                self.interval = self.first_interval / self.interval_delimeter
                start_time = self.get_timestamp(datetime.datetime.utcnow())
                rate_message_count = 0

            if self.message_number > self.message_max_count:
                self.stop()

            #sleep between two publishings
            if self.interval:
                time.sleep(self.interval)

    def start_publishing(self):
        LOGGER.info('Issuing consumer related RPC commands')
        self.publish_message()

    def on_bindok(self, unused_frame):
        LOGGER.info('Queue bound')
        self.start_publishing()

    def close_channel(self):
        LOGGER.info('Closing the channel')
        if self.channel:
            self.channel.close()

    def open_channel(self):
        LOGGER.info('Creating a new channel')
        self.connection.channel(on_open_callback=self.on_channel_open)

    def run(self):
        self.connection = self.connect()
        self.connection.ioloop.start()

    def stop(self):
        LOGGER.info('Stopping')
        self.stopping = True
        self.close_channel()
        self.close_connection()
        LOGGER.info('Stopped')
        sys.exit(0)
