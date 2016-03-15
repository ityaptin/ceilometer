#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""LMA storage backend"""

import elasticsearch as es
import influxdb
import influxdb.exceptions
import influxdb.resultset
from oslo_config import cfg
from oslo_log import log
from oslo_utils import netutils

from ceilometer.i18n import _LE
from ceilometer.storage import base
from ceilometer.storage.es import utils as es_utils
from ceilometer.storage.influx import utils as influx_utils
from ceilometer import utils

LOG = log.getLogger(__name__)


AVAILABLE_CAPABILITIES = {
    'resources': {'query': {'simple': True,
                            'metadata': True}},
    'statistics': {'groupby': True,
                   'query': {'simple': True,
                             'metadata': True},
                   'aggregation': {'standard': True,
                                   'selectable': {'max': True,
                                                  'min': True,
                                                  'sum': True,
                                                  'avg': True,
                                                  'count': True,
                                                  'stddev': True}}},
    'meters': {'query': {'simple': True,
                         'metadata': True}},
    'samples': {'query': {'simple': True,
                          'metadata': True,
                          'complex': True}},
}

AVAILABLE_STORAGE_CAPABILITIES = {
    'storage': {'production_ready': False},
}


class Connection(base.Connection):
    """Get the Ceilometer data from InfluxDB and ElasticSearch databases.

    Samples are stored in the next format in InfluxDB:
    - measurement: sample
    - tags (indexed): user_id, resource_id, project_id, source and
    configured metadata fields
    - fields (not indexed): counter_type -> type, counter_unit -> unit,
    counter_volume -> value, counter_name -> meter, message_id,
    message_signature, timestamp and recorded_at.

    Resources and meters are stored in the ElasticSearch.
    Resources:
     {
      "_index": "ceilometer_resource",
      "_type": "<source>",
      "_id": "<resource_id>",
      "_source":{
          "first_sample_timestamp": "<datetime in isoformat>",
          "last_sample_timestamp": "<datetime in isoformat>",
          "project_id": "<project_id>",
          "user_id": "<user_id>",
          "metadata": {
              "foo" : "bar",
              "foofoo" : {"barbar": {"foo": "bar"}}
          }
       }
    }

    Meters:
    {"_index": "ceilometer_meter",
     "_type": "meter",
     "_id":"<resource_id>.<meter_name>"
     "_source":{
        "project_id": "<project_id>",
        "user_id": "<user_id>",
        "meter_type": "<counter_type>",
        "meter_unit": "<counter_unit>"
        "source": <source>,
        "meter_name": <meter_name>,
        "metadata": {
            "foo" : "bar",
            "foofoo" : {"barbar": {"foo": "bar"}}
        }

    This class doesn't implements `record_metering_data` method, because
    data will be recorded by LMA Collector service.

    """

    CAPABILITIES = utils.update_nested(base.Connection.CAPABILITIES,
                                       AVAILABLE_CAPABILITIES)

    STORAGE_CAPABILITIES = utils.update_nested(
        base.Connection.STORAGE_CAPABILITIES,
        AVAILABLE_STORAGE_CAPABILITIES,
    )

    def __init__(self, url):
        url_split = netutils.urlsplit(cfg.CONF.database.resource_connection)
        self.resource_connection = es.Elasticsearch(url_split.netloc)

        user, pwd, host, port, self.database = influx_utils.split_url(url)
        self.sample_connection = influxdb.InfluxDBClient(host, port, user, pwd,
                                                         self.database)

    def upgrade(self):
        self.upgrade_resource_database()
        self.upgrade_sample_database()

    def upgrade_resource_database(self):
        iclient = es.client.IndicesClient(self.resource_connection)
        template = {
            'template': 'ceilometer_*',
            'mappings': {
                '_default_': {
                    'properties': {
                        'first_sample_timestamp': {'type': 'date'},
                        'last_sample_timestamp': {'type': 'date'},
                        'metadata': {'type': 'nested'}
                    }
                }
            }
        }
        iclient.put_template(name='ceilometer_resource_template',
                             body=template)

    def upgrade_sample_database(self):
        try:
            self.sample_connection.create_database(self.database)
        except influxdb.exceptions.InfluxDBClientError as e:
            if "database already exists" not in e.content:
                raise

    def get_meters(self, user=None, project=None, resource=None, source=None,
                   metaquery=None, limit=None, unique=None):
        if limit == 0:
            return

        q_args = es_utils.make_meter_query(es_utils.METER_INDEX, resource,
                                           user, project, source,
                                           metaquery, limit)
        results = self.resource_connection.search(
            fields=['_type', '_id', '_source'], **q_args)
        return es_utils.search_results_to_meters(results)

    def get_resources(self, user=None, project=None, source=None,
                      start_timestamp=None, start_timestamp_op=None,
                      end_timestamp=None, end_timestamp_op=None,
                      metaquery=None, resource=None, limit=None):
        if limit == 0:
            return

        q_args = es_utils.make_resource_query(es_utils.RESOURCE_INDEX,
                                              user, project, source,
                                              start_timestamp,
                                              start_timestamp_op,
                                              end_timestamp, end_timestamp_op,
                                              metaquery, resource, limit)
        results = self.resource_connection.search(
            fields=['_type', '_id', '_source'], **q_args)
        return es_utils.search_results_to_resources(results)

    def get_meter_statistics(self, sample_filter, period=None, groupby=None,
                             aggregate=None):

        # Note(ityaptin) InfluxDB should to have a lower bound of the time
        # of query, else it will be defined as 1970-01-01T00:00:00.
        if not sample_filter.start_timestamp:
            try:
                start_timestamp = self.get_oldest_timestamp(sample_filter)
                sample_filter.start_timestamp = start_timestamp
            except base.NoResultFound:
                return []

        query = influx_utils.make_aggregate_query(sample_filter, period,
                                                  groupby, aggregate)
        response = self.query(query)
        stats = []
        for serie, points in response.items():
            measurement, tags = serie
            for point in points or []:
                stats.append(
                    influx_utils.point_to_stat(point, tags, period, aggregate))

        return [stat for stat in stats if stat]

    def get_samples(self, sample_filter, limit=None):
        if limit is 0:
            return
        response = self.query(
            influx_utils.make_list_query(sample_filter, limit))
        for point in response.get_points(influx_utils.MEASUREMENT):
            yield influx_utils.point_to_sample(point)

    def query_samples(self, filter_expr=None, orderby=None, limit=None):
        q = influx_utils.make_complex_query(filter_expr, limit)
        response = self.query(q)

        samples = []
        for point in response.get_points(influx_utils.MEASUREMENT):
            samples.append(influx_utils.point_to_sample(point))

        return influx_utils.sort_samples(samples, orderby)

    def get_oldest_timestamp(self, sample_filter):
        """Find timestamp of the first matching sample in the database."""

        response = self.query(
            influx_utils.make_time_bounds_query(sample_filter))
        try:
            first_point = response.get_points(influx_utils.MEASUREMENT).next()
        except StopIteration:
            raise base.NoResultFound()

        start_timestamp = utils.sanitize_timestamp(first_point['first'])
        return start_timestamp

    def query(self, q):
        """Make a query to InfluxDB database.

          :param q: Query string in InfluxDB query format.
          :returns a response ResultSet
        """
        LOG.debug("InfluxDB query requested: %s" % q)
        try:
            return self.sample_connection.query(q)
        except influxdb.exceptions.InfluxDBClientError as e:
            LOG.exception(_LE("Client error during the InfluxDB query: %s"), e)
            return influxdb.resultset.ResultSet({})
