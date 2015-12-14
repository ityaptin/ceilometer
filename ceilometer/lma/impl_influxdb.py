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
"""InfluxDB meters storage backend"""

import influxdb
import influxdb.exceptions
import influxdb.resultset
from oslo_log import log
import six.moves.urllib.parse as urlparse
import operator

from ceilometer.storage import base
from ceilometer import utils
from ceilometer.lma.influx import utils as influx_utils

MEASUREMENT = "ceilometer"

LOG = log.getLogger(__name__)


class Connection(base.Connection):
    """Put the meters into a InlfuxDB database.

    - measurement: ceilometer
    - tags: metadata and indexed fields
    - fields: type, unit, volume
    """

    def __init__(self, url):
        super(Connection, self).__init__(url)

        user = None
        pwd = None
        host = None
        port = None

        parts = urlparse.urlparse(url)
        user_def, host_def = urlparse.splituser(parts.netloc)

        if user_def:
            auth = user_def.split(":", 1)
            if len(auth) != 2:
                raise ValueError()
            user = auth[0]
            pwd = auth[1]
        if host_def:
            loc = host_def.split(":", 1)
            host = loc[0]
            if len(loc) == 2:
                try:
                    port = int(loc[1])
                except ValueError:
                    raise ValueError("Port have a invalid definition.")
        if not parts.path:
            raise ValueError("Database should be defined")

        self.database = parts.path[1:]
        self.conn = influxdb.InfluxDBClient(host, port, user, pwd,
                                            self.database)

        self.upgrade()

    def upgrade(self):
        try:
            self.conn.create_database(self.database)
        except influxdb.exceptions.InfluxDBClientError as e:
            if "database already exists" not in e.content:
                raise

    def get_meter_statistics(self,
                             sample_filter,
                             period=None,
                             groupby=None,
                             aggregate=None):
        """Return an iterable of models.Statistics instance.

        Items are containing meter statistics described by the query
        parameters. The filter must have a meter value set.
        :param sample_filter: Simple filter that matches samples before
        aggregate.
        :param period: Size of aggregation periods in seconds.
        :param groupby: List of groupby fields.
        :param aggregate: Aggregate functions with params.
        """
        if not sample_filter.start_timestamp:
            start_timestamp = self.get_oldest_timestamp(sample_filter)
            sample_filter.start_timestamp = start_timestamp

        query = influx_utils.make_aggregate_query(sample_filter, period,
                                                     groupby, aggregate)
        response = self.query(query)
        stats = []
        for serie, points in response.items():
            measurement, tags = serie
            for point in points or []:
                 stats.append(
                     influx_utils.point_to_stat(point, tags,
                                                period, aggregate))
        return sorted(stats, key=lambda stat: stat.period_start)

    def get_oldest_timestamp(self, sample_filter):
        response = self.query(
            influx_utils.make_time_bounds_query(sample_filter))
        first_point = response.get_points(MEASUREMENT).next()
        start_timestamp = utils.sanitize_timestamp(first_point['first'])
        return start_timestamp

    def query(self, q):
        """Make a query to InfluxDB database.

          :param q: Query string in InfluxDB query format.
          :returns a response ResultSet
        """
        try:
            return self.conn.query(q)
        except influxdb.exceptions.InfluxDBClientError as e:
            LOG.exception("Client error during the InfluxDB query: %s", e)
            return influxdb.resultset.ResultSet({})

    def get_samples(self, sample_filter, limit=None):
        """Return an iterable of model.Sample instances.

        :param sample_filter: Filter.
        :param limit: Maximum number of results to return."""
        if limit is 0:
            return
        response = self.query(
            influx_utils.make_list_query(sample_filter, limit))
        samples = []
        for point in response.get_points("ceilometer"):
            samples.append(influx_utils.point_to_sample(point))
        return sorted(samples, key=lambda sample: sample.timestamp)


