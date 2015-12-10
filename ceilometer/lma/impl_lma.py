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

from oslo_config import cfg

from ceilometer.lma import impl_elasticsearch as es
from ceilometer.lma import impl_influxdb as influx
from ceilometer.storage import base

OPTS = [
    cfg.StrOpt('resource_connection',
               secret=True,
               default=None,
               help='The connection string used to connect to the resource '
               'database. (if unset, connection is used)'),
]

cfg.CONF.register_opts(OPTS, group='database')


class Connection(base.Connection):
    def __init__(self, url):
        self.resource_conn = es.Connection(cfg.CONF.database.resource_connection)
        self.samples_conn = influx.Connection(url)

    def get_meters(self, user=None, project=None, resource=None, source=None,
                   metaquery=None, limit=None):
        return self.resource_conn.get_meters(user, project, resource, source,
                                             metaquery, limit)

    def get_samples(self, sample_filter, limit=None):
        return self.samples_conn.get_samples(sample_filter, limit)

    def get_meter_statistics(self, sample_filter, period=None, groupby=None,
                             aggregate=None):
        return self.samples_conn.get_meter_statistics(sample_filter, period,
                                                      groupby, aggregate)

    def upgrade(self):
        self.resource_conn.upgrade()
        self.samples_conn.upgrade()

    def get_resources(self, user=None, project=None, source=None,
                      start_timestamp=None, start_timestamp_op=None,
                      end_timestamp=None, end_timestamp_op=None,
                      metaquery=None, resource=None, limit=None):
        return self.resource_conn.get_resources(user, project, source,
                                                start_timestamp,
                                                start_timestamp_op,
                                                end_timestamp,
                                                end_timestamp_op,
                                                metaquery,
                                                resource,
                                                limit)
