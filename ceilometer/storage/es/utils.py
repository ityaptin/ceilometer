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

import six

from ceilometer.storage import models
from ceilometer import utils

METER_INDEX = "ceilometer_meter"
RESOURCE_INDEX = "ceilometer_resource"


def make_resource_query(index, user=None, project=None, source=None,
                        start_timestamp=None, start_timestamp_op=None,
                        end_timestamp=None, end_timestamp_op=None,
                        metaquery=None, resource=None, limit=None):
    """Make filter query for the Resource list request."""

    q_args = {}
    filters = []
    if limit is not None:
        q_args['size'] = limit
    q_args['index'] = index
    if source is not None:
        q_args['doc_type'] = source
    if resource is not None:
        filters.append({'term': {'_id': resource}})
    if user is not None:
        filters.append({'term': {'user_id': user}})
    if project is not None:
        filters.append({'term': {'project_id': project}})
    if start_timestamp or end_timestamp:
        ts_filter = {}
        st_op = start_timestamp_op or 'gte'
        et_op = end_timestamp_op or 'lt'
        if start_timestamp:
            ts_filter[st_op] = start_timestamp
        if end_timestamp:
            ts_filter[et_op] = end_timestamp
        filters.append({'range': {'last_sample_timestamp': ts_filter}})
    if metaquery is not None:
        for key, value in six.iteritems(metaquery):
            filters.append({'term': {key: value}})
    q_args['body'] = {
        'query': {
            'filtered': {
                'filter': {
                    'bool': {'must': filters}
                }
            }
        }
    }
    return q_args


def make_meter_query(index, resource=None, user=None,
                     project=None, source=None, metaquery=None,
                     limit=None):
    """Make filter query for the Meter list request."""

    q_args = {}
    filters = []
    if limit is not None:
        q_args['size'] = limit
    q_args['index'] = index
    if resource is not None:
        filters.append({'term': {'resource_id': resource}})
    if user is not None:
        filters.append({'term': {'user_id': user}})
    if project is not None:
        filters.append({'term': {'project_id': project}})
    if source is not None:
        filters.append({'term': {'source': source}})
    if metaquery is not None:
        for key, value in six.iteritems(metaquery):
            filters.append({'term': {key: value}})
    q_args['body'] = {
        'query': {
            'filtered': {
                'filter': {
                    'bool': {
                        'must': filters}
                }
            }
        }
    }
    return q_args


def search_results_to_resources(results):
    """Transforms results of the search to the Resource instances."""

    for record in results['hits']['hits']:
        yield models.Resource(
            resource_id=record['_id'],
            first_sample_timestamp=utils.sanitize_timestamp(
                record['_source'].get('first_sample_timestamp')),
            last_sample_timestamp=utils.sanitize_timestamp(
                record['_source']['last_sample_timestamp']),
            source=record['_type'],
            project_id=record['_source'].get('project_id'),
            user_id=record['_source'].get('user_id'),
            metadata=record['_source']['metadata']
        )


def search_results_to_meters(results):
    """Transforms results of the search to the Meter instances."""

    for record in results['hits']['hits']:
            yield models.Meter(
                name=record['_source']['meter_name'],
                type=record['_source']['meter_type'],
                unit=record['_source']['meter_unit'],
                resource_id=record['_source']['resource_id'],
                source=record['_source'].get('source'),
                project_id=record['_source'].get('project_id'),
                user_id=record['_source'].get('user_id'),
            )
