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

import datetime

from oslo_utils import timeutils
import six
import six.moves.urllib.parse as urlparse

from ceilometer.storage import models
from ceilometer import utils


MEASUREMENT = "sample"

DEFAULT_AGGREGATES = ["mean", "sum", "count", "min", "max"]

AGGREGATE_FUNC_TRANSITIONS = {
    "avg": "mean"
}

BACK_FUNC_TRANSITIONS = {
    "mean": "avg"
}

FIELD_TRANSITIONS = {
    "counter_name": "meter",
    "counter_volume": "value",
    "counter_unit": "unit",
    "counter_type": "type",
    "timestamp": "time",
}


binary = set(["and", "or"])
negative = "ne"

ops = {
    "eq": "=",
    "ge": ">=",
    "le": "<=",
    "gt": ">",
    "lt": "<=",
    "ne": "<>"
}

inverted_ops = {
    "=": "<>",
    ">=": "<",
    ">": "<=",
    "<=": ">",
    "<": ">=",
    "<>": "=",
    "and": "or",
    "or": "and"
}


def escape_value(value):
    """Escape and transform values for the query.

    :param value - value for the query, it may be string, number or datetime
    :returns escaped value
    """
    if isinstance(value, six.string_types):
        return '\'%s\'' % value
    elif isinstance(value, datetime.datetime):
        return '\'%s\'' % timeutils.isotime(value)
    else:
        return value


def parse_comparison_operator(op, exp, is_negative=False):
    """Parse simple boolean expression for the ComplexQuery.

    :param op - comparison operator
    :param exp - dict with field in key and value
    :param is_negative - flag for inverting operator

    :return expression on the InfluxDB Query lang
    :raises ValueError if format of the expression is invalid
    """
    items = exp.items()
    op = ops.get(op, op)
    op = inverted_ops.get(op, op) if is_negative else op
    if len(items) == 1:
        key, value = items[0]
        key = FIELD_TRANSITIONS.get(key, key)
        return "\"%s\"%s%s" % (key, op, escape_value(value))
    else:
        raise ValueError("Expression should be a dict with size equals 1.")


def parse_bool_operator(op, expressions, is_negative=False):
    """Parse bool operations for the ComplexQuery.

    :param op - AND or OR operator
    :param expressions - list with comparison defines
    :param is_negative - flag for inverting operator

    :return expression on the InfluxDB query lang
    """
    op = inverted_ops.get(op, op) if is_negative else op
    return "(" + " {op} ".format(op=op).join(
        [parse_filter_expression(e, is_negative) for e in expressions]) + ")"


def parse_filter_expression(expression, is_negative=False):
    """Parse ComplexQuery expression to the InfluxDB Query lang.

    :param expression - ComplexQuery expression
    :param is_negative - flag for inverting operators

    :return expression on the InfluxDB query lang
    """

    for op, exp in expression.items():
        if op.lower() in binary:
            return parse_bool_operator(op, exp, is_negative)
        elif op.lower() == "ne":
            is_negative = True
            return parse_filter_expression(exp, is_negative)
        else:
            return parse_comparison_operator(op, exp, is_negative)


def make_complex_filter_query(filter):
    """Parse complex query filter to the InfluxDB query language.

    :param filter - complex query filter
    """

    if filter:
        return parse_filter_expression(filter)
    else:
        return None


def make_simple_filter_query(sample_filter, require_meter=False):
    """Make InfluxDB query from the SampleFilter.

    :param sample_filter - SampleFilter instance.
    :param require_meter - flag for meter parameter requirement
    """

    expressions = []
    if sample_filter.user:
        expressions.append(("user_id", "=", sample_filter.user))
    if sample_filter.project:
        expressions.append(("project_id", "=", sample_filter.project))
    if sample_filter.meter:
        expressions.append(("meter", "=", sample_filter.meter))
    elif require_meter:
        raise RuntimeError('Missing required meter specifier')

    start_op = sample_filter.start_timestamp_op or ">"

    if sample_filter.start_timestamp:
        expressions.append(("time",
                            ops.get(start_op, start_op),
                            sample_filter.start_timestamp))

    end_op = sample_filter.end_timestamp_op or "<"
    if sample_filter.end_timestamp:
        expressions.append(("time",
                            ops.get(end_op, end_op),
                            sample_filter.end_timestamp))

    if sample_filter.resource:
        expressions.append(("resource_id", "=", sample_filter.resource))
    if sample_filter.source:
        expressions.append(("source", "=", sample_filter.source))
    if sample_filter.message_id:
        expressions.append(("message_id", "=", sample_filter.message_id))

    if sample_filter.metaquery:
        for field, value in sample_filter.metaquery.items():
            expressions.append((field, "=", value))

    query = " and ".join(("\"%s\"%s%s" % (field, op, escape_value(value))
                          for field, op, value in expressions))

    return query


def make_groupby(period, groupby):
    """Make GROUP BY part of the InfluxDB query.

    :param period - time in second for the grouping by.
    :param groupby - list of fields for group by

    :return str like `field1, field2, time(10s)`
    """

    return ", ".join((groupby or []) +
                     (["time({period}s)".format(period=period)]
                      if period > 0 else []))


def make_aggregation(aggregate):
    """Make a aggregate part of the InfluxDB query.

    :param aggregate - list of aggregate functions.

    :returns str like `count(value) as count, sum(value) as sum`
    """

    aggregates = []

    if not aggregate:
        for func in DEFAULT_AGGREGATES:
            aggregates.append("{func}(value) as {func}".format(func=func))
    else:
        for description in aggregate:
            func = description.func
            aggregates.append("{func}(value) as {func}".format(
                func=AGGREGATE_FUNC_TRANSITIONS.get(func, func)))

    return ", ".join(aggregates + make_duration_aggregates())


def make_duration_aggregates():
    """Make aggregation for the finding first and last timestamps in db."""

    duration_aggregates = ["{func}(timestamp) as {func}".format(func=func)
                           for func in ("last", "first")]
    return duration_aggregates


def construct_query(select=None, where=None, group=None, order=None,
                    limit=None):
    """Construct InfluxDB query from the parts."""

    return " ".join((
        "SELECT {select} FROM {name}".format(name=MEASUREMENT, select=select)
        if select else "SELECT * FROM {name}".format(name=MEASUREMENT),
        "WHERE {where}".format(where=where) if where else "",
        "GROUP BY {group} file(none)".format(group=group) if group else "",
        "ORDER BY time desc" if order else "",
        "LIMIT {limit}".format(limit=limit) if limit else ""
    ))


def make_aggregate_query(sample_filter, period, groupby, aggregate):
    """Make query for the statistics request."""

    return construct_query(make_aggregation(aggregate),
                           make_simple_filter_query(sample_filter),
                           make_groupby(period, groupby),
                           order=True)


def make_time_bounds_query(sample_filter):
    """Make query for the finding last and first points in db."""

    return construct_query(", ".join(make_duration_aggregates()),
                           make_simple_filter_query(sample_filter))


def make_list_query(sample_filter, limit):
    """Make query for the simple list request."""

    return construct_query(where=make_simple_filter_query(sample_filter),
                           limit=limit,
                           order=True)


def make_complex_query(filter, limit):
    """Make query for the complex request."""

    return construct_query(where=make_complex_filter_query(filter),
                           limit=limit,
                           order=True)


def point_to_stat(point, tags, period, aggregate):
    """Convert InfluxDB points to the Statistics instances."""

    kwargs = {}
    if not point['last'] or not point['first']:
        return
    if not aggregate:
        for func in DEFAULT_AGGREGATES:
            kwargs[BACK_FUNC_TRANSITIONS.get(func, func)] = point.get(func)
    else:
        kwargs['aggregate'] = {}
        for description in aggregate:
            func = description.func
            kwargs['aggregate'][func] = point.get(
                AGGREGATE_FUNC_TRANSITIONS.get(func, func))

    kwargs["groupby"] = tags or {}
    kwargs["duration_start"] = utils.sanitize_timestamp(point["first"])
    kwargs["duration_end"] = utils.sanitize_timestamp(point["last"])
    kwargs["duration"] = (kwargs["duration_end"] -
                          kwargs["duration_start"]).total_seconds()
    kwargs["period"] = period
    kwargs["period_start"] = utils.sanitize_timestamp(point["time"])
    kwargs["period_end"] = (utils.sanitize_timestamp(point["time"]) +
                            datetime.timedelta(seconds=period or 0))
    kwargs["unit"] = "%"
    return models.Statistics(**kwargs)


def transform_metadata(point):
    """Bring metadata from the point and put it to the dict."""

    metadata = dict()
    for key, value in point.items():
        if value and key.startswith("metadata."):
            metadata[key[len("metadata."):]] = value
    return metadata


def point_to_sample(point):
    """Transform point to the Sample instance."""

    return models.Sample(
        point.get("source"),
        point["meter"],
        point["type"],
        point["unit"],
        point["value"],
        point.get("user_id"),
        point.get("project_id"),
        point.get("resource_id"),
        utils.sanitize_timestamp(point["timestamp"]),
        transform_metadata(point),
        point.get("message_id"),
        point.get("message_signature"),
        None
    )


def split_url(url):
    """Split InfluxDB url ot the host, port, user, password and database."""

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

    database = parts.path[1:]
    return user, pwd, host, port, database


def sort_samples(samples, orderby):
    if not orderby:
        return samples

    def compare(a, b):
        for rule in orderby:
            for field, order in six.iteritems(rule):
                res = cmp(getattr(a, field), getattr(b, field))
                if res != 0:
                    return res * 1 if order.lower() == 'asc' else -1

    return sorted(samples, cmp=compare)
