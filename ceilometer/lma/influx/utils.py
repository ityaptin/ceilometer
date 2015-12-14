import datetime
from ceilometer.storage import models
from ceilometer import utils

MEASUREMENT = "ceilometer"

DEFAULT_AGGREGATES = ["mean", "sum", "count", "min", "max"]

TRANSITION = {
    "avg": "mean"
}

DETRANSITION = {
    "mean": "avg"
}

OP_SIGN = {'eq': '=', 'lt': '<', 'le': '<=', 'ne': '!=', 'gt': '>', 'ge': '>='}


def make_simple_filter_query(sample_filter, require_meter=False):
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
                            OP_SIGN.get(start_op, start_op),
                            sample_filter.start_timestamp))

    end_op = sample_filter.end_timestamp_op or "<"
    if sample_filter.end_timestamp:
        expressions.append(("time",
                            OP_SIGN.get(end_op, end_op),
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

    query = " and ".join(("%s%s'%s'" % exp for exp in expressions))

    return query


def make_groupby(period, groupby):
    return ", ".join((groupby or []) +
                     (["time({period}s)".format(period=period)]
                      if period > 0 else []))


def make_aggregation(aggregate):
    aggregates = []

    if not aggregate:
        for func in DEFAULT_AGGREGATES:
            aggregates.append("{func}(value) as {func}".format(func=func))
    else:
        for description in aggregate:
            func = description.func
            aggregates.append("{func}(value) as {func}".format(
                func=TRANSITION.get(func, func)))

    return ", ".join(aggregates + make_duration_aggregates())


def make_duration_aggregates():
    duration_aggregates = ["{func}(timestamp) as {func}".format(func=func)
                           for func in ("last", "first")]
    return duration_aggregates

def construct_query(select=None, where=None, group=None, order=None, limit=None):
    return " ".join((
        "SELECT {select} FROM {name}".format(name=MEASUREMENT, select=select)
        if select else "SELECT * FROM {name}".format(name=MEASUREMENT),
        "WHERE {where}".format(where=where) if where else "",
        "GROUP BY {group} file(none)".format(group=group) if group else "",
        "ORDER BY time desc" if order else "",
        "LIMIT {limit}".format(limit=limit) if limit else ""
    ))


def make_aggregate_query(sample_filter, period, groupby, aggregate):
    return construct_query(make_aggregation(aggregate),
                           make_simple_filter_query(sample_filter),
                           make_groupby(period, groupby),
                           order=True)


def make_time_bounds_query(sample_filter):
    return construct_query(make_duration_aggregates(),
                           make_simple_filter_query(sample_filter))


def make_list_query(sample_filter, limit):
    return construct_query(where=make_simple_filter_query(sample_filter),
                           limit=limit,
                           order=True)


def point_to_stat(point, tags, period, aggregate):
    kwargs = {}
    if not point['last'] or not point['first']:
        return
    if not aggregate:
        for func in DEFAULT_AGGREGATES:
            kwargs[DETRANSITION.get(func, func)] = point.get(func)
    else:
        kwargs['aggregate'] = {}
        for description in aggregate:
            func = description.func
            kwargs['aggregate'][func] = point.get(TRANSITION.get(func, func))

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
    metadata = dict()
    for key, value in point.items():
        if key.startswith("metadata."):
            metadata[key[9:]] = value
    return metadata


def point_to_sample(point):
    return models.Sample(
        point["source"],
        point["meter"],
        point["type"],
        point["unit"],
        point["value"],
        point["user_id"],
        point["project_id"],
        point["resource_id"],
        utils.sanitize_timestamp(point["timestamp"]),
        transform_metadata(point),
        point["message_id"],
        point["message_signature"],
        utils.sanitize_timestamp(point["recorded_at"])
    )
