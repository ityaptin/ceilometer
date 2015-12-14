import random
import datetime
import iso8601
import uuid
import time
import influxdb
import influxdb.exceptions
import argparse
import elasticsearch as es
from elasticsearch import helpers

EPOCH = datetime.datetime(1970, 1, 1)

metadata_fields = ["stack", "type", "name", "ref", "owner", "data",
                   "christmas", "fill", "field", "metadata"]
metrics = [{"counter_name": "cpu_util", "counter_type": "gauge",
            "counter_unit": "%"},
           {"counter_name": "cpu", "counter_type": "delta",
            "counter_unit": "time"},
           {"counter_name": "vcpus", "counter_type": "delta",
            "counter_unit": "vcpus"},
           ]

projects = ["2df45f4975d742ecbbd8d517a1cb7bda",
            "63a09ce332674142a6603216d0c2af63"]

users = ["236f91d276384f6aab93445fd8c4d2a4", "40ed36e162704308a60f90c178e64502",
         "99e130ad66b74865b0c34f5a1895559a", "763983a38ab74da49e5ed62e6efec2ad",
         "cb7d4a4000934e349e13bfbefdc19790", "167850fef75a4f578464fbbb9748cb31",
         "678b3974045a40a498e772702f1caf76", "610c8428f5f34f5b903e483225931c99",
         "764efdb221d44d0c80eb59c1dd968b84"]


def create_database(host, port, database, recreate=False):
    client = influxdb.InfluxDBClient(host, port)
    if recreate:
        try:
            client.drop_database(database)
        except influxdb.exceptions.InfluxDBClientError as e:
            print "Error during the database %s drop: %s" % (database, e)
    try:
        client.create_database(database)
    except influxdb.exceptions.InfluxDBClientError as e:
        print "Error during the database %s create: %s" % (database, e)


def create_metadata():
    metadata = dict(
        (field, "%s_%s" % (field, random.randint(0, 1000)))
        for field in metadata_fields
    )
    return metadata


def get_samples_batch(batch_size, start, period, recorded, max,
                      resources):
    for i in xrange(min(batch_size, max - recorded)):
        start += period
        resource = random.choice(resources)
        yield dict(
            counter_name=resource[4]["counter_name"],
            counter_type=resource[4]["counter_type"],
            counter_unit=resource[4]["counter_unit"],
            counter_volume=random.random() * 800,
            user_id=resource[2],
            project_id=resource[1],
            resource_id=resource[0],
            resource_metadata=resource[3],
            timestamp=start,
            recorded_at=start,
            source="openstack",
            message_id=uuid.uuid4().hex,
            message_signature=uuid.uuid4().hex
        )


def sample_to_point(sample):
    metadata = sample.get("resource_metadata")
    metadata = dict(("metadata.%s" % field, value)
                    for field, value in metadata.items())
    return dict(
        measurement="ceilometer",
        fields=dict(
            type=sample.get("counter_type"),
            unit=sample.get("counter_unit"),
            value=sample.get("counter_volume"),
            recorded_at=str(sample.get("recorded_at")),
            timestamp=str(sample.get("timestamp")),
            message_signature=sample.get("message_signature")
        ),
        tags=dict(
            meter=sample.get("counter_name"),
            user_id=sample.get('user_id'),
            project_id=sample.get('project_id'),
            resource_id=sample.get('resource_id'),
            source=sample.get('source'),
            message_id=sample.get("message_id"),
            **metadata
        ),
        time=sample.get('timestamp')
    )


def make_data(host, inport, esport, user, pwd, database, count, resource_count,
              batch, sleep, start, period):
    client = influxdb.InfluxDBClient(host, inport, username=user,
                                     password=pwd, database=database)
    es_client = es.Elasticsearch(["%s:%s" % (host, esport)])
    recorded = 0
    resource_usage = {}
    resources = create_resources(resource_count)

    def mark_resource_usage(sample):
        rid = sample["resource_id"]
        resource_usage[rid] = max(sample["timestamp"],
                                  resource_usage.get(rid, EPOCH))
        return True

    while recorded < count:
        batch_start = start + period * recorded
        points = [sample_to_point(sample)
                  for sample in get_samples_batch(batch, batch_start, period,
                                                  recorded, count, resources)
                  if mark_resource_usage(sample)]
        try:
            client.write_points(points, "ms", database)
            recorded += len(points)
            print "%s measurements have already been written." % recorded
        except influxdb.exceptions.InfluxDBClientError as e:
            print e
        time.sleep(sleep)
    try:
        write_resources(es_client, resources, resource_usage)
        write_meters(es_client, resources)
    except Exception as e:
        print e


def create_resources(resource_count):
    resources = []
    for i in range(resource_count):
        resources.append(
            ("resource_%s" % i,
             random.choice(projects),
             random.choice(users),
             create_metadata(),
             random.choice(metrics))
        )
    return resources


def get_time_options(start, count):
    if start:
        start = iso8601.parse_date(start)
    else:
        start = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    interval = float((datetime.datetime.utcnow() - start).total_seconds())

    return start, datetime.timedelta(seconds=interval / count)


def write_resources(client, resource_tuples, timestamps):
    index = 'resource'
    r_type = 'openstack'

    def _build_bulk_resources():
        for resource in resource_tuples:
            yield {
                '_op_type': 'create',
                '_index': index,
                '_type': r_type,
                '_id': resource[0],
                '_source': {
                    'start_sample_timestamp': None,
                    'last_sample_timestamp': timestamps.get(resource[0]),
                    'project_id': resource[1],
                    'user_id': resource[2],
                    'metadata': resource[3]
                }
            }

    client.indices.delete(index=index, ignore=[400, 404])
    for ok, result in helpers.streaming_bulk(
            client, _build_bulk_resources()):
        if not ok:
            __, result = result.popitem()
            print result
    print "%s resources have been writed" % len(resource_tuples)


def write_meters(client, resource_tuples):
    index = 'meter'

    def _build_bulk_meters():
        for resource in resource_tuples:
            meter = resource[4]
            yield {
                '_op_type': 'create',
                '_index': index,
                '_type': resource[0],
                '_id': meter['counter_name'],
                '_source': {
                    'project_id': resource[1],
                    'user_id': resource[2],
                    'meter_type': meter['counter_type'],
                    'meter_unit': meter['counter_unit'],
                    'source': 'openstack',
                    'metadata': resource[3]
                }
            }

    client.indices.delete(index=index, ignore=[400, 404])
    for ok, result in helpers.streaming_bulk(
            client, _build_bulk_meters()):
        if not ok:
            __, result = result.popitem()
            print result
    print "%s meters have been writed" % len(resource_tuples)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database",
                        default="ceilometer",
                        help="InfluxDB database.")
    parser.add_argument("--influxdb-port",
                        type=int,
                        default=8086,
                        dest="influxdb_port")
    parser.add_argument("--es-port",
                        type=int,
                        default=9200,
                        dest="es_port")
    parser.add_argument("--resources-count",
                        type=int,
                        default=100,
                        dest="rcount")
    parser.add_argument("--recreate",
                        type=bool,
                        default=False,
                        dest="recreate")
    parser.add_argument("--count",
                        default=10000,
                        type=int)
    parser.add_argument("--batch-size",
                        type=int,
                        default=500,
                        dest="batch")
    parser.add_argument("--host",
                        default="localhost")
    parser.add_argument("--port",
                        type=int,
                        default=8086)
    parser.add_argument("--sleep",
                        type=float,
                        default=0.,
                        dest="sleep")
    parser.add_argument("--start-datetime",
                        default=None,
                        dest="start",
                        help="Start datetime in izo. Now - 1 day "
                             "by default")
    parser.add_argument("--user",
                        default=None)
    parser.add_argument("--pwd",
                        default=None)
    args = parser.parse_args()
    create_database(args.host, args.influxdb_port,
                    args.database, args.recreate)
    start, period = get_time_options(args.start, args.count)
    make_data(args.host, args.influxdb_port, args.es_port, args.user,
              args.pwd, args.database, args.count, args.rcount, args.batch,
              args.sleep, start, period)


if __name__ == '__main__':
    main()
