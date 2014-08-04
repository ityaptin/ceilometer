from eventlet import patcher
import multiprocessing
import os
import time
import dbtools
import sys
threading = patcher.original("threading")
import errno
import subprocess
import timeit
import shutil

import send_sample_messages

def check_log_file_updates(proc, log_file):
    log_file_size = 0
    stat_size = os.stat(log_file).st_size
    while log_file_size != stat_size:
        log_file_size = stat_size
        time.sleep(300)
        stat_size = os.stat(log_file).st_size
    proc.kill()


class CollectorPerformanceTest():
    def __init__(self, argv, config_file, profile_dir):
        self.argv = argv
        self.config_file = config_file
        self.log_file = argv.log_file
        self.profile_dir = profile_dir

    def start_collector(self):
        return subprocess.Popen([self.argv.bin_dir + '/ceilometer-collector',
                                 '--config-file', self.config_file,
                                 '--log-file', self.log_file])

    def start(self):
        multiprocessing.Process(target=send_sample_messages.send_test_data,
                                args=[self.argv]).start()
        collector_proc = self.start_collector()
        check_log_file_updates(collector_proc, self.log_file)


class ApiPerformanceTest(object):
    def __init__(self, argv, config_file, profile_dir):
        self.config_file = config_file
        self.log_file = argv.log_file
        self.profile_dir = profile_dir
        self.argv = argv

    def make_api_requests(self):
        queries = ["samples?limit=100", "samples?limit=1000",
                   "samples?limit=5000", "samples?limit=10000",
                   "samples?limit=40000","meters",
                   "meters/instance/statistics", "resources"]
        with open(self.argv.profile_dir + "/api-time", "a") as f:
            url = "http://localhost:%s/v2/" % self.argv
            for query in queries:
                cmd = ("requests.get('%(url)s%(q)s', "
                       "headers={'X-Auth-Token':'%(token)s'})" %
                       dict(url=url,
                            q=query,
                            token=self.argv.auth_token))
                t = timeit.timeit(cmd, setup="import requests", number=1)
                f.write("%s : %s\n" % (query, t))

    def start(self):
        proc = subprocess.Popen([self.argv.bin_dir + '/ceilometer-api',
                                 "--config-file", self.config_file,
                                 "--log-file", self.log_file], )
        time.sleep(30)
        self.make_api_requests()
        check_log_file_updates(proc, self.log_file)


def mkdir(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def get_parser():
    parser = send_sample_messages.get_parser()
    parser.add_argument(
        '--result-dir',
        default='/tmp/collector-tests',
        help='Directory for profile results',
    )
    parser.add_argument(
        '--config-template',
        help='Config file for collector',
    )
    parser.add_argument(
        '--log-file',
        help='Log file for collector',
    )
    parser.add_argument(
        '--db-connections-file',
        help='File with test db connections',
    )
    parser.add_argument(
        '--api-port',
        default=8777,
        type=int,
        help='Api port',
    )
    parser.add_argument(
        '--auth-token',
        help="Keystone token for api requests"
    )
    parser.add_argument(
        '--bin-dir',
        default='/usr/bin/local/',
        help="Directory with ceilometer binaries"
    )

    return parser


def get_connections(db_connections_file):
    with open(db_connections_file, 'r') as f:
        return f.readlines()


def create_config_file(args, conn, test_result_dir):
    config_file = test_result_dir + "/ceilometer.conf"
    shutil.copy(args.config_file, config_file)
    with open(config_file, 'a') as f:
        f.write(("[DEFAULT]\n"
                 "use_profiler=True\n"
                 "profile_dir=%(profile_dir)s\n"
                 "\n[publisher_rpc]\n"
                 "metering_topic=%(rabbit_topic)s\n"
                 "\n[api]\n"
                 "port=%(api_port)s\n"
                 "\n[database]\n"
                 "connection=%(conn)s" % {'profile_dir': test_result_dir,
                                          'rabbit_topic': args.rabbit_topic,
                                          'api_port': args.api_port,
                                          'conn': conn}))
    return config_file


def main(argv):
    args = get_parser().parse_known_args(argv)[0]
    result_dir = args.result_dir
    mkdir(result_dir)
    connections = get_connections(args.db_connections_file)
    for conn in connections:
        backend_result_dir = "%s/ceilometer_test_%s" % (result_dir, conn[:5])
        mkdir(backend_result_dir)
        config_file = create_config_file(args, conn, backend_result_dir)
        argv.config_file = config_file
        dbtools.clear_and_dbsync(config_file)
        CollectorPerformanceTest(args,
                                 config_file,
                                 backend_result_dir).start()
        ApiPerformanceTest(args, config_file, backend_result_dir).start()

if __name__ == '__main__':
    main(sys.argv[1:])