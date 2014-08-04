#!/usr/bin/env python
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

import os
import pstats
import subprocess
from eventlet import patcher
import tempfile
import time
threading = patcher.original("threading")
import shutil
import optparse
import glob
import timeit
import requests

import samplegenerator
from ceilometer.openstack.common import log


parser = optparse.OptionParser()
options = [
    optparse.Option("--rabbit-userid", help="Rabbit userid",
                    dest="rabbit_userid"),

    optparse.Option("--rabbit-password", help="Rabbit password",
                    dest="rabbit_password"),

    optparse.Option("--rabbit-port", help="Rabbit port",
                    dest="rabbit_port", type="int"),

    optparse.Option("--rabbit-host", help="Rabbit host",
                    dest="rabbit_host"),

    optparse.Option("--config-directory", help="Directory with config files",
                    dest="config_directory"),

    optparse.Option("--config-file", help="Config file",
                    dest="config_file"),

    optparse.Option("--bin-directory", help="Ceilometer bins",
                    dest="bin_directory"),

    optparse.Option("--rabbit-exchange", help="Rabbit exchange name",
                    dest="rabbit_exchange"),

    optparse.Option("--os-auth-token", help="Auth token for api requests",
                    dest="auth_token"),

    optparse.Option("--api-port", help="Api port", default=17278,
                    dest="api_port"),


]

parser.add_options(options)


class OneInstanceApiJob(object):
    def __init__(self, tmp_dir, config_file,
                 api_port,
                 auth_token,
                 bin_directory="/usr/local/bin/",
                 log_file=None):
        self.config_file = config_file
        self.temp_folder = tempfile.mkdtemp(prefix="stats", dir=tmp_dir)
        self.log_file = log_file or tempfile.mktemp(prefix="api-",
                                                    suffix=".log",
                                                    dir=tmp_dir)
        self.bin_file = "%s/ceilometer-api" % bin_directory
        self.api_port = api_port
        self.auth_token = auth_token

    def run(self):
        print("One instance api job started")
        print("Config file %s" % self.config_file)
        print("Log file %s" % self.log_file)
        print("Bin file %s" % self.bin_file)
        proc = subprocess.Popen([self.bin_file, "--config-file",
                                 self.config_file,
                                 "--log-file", self.log_file], )
        # import pdb
        # pdb.set_trace()
        time.sleep(10)
        self.make_api_requests()
        check_running(proc, self.log_file, step=70)

    def make_api_requests(self):
        queries = ["samples?limit=100", "samples?limit=1000",
                   "samples?limit=5000", "samples?limit=10000",
                   "meters", "meters/instance/statistics", "resources"]
        with open(self.temp_folder + "/api-time", "a") as f:
            url = "http://localhost:%s/v2/" % self.api_port
            for query in queries:
                cmd = "requests.get('%(url)s%(q)s', headers={'X-Auth-Token':'%(token)s'})" % \
                      dict(url=url,
                           q=query,
                           token=self.auth_token)
                t = timeit.timeit(cmd, setup="import requests", number=1)
                f.write("%s : %s\n" % (query, t))


class OneInstanceCollectorJob(object):
    def __init__(self, tmp_dir, rabbit_url, config_file,
                 rabbit_exchange, bin_directory="/usr/local/bin/",
                 log_file=None):
        self.config_file = config_file
        self.temp_folder = tempfile.mkdtemp(prefix="stats", dir=tmp_dir)
        self.log_file = log_file or tempfile.mktemp(prefix="collector",
                                                    suffix=".log",
                                                    dir=tmp_dir)
        self.bin_file = "%s/ceilometer-collector" % bin_directory
        self.gen = samplegenerator.SamplesPublisher(rabbit_url,
                                                    max_count=100000,
                                                    exchange=rabbit_exchange,
                                                    interval=0.001,
                                                    rate_step=0,
                                                    resource_count=3000)
    #
    def run(self):
        print("One instance collector test started")
        print("Log file %s" % self.log_file)
        print("Bin file %s" % self.bin_file)
        threading.Thread(target=self.generate_samples).start()
        proc = subprocess.Popen([self.bin_file, "--config-file",
                                 self.config_file,
                                 "--log-file", self.log_file],)
        check_running(proc, self.log_file)
        print("Generating finished")
        return self.temp_folder

    def generate_samples(self):
        print("Samples generator")
        self.gen.run()


def check_running(proc, log_file, step=60):
    prev_size = 0
    is_running = True
    while is_running:
        time.sleep(step)
        size = os.stat(log_file).st_size
        is_running = prev_size != size
        prev_size = size
    os.kill(proc.pid, 9)
    proc.terminate()


def add_profile_config(config_file, tmp_dir, transport_url, topic, api_port,
                       **kwargs):
    with open(config_file, "a") as out:
        out.write("\n\n[DEFAULT]\n")
        out.write("use_profiler=True\n")
        out.write("profile_dir=%s\n" % tmp_dir)
        for k, v in kwargs.items():
            out.write("%s=%s\n" % (k, v))
        out.write("\n[publisher_rpc]\n")
        out.write("metering_topic=%s\n" % topic)
        out.write("\n[api]\n")
        out.write("port=%s" % api_port)


def prepare_dir(opt, origin_config_file, rabbit_exchange, rabbit_host,
                rabbit_password, rabbit_port, rabbit_url, rabbit_userid,
                service):
    tmp_dir = tempfile.mkdtemp(
        prefix="%s-%s-tests-" % (service, origin_config_file.rsplit("/")[-1]))
    config_file = "%s/%s" % (tmp_dir, "ceilometer.conf")
    shutil.copy(origin_config_file, config_file)
    add_profile_config(config_file, tmp_dir,
                       rabbit_url,
                       rabbit_exchange,
                       rabbit_userid=rabbit_userid,
                       rabbit_host=rabbit_host,
                       rabbit_password=rabbit_password,
                       rabbit_port=rabbit_port,
                       api_port=opt.api_port)
    return config_file, tmp_dir


def main():
    opt, args = parser.parse_args()

    rabbit_userid = opt.rabbit_userid
    rabbit_password = opt.rabbit_password
    rabbit_host = opt.rabbit_host
    rabbit_port = opt.rabbit_port
    rabbit_exchange = opt.rabbit_exchange
    rabbit_url = "rabbit://%(userid)s:%(password)s@%(host)s:%(port)i/%%2F" % \
                 dict(userid=rabbit_userid, password=rabbit_password,
                      host=rabbit_host, port=rabbit_port)

    bin_folder = opt.bin_directory

    config_files = []
    if opt.config_directory:
        config_files = glob.glob(opt.config_directory + "/ceilometer.conf*")

    if opt.config_file:
        config_files.append(opt.config_file)

    try:
        for origin_config_file in config_files:
            config_file, tmp_dir = prepare_dir(opt, origin_config_file,
                                               rabbit_exchange, rabbit_host,
                                               rabbit_password, rabbit_port,
                                               rabbit_url, rabbit_userid,
                                               "collector")
            OneInstanceCollectorJob(tmp_dir=tmp_dir,
                                    bin_directory=bin_folder,
                                    rabbit_url=rabbit_url,
                                    config_file=config_file,
                                    rabbit_exchange=rabbit_exchange).run()
            config_file, tmp_dir = prepare_dir(opt, origin_config_file,
                                               rabbit_exchange, rabbit_host,
                                               rabbit_password, rabbit_port,
                                               rabbit_url, rabbit_userid,
                                               "api")
            OneInstanceApiJob(tmp_dir=tmp_dir,
                              bin_directory=bin_folder,
                              config_file=config_file,
                              api_port=opt.api_port,
                              auth_token=opt.auth_token).run()
            print "Tmp directory %s" % tmp_dir
            print("Config file %s" % config_file)
    except Exception as e:
        print(e)

main()