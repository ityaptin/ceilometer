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
"""Base class for plugins.
"""

import cProfile
from eventlet import patcher

threading = patcher.original('threading')
import time
from oslo.config import cfg

from ceilometer.openstack.common import log

LOG = log.getLogger(__name__)

profiler_opts = [
    cfg.BoolOpt('use_profiler',
                default=False),
    cfg.StrOpt('profile_dir',
               default=""),
]
cfg.CONF.register_opts(profiler_opts)


def profile(service):
    profiler = cProfile.Profile()

    def dump_stats():

        directory = cfg.CONF.profile_dir
        index = 0
        while True:
            time.sleep(60)
            index += 1
            profiler.disable()
            fname = directory + '/%s-%s.pstats' % (service, index)
            profiler.dump_stats(fname)
            profiler.clear()
            profiler.enable()

    profiler.enable()
    dumper = threading.Thread(target=dump_stats)
    dumper.start()