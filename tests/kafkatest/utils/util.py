# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from kafkatest import __version__ as __kafkatest_version__

import re


def kafkatest_version():
    """Return string representation of current ducktape version."""
    return __kafkatest_version__


def _kafka_jar_versions(proc_string):
    """Return all kafka versions explicitly in the process classpath"""
    versions = re.findall("kafka_[0-9\.]+-([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)", proc_string)
    return set(versions)


def is_version(node, version, proc_grep_string="kafka.properties"):
    """Heuristic to check that only the specified version appears in the classpath of the process
    A useful tool to aid in checking that service version apis are working correctly.
    """
    lines = [l for l in node.account.ssh_capture("ps ax | grep %s | grep -v grep" % proc_grep_string)]
    assert len(lines) == 1

    return _kafka_jar_versions(lines[0]) == {str(version)}
