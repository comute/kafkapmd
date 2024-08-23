#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

echo "Running gradlew with timeout"

timeout 5m ./gradlew --build-cache --scan --continue \
  -PtestLoggingEvents=started,passed,skipped,failed \
  -PignoreFailures=true -PmaxParallelForks=2 \
  -PmaxTestRetries=1 -PmaxTestRetryFailures=10 \
  test

exitCode=$?
if [ exitCode -eq 124 ]; then
    echo "Timed out. Attempting to publish build scan"
    ./gradlew buildScanPublishPrevious
    exit 1
elif [ exitCode -eq 127 ]; then
    echo "Killed. Attempting to publish build scan"
    ./gradlew buildScanPublishPrevious
    exit 1
fi

exit exitCode