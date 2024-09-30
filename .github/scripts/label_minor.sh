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

LABEL_NAME=small
MIN_SIZE=100

pr_diff=$(gh pr diff $PR_NUM -R $GITHUB_REPOSITORY)

insertions=$(printf "$pr_diff" | grep '^+' | wc -l)
deletions=$(printf "$pr_diff" | grep '^-' | wc -l)

total_changes=$((insertions + deletions))
if [ "$total_changes" -lt "$MIN_SIZE" ]; then
    gh issue edit $PR_NUM --add-label $LABEL_NAME -R $GITHUB_REPOSITORY
else
    gh issue edit $PR_NUM --remove-label $LABEL_NAME -R $GITHUB_REPOSITORY
fi
