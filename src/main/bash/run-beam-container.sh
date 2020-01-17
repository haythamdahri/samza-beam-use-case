#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Check if server is set. If not - set server optimization
[[ $JAVA_OPTS != *-server* ]] && export JAVA_OPTS="$JAVA_OPTS -server"

# Set container ID system property for use in Log4J
[[ $JAVA_OPTS != *-Dsamza.container.id* && ! -z "$SAMZA_CONTAINER_ID" ]] && export JAVA_OPTS="$JAVA_OPTS -Dsamza.container.id=$SAMZA_CONTAINER_ID"

# Set container name system property for use in Log4J
[[ $JAVA_OPTS != *-Dsamza.container.name* && ! -z "$SAMZA_CONTAINER_ID" ]] && export JAVA_OPTS="$JAVA_OPTS -Dsamza.container.name=samza-container-$SAMZA_CONTAINER_ID"

home_dir=`pwd`
base_dir=$(dirname $0)/..
cd $base_dir
base_dir=`pwd`
cd $home_dir

override="{\"task.execute\":\"bin/run-beam-container.sh $@\"}"

exec $(dirname $0)/run-class.sh $1 --runner=org.apache.beam.runners.samza.SamzaRunner --configFactory=org.apache.beam.runners.samza.container.ContainerCfgFactory --configOverride="$override" "${@:2}"