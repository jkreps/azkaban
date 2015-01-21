#!/bin/bash

#
#   Copyright 2010 LinkedIn, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

PRGDIR=`dirname "$0"`
AZKABAN_HOME=`cd "$PRGDIR/.." ; pwd`

base_dir=$(dirname $0)/..

for file in $base_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/dist/azkaban/jars/*.jar;
do
	CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/dist/azkaban-common/jars/*.jar;
do
	CLASSPATH=$CLASSPATH:$file
done

if [ -z $AZKABAN_OPTS ]; then
  AZKABAN_OPTS="-Xmx2G -server -Dcom.sun.management.jmxremote"
fi

java -Dlog4j.configuration=file://$AZKABAN_HOME/azkaban/log4j.xml $AZKABAN_OPTS -cp $CLASSPATH azkaban.app.AzkabanApp --static-dir $base_dir/azkaban/web/static $@
