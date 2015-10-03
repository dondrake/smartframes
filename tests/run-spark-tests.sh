#!/bin/bash
# Copyright 2015 Don Drake don@drakeconsulting.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Drake
# run all tests through different versions of Spark
#

SRCDIR="$( cd -P "$( dirname $"{BASH_SOURCE[0]}" )" && pwd)"

function run_test() {
    echo "Running test $1"

    LOG_FILE=$1.log
    spark-submit \
        --master local[2] \
        --driver-memory 4G \
        --py-files $SRCDIR/../smartframes/SmartFrames.py \
        $1 | tee $LOG_FILE 2>&1
    RETVAL=$?
    if [[ $RETVAL != 0 ]]; then
        echo "Test $1 failed."
        exit 1
    fi
}

SPARK_HOMES="~/spark/spark-1.3.1-bin-hadoop2.6 ~/spark/spark-1.4.1-bin-hadoop2.6 ~/spark/spark-1.5.1-bin-hadoop2.6"

for S_HOME in $SPARK_HOMES; do
    export SPARK_HOME=$S_HOME
    export PYSPARK_DRIVER_PYTHON=`which python`
    export PATH=$SPARK_HOME/bin:$PATH


    run_test $SRCDIR/test_SmartFrames.py
done
