#!/bin/bash

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
