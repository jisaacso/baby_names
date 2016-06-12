#!/bin/bash -x

export MASTER='local[4]'
export SPARK_HOME='thirdparty/spark-1.6.0-bin-hadoop2.6'
export PYSPARK_PYTHON=ipython

$SPARK_HOME/bin/pyspark \
    --executor-memory 8G \
    --conf spark.mesos.executor.memoryOverhead=2000 \
    --total-executor-cores 4 \
    --conf spark.task.cpus=1 \