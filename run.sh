#!/bin/bash -x

export MASTER='local[4]'
export SPARK_HOME='thirdparty/spark-1.6.1-bin-hadoop2.6'
export PYSPARK_PYTHON=ipython

$SPARK_HOME/bin/spark-submit \
    --executor-memory 8G \
    --conf spark.mesos.executor.memoryOverhead=2000 \
    --total-executor-cores 4 \
    --conf spark.task.cpus=1 \
    baby_names/questions.py
    
