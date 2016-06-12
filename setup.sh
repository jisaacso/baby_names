#!/bin/bash

if [ ! -d "results" ]
then
    mkdir results
fi

echo "Fetching Spark-1.6.1"
if [ ! -d "thirdparty" ]
then
    mkdir thirdparty
    pushd thirdparty
    wget http://mirror.metrocast.net/apache/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz
    mv spark-1.6.1-bin-hadoop2.6.tgz spark.tgz
    tar xf spark.tgz
    rm spark.tgz
    popd
fi

echo "Fetching Enron dataset"
if [ ! -d "data" ]
then
    mkdir data
    pushd data
    wget https://www.ssa.gov/oact/babynames/names.zip
    unzip names.zip
    rm names.zip
    popd
fi

echo "Adding year to each line"
python baby_names/preprocess.py
