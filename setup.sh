#!/bin/bash

if [ ! -d "results" ]
then
    mkdir results
fi

echo "Fetching Spark-1.6.0"
if [ ! -d "thirdparty" ]
then
    mkdir thirdparty
    pushd thirdparty
    wget http://mirror.cogentco.com/pub/apache/spark/spark-1.6.0/spark-1.6.0-bin-hadoop2.6.tgz
    mv spark-1.6.0-bin-hadoop2.6.tgz spark.tgz
    tar xf spark.tgz
    popd
fi

cd ..

echo "Fetching Enron dataset"
if [ ! -d "data" ]
then
    mkdir data
    pushd data
    wget https://www.ssa.gov/oact/babynames/names.zip
    unzip names.zip
    popd
fi
