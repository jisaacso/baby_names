# Prerequisities: Please run

./setup.sh

to download spark-1.6.0 into a `thirdparty` directory and
download the data.gov baby dataset into a `data` directory.

# Running
Simply:

./run.sh

This will spawn a pyspark job (4 tasks, 8G executor) which
will answer all 3 questions sent in the email and save the
results into a directory `results`.


# Assumptions: