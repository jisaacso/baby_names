# Prerequisities: Please run

./setup.sh

to download spark-1.6.1 into a `thirdparty` directory and
download the data.gov baby dataset into a `data` directory.
This also preprocesses the dataset by appending a column
(year) onto every line.

# Running
Simply:

./run.sh

This will spawn a pyspark job (4 tasks, 8G executor) which
will answer all 4 questions sent in the prompt and save the
results into a directory `results`.
