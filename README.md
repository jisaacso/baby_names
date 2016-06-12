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

# Notes
Questions 4: find the most gender neutral names in 2014.
Here, "gender neutral" is defined as the names are as close 
to 50% Male / 50% Female as possible. It turns out there
are many names which are exactly 50/50 so I subsequently
order 50/50 names by their total count in 2014.
    
