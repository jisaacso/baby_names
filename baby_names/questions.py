import os
from pyspark import SparkContext

def num_baracks_by_year(rdd):
    baracks = (rdd
               .filter(lambda line: line[0] == 'Barack')
               .map(lambda ary: (ary[2], ary[3])))

    num_baracks = baracks.takeOrdered(1000, key=lambda x: x[1])
    max_year = year_with_most_baracks(baracks)
    
    return num_baracks, max_year

def year_with_most_baracks(barack_rdd):
    year_with_most = (barack_rdd
                   .takeOrdered(1, key=lambda x: -int(x[0])))

    return year_with_most

def write_stats(fname, stats):
    print stats
    with open(fname, 'w') as fout:
        for stat in stats:
            fout.write(','.join(stat) + '\n')
            

if __name__ == '__main__':
    outdir = 'results'
    sc = SparkContext()

    rdd = (sc.textFile('data/*.txt2')
           .map(lambda line: line.split(',')))

    num_baracks, max_year = num_baracks_by_year(rdd)
    write_stats(os.path.join(outdir, 'num_baracks_by_year'), num_baracks)
    write_stats(os.path.join(outdir, 'year_with_most_baracks'), max_year)


