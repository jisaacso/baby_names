import os
from pyspark import SparkContext
from collections import namedtuple
nametup = namedtuple('nametup', ['name', 'gender', 'count', 'year'])

def num_baracks_by_year(rdd):
    baracks = (rdd
               .filter(lambda line: line.name == 'Barack')
               .map(lambda ary: (ary.count, ary.year)))

    num_baracks = baracks.takeOrdered(1000, key=lambda x: x[1])
    max_year = year_with_most_baracks(baracks)
    
    return num_baracks, max_year


def year_with_most_baracks(barack_rdd):
    year_with_most = (barack_rdd
                   .takeOrdered(1, key=lambda x: -int(x[0])))

    return year_with_most


def probably_from_gender_tup(gender_tup, gender_to_return):

    # If the name doesn't exist, return nothing
    if not gender_tup:
        return 'NaN'

    # If the name was only a girl or boy, probably will
    # be either 0 (match) or 1 (mismatch)
    if len(gender_tup) == 1:
        if gender_tup[0][0] == gender_to_return:
            return 1
        else:
            return 0

    # Otherwise, assume 2 genders were returned
    # and calculate the probably: match / total
    if gender_tup[0][0] == 'F':
        f = int(gender_tup[0][1])
        m = int(gender_tup[1][1])
    else:
        f = int(gender_tup[1][1])
        m = int(gender_tup[0][1])
    t = float(f + m)

    if gender_to_return == 'M':
        return m / t
    elif gender_to_return == 'F':
        return f / t
    else:
        raise Exception('Invalid Gender option')


def name_by_gender_for_year(rdd, name, year):
    name_by_gender = (rdd.filter(lambda line: line.name == name)
                      .filter(lambda line: line.year==str(year))
                      .map(lambda line: (line.gender, line.count))
                      .collect())

    return probably_from_gender_tup(name_by_gender, 'M')


def write_stats(fname, stats):
    print stats
    with open(fname, 'w') as fout:
        if type(stats) == list:
            for stat in stats:
                fout.write(','.join(stat) + '\n')
        else:
            fout.write('%s' % stats)

                

if __name__ == '__main__':
    outdir = 'results'
    sc = SparkContext()

    rdd = (sc.textFile('data/*.txt2')
           .map(lambda line: line.split(','))
           .map(lambda line: nametup(*line)))

    #num_baracks, max_year = num_baracks_by_year(rdd)
    #write_stats(os.path.join(outdir, 'num_baracks_by_year'), num_baracks)
    #write_stats(os.path.join(outdir, 'year_with_most_baracks'), max_year)

    pr_jordan_2003 = name_by_gender_for_year(rdd, 'Jordan', 2003)
    write_stats(os.path.join(outdir, 'jordan_2003'), pr_jordan_2003)

    pr_jordan_2013 = name_by_gender_for_year(rdd, 'Jordan', 2013)
    write_stats(os.path.join(outdir, 'jordan_2013'), pr_jordan_2013)

    pr_tesla_2003 = name_by_gender_for_year(rdd, 'Tesla', 2003)
    write_stats(os.path.join(outdir, 'tesla_2003'), pr_tesla_2003)

    pr_tesla_2013 = name_by_gender_for_year(rdd, 'Tesla', 2013)
    write_stats(os.path.join(outdir, 'tesla_2013'), pr_tesla_2013)

