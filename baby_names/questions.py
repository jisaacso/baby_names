import os
from pyspark import SparkContext
from collections import namedtuple
nametup = namedtuple('nametup', ['name', 'gender', 'count', 'year'])

def num_baracks_by_year(rdd):
    """Question 1: how many babies named Barack for each year
    
    Parameters
    ----------
    rdd: SparkContext.RDD
        A RDD of baby name namedtuples
    
    Returns
    -------
    list:
        A list of tuples, (num Baracks, year)
    tuple:
        (num Baracks, year with most Baracks)
    """

    baracks = (rdd
               # Filter for Barack
               .filter(lambda line: line.name == 'Barack')
               # Construct count, year tuple
               .map(lambda ary: (ary.count, ary.year)))

    num_baracks = baracks.takeOrdered(1000, key=lambda x: x[1])
    max_year = year_with_most_baracks(baracks)
    
    return num_baracks, max_year


def year_with_most_baracks(barack_rdd):
    """Given an rdd filtered by Barack, return 
    the year with the most names
    
    Parameters
    ----------
    barack_rdd: SparkContext.RDD
        A RDD of Barack name counts by year
    
    Returns
    -------
    tuple:
        (num Baracks, year with most Baracks)
    """
    year_with_most = (barack_rdd
                      # Take the largest count
                      .takeOrdered(1, key=lambda x: -int(x[0])))

    return year_with_most


def probably_from_gender_tup(gender_tup, gender_to_return):
    """Helper function to calculate 'probability' of 
    a gender given tuples with gender, count
    
    Parameters
    ----------
    gender_tup: list[tuples]
        A list of tuples containing (gender, count)
    
    gender_to_return: str
        gender to return, selected from {'M', 'F'}
    Returns
    -------
    float:
        probably of gender
    """

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
    """Questions 2 and 3: calculate probably of gender
    given name and year of child
    
    Parameters
    ----------
    rdd: SparkContext.RDD
        A RDD of baby name namedtuples
    name: str
        Name of child to find
    year: int
        Year of birth to calculate on

    Returns
    -------
    float:
        probably of gender
    """
    name_by_gender = (rdd
                      # Filter out gender and year
                      .filter(lambda line: line.name == name)
                      .filter(lambda line: line.year==str(year))

                      # extract gender, count
                      .map(lambda line: (line.gender, line.count))
                      .collect())

    return probably_from_gender_tup(name_by_gender, 'M')


def most_gender_neutral(rdd):
    """Questions 4: find the most gender neutral names in 2014.
    Here, "gender neutral" is defined as the names are as close 
    to 50% Male / 50% Female as possible. It turns out there
    are many names which are exactly 50/50 so I subsequently
    order 50/50 names by their total count in 2014.
    
    Parameters
    ----------
    rdd: SparkContext.RDD
        A RDD of baby name namedtuples

    Returns
    -------
    list(tuple):
        list of (name, count, gender %)
    """
    total_names = (rdd

                   # Filter out for year 2014
                   .filter(lambda line: line.year == '2014')

                   # Count number per name (name, count)
                   .map(lambda line: (line.name, int(line.count)))
                   .reduceByKey(lambda x,y: x+y))

    name_by_gender = (rdd

                      # Filter out for year 2014
                      .filter(lambda line: line.year == '2014')

                      # Count number per (name-gender)
                      .map(lambda line: ((line.name, line.gender),
                                         int(line.count)))

                      # Rearrange to (name, (gender, count)) for join
                      .reduceByKey(lambda x,y: x+y)
                      .map(lambda (k, v): (k[0], [k[1], v])))
    
    most_neutral = (name_by_gender
                    # Joining two RDDs of the form (K, V1).join( (K, V2) ) yields
                    # one RDD of the form (K, (V1, V2))
                    .join(total_names)

                    # reduce RDD size, get rid of single gender names
                    .filter(lambda (name, tup): not tup[0][1] == tup[1])

                    # Pick one of the genders since pr(M) = pr(F)
                    .filter(lambda (name, tup): tup[0][0] == 'M')
                    
                    # Calculate gender ratio: count(M) / count(total)
                    .map(lambda (name, tup): (name, tup[1], tup[0][1] / float(tup[1])))

                    # Extract gender ratio = 0.5
                    .filter(lambda tup: tup[2] == 0.5)

                    # Take the 5 most frequent names
                    .takeOrdered(5, key=lambda tup: -tup[1]))

    return most_neutral


def write_stats(fname, stats):
    print stats
    with open(fname, 'w') as fout:
        if type(stats) == list:
            for stat in stats:
                stat = [str(thing) for thing in stat]
                fout.write(','.join(stat) + '\n')
        else:
            fout.write('%s' % stats)

                

if __name__ == '__main__':
    outdir = 'results'
    sc = SparkContext()

    rdd = (sc.textFile('data/*.txt2')
           # Map each line into a nice tuple object
           .map(lambda line: line.split(','))
           .map(lambda line: nametup(*line)))

    num_baracks, max_year = num_baracks_by_year(rdd)
    write_stats(os.path.join(outdir, 'num_baracks_by_year'), num_baracks)
    write_stats(os.path.join(outdir, 'year_with_most_baracks'), max_year)
    
    pr_jordan_2003 = name_by_gender_for_year(rdd, 'Jordan', 2003)
    write_stats(os.path.join(outdir, 'jordan_2003'), pr_jordan_2003)

    pr_jordan_2013 = name_by_gender_for_year(rdd, 'Jordan', 2013)
    write_stats(os.path.join(outdir, 'jordan_2013'), pr_jordan_2013)

    pr_tesla_2003 = name_by_gender_for_year(rdd, 'Tesla', 2003)
    write_stats(os.path.join(outdir, 'tesla_2003'), pr_tesla_2003)

    pr_tesla_2013 = name_by_gender_for_year(rdd, 'Tesla', 2013)
    write_stats(os.path.join(outdir, 'tesla_2013'), pr_tesla_2013)

    gender_neutral = most_gender_neutral(rdd)
    write_stats(os.path.join(outdir, 'most_gender_neutral'), gender_neutral)
