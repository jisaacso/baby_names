import os

def clean_line(line):
    return line.rstrip('\n').replace('\r', '')

def add_year_to_line():
    """Helper function to add the year of birth
    (as taken from the file name) onto the end
    of each line within the file. This is because spark
    loses context of file name when reading in parallel.

    Parameters
    ----------
    None
    
    Returns
    ----------
    None
    """
    for fname in os.listdir('data'):
        if fname.endswith('.txt'):
            yr = fname.lstrip('yob').rstrip('.txt')
            with open('data/' + fname, 'r') as fin, open('data/' + fname + '2', 'w') as fout:
                for line in fin.readlines():
                    fout.write('%(line)s,%(year)s\n' % {'line': clean_line(line),
                                                         'year': yr})
                    
if __name__ == '__main__':
    add_year_to_line()
