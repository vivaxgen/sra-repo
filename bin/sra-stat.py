#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

__copyright__ = '''
sra-validator - command line utility for validating SRA reads
[https://github.com/vivaxgen/SRA-repo]
(c) 2023 Hidayat Trimarsanto <trimarsanto@gmail.com>

All right reserved.
This software is licensed under MIT license.
'''

import sys
import os
import argparse
import argcomplete
from utils import cexit, cerr


def init_argparse():
    p = argparse.ArgumentParser(
        description='sra-validator'
    )

    p.add_argument('-j', '--jobs', default=8, type=int,
                   help='number of jobs (or processors) to use')
    p.add_argument('infiles', nargs='+',
                   help='fastq files to be validated')
    return p


def main():
    p = init_argparse()
    argcomplete.autocomplete(p)
    args = p.parse_args()

    from concurrent.futures import ProcessPoolExecutor
    import pandas as pd

    infile_list = []
    maxlen_list = []
    minlen_list = []
    avglen_list = []
    minqual_list = []
    maxqual_list = []

    with ProcessPoolExecutor(max_workers=min(len(args.infiles), args.jobs)) as executor:
        for results in executor.map(stat_file, args.infiles):
            infile, reads, bases, minlen, avglen, maxlen, minqual, maxqual = results
            infile_list.append(infile)
            maxlen_list.append(maxlen)
            minlen_list.append(minlen)
            avglen_list.append(avglen)
            minqual_list.append(minqual)
            maxqual_list.append(maxqual)
            cerr(f'[Finish stating file: {infile}]')

    df = pd.DataFrame({'FILE': infile_list,
                       'MAXLEN': maxlen_list, 'MINLEN': minlen_list, 'AVGLEN': avglen_list,
                       'MINQUAL': minqual_list, 'MAXQUAL': maxqual_list})
    print(df)


def stat_file(infile):

    import pyfastx
    import statistics

    fq = pyfastx.Fastq(infile, build_index=False)
    lengths = []
    quals = []
    for name, seq, qual in fq:
        lengths.append(len(qual))
        quals.append(quals)

    return (infile,
            len(lengths),
            sum(lengths),
            min(lengths),
            statistics.mean(lengths),
            max(lengths),
            min(map(min, quals)),
            max(map(max, quals)),
            )


if __name__ == '__main__':
    main()

# EOF
