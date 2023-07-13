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
from sra_repo.utils import cexit, cerr


def init_argparse():
    p = argparse.ArgumentParser(
        description='sra-validator'
    )

    p.add_argument('--reads', required=True, type=int,
                   help='number of total reads per fastq files')
    p.add_argument('--bases', required=True, type=int,
                   help='number of total bases from all fastq files')
    p.add_argument('infiles', nargs='+',
                   help='fastq files to be validated')
    return p


def main():
    p = init_argparse()
    argcomplete.autocomplete(p)
    args = p.parse_args()

    from concurrent.futures import ProcessPoolExecutor
    import pathlib

    try:
        file_bases = []
        file_reads = []
        file_names = []
        with ProcessPoolExecutor(max_workers=min(len(args.infiles), 8)) as executor:
            for infile, reads, bases in executor.map(count_file, args.infiles):
                file_names.append(infile)
                file_bases.append(bases)
                file_reads.append(reads)

        for (file_name, file_base, file_read) in zip(file_names, file_bases, file_reads):
            cerr(f'File: {file_name}\n- reads: {file_read}\n- bases: {file_base}')

        total_bases = sum(file_bases)

        if total_bases != args.bases:
            raise ValueError(f'Total bases {total_bases} does not match {args.bases}!')

        if len(set(file_reads)) != 1:
            raise ValueError('Read counts are not identical for all fastq files')

        if (file_reads[0] != args.reads and file_reads[0] * 2 != args.reads):
            raise ValueError(f'Read counts {file_reads[0]} does not match {args.reads}')

        cerr('Files are correct.')

    finally:

        # remove .fxi files
        for infile in args.infiles:
            path = pathlib.Path(infile + '.fxi')
            path.unlink(missing_ok=True)


def count_file(infile):

    import pyfastx

    bases = 0
    reads = 0
    fq = pyfastx.Fastq(infile, build_index=False)
    for name, seq, qual in fq:
        reads += 1
        bases += len(seq)
    return (infile, reads, bases)


if __name__ == '__main__':
    main()

# EOF
