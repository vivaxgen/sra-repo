#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

__copyright__ = '''
SRA-repo - command line utility for SRA database
[https://github.com/vivaxgen/SRA-repo]
(c) 2022 Hidayat Trimarsanto <trimarsanto@gmail.com>

All right reserved.
This software is licensed under MIT license.
'''

import sys
import os
import argcomplete
from sra_repo import cmds
from sra_repo.utils import cexit


def do(args):

    cmds.do_command(args)


def main():
    p = cmds.init_argparse()
    argcomplete.autocomplete(p)
    args = p.parse_args()

    # set default root fs
    if args.rootfs is None:
        args.rootfs = os.environ.get('SRA_REPO_STORE', None)
    if not args.rootfs:
        cexit('ERROR: please set SRA_REPO_STORE or supply --rootfs')

    do(args)


if __name__ == '__main__':
    main()

# EOF
