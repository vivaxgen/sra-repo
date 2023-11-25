#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK

__copyright__ = '''
SRA-mgr - command line utility for SRA database
[https://github.com/vivaxgen/SRA-repo]
(c) 2023 Hidayat Trimarsanto <trimarsanto@gmail.com>

All right reserved.
This software is licensed under MIT license.
'''

import os
import argparse
import argcomplete
from sra_repo.utils import cexit


def init_argparse():
    p = argparse.ArgumentParser(
        description='SRA-mgr - management utility'
    )

    cmds = p.add_subparsers(required=True, dest='command')

    # command: update-metadata
    cmd_010 = cmds.add_parser('update-metadata')
    cmd_010.add_argument('--all', default=False, action='store_true')
    cmd_010.add_argument('sraids', nargs='*')

    # command: fix-file-permission
    cmd_020 = cmds.add_parser('fix-file-permission')
    cmd_020.add_argument('--all', default=False, action='store_true')
    cmd_020.add_argument('sraids', nargs='*')


def do(args):

    from sra_repo import filestore

    fs = filestore.SRAFileStorage(args.rootfs)

    match args.command:

        case 'update-metadata':
            do_update_metadata(args, fs)

        case 'fix-file-permission':
            do_fix_file_permission(args, fs)

        case _:
            cexit('ERR: please provide command')


def do_update_metadata(args, fs):

    if args.all:
        sraids = fs.list()
    else:
        sraids = args.sraids

    for sra_id in sraids:

        revalidation_list = []

        try:
            # get file info first to determine sites
            info = fs.get_validation_info(sra_id)

            source_site = info['source']
            

            continue

        except FileNotFoundError:
            # not prior info, need to perform revalidation
            revalidation_list.append(sra_id)

    if any(revalidation_list):

        import types
        from sra_repo.cmds import do_check

        ns_args = types.SimpleNamespace(
            sraids=revalidation_list,
            idfile=None,
            samplefile=None,
            count=-1,
            validate=True,
            showcmds=False
        )

        do_check(ns_args, fs)


def do_fix_file_permission(args, fs):
    raise NotImplementedError()


def main():
    p = init_argparse()
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
