
import os
import argparse

from sra_repo.utils import cerr, cout, cexit, byte_conversion


def site_args(p):

    p.add_argument('--site', default='ena-entrez',
                   choices=['ena', 'entrez', 'ena-entrez', 'entrez-ena'],
                   help='set the site databaset repository to choose between EBI/ENA and '
                        'NCBI/Entrez [ena-entrez]')


def input_args(p):

    p.add_argument('--idfile', default=None,
                   help='file containing list of SRA IDs')
    p.add_argument('--samplefile', default=None,
                   help='a tab-delimited sample manifest and their '
                        'associated SRA IDs')
    p.add_argument('SRAIDs', nargs='*',
                   help='list of SRA IDs')


def init_argparse():
    p = argparse.ArgumentParser(
        description='SRA-repo'
    )

    cmds = p.add_subparsers(required=True, dest='command')

    # command: store
    cmd_store = cmds.add_parser('store',
                                help='store read file(s) to database')
    cmd_store.add_argument('infiles', nargs='+')

    # command: path
    cmd_path = cmds.add_parser('path',
                               help='show file path')
    cmd_path.add_argument('--newline', default=False, action='store_true',
                          help='use newline for separator instead of space')
    input_args(cmd_path)

    # command: path
    cmd_info = cmds.add_parser('info',
                               help='show SRA information')
    input_args(cmd_info)

    # command: check
    cmd_check = cmds.add_parser('check',
                                help='check SRA ACCID in database')
    cmd_check.add_argument('--validate', default=False, action='store_true',
                           help='perform validation on number of reads and bases')
    cmd_check.add_argument('--showcmds', default=False, action='store_true',
                           help='show commands to be remotely run')
    cmd_check.add_argument('--ntasks', default=4, type=int,
                           help='number of threads (ie samples) to run in parallel [4]')
    cmd_check.add_argument('--count', default=-1, type=int,
                           help='number of SRA IDs to be checked')
    site_args(cmd_check)
    input_args(cmd_check)

    # command: link
    cmd_link = cmds.add_parser('link',
                               help='link read files(s) from sample file '
                               'or SRA id file')
    cmd_link.add_argument('--check', default=False, action='store_true',
                          help='check existence of fastq files in DB')
    cmd_link.add_argument('--hard', default=False, action='store_true',
                          help='create hardlink instead of symlink(s) to outdir')
    cmd_link.add_argument('-o', '--outfile',
                          help='output file containing sample manifest and their '
                          'associated fastq read files')
    cmd_link.add_argument('--outdir',  required=True,
                          help='output directory')
    input_args(cmd_link)

    # command: fetch
    cmd_fetch = cmds.add_parser('fetch',
                                help='fetch  read files by ACC IDs')
    cmd_fetch.add_argument('--force', default=False, action='store_true',
                           help='force downloading even when SRA ACCIDs exist in DB')
    cmd_fetch.add_argument('--tmpdir', default=None,
                           help='directory to temporarily put donwloaded files, overriding '
                           'SRA_REPO_TMPDIR env')
    cmd_fetch.add_argument('--ntasks', default=4, type=int,
                           help='number or tasks/workers performing downloads [4]')
    cmd_fetch.add_argument('--count', default=-1, type=int,
                           help='use for debugging - number of SRAs to download from list [-1]')
    cmd_fetch.add_argument('--reverselist', default=False, action='store_true',
                           help='reverse the list of ACC IDs')
    cmd_fetch.add_argument('--showcmds', default=False, action='store_true',
                           help='show commands to run externally')
    cmd_fetch.add_argument('--showurl', default=False, action='store_true',
                           help='show URL during downloading')
    cmd_fetch.add_argument('--targetdir', default=None,
                           help='instead of storing to central repository, move the '
                           'downloaded files to this target directory')
    site_args(cmd_fetch)
    input_args(cmd_fetch)

    # command: list
    cmd_list = cmds.add_parser('list',
                               help='list SRA IDs in DB')
    cmd_list.add_argument('-a', '--all', default=False, action='store_true',
                          help='list all SRA IDs stored in DB')
    cmd_list.add_argument('patterns', nargs='*',
                          help='pattern for SRA ids')

    # command: inventory
    cmd_inventory = cmds.add_parser('inventory',
                                    help='view inventory information')
    cmd_inventory.add_argument('--species', default=False, action='store_true',
                               help='count number of each species')

    # common arguments
    p.add_argument('--rootfs', default=None,
                   help='set root storage filesystem, default is using environment '
                        'SRA_REPO_STORE')

    return p


def do_command(args):

    from sra_repo import filestore

    fs = filestore.SRAFileStorage(args.rootfs)

    match args.command:

        case 'store':
            do_store(args, fs)

        case 'check':
            do_check(args, fs)

        case  'fetch':
            do_fetch(args, fs)

        case 'list':
            do_list(args, fs)

        case 'link':
            do_link(args, fs)

        case 'path':
            do_path(args, fs)

        case 'info':
            do_info(args, fs)

        case 'inventory':
            do_inventory(args, fs)

        case _:
            cexit('ERR: please provide command')


def do_store(args, fs):

    raise NotImplementedError('store command is disabled for now, please use fetch')

    for infile in args.infiles:
        dbpath = fs.store(infile)
        cerr(f'  {infile} => {dbpath}')


def do_check(args, fs):

    from sra_repo import sra_validator

    sraids = get_sraids(args)

    if args.count > 0:
        sraids = sraids[:args.count]

    helpers = get_helpers(args)

    validator = sra_validator.SRA_Validator(
        sraids,
        fs,
        helpers=helpers,
        validate=args.validate,
        showcmds=args.showcmds
    )
    validator.validate(threads=args.ntasks)

    not_finished = len(sraids) - validator.finished
    errors = validator.err_sraids
    if any(errors) or not_finished > 0:
        cerr(f'Unfinished validation: {not_finished}')
        cerr(f'Errors found in {len(errors)} SRA:')
        cerr('\n'.join(errors))
    if args.validate:
        cerr(f'{validator.finished - len(errors)} SRA ID(s) have been validated successfully.')
    else:
        cerr(f'{validator.finished - len(errors)} SRA ID(s) are in repository '
             f'(but no validation checks were performed)')


def do_link(args, fs):

    # either use sample manifest with enaid or use
    # enaids in command line arguments

    import pandas as pd
    import pathlib

    outdir = pathlib.Path(args.outdir)
    if not outdir.is_dir():
        cexit(f'ERR: directory [{outdir}] does not exist. Please create first!')

    if args.samplefile:
        # read sample file

        samples = []
        fastq_files = []

        for sample, sra_ids in iter_samplefile(args.samplefile):
            current_reads = []

            if sample.startswith('#'):
                continue

            for sra_id in sra_ids:

                try:
                    read_files = fs.link(sra_id, outdir, dryrun=args.check)
                    current_reads.append(read_files)

                except ValueError:
                    cerr(f'WARN: SRA ID {sra_id} is not in the database. '
                         f'Please run ena-repo fetch first.')

                except FileExistsError as err:
                    cerr(f'WARN: file {err.filename2} for SRA {sra_id} is already exist '
                         f'in directory [{outdir}]')

            samples.append(sample)
            fastq_files.append(current_reads)

        if args.outfile:
            # write sample/fastq manifest file

            fastq_paths = []
            for fastq_pairs in fastq_files:
                if len(fastq_pairs) == 0:
                    fastq_paths.append('')
                else:
                    fastq_paths.append(
                        ';'.join(','.join(paired_file) for paired_file in fastq_pairs)
                    )

            outfile_df = pd.DataFrame({'SAMPLE': samples, 'FASTQ': fastq_paths})
            outfile_df.to_csv(args.outfile, index=False, sep='\t')

        cerr(f'INFO: linked {len(fastq_files)} paired FASTQ files for {len(samples)} sample(s) ')

    else:
        sraids = []
        if args.idfile:
            with open(args.idfile) as f:
                sraids += [x.strip() for x in f.read().split()]
        sraids += args.SRAIDs

        read_files = []
        counter = 0
        for sra_id in sraids:
            try:
                read_files += fs.link(sra_id, outdir, dryrun=args.check)
                counter += 1
            except ValueError as err:
                cerr(f'WARN: SRA ID {sra_id} is not in the database. '
                     f'Please run ena-repo fetch first.')
            except FileExistsError as err:
                cerr(f'ERR: file {err.filename2} for SRA {sra_id} is already exist '
                     f'in directory: {outdir}')

        cerr(f'INFO: linked {len(read_files)} from {counter} SRA id(s)')


def do_list(args, fs):

    sra_list = fs.list()

    cout(f'Total SRA number: {len(sra_list)}')


def do_path(args, fs):

    # show paths from SRA ids file, sample file or list of SRA IDs in command line

    SRAIDs = get_sraids(args)

    path_lists = []
    for sra_id in SRAIDs:
        path_lists += fs.get_read_files(sra_id)

    sep = ' '
    if args.newline:
        sep = '\n'

    output = sep.join([f.as_posix() for f in path_lists])
    cout(output)


def do_info(args, fs):

    import yaml
    # show paths from SRA ids file, sample file or list of SRA IDs in command line

    SRAIDs = get_sraids(args)

    info_list = []
    err_list = []
    for sra_id in SRAIDs:
        if not fs.check(sra_id=sra_id, throw_exc=False):
            err_list.append(f'{sra_id} is not found in database')
            continue
        try:
            info_list.append(fs.get_validation_info(sra_id))
        except FileNotFoundError:
            err_list.append(f'{sra_id} does not have info file. '
                            'Please run: sra-repo.py check --validate')

    d = dict(info=info_list, error=err_list)
    cout(yaml.dump(d))


def do_fetch(args, fs):
    """ fetch fastq files from provided ENA IDs """

    from sra_repo.sra_downloader import SRA_Fetcher

    sraids = get_sraids(args)

    # check occurence of ENA IDs
    sraid_dl = []

    total = len(sraids)
    existed = 0
    skipped = 0
    for idx, sra_id in enumerate(sraids, 1):
        if sra_id.startswith('#'):
            cerr(f'({idx}/{total}) WARN: skipping {sra_id}')
            skipped += 1
            continue
        if not args.force and fs.check(sra_id=sra_id, throw_exc=False):
            # cerr(f'({idx}/{total}) WARN: ENA ACCID {ena_acc} is already existed.')
            existed += 1
            continue
        sraid_dl.append(sra_id)
    cerr(f'Total: {total}\nExisted: {existed}\nSkipped: {skipped}')

    # prepare tmp dir
    if args.tmpdir is None:
        args.tmpdir = os.environ.get('SRA_REPO_TMPDIR', None)
    if not args.tmpdir:
        cexit('ERROR: please set SRA_REPO_TMPDIR or supply --tmpdir')

    repos = get_helpers(args)

    fetcher = SRA_Fetcher(
        sraid_dl,
        filestore=fs,
        temp_directory=args.tmpdir,
        repos=repos,
        showcmds=args.showcmds,
        showurl=args.showurl,
        target_directory=args.targetdir
    )

    fetcher.fetch(ntasks=args.ntasks, count=args.count)

    if any(fetcher.sra_errors) or any(fetcher.sra_d):
        cerr(f'Completed {fetcher.completed} out of {len(sraid_dl)} SRAs to download.')
        cerr(f'WARNING: there are unsuccessful {len(fetcher.sra_errors) + len(fetcher.sra_d)} SRA(s) downloads:')
        cerr('\n'.join(f'{sra_id}: {msg}' for (sra_id, msg) in fetcher.sra_errors.items()))
        cerr('\n'.join(fetcher.sra_d.keys()))
    else:
        cerr(f'All {fetcher.completed} SRA(s) have been successfully downloaded.')

    # report = ena_downloader.fetch_ena(enaid_dl, args.tmpdir, fs)


def do_delete(args, fs):

    for ena_acc in args.ENAIDs:
        fs.delete(ena_acc)


def do_inventory(args, fs):

    import shutil

    cerr(f'Root DB directory: {fs.__storage_root_path__}')

    sra_list = fs.list()
    cerr(f'Total SRA number: {len(sra_list)}')

    res = shutil.disk_usage(fs.__storage_root_path__)
    cerr(f'Used space: {byte_conversion(res.used)}')
    cerr(f'Free space: {byte_conversion(res.free)}')
    cerr(f'Allocated space: {byte_conversion(res.total)}')


def iter_samplefile(samplefile):
    """ return a list of (sample, [SRAID, ...]) """

    import pandas as pd

    if ':' in samplefile:
        samplefile, column_specs = samplefile.split(':')
        sample_column, ena_column = column_specs.split(',')
    else:
        sample_column, ena_column = 'SAMPLE', 'ENA'

    if samplefile.endswith('.xls') or samplefile.endswith('.xlsx'):
        df = pd.read_excel(samplefile)
    else:
        df = pd.read_table(samplefile, sep=None, engine='python')

    for idx, row in df.iterrows():

        sample = row[sample_column]
        ena_accs = row[ena_column]
        if not ena_accs or type(ena_accs) != str:
            continue

        yield (sample, [x.strip() for x in ena_accs.split(',')])


def iter_srafile(samplefile):
    """ return a list of [[SRAID, ...], ...] from a tabulated file (columnar data
        with header) in either tab-delimited (.tsv), comma-delimited (.csv) or
        Excel (.xls or .xlsx) format
        samplefile should be in the file_path:column_name format, eg:
        my_directory/my_file.tsv:SRAID
    """

    import pandas as pd

    if ':' in samplefile:
        samplefile, sra_column = samplefile.split(':')
    else:
        sra_column = 'SRA'

    if samplefile.endswith('.xls') or samplefile.endswith('.xlsx'):
        df = pd.read_excel(samplefile)
    else:
        df = pd.read_table(samplefile, sep=None, engine='python')

    for idx, row in df.iterrows():

        sra_accs = row[sra_column]
        if not sra_accs or type(sra_accs) != str:
            continue

        yield [x.strip() for x in sra_accs.split(',')]


def get_sraids(args):

    SRAIDs = []

    if args.idfile:

        # id file should contain SRA id per line without header

        with open(args.idfile) as f:
            SRAIDs += [x.strip() for x in f.read().split()]

    if args.samplefile:
        for sra_ids in iter_srafile(args.samplefile):
            SRAIDs += sra_ids

    if any(args.SRAIDs):
        SRAIDs += args.SRAIDs

    return SRAIDs


def get_helpers(args):

    match args.site:
        case 'ena':
            from sra_repo import ena_helper
            repos = [ena_helper.ENA_Helper]

        case 'entrez':
            from sra_repo import entrez_helper
            repos = [entrez_helper.Entrez_Helper]

        case 'ena-entrez':
            from sra_repo import ena_helper, entrez_helper
            repos = [ena_helper.ENA_Helper, entrez_helper.Entrez_Helper]

        case 'entrez-ena':
            from sra_repo import ena_helper, entrez_helper
            repos = [entrez_helper.Entrez_Helper, ena_helper.ENA_Helper]

        case _:
            cexit('Please provide correct --site option')

    return repos

# EOF
