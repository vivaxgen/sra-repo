
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
import time
import threading
import subprocess

from sra_repo.utils import cerr, md5sum_file

"""
Entrez XML attributes

{'accession': 'ERR2891930',
 'alias': 'SC_RUN_26267_4#16',
 'total_spots': '17548078',
 'total_bases': '5299519556',
 'size': '2211270698',
 'load_done': 'true',
 'published': '2018-11-14 09:24:45',
 'is_public': 'true',
 'cluster_name': 'public',
 'has_taxanalysis': '1',
 'static_data_available': '1'}


ENA JSON Filereport

[
{"run_accession":"ERR9907925","read_count":"38566330","base_count":"5823515830"}
]
"""


class SRA_Validator(object):

    def __init__(self, sraids, fs, *, validate=False, helpers=[], showcmds=False):
        self.sraids = sraids
        self.fs = fs
        self.validate_flag = validate
        self.showcmds_flag = showcmds
        self.helpers = [class_(self) for class_ in helpers]
        self.err_sraids = []
        self.finished = 0
        self._lock = threading.Lock()

    def validate(self, threads=4):

        if threads == 1:
            for idx, sra_id in enumerate(self.sraids):
                self._validate(sra_id, idx)
            return

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = []
            for idx, sra_id in enumerate(self.sraids, 1):
                futures.append(
                    executor.submit(
                        self._validate,
                        sra_id,
                        idx,
                    )
                )
                time.sleep(0.75)

            # finished up all futures
            for future in as_completed(futures):
                # get the result
                future.result()

    def _validate(self, sra_id, idx):

        try:

            # check whether it is in database
            self.fs.check(sra_id=sra_id, verify=False, throw_exc=True)

            # check if we want to do full validation
            if not self.validate_flag:
                return

            cerr(f'[{idx}/{len(self.sraids)}] - loading information for {sra_id}')

            read_files = self.fs.get_read_files(sra_id)

            need_revalidation = False
            try:
                info = self.fs.get_validation_info(sra_id)
                self.validate_filesize(sra_id, read_files, info)
                self.validate_md5sum(sra_id, read_files, info)
                cerr(f'[{idx}/{len(self.sraids)}] - MD5sum matched for {sra_id}')

            except NotImplementedError:

                raise

            except FileNotFoundError as err:

                if err.filename.endswith('.json'):
                    # no json file exists yet, so we need to perform revalidation

                    need_revalidation = True

                else:
                    raise

            if need_revalidation:
                cerr(f'[{idx}/{len(self.sraids)}] - revalidating for {sra_id}')
                info = self.revalidate(sra_id, read_files)
                self.fs.store_validation_info(sra_id, info)
                cerr(f'[{idx}/{len(self.sraids)}] - info file stored for {sra_id}')

        except ValueError as err:

            self.err_sraids.append(
                f'{sra_id} - {str(err)}'
            )

        finally:

            finished = 0
            with self._lock:
                self.finished += 1
                finished = self.finished

            cerr(f'[{finished}/{len(self.sraids)}] - finished validating')

    def validate_md5sum(self, sra_id, read_files, info):

        for read_file in read_files:
            if md5sum_file(read_file) != info.get_md5(read_file.name):
                raise ValueError(
                    f'{sra_id} - validation error, mismatched md5sum for {read_file.name}'
                )

    def validate_filesize(self, sra_id, read_files, info):

        for read_file in read_files:
            if read_file.stat().st_size != info.get_size(read_file.name):
                raise ValueError(
                    f'{sra_id} - validation error, mismatched file size for {read_file.name}'
                )

    def revalidate(self, sra_id, read_files):
        """ perform validation first """

        # we try validating with ENA first (since it has size & md5sum of FASTQ files)
        # and then use Entrez if ENA failed

        info = None
        err_msg = []

        for func in [self.revalidate_with_ENA, self.revalidate_with_Entrez]:

            try:
                info = func(sra_id, read_files)
                break

            except ValueError as err:
                err_msg.append(f'Error validation for {sra_id} with error msg: {err}')

        if not info:
            raise ValueError(f'Validation with ENA and Entrez failed for {sra_id} '
                             f'with following messages:\n' + '\n'.join(err_msg))

        return info

    def revalidate_with_ENA(self, sra_id, read_files):

        from sra_repo.ena_helper import ENA_Helper

        info = ENA_Helper(None).get_sra_info(sra_id)

        # check with size first
        # cerr('check file size')
        self.validate_filesize(sra_id, read_files, info)

        # check with MD5sums
        # cerr('check MD5sum')
        self.validate_md5sum(sra_id, read_files, info)

        return info

    def revalidate_with_Entrez(self, sra_id, read_files):

        from sra_repo.entrez_helper import Entrez_Helper

        info = Entrez_Helper(None).get_sra_info(sra_id)

        # check for total read and base counts

        retcode = validate_read_base_counts(read_files,
                                            info.read_count,
                                            info.base_count,
                                            prefix_cmd=['srun'])

        if retcode != 0:
            raise ValueError('read and base counts of {sra_id} do not match')

        info.files = [p.name for p in read_files]
        info.md5sums = [md5sum_file(p) for p in read_files]
        info.sizes = [p.stat().st_size for p in read_files]

        return info


def validate_read_base_counts(read_files, read_count, base_count, prefix_cmd=[], showcmds=False):
    cmds = prefix_cmd + ['sra-validator.py',
                         '--bases', str(base_count),
                         '--reads', str(read_count)
                         ] + read_files

    if showcmds:
        cerr(f' - will run: {" ".join(cmds)}')
    return subprocess.call(
        cmds, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )


# EOF
