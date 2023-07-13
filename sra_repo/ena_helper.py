
import requests
import pathlib
import subprocess
import time

from sra_repo.utils import md5sum_file
from sra_repo.filestore import SRA_Info


def get_ena_filereport(sra_id, query):

    payload = dict(result='read_run',
                   fields=query,
                   format='JSON',
                   accession=sra_id,
                   )

    status_code = -1
    tries = 0

    while status_code != 200 and tries < 5:
        r = requests.get('https://www.ebi.ac.uk/ena/portal/api/filereport',
                         params=payload)

        status_code = r.status_code

        if status_code == 429:
            time.sleep(5)
            tries += 1
            continue

        if status_code != 200:
            raise ValueError(f'SRA accession not found: {sra_id} with HTTP Error {r.status_code} '
                             f'and error message of: {r.content}')

    result_resp = r.json()

    if not any(result_resp):
        raise ValueError(f'SRA accession probably suppresed or not existed: {sra_id}')

    return result_resp


class ENA_Helper(object):

    label = 'EBI/ENA'

    def __init__(self, parent):
        self.parent = parent

    def get_sra_info(self, sra_id):
        """ return urls, paths, total_read_count, total_base_count """
        resp = get_ena_filereport(
            sra_id,
            'fastq_ftp,submitted_ftp,read_count,base_count,fastq_md5,fastq_bytes'
        )[0]

        for tag in ['fastq_ftp', 'submitted_ftp']:
            if tag in resp and (urls := resp[tag]):
                urls = ['ftp://' + url for url in urls.split(';')]
                files = [pathlib.Path(url).name for url in urls]
                break
        else:
            urls, files = [], []

        # create dictionary

        sra_info = SRA_Info(
            sra_id=sra_id,
            urls=urls,
            source='EBI/ENA',
            read_count=int(resp.get('read_count', -1)),
            base_count=int(resp.get('base_count', -1)),
            files=files,
            sizes=list(int(s) for s in resp['fastq_bytes'].split(';')),
            md5sums=resp['fastq_md5'].split(';')

        )

        return sra_info

    def process_file(self, path, sra):

        _c = self.parent.console.log

        _c(f'Processing {path}...')

        if path.name.endswith('fastq.gz'):
            _c(f'Verifying {path}')

            if '_' not in path.name:
                # for singleton reads, just discarded for now
                idx = sra.paths.index(path)
                del sra.paths[idx]
                sra.info.remove_file(path.name)
                return

            # with EBI/ENA repository, we will have MD5sum hash to use
            for (source_path, source_md5sum) in zip(sra.paths, sra.md5sums):
                if path == source_path:
                    md5sum = md5sum_file(source_path)
                    if md5sum != source_md5sum:
                        _c(f'Corrupt file {path}')
                        sra.error += 1
                    break
            else:
                _c(f'Can not verifying {path}')
                sra.error += 1

        elif path.name.endswith('.cram'):

            # use bcftools fastq to convert srr file to double fastq files
            try:
                fastq_filenames = [path.with_suffix('_1.fastq.gz'), path.with_suffix('_2.fastq.gz')]
                sra.paths = cram_to_fastq(path, fastq_filenames)

            except:
                _c(f'Error converting {path} to fastq files')
                sra.error += 1
                raise

            # change sra.pending to 1 to ensure that store() is executed after this call
            sra.pending = 1


def cram_to_fastq(localpath, destfiles):

    # need to run this:
    # $ samtools collate -u -f -O file_cram.cram |
    #   samtools fastq -1 read_1.fastq.gz -2 read_2.fastq.gz -0 /dev/null -s /dev/null -n

    cmds = (f"samtools collate -u -f -O {localpath} | "
            f"samtools fastq -1 {destfiles[0]} -2 {destfiles[1]} -0 /dev/null -s /dev/null -n")

    bash_cmds = ['bash', '-c', cmds]
    run_cmds = ['srun'] + bash_cmds
    if subprocess.call(
        run_cmds, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    ) != 0:
        raise ValueError('process did not finish properly')

    return destfiles


# EOF
