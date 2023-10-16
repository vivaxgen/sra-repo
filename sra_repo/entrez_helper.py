
import requests
import pathlib
import subprocess
import time

import xml.etree.ElementTree as ET

from sra_repo.utils import md5sum_file
from sra_repo.filestore import SRA_Info
from sra_repo.sra_validator import validate_read_base_counts


def get_xml_entry(acc_id):

    payload = dict(db='sra',
                   id=acc_id,
                   )

    status_code = -1
    tries = 0

    while status_code != 200 and tries < 5:
        r = requests.get('https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi',
                         params=payload)
        status_code = r.status_code

        if status_code == 429:
            time.sleep(5)
            tries += 1
            continue

        if status_code != 200:
            raise ValueError(f'SRA accession not found: {acc_id} with HTTP Error {r.status_code} '
                             f'and error message of: {r.content}')

    return ET.fromstring(r.content)


class Entrez_Helper(object):

    label = 'NCBI/Entrez'

    def __init__(self, parent, showcmds=False):
        self.parent = parent
        self.showcmds = showcmds

    def get_sra_info(self, sra_id):
        """ return urls, paths, total_read_count, total_base_count """

        root = get_xml_entry(sra_id)

        sras = root.findall('.//SRAFile')

        if not any(sras):
            raise ValueError(f'ENA accession probably suppresed: {sra_id}')

        for el in sras:
            if el.attrib['cluster'] == 'public' and el.attrib['semantic_name'] == 'SRA Normalized':
                urls = el.attrib['url']
                break
        else:
            raise ValueError(f'SRA accession {sra_id} does not have public entry.')

        urls = urls.split(';')

        paths = [pathlib.Path(url).name for url in urls]
        if paths[0] != sra_id:
            raise ValueError(f'SRA accession {sra_id} has {paths[0]} file.')

        runs = root.findall('*//RUN')

        if len(runs) != 1:
            raise ValueError(
                f'{sra_id} - runs is not single item!'
            )

        curr_run = runs[0]
        if curr_run.attrib['accession'] != sra_id:
            raise ValueError(
                f'{sra_id} - accession is not identical with {curr_run.attrib["accession"]}'
            )

        # prepare metadata
        metadata = dict(
            tax_id=root.find('./EXPERIMENT_PACKAGE/SAMPLE/SAMPLE_NAME/TAXON_ID').text,
            species=root.find('./EXPERIMENT_PACKAGE/SAMPLE/SAMPLE_NAME/SCIENTIFIC_NAME').text,
            study_id=root.find(
                './EXPERIMENT_PACKAGE/STUDY/IDENTIFIERS/EXTERNAL_ID[@namespace="BioProject"]'
            ).text,
            sample_id=root.find(
                './EXPERIMENT_PACKAGE/SAMPLE/IDENTIFIERS/EXTERNAL_ID[@namespace="BioSample"]'
            ).text,
            sample=root.find('./EXPERIMENT_PACKAGE/SAMPLE/TITLE').text,
            experiment_id=root.find(
                './EXPERIMENT_PACKAGE/EXPERIMENT/IDENTIFIERS/PRIMARY_ID'
            ).text,
        )

        # create dictionary
        sra_info = SRA_Info(
            sra_id=sra_id,
            urls=urls,
            source='NCBI/Entrez',
            files=paths,
            read_count=int(curr_run.attrib['total_spots']),
            base_count=int(curr_run.attrib['total_bases']),
            md5sums=None,
            sizes=None,
            metadata=metadata,
        )

        return sra_info

    def process_file(self, path, sra):

        _c = self.parent.console.log

        _c(f'Processing {path}...')

        # run fasterq-dump
        _c(f'Converting {path} to fastq files...')
        temp_dir = path.parent.as_posix()
        cmds = ['fasterq-dump', '-O', temp_dir, '-t', temp_dir, path.as_posix()]
        run_cmds = ['srun'] + cmds
        if self.showcmds:
            _c('Running: ' + ' '.join(run_cmds))
        if subprocess.call(
            run_cmds, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        ) != 0:
            _c(f'ERR during fasterq-dump for SRA {sra.acc_id}')
            sra.error += 1
            return

        # run gzip
        _c(f'Compressing {path}')
        cmds = ['parallel', 'gzip', '-f', ':::', f'{path}_1.fastq', f'{path}_2.fastq']
        run_cmds = ['srun'] + cmds
        if self.showcmds:
            _c('Running: ' + ' '.join(run_cmds))
        if subprocess.call(
            run_cmds, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        ) != 0:
            _c(f'ERR during compressing fastq files for SRA {sra.acc_id}')
            sra.error += 1
            return

        sra.paths = [pathlib.Path(f'{path}_1.fastq.gz'), pathlib.Path(f'{path}_2.fastq.gz')]

        _c(f'Validating read and base counts for {sra.acc_id}')
        retcode = validate_read_base_counts(sra.paths,
                                            sra.read_count, sra.base_count,
                                            prefix_cmd=['srun'])
        if retcode != 0:
            _c(f'ERR during validation of read and base count for SRA {sra.acc_id}')
            sra.error += 1
            return

        _c(f'Calculating MD5 sum hashes for {sra.acc_id}')
        sra.info.files = [p.name for p in sra.paths]
        sra.info.md5sums = sra.md5sums = [md5sum_file(p) for p in sra.paths]
        sra.info.sizes = [p.stat().st_size for p in sra.paths]

        path.unlink()
        _c(f'Removed {path}')

# EOF
