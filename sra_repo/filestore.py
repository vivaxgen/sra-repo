
import re
import pathlib
import shutil
import stat
import json

from dataclasses import dataclass
from flufl.lock import Lock, TimeOutError
from sra_repo.utils import cexit, cerr, check_gzip_file


re_sraid = re.compile('(\D+)(.+)')

proper_prefixes = ['ERR', 'SRR', 'SRS']


@dataclass
class SRA_Info:
    """ make validation info as a class instead of just a dictionary to
        enforce its structure
    """

    sra_id: str
    source: str
    urls: list[str]
    read_count: int
    base_count: int
    files: list[str]
    sizes: list[int] | None
    md5sums: list[str] | None
    metadata: dict[str] | None

    def _idx(self, filename):
        return self.files.index(filename)

    def get_size(self, filename):
        return self.sizes[self._idx(filename)]

    def get_md5(self, filename):
        return self.md5sums[self._idx(filename)]

    def set_size(self, filename, size):
        self.sizes[self._idx(filename)] = size

    def set_md5(self, filename, md5sum):
        self.md5sums[self._idx(filename)] = md5sum

    def set_files(self, files):
        self.files = files
        self.sizes = [-1] * len(files)
        self.md5sums = [-1] * len(files)

    def remove_file(self, filename):
        idx = self.files.index(filename)
        del self.files[idx]
        del self.sizes[idx]
        del self.md5sums[idx]

    def save(self, path):
        d = dict(
            sra_id=self.sra_id,
            source=self.source,
            urls=self.urls,
            read_count=self.read_count,
            base_count=self.base_count,
            files=self.files,
            sizes=self.sizes,
            md5sums=self.md5sums
        )
        with open(path, 'w') as f:
            json.dump(d, f)

    @classmethod
    def load(cls, path):
        with open(path) as f:
            d = json.load(f)
        return cls(**d)


class SRAFileStorage(object):
    """ SRA FASTQ file repository system

    """

    SRA_Info = SRA_Info

    dir_secure_mode = (stat.S_IRUSR | stat.S_IXUSR |
                       stat.S_IRGRP | stat.S_IXGRP |
                       stat.S_IROTH | stat.S_IXOTH)

    dir_edit_mode = (stat.S_IWUSR | stat.S_IWGRP |
                     stat.S_IRUSR | stat.S_IXUSR |
                     stat.S_IRGRP | stat.S_IXGRP |
                     stat.S_IROTH | stat.S_IXOTH)

    def __init__(self, storage_root_path: pathlib.Path):
        self.__storage_root_path__ = pathlib.Path(storage_root_path)
        if not (self.__storage_root_path__ / '.sra-repo-db').is_file():
            cexit(f'ERROR: root fs {self.__storage_root_path__} is not a SRA repo storage')

    def store(
        self,
        sra_id: str,
        fullpaths: list[pathlib.Path],
        info: SRA_Info,
        *,
        use_move: bool = False,
    ):
        fullpaths = [pathlib.Path(fullpath) for fullpath in fullpaths]

        # cheap sanity checks

        # 1 - check extension
        if not all([path.name.lower().endswith('.fastq.gz') for path in fullpaths]):
            raise ValueError('not all fullpaths have .fastq.gz extension')

        # 2 - check name
        if not all('_' in path.name for path in fullpaths):
            raise ValueError('not all fullpaths have proper paired fastq filename')

        # 3 - crosscheck again for file sizes
        for p in fullpaths:
            if p.stat().st_size != info.get_size(p.name):
                raise ValueError('file {p} has different file size from SRA info')

        store_dir = self.get_dirpath(sra_id)
        store_dir.mkdir(parents=True, exist_ok=True)
        sra_lock = Lock(self.get_lockfile(store_dir), default_timeout=5)

        try:
            store_dir.chmod(self.dir_edit_mode)
            with sra_lock:

                # save the fastq files
                for path in fullpaths:
                    self.store_fastq(path, store_dir=store_dir, use_move=use_move)

                # save the read and base counts
                self.__store_validation_info(
                    store_dir,
                    info
                )

        except TimeOutError:
            raise ValueError(f'timeout lock error for SRA {sra_id}')

        finally:
            store_dir.chmod(self.dir_secure_mode)

    def store_fastq(
        self,
        fullpath: pathlib.Path,
        store_dir: pathlib.Path,
        *,
        use_move: bool = False,
    ):
        filename = fullpath.name

        dest_file = store_dir / filename
        unlink_if_exists(dest_file)

        if use_move:
            shutil.move(fullpath, store_dir)
        else:
            shutil.copy2(fullpath, store_dir)
        dest_file.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

        # excerpt code for changing mode
        # filename = "path/to/file"
        # mode = os.stat(filename).st_mode
        # ro_mask = 0o777 ^ (stat.S_IWRITE | stat.S_IWGRP | stat.S_IWOTH)
        # os.chmod(filename, mode & ro_mask)

    def __store_validation_info(
        self,
        store_dir: pathlib.Path,
        info: SRA_Info,
    ):
        """validation info

        we store validation info as a dictionary of:
         - total read count
         - total base count
         - filesizes and md5sum of all fastq files
        """

        info_file = store_dir / 'info.json'
        unlink_if_exists(info_file)
        info.save(info_file)
        info_file.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

    def store_validation_info(
        self,
        sra_id: str,
        info: SRA_Info,
    ):

        store_dir = self.get_dirpath(sra_id)
        sra_lock = Lock(self.get_lockfile(store_dir), default_timeout=5)

        try:
            store_dir.chmod(self.dir_edit_mode)
            with sra_lock:
                self.__store_validation_info(store_dir, info)

        finally:
            store_dir.chmod(self.dir_secure_mode)

    def get_validation_info(self, sra_id: str):
        store_dir = self.get_dirpath(sra_id)
        info_file = store_dir / 'info.json'
        return SRA_Info.load(info_file)

    def link(
        self,
        sraid: str,
        outdir: str | pathlib.Path,
        dryrun: bool = False,
        flat: bool = True,
    ):
        """ create a symbolic link from source fastq file to outdir, return the path(s)
            to all fastq read files """

        store_dir = self.get_dirpath(sraid)
        if not self.check(store_dir=store_dir):
            raise ValueError(f'ERR: store dir {store_dir} does not exist!')

        files = []

        if flat:
            # create a symlink of files in outdir
            for a_file in store_dir.iterdir():
                # a_file is in absolute path
                if not a_file.name.endswith('.fastq.gz'):
                    continue
                target_link = outdir / a_file.parts[-1]
                if not dryrun:
                    target_link.symlink_to(a_file)
                else:
                    cerr(f' {target_link} -> {a_file}')
                files.append(a_file.parts[-1])

        else:
            # create a symlink of ena dir in outdir
            target_link = outdir / sraid
            if not dryrun:
                target_link.symlink_to(store_dir)
            else:
                cerr(f' {target_link} -> {store_dir}')
            for a_file in target_link.iterdir():
                files.append()

        return files

    def list(self, pattern: str | None = None):
        """ provide unsorted list of all available SRA IDs """

        sra_ids = []

        # walk across 1st layer
        for dir_1 in self.__storage_root_path__.iterdir():

            if dir_1.name == '.sra-repo-db':
                continue

            # walk across 2nd layer
            for dir_2 in dir_1.iterdir():

                for sra_id in dir_2.iterdir():
                    sra_ids.append(sra_id)

        if not pattern:
            return sra_ids

        # process pattern here
        return sra_ids

    def delete(self, sra_id: str):
        store_dir = self.get_dirpath(sra_id)
        if not store_dir.is_dir():
            raise ValueError(f'SRA ID: {sra_id} does not exist in DB')
        shutil.rmdir(store_dir)

    def get_dirpath(self, sra_id: str, check: bool = False):
        """ return a Path """

        prefix, suffix = re_sraid.match(sra_id).groups()
        if prefix not in proper_prefixes:
            cexit(f'ERR: prefix {prefix} is not recognized.')

        dir_1 = suffix[:2]
        dir_2 = suffix[2:4]

        path = self.__storage_root_path__ / dir_1 / dir_2 / sra_id
        if check:
            if not path.is_dir():
                raise ValueError(f'SRA {sra_id} does not exist!')
        return path

    def get_read_files(self, sra_id: str):
        sra_dir = self.get_dirpath(sra_id, check=True)
        return [a_file for a_file in sra_dir.iterdir() if a_file.name.endswith('.fastq.gz')]

    def check(self,
              *,
              sra_id: str | None = None,
              store_dir: str | None = None,
              verify: bool = False,
              throw_exc: bool = True
    ):

        if sra_id:
            store_dir = self.get_dirpath(sra_id)

        if store_dir:
            try:
                if not store_dir.is_dir():
                    raise ValueError(f'SRA {sra_id} does not exist!')
                if len(list(store_dir.iterdir())) == 0:
                    return ValueError(f'SRA {sra_id} does not have any files!')
                if verify:
                    for a_file in store_dir.iterdir():
                        cerr(f' - verifying {a_file}')
                        if not check_gzip_file(a_file):
                            raise ValueError(f'SRA {sra_id} with file {a_file} is not verified!')
            except:
                if throw_exc:
                    raise
                return False

            return True

        return False

    def get_lockfile(self, store_dir: str | pathlib.Path):
        return (self.__storage_root_path__ / '.lock' / store_dir.name).as_posix()


def unlink_if_exists(path: pathlib.Path):
    if path.is_file():
        # this file exists, need to remove it first
        path.chmod(stat.S_IWUSR | stat.S_IWGRP)
        path.unlink()

# EOF
