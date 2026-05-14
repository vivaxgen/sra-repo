import io
import pathlib
import shutil


from dataclasses import dataclass
from threading import Lock, Thread
from queue import Queue
from typing import Any
from rich.progress import Console

#from sra_repo import download_utils3 as download_utils
from sra_repo.filestore import SRA_Info
from sra_repo import pycurl_downloader


@dataclass
class SRA(object):

    acc_id: str
    urls: list[str]
    paths: list[str]
    md5sums: list[str]
    filesizes: list[int]
    read_count: int
    base_count: int
    info: SRA_Info
    pending: int = 0
    error: int = 0
    errmsg: str = ""
    metadata: dict | None = None

    helper: Any = None


class SRA_Fetcher(object):

    helpers = []

    def __init__(
        self,
        sraids,
        *,
        filestore,
        temp_directory,
        repos,
        showcmds=False,
        showurl=False,
        target_directory=None,
        resume=False,
        show_sra_info=False,
    ):

        self.sraids = sraids
        self.filestore = filestore
        self.temp_directory = pathlib.Path(temp_directory)
        self.showcmds = showcmds
        self.showurl = showurl
        self.show_sra_info = show_sra_info
        self.target_directory = target_directory
        self.resume = resume

        self.sra_d = {}
        self.path_d = {}
        self.errbuf = io.StringIO()
        self.completed = 0
        self.sra_errors = {}
        self.url_path_queue = Queue(3)
        self.current_count = 0

        # acquire this lock if we need to modify any of the above variables
        # to prevent race condition
        self.lock = Lock()

        # variables only modified by prepare_url() thread
        self.total = 0

        self.console = Console()

        self.helpers = [
            r(self, showcmds=showcmds, show_sra_info=self.show_sra_info) for r in repos
        ]

    def fetch(self, ntasks=1, count=-1, tries=3):

        if count > 0:
            self.sraids = self.sraids[:count]

        if ntasks > 1:
            t = self.start_url_fetcher()
        else:
            self.prepare_url()

        # perform downloads
        download_utils.download(
            iter(self.url_path_queue.get, None),
            total=self.get_total,
            ntasks=ntasks,
            before_started=self._before_started,
            after_finished=self._after_finished,
            console=self.console,
            tries=tries,
            resume=self.resume,
        )
        if ntasks > 1 and t:
            t.join()

    def fetch(self, ntasks=1, count=-1, tries=3):

        if count > 0:
            self.sraids = self.sraids[:count]

        if ntasks > 1:
            t = self.start_url_fetcher()
        else:
            self.prepare_url()

        # perform downloads
        manager = download_utils.DownloadManager(console=self.console)
        manager.download_multiple(
            iter(self.url_path_queue.get, None),
            max_workers=ntasks,
            resume=self.resume,
            before_started=self._before_started,
            after_finished=self._after_finished,
        )

    def fetch(self, ntasks=1, count=-1, tries=3):

        if count > 0:
            self.sraids = self.sraids[:count]

        if ntasks > 1:
            t = self.start_url_fetcher()
        else:
            self.prepare_url()

        # perform downloads
        manager = pycurl_downloader.DownloadManager(
            console=self.console,
            workers=ntasks,
            progress_label_factory=self._get_download_label,
        )
        manager.add_before_started_hook(self._before_started)
        manager.add_after_finished_hook(self._after_finished)
        manager.run(self.url_path_queue)

    def start_url_fetcher(self):

        t = Thread(target=self.prepare_url)
        t.start()
        return t

    def prepare_url(self):

        _c = self.console.log

        # prepare SRA instances
        for idx, sra_id in enumerate(self.sraids, 1):

            indicator = f"[{idx}/{len(self.sraids)}]"
            sra = None

            errmsgs = []
            for helper in self.helpers:
                try:
                    _c(
                        f"{indicator} Requesting information from {helper.label} for {sra_id}"
                    )
                    info = helper.get_sra_info(sra_id)
                    urls, filenames = info.urls, info.files

                    if not any(urls):
                        raise ValueError(
                            f"SRA {sra_id} does not have any files [{helper.label}]"
                        )

                    paths = [self.temp_directory / fn for fn in filenames]
                    sra = SRA(
                        acc_id=sra_id,
                        urls=urls,
                        paths=paths,
                        md5sums=info.md5sums,
                        filesizes=info.sizes,
                        read_count=info.read_count,
                        base_count=info.base_count,
                        info=info,
                        pending=len(urls),
                        helper=helper,
                    )

                    with self.lock:
                        self.sra_d[sra_id] = sra
                        for path in paths:
                            self.path_d[str(path)] = sra

                    _c(f"{indicator} Queueing {sra_id} for download")
                    for url, local_path in zip(urls, paths):
                        # self.url_path_queue.put((url, local_path))
                        self.url_path_queue.put(
                            pycurl_downloader.DownloadTask(
                                url=url, output_path=local_path
                            )
                        )
                        self.total += 1

                    break

                except ValueError as exc:
                    _c(
                        f"{indicator} Error accessing info from {helper.label} for {sra_id}"
                    )
                    self.sra_errors[sra_id] = f"WARN: {exc}"

            else:

                # for-loop is exhausted meaning we don't get SRA urls
                if errmsgs:
                    self.errbuf.write("\n".join(errmsgs))

        self.url_path_queue.put(None)

    def _get_download_label(self, task):
        with self.lock:
            self.current_count += 1
            idx = self.current_count

        sra = self.path_d[str(task.output_path)]
        return f"[{idx}/{self.total}] <{sra.helper.label}> {task.output_path.name}"

    def _before_started(self, url, localpth):
        if self.showurl:
            self.console.log(f"Start downloading: {url}")

    def _after_finished(self, url, localpath):

        _c = self.console.log
        sra = self.path_d[localpath]

        localpath = pathlib.Path(localpath)
        sra.helper.process_file(localpath, sra)

        with self.lock:
            sra.pending += -1
            # _c(f'INFO: sra.pending = {sra.pending} for SRA: {sra.acc_id}')
            if sra.pending == 0:
                if sra.error:
                    # we  found error, just return without storing files
                    _c(f"ERROR found during post-downloading {sra.acc_id}. Skipping...")
                    return

                if self.target_directory is not None:
                    # instead of storing to the fs database, just move to target dir
                    for srapath in sra.paths:
                        _c(f"Moving {srapath} to {self.target_directory}")
                        shutil.move(srapath, self.target_directory)
                else:
                    self.filestore.store(
                        sra.acc_id,
                        sra.paths,
                        sra.info,
                        use_move=True,
                    )

                self.completed += 1
                _c(
                    f"({self.completed}/{len(self.sraids)}) "
                    f"Stored {len(sra.paths)} file(s) for {sra.acc_id}"
                )

                # remove ena from sra_d
                del self.sra_d[sra.acc_id]

        self.url_path_queue.task_done()

    def get_total(self):
        return self.total

    def show_dict(self):
        print(self)
        print(self.__dict__)


# EOF
