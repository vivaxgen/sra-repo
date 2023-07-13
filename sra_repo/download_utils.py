
"""
A rudimentary URL downloader (like wget or curl) to demonstrate Rich progress bars.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Callable, Any
import pycurl
import errno


from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

block_size = 128 * 1024
fatal_error = False


class EasyCURL(object):

    def __init__(self, progress, proxy=None):
        self.proxy = proxy
        self.curl = None
        self.resume_from = 0
        self.downloaded = -1
        self.total_size = 0
        self.console = None
        self.taskid = None
        self.progress = progress

    def _progress_monitor(self, download_t, download_d, upload_t, upload_d):
        # download_t = total for this session (after resume)
        # download_d = current downloaded for this session
        # cerr(f'dl progress: {download_t}, {download_d}')
        if self.total_size == 0 and download_t > 0 and download_d == 0:
            self.total_size = download_t
            self.progress.start_task(self.task_id)
            self.progress.update(self.task_id, total=self.total_size)
        self.downloaded = download_d + self.resume_from
        if self.total_size > 0:
            self.progress.update(self.task_id, completed=self.downloaded)
            # cerr(f'Progress: {self.downloaded/self.total_size} {self.downloaded} {self.total_size}')
            if self.downloaded == self.total_size:
                self.progress.update(self.task_id, visible=False)

    def download(
        self, url, target_path, resume=False, progress_func=None,
        before_started=False,
        after_finished=False,
        tries=3,
    ):
        global fatal_error

        _c = self.progress.console.log

        completed = False
        while tries > 0 and not fatal_error and not completed:

            if progress_func:
                self.task_id = self.progress.add_task(
                    "download",
                    filename=progress_func() if callable(progress_func) else progress_func,
                    start=False)
                self.progress.update(self.task_id, total=0)

            if before_started:
                before_started(url, target_path)

            try:
                tries -= 1
                completed = self._download(url, target_path, resume)

            # handling error
            except pycurl.error as err:
                eno, msg = err.args
                if eno == pycurl.E_WRITE_ERROR:
                    fatal_error = True
                _c(f'ERROR downloading {url}!. Error is {type(err)} with msg: {str(err)} '
                   f'{"Aborting..." if tries == 0 else "Retrying..."}')
                continue

            except OSError as err:
                # catch OS errors
                if err.errno == errno.ENOSPC:
                    fatal_error = True
                    _c('FATAL ERROR: not enough disk space. Aborting...')
                    break

            except Exception as err:
                # catch all errors
                resume = True
                _c(f'ERROR downloading {url}!. Error is {type(err)} with msg: {str(err)} '
                   f'{"Aborting..." if tries == 0 else "Retrying..."}')
                continue

            finally:
                if self.task_id and self.progress:
                    self.progress.remove_task(self.task_id)
                    self.task_id = None

            # the following will be executed only if download process completed successfully
            if completed and after_finished:
                after_finished(url, target_path)

    def _download(
        self, url, target_path, resume=False,
    ):

        _c = self.progress.console.log

        while self.downloaded < self.total_size:

            # reset counter
            self.downloaded = -1
            self.total_size = 0

            # check if file is already exists:
            mode = 'wb'
            if resume:
                if target_path.is_file():
                    self.resume_from = target_path.stat().st_size
                    _c(f'Started at: {self.resume_from}')
                    mode = 'ab'

            # set resume for persistent download
            resume = True

            with open(target_path, mode) as dest_file:

                self.curl = c = pycurl.Curl()
                c.setopt(c.URL, url)

                if self.resume_from > 0:
                    c.setopt(c.RESUME_FROM, self.resume_from)
                c.setopt(c.WRITEDATA, dest_file)

                # display progress
                c.setopt(c.NOPROGRESS, False)
                c.setopt(c.XFERINFOFUNCTION, self._progress_monitor)

                # perform download
                c.perform()
                c.close()

            # cerr(f'self.downloaded: {self.downloaded}, self.total_size: {self.total_size}')
        return True


def download(url_dest_paths: Iterable[tuple[str, str]],
             total: int | Callable = -1,
             ntasks: int = 4,
             before_started: Callable[[str, Any, Any], None] | None = None,
             after_finished: Callable[[str, Any, Any], None] | None = None,
             console: Any = None):
    """ Download multiple urls to the given destination paths (including filenames),
        and for each finished download, execute after_finsihed function.
    """
    global fatal_error

    progress = Progress(
        TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
        console=console,
        refresh_per_second=2,
    )

    _c = progress.console.log

    if ntasks == 1:
        with progress:
            for idx, (url, dest_path) in enumerate(url_dest_paths, 1):
                if fatal_error:
                    break

                actual_total = total() if callable(total) else total

                ec = EasyCURL(progress=progress)
                ec.download(url, dest_path, False,
                            f'[{idx}/{actual_total}] {dest_path.name}',
                            before_started, after_finished)

        _c('All files has been downloaded')
        return

    with progress:
        with ThreadPoolExecutor(max_workers=ntasks) as pool:
            futures = []
            for idx, (url, dest_path) in enumerate(url_dest_paths, 1):
                if fatal_error:
                    break

                def label(idx=idx, total=total, filename=dest_path.name):
                    return f'[{idx}/{total() if callable(total) else total}] {filename}'

                ec = EasyCURL(progress=progress)
                futures.append(
                    pool.submit(ec.download, url, dest_path, False,
                                label,
                                before_started, after_finished)
                )
                time.sleep(1)

            # catch all exceptions here
            for future in as_completed(futures):
                # get the result
                future.result()

        _c('All files has been downloaded')

# EOF
