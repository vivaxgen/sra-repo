"""
A rudimentary URL downloader (like wget or curl) to demonstrate Rich progress bars.
"""

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Callable, Any
from urllib.parse import urlparse
import pycurl
import errno
import random


from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

block_size = 128 * 1024


def get_protocol(url):
    parsed_url = urlparse(url)  # Parse the URL
    return parsed_url.scheme.lower()  # Extract the protocol (scheme)


class DownloadMonitor:
    """A thread-safe object to signal a fatal error across all workers."""

    def __init__(self):
        self.fatal_error_event = threading.Event()

    def signal_fatal_error(self):
        """Set the event to stop all other running downloads."""
        self.fatal_error_event.set()

    def is_fatal_error(self):
        """Check if a fatal error has occurred."""
        return self.fatal_error_event.is_set()


# Remove the global declaration:
# fatal_error = False # <-- REMOVE THIS GLOBAL


class EasyCURL(object):

    def __init__(self, progress, monitor, proxy=None):
        self.proxy = proxy
        self.curl = None
        self.url = None
        self.resume_from = 0
        self.downloaded = 0  # -1
        self.total_size = 0
        self.monitor = monitor
        self.console = None
        self.task_id = None
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

    def _progress_monitor(self, download_t, download_d, upload_t, upload_d):
        # download_t = total for this session (after resume)
        # download_d = current downloaded for this session

        # Only start the task once we have a valid total size
        if (
            self.task_id
            and self.total_size > 0
            and not self.progress.tasks[self.task_id].started
        ):
            self.progress.start_task(self.task_id)
            self.progress.update(self.task_id, total=self.total_size)

        # The total downloaded is the downloaded in this session PLUS what we resumed from.
        self.downloaded = download_d + self.resume_from

        if self.task_id and self.total_size > 0:
            self.progress.update(
                self.task_id, completed=min(self.downloaded, self.total_size)
            )

            # Use min() in case of server misreporting size.
            if self.downloaded >= self.total_size:
                self.progress.update(self.task_id, visible=False)

    def _progress_monitor(self, download_t, download_d, upload_t, upload_d):

        # 1. Determine absolute total size (if not set)
        if self.task_id and self.total_size == 0 and self.curl:
            try:
                # Get the absolute total size for the file
                absolute_total_size = self.curl.getinfo(pycurl.TOTAL_SIZE_DOWNLOAD_T)
                if absolute_total_size > 0:
                    self.total_size = absolute_total_size
                    self.progress.update(self.task_id, total=self.total_size)
                else:
                    self.total_size = download_t
            except:
                # If curl.getinfo fails (transfer not yet initialized), proceed
                self.total_size = download_t

        # 2. Start task if total size is now known
        if (
            self.task_id
            and self.total_size > 0
            and not self.progress.tasks[self.task_id].started
        ):
            self.progress.start_task(self.task_id)

        # 3. Update completed bytes (absolute)
        self.downloaded = download_d + self.resume_from

        if self.task_id and self.total_size >= 0:  # Check >= 0 to handle unknown size
            self.progress.update(
                self.task_id,
                completed=min(self.downloaded, self.total_size or self.downloaded + 1),
            )

            if self.total_size > 0 and self.downloaded >= self.total_size:
                self.progress.update(self.task_id, visible=False)

    def _progress_monitor(self, download_t, download_d, upload_t, upload_d):

        # 1. Determine absolute total size (if not set) using pycurl info
        if self.task_id and self.total_size <= 0 and self.curl:
            try:
                # Use pycurl's transfer info to get the absolute total size
                absolute_total_size = self.curl.getinfo(pycurl.TOTAL_SIZE_DOWNLOAD_T)

                # We need to handle the case where the server reports 0 for unknown size
                if absolute_total_size > 0:
                    self.total_size = absolute_total_size
                    self.progress.update(self.task_id, total=self.total_size)

                else:
                    self.total_size = download_t

            except pycurl.error:
                # Occurs if transfer hasn't started yet. Ignore.
                self.total_size = download_t

            self.progress.console.log(
                f"total_size set to {self.total_size} for url = {self.url}"
            )

        # 2. Start task if total size is now known
        if (
            self.task_id
            and self.total_size > 0
            and not self.progress.tasks[self.task_id].started
        ):
            self.progress.start_task(self.task_id)

        # 3. Update completed bytes (absolute)
        self.downloaded = download_d + self.resume_from

        # If total_size is 0 (unknown size), set total to a slightly larger value than downloaded
        # to ensure the bar fills up to 100% when complete, but doesn't fill instantly.
        task_total = self.total_size if self.total_size > 0 else self.downloaded + 1

        if self.task_id:
            # Update the progress bar, ensuring completed is not greater than the total
            self.progress.update(
                self.task_id, completed=min(self.downloaded, task_total)
            )

            # Hide the task when done
            if self.total_size > 0 and self.downloaded >= self.total_size:
                self.progress.update(self.task_id, visible=False)
            # If size is unknown (total_size == 0) and we're done (e.g., connection closes),
            # this update will be handled implicitly by the success/failure return of _download.

    def _progress_monitor(self, download_t, download_d, upload_t, upload_d):

        task_obj = None

        # --- CORRECT MODIFICATION START ---
        # 1. Safely retrieve the task object to handle race conditions (IndexError/KeyError)
        try:
            # We must use self.progress.tasks[self.task_id] to access the task,
            # and catch the exception if another thread removed it.
            task_obj = self.progress.tasks[self.task_id]
        except (KeyError, IndexError):
            # The task has been removed by another thread (likely in the 'finally' block),
            # so we exit gracefully.
            return
        # --- CORRECT MODIFICATION END ---

        # 1. Determine absolute total size (self.total_size)
        # This logic is triggered ONLY ONCE when the first progress callback occurs.
        if self.task_id and self.total_size <= 0 and self.curl and download_t > 0:

            # total_t is the REMAINING size for this session (for both protocols when resuming).
            # Absolute Total Size = Remaining Size (download_t) + Bytes Already Downloaded (self.resume_from)
            self.total_size = int(download_t + self.resume_from)

            # Start the task and set the total
            self.progress.update(self.task_id, total=self.total_size)

            # Log the successful size determination
            # cerr(f"Size determined: {self.total_size} (DL_T: {download_t}, Resume: {self.resume_from})")

        # 2. Start task if total size is now known
        if not task_obj.started and self.total_size > 0:
            self.progress.start_task(self.task_id)

        # 3. Update completed bytes (absolute)
        # download_d is the downloaded amount in the CURRENT session.
        self.downloaded = download_d + self.resume_from

        # If total_size is 0 (unknown size), handle it by setting a max value.
        task_total = (
            self.total_size if self.total_size > 0 else max(self.downloaded + 1, 1)
        )

        if self.task_id:
            # Update the progress bar, ensuring completed is not greater than the total
            self.progress.update(
                self.task_id, completed=min(self.downloaded, task_total)
            )

            # Hide the task when done
            if self.total_size > 0 and self.downloaded >= self.total_size:
                self.progress.update(self.task_id, visible=False)

    def download(
        self,
        url,
        target_path,
        resume=False,
        progress_func=None,
        before_started=False,
        after_finished=False,
        tries=3,
    ):
        global fatal_error

        _c = self.progress.console.log
        self.url = url

        completed = False
        while (tries < 0 or tries > 0) and not fatal_error and not completed:

            if progress_func:
                self.task_id = self.progress.add_task(
                    "download",
                    filename=(
                        progress_func() if callable(progress_func) else progress_func
                    ),
                    start=False,
                )
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
                    _c("FATAL ERROR: cannot write to disk. Aborting...")
                    break
                _c(
                    f"ERROR downloading {url}!. Error is {type(err)} with msg: {str(err)} "
                    + (f"Aborting..." if tries == 0 else f"Retrying [{tries} more]...")
                )

            except OSError as err:
                # catch OS errors
                if err.errno == errno.ENOSPC:
                    fatal_error = True
                    _c("FATAL ERROR: not enough disk space. Aborting...")
                    break

            except Exception as err:
                # catch all errors
                resume = True
                _c(
                    f"ERROR downloading {url}!. Error is {type(err)} with msg: {str(err)} "
                    + (f"Aborting..." if tries == 0 else f"Retrying [{tries} more]...")
                )

            finally:
                if self.task_id and self.progress:
                    self.progress.remove_task(self.task_id)
                    self.task_id = None

            # the following will be executed only if download process completed successfully
            if completed and after_finished:
                after_finished(url, target_path)
                break

            if tries < 0 or tries > 0:
                delay = 2 + random.random() * 5
                _c(f"sleeping for {delay:1.2f} seconds before retrying {url}")
                time.sleep(delay)

    def download(
        self,
        url,
        target_path,
        resume=False,
        progress_func=None,
        before_started=False,
        after_finished=False,
        tries=3,
    ):

        _c = self.progress.console.log
        completed = False

        # Check for global fatal error at the start
        while (
            (tries < 0 or tries > 0)
            and not self.monitor.is_fatal_error()
            and not completed
        ):  # <-- Use self.monitor

            # Progress bar management is tricky during retries.
            # We create/update the task *before* the first try, or update it if retrying.
            if progress_func:
                if self.task_id is None:
                    # Create task for the first time
                    self.task_id = self.progress.add_task(
                        "download",
                        filename=(
                            progress_func()
                            if callable(progress_func)
                            else progress_func
                        ),
                        start=False,
                    )
                # Update filename/label for retries, and ensure total is reset/0
                self.progress.update(
                    self.task_id,
                    filename=(
                        progress_func() if callable(progress_func) else progress_func
                    ),
                    total=0,
                )

            if before_started:
                before_started(url, target_path)

            try:
                tries -= 1

                # OPTIONAL: Add a "Retrying" message to the progress bar filename
                if tries > 0:
                    _c(f"Attempting download for {url}. Tries remaining: {tries}")

                completed = self._download(url, target_path, resume)

            # handling error
            except pycurl.error as err:
                eno, msg = err.args
                if eno == pycurl.E_WRITE_ERROR:
                    self.monitor.signal_fatal_error()  # <-- Use self.monitor
                    _c("FATAL ERROR: cannot write to disk. Aborting all tasks...")
                    break
                _c(
                    f"ERROR downloading {url}!. Error is {type(err)} with msg: {str(err)} "
                    + (f"Aborting..." if tries == 0 else f"Retrying [{tries} more]...")
                )

                # Check for fatal signal after logging the error
                if self.monitor.is_fatal_error():
                    break

            except OSError as err:
                if err.errno == errno.ENOSPC:
                    self.monitor.signal_fatal_error()  # <-- Use self.monitor
                    _c("FATAL ERROR: not enough disk space. Aborting all tasks...")
                    break

                # Generic OSError handling for retries
                _c(
                    f"OS ERROR downloading {url}!. Error is {type(err)} with msg: {str(err)} "
                    + (f"Aborting..." if tries == 0 else f"Retrying [{tries} more]...")
                )

            except Exception as err:
                # catch all errors
                resume = (
                    True  # Keep resume on general error to pick up partial download
                )
                _c(
                    f"ERROR downloading {url}!. Error is {type(err)} with msg: {str(err)} "
                    + (f"Aborting..." if tries == 0 else f"Retrying [{tries} more]...")
                )

            finally:
                # Remove task ONLY if completed OR if a fatal error occurred
                if completed or self.monitor.is_fatal_error():
                    if self.task_id and self.progress:
                        self.progress.remove_task(self.task_id)
                        self.task_id = None

            # The following will be executed only if download process completed successfully
            if completed and after_finished:
                after_finished(url, target_path)
                break

            if tries < 0 or tries > 0 and not self.monitor.is_fatal_error():
                delay = 2 + random.random() * 5
                _c(f"sleeping for {delay:1.2f} seconds before retrying {url}")
                time.sleep(delay)

    def _download(
        self,
        url,
        target_path,
        resume=False,
    ):

        _c = self.progress.console.log

        while self.downloaded < self.total_size:

            # reset counter
            self.downloaded = -1
            self.total_size = 0

            # check if file is already exists:
            mode = "wb"
            if resume:
                if target_path.is_file():
                    self.resume_from = target_path.stat().st_size
                    _c(f"Started at: {self.resume_from}")
                    mode = "ab"

            # set resume for persistent download
            resume = True

            with open(target_path, mode) as dest_file:

                self.curl = c = pycurl.Curl()
                c.setopt(c.URL, url)

                if get_protocol(url) == "ftp":
                    c.setopt(c.FTP_USE_EPSV, 0)  # Disable passive mode, use active mode

                if self.resume_from > 0:
                    c.setopt(c.RESUME_FROM, self.resume_from)
                c.setopt(c.WRITEDATA, dest_file)

                # display progress
                c.setopt(c.NOPROGRESS, False)
                c.setopt(c.XFERINFOFUNCTION, self._progress_monitor)

                # perform download
                _c(f"Connecting to {url}...")
                try:
                    c.perform()
                except:
                    raise
                finally:
                    c.close()
                    self.curl = None

        _c(f"Downloaded: {self.downloaded} out of: {self.total_size} for {url}")
        if self.total_size == 0:
            return False
        return True

    def _download(
        self,
        url,
        target_path,
        resume=False,
    ):

        _c = self.progress.console.log
        protocol = get_protocol(url)  # <-- Get protocol early

        # 1. Reset state for a new connection attempt
        self.resume_from = 0
        self.downloaded = 0
        self.total_size = 0  # Reset, will be set by the header function

        # 2. Check for resume status and set file mode/resume_from
        mode = "wb"
        if resume:
            if target_path.is_file():
                self.resume_from = target_path.stat().st_size
                _c(f"Resuming download from: {self.resume_from} bytes")
                mode = "ab"

        with open(target_path, mode) as dest_file:

            # 3. Setup Curl handle
            self.curl = c = pycurl.Curl()
            c.setopt(c.URL, url)

            if protocol in ("ftp", "ftps"):
                c.setopt(c.FTP_USE_EPSV, 0)  # Disable passive mode
                # For FTP, libcurl should use the SIZE command, which sets download_t > 0

            if self.resume_from > 0:
                c.setopt(c.RESUME_FROM, self.resume_from)
            c.setopt(c.WRITEDATA, dest_file)

            # display progress
            c.setopt(c.NOPROGRESS, False)
            c.setopt(c.XFERINFOFUNCTION, self._progress_monitor)

            # Allow pycurl to follow redirects
            c.setopt(c.FOLLOWLOCATION, True)

            # Set a connection timeout to prevent indefinite hangs
            c.setopt(c.CONNECTTIMEOUT, 30)

            # 4. Perform download
            _c(f"Connecting to {url}...")
            try:
                c.perform()

                # FINAL SIZE CHECK: For FTP (and as a fallback for HTTP),
                # use getinfo() after the transfer is complete or after a successful connection.
                # Since the size is communicated during the handshake, we must ensure the
                # total_size is correctly picked up by the progress monitor (next step).

                # Check for successful download completion
                if self.downloaded < self.total_size:
                    # If total_size was set and we didn't reach it, it was a partial download (error)
                    _c(
                        f"Incomplete download for {url}. Total size: {self.total_size}, downloaded: {self.downloaded}"
                    )
                    return False

            except:
                raise  # Re-raise error for the outer 'download' method to catch
            finally:
                c.close()
                self.curl = None  # <-- Clean up self.curl

        # 5. Final check and return
        if self.total_size == 0 and self.downloaded > 0:
            # Handle cases where Content-Length is not provided (e.g., streaming)
            # and the download completed.
            _c(
                f"Download complete (size unknown) for {url}. Downloaded: {self.downloaded}"
            )
            return True

        elif self.downloaded == self.total_size and self.total_size > 0:
            _c(
                f"Download complete: {self.downloaded} out of: {self.total_size} for {url}"
            )
            return True

        return False  # Should only be reached on failure or incomplete state


def download(
    url_dest_paths: Iterable[tuple[str, str]],
    total: int | Callable = -1,
    ntasks: int = 4,
    before_started: Callable[[str, Any, Any], None] | None = None,
    after_finished: Callable[[str, Any, Any], None] | None = None,
    console: Any = None,
    tries: int = 3,
):
    """Download multiple urls to the given destination paths (including filenames),
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
                ec.download(
                    url,
                    dest_path,
                    False,
                    f"[{idx}/{actual_total}] {dest_path.name}",
                    before_started,
                    after_finished,
                    tries,
                )

        _c("All files has been downloaded")
        return

    with progress:
        with ThreadPoolExecutor(max_workers=ntasks) as pool:
            futures = []
            for idx, (url, dest_path) in enumerate(url_dest_paths, 1):
                if fatal_error:
                    break

                def label(idx=idx, total=total, filename=dest_path.name):
                    return f"[{idx}/{total() if callable(total) else total}] {filename}"

                ec = EasyCURL(progress=progress)
                futures.append(
                    pool.submit(
                        ec.download,
                        url,
                        dest_path,
                        False,
                        label,
                        before_started,
                        after_finished,
                        tries,
                    )
                )
                time.sleep(2)

            # catch all exceptions here
            for future in as_completed(futures):
                # get the result
                future.result()

        _c("All files has been processed.")


def download(
    url_dest_paths: Iterable[tuple[str, str]],
    total: int | Callable = -1,
    ntasks: int = 4,
    before_started: Callable[[str, Any, Any], None] | None = None,
    after_finished: Callable[[str, Any, Any], None] | None = None,
    console: Any = None,
    tries: int = 3,
):
    """Download multiple urls to the given destination paths (including filenames),
    and for each finished download, execute after_finsihed function.
    """

    # NO GLOBAL: Remove 'global fatal_error'

    monitor = DownloadMonitor()  # <-- NEW Monitor instance

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
                if monitor.is_fatal_error():  # <-- Use monitor
                    break

                actual_total = total() if callable(total) else total

                ec = EasyCURL(progress=progress, monitor=monitor)  # <-- Pass monitor
                ec.download(
                    url,
                    dest_path,
                    False,
                    f"[{idx}/{actual_total}] {dest_path.name}",
                    before_started,
                    after_finished,
                    tries,
                )

        _c(
            "All files have been downloaded"
            if not monitor.is_fatal_error()
            else "Aborted due to fatal error."
        )
        return

    # Multithreaded section
    with progress:
        with ThreadPoolExecutor(max_workers=ntasks) as pool:
            futures = []
            for idx, (url, dest_path) in enumerate(url_dest_paths, 1):
                if monitor.is_fatal_error():  # <-- Use monitor
                    break

                def label(idx=idx, total=total, filename=dest_path.name):
                    return f"[{idx}/{total() if callable(total) else total}] {filename}"

                ec = EasyCURL(progress=progress, monitor=monitor)  # <-- Pass monitor
                futures.append(
                    pool.submit(
                        ec.download,
                        url,
                        dest_path,
                        False,
                        label,
                        before_started,
                        after_finished,
                        tries,
                    )
                )
                # REMOVE: time.sleep(2) # <-- REMOVED UNNECESSARY BLOCKING

            # Catch all exceptions here
            for future in as_completed(futures):
                try:
                    # get the result (which is None, but ensures exceptions are propagated)
                    future.result()
                except Exception as e:
                    # Log unexpected exceptions not caught by EasyCURL.download
                    _c(f"An unexpected error occurred in a thread: {e}")
                    # If an unexpected exception happens, we might signal a fatal error too
                    monitor.signal_fatal_error()

        _c(
            "All files have been processed."
            if not monitor.is_fatal_error()
            else "Aborted due to fatal error."
        )


# EOF
