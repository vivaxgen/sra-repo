from __future__ import annotations

import os
import queue
import random
import threading
import time
import errno
from concurrent.futures import ThreadPoolExecutor, Future, wait, FIRST_EXCEPTION
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Iterator, Optional, Union, Any

import pycurl
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

Hook = Callable[[str, str], None]
LabelFactory = Callable[["DownloadTask"], str]

_REMOTE_SIZE_CACHE_KEY = "_pycurl_remote_size"
_REMOTE_SIZE_KNOWN_KEY = "_pycurl_remote_size_known"


class DownloadError(Exception):
    """Base error for download failures."""


class RecoverableDownloadError(DownloadError):
    """Errors that can be solved by retrying the task."""


class NonRecoverableDownloadError(DownloadError):
    """Errors that only affect the current task and should not be retried further."""


class FatalDownloadError(DownloadError):
    """Errors that should abort every download."""


@dataclass
class RetryPolicy:
    max_attempts: int = 15
    base_delay: float = 1.25
    max_delay: float = 40.0
    backoff: float = 2.0
    jitter: float = 0.2

    def compute_delay(self, attempt: int) -> float:
        delay = min(
            self.base_delay * (self.backoff ** max(0, attempt - 1)), self.max_delay
        )
        return delay * random.uniform(1 - self.jitter, 1 + self.jitter)


@dataclass
class DownloadTask:
    url: str
    output_path: Union[str, Path]
    headers: dict[str, str] = field(default_factory=dict)
    proxy: Optional[str] = None
    before_started: Optional[Hook] = None
    after_finished: Optional[Hook] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class _NullHeaderSink:
    def __call__(self, header: bytes) -> int:  # returns consumed byte count
        return len(header)


_NULL_HEADER_SINK = _NullHeaderSink()


class DownloadManager:
    def __init__(
        self,
        workers: int = 4,
        retry_policy: Optional[RetryPolicy] = None,
        console: Optional[Console] = None,
        progress: Optional[Progress] = None,
        default_proxy: Optional[str] = None,
        connect_timeout: int = 20,
        transfer_timeout: int = 0,
        low_speed_limit: int = 1,
        low_speed_time: int = 30,
        max_redirects: int = 8,
        ftp_response_timeout: int = 60,
        progress_label_factory: Optional[LabelFactory] = None,
    ):
        self.workers = workers
        self.retry_policy = retry_policy or RetryPolicy()
        self.console = console or Console()
        self._owns_progress = progress is None
        self.progress = progress or Progress(
            TextColumn("[bold blue]{task.fields[url]}[/]"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            DownloadColumn(),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            console=self.console,
            expand=True,
            refresh_per_second=1,
        )

        self.default_proxy = default_proxy
        self.connect_timeout = connect_timeout
        self.transfer_timeout = transfer_timeout
        self.low_speed_limit = low_speed_limit
        self.low_speed_time = low_speed_time
        self.max_redirects = max_redirects
        self.ftp_response_timeout = ftp_response_timeout

        self.before_started_hooks: list[Hook] = []
        self.after_finished_hooks: list[Hook] = []

        self._stop_event = threading.Event()
        self._progress_lock = threading.Lock()
        self._progress_label_factory: LabelFactory = (
            progress_label_factory or self._default_progress_label
        )

    def add_before_started_hook(self, hook: Hook) -> None:
        self.before_started_hooks.append(hook)

    def add_after_finished_hook(self, hook: Hook) -> None:
        self.after_finished_hooks.append(hook)

    def attach_console(self, console: Console) -> None:
        self.console = console

    def stop(self) -> None:
        self._stop_event.set()

    def run(self, tasks: Union[Iterable[DownloadTask], queue.Queue]) -> None:
        self.console.log("[green]Download manager started")
        if self._owns_progress:
            self.progress.start()
        try:
            with ThreadPoolExecutor(
                max_workers=self.workers, thread_name_prefix="pycurl-dl"
            ) as pool:
                futures: set[Future] = set()
                try:
                    for task in self._task_iterator(tasks):
                        if self._stop_event.is_set():
                            break
                        future = pool.submit(self._download_with_retries, task)
                        futures.add(future)
                        self._cleanup_futures(futures)
                    self._await_futures(futures)
                except FatalDownloadError:
                    self.console.log(
                        "[red]Fatal error detected, cancelling remaining downloads"
                    )
                    self._stop_event.set()
                    for future in futures:
                        future.cancel()
                    raise
        finally:
            if self._owns_progress:
                self.progress.stop()
            self.console.log("[green]Download manager finished")

    def _task_iterator(
        self, tasks: Union[Iterable[DownloadTask], queue.Queue]
    ) -> Iterator[DownloadTask]:
        if isinstance(tasks, queue.Queue):
            while not self._stop_event.is_set():
                try:
                    item = tasks.get(timeout=0.2)
                except queue.Empty:
                    continue
                if item is None:
                    self.console.log("[yellow]Received sentinel, stopping task intake")
                    break
                yield item
        else:
            for item in tasks:
                if self._stop_event.is_set():
                    break
                yield item

    def _cleanup_futures(self, futures: set[Future]) -> None:
        done = {f for f in futures if f.done()}
        for finished in done:
            futures.remove(finished)
            exc = finished.exception()
            if exc:
                if isinstance(exc, FatalDownloadError):
                    raise exc
                if isinstance(exc, NonRecoverableDownloadError):
                    self.console.log(f"[yellow]Task failed permanently: {exc}")
                elif isinstance(exc, RecoverableDownloadError):
                    self.console.log(f"[yellow]Exhausted retries: {exc}")

    def _await_futures(self, futures: set[Future]) -> None:
        while futures:
            done, futures = wait(futures, return_when=FIRST_EXCEPTION)
            for finished in done:
                exc = finished.exception()
                if isinstance(exc, FatalDownloadError):
                    raise exc
                if isinstance(exc, DownloadError):
                    self.console.log(f"[yellow]{exc}")

    def _download_with_retries(self, task: DownloadTask) -> None:
        attempt = 0
        last_error: Optional[DownloadError] = None
        while (
            not self._stop_event.is_set() and attempt < self.retry_policy.max_attempts
        ):
            attempt += 1
            try:
                self.console.log(f"[cyan]Attempt {attempt} for {task.url}")
                self._execute_download(task)
                return
            except RecoverableDownloadError as exc:
                last_error = exc
                delay = self.retry_policy.compute_delay(attempt)
                self.console.log(
                    f"[yellow]Recoverable error for {task.url}: {exc} -> retrying in {delay:.2f}s"
                )
                time.sleep(delay)
            except NonRecoverableDownloadError:
                raise
        if last_error:
            raise last_error

    def _execute_download(self, task: DownloadTask) -> None:
        output_path = Path(task.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        start_offset, remote_size = self._determine_resume_offset(task, output_path)

        if remote_size is not None and start_offset >= remote_size > 0:
            self.console.log(f"[blue]Skipping {task.url}, already fully downloaded")
            if task.after_finished:
                task.after_finished(task.url, str(output_path))
            for hook in self.after_finished_hooks:
                hook(task.url, str(output_path))
            return

        for hook in self.before_started_hooks:
            hook(task.url, str(output_path))
        if task.before_started:
            task.before_started(task.url, str(output_path))

        task_id = self._create_progress_task(task, start_offset)
        try:
            with output_path.open("ab") as handle:
                writer = _SafeFileWriter(handle)
                curl = pycurl.Curl()
                try:
                    self._configure_curl(curl, task, writer, start_offset, task_id)
                    curl.perform()
                    self._validate_after_perform(curl)
                except pycurl.error as exc:
                    raise self._map_pycurl_error(exc, task.url) from exc
                finally:
                    curl.close()
        except OSError as exc:
            if exc.errno in (errno.ENOSPC,):
                raise FatalDownloadError(
                    f"No space left while downloading {task.url}"
                ) from exc
            if exc.errno in (errno.EACCES,):
                raise FatalDownloadError(
                    f"No permission to write {output_path}"
                ) from exc
            raise RecoverableDownloadError(f"I/O error for {task.url}: {exc}") from exc
        finally:
            self._remove_progress_task(task_id)

        if task.after_finished:
            task.after_finished(task.url, str(output_path))
        for hook in self.after_finished_hooks:
            hook(task.url, str(output_path))
        self.console.log(f"[green]Completed download: {task.url}")

    def _configure_curl(
        self,
        curl: pycurl.Curl,
        task: DownloadTask,
        writer: _SafeFileWriter,
        start_offset: int,
        task_id: TaskID,
    ) -> None:
        curl.setopt(pycurl.VERBOSE, 0)
        curl.setopt(pycurl.URL, task.url)
        curl.setopt(pycurl.NOPROGRESS, False)
        curl.setopt(
            pycurl.XFERINFOFUNCTION, self._progress_callback(task_id, start_offset)
        )
        curl.setopt(pycurl.WRITEDATA, writer)
        curl.setopt(pycurl.HEADERFUNCTION, _NULL_HEADER_SINK)
        curl.setopt(pycurl.FOLLOWLOCATION, True)
        curl.setopt(pycurl.MAXREDIRS, self.max_redirects)
        curl.setopt(pycurl.CONNECTTIMEOUT, self.connect_timeout)
        if self.transfer_timeout:
            curl.setopt(pycurl.TIMEOUT, self.transfer_timeout)
        curl.setopt(pycurl.LOW_SPEED_LIMIT, self.low_speed_limit)
        curl.setopt(pycurl.LOW_SPEED_TIME, self.low_speed_time)
        curl.setopt(pycurl.RESUME_FROM, start_offset)
        if hasattr(pycurl, "RESUME_FROM_LARGE"):
            curl.setopt(pycurl.RESUME_FROM_LARGE, start_offset)
        curl.setopt(
            pycurl.PROTOCOLS, pycurl.PROTO_HTTP | pycurl.PROTO_HTTPS | pycurl.PROTO_FTP
        )
        curl.setopt(pycurl.FTP_RESPONSE_TIMEOUT, self.ftp_response_timeout)

        headers = [f"{k}: {v}" for k, v in task.headers.items()]
        if headers:
            curl.setopt(pycurl.HTTPHEADER, headers)

        proxy = task.proxy or self.default_proxy
        if proxy:
            curl.setopt(pycurl.PROXY, proxy)

    def _validate_after_perform(self, curl: pycurl.Curl) -> None:
        http_code = int(curl.getinfo(pycurl.RESPONSE_CODE) or 0)
        if http_code >= 400:
            raise self._map_http_error(http_code)

    def _progress_callback(
        self, task_id: TaskID, start_offset: int
    ) -> Callable[[float, float, float, float], int]:
        def _callback(download_total: float, download_now: float, *_: float) -> int:
            if self._stop_event.is_set():
                return 1
            with self._progress_lock:
                total = start_offset + download_total if download_total > 0 else None
                if total is not None:
                    self.progress.update(task_id, total=total)
                self.progress.update(task_id, completed=start_offset + download_now)
            return 0

        return _callback

    def _create_progress_task(self, task: DownloadTask, completed: int) -> TaskID:
        try:
            label = self._progress_label_factory(task)
        except Exception as exc:
            label = f"<label factory failed with: {exc}>"
        with self._progress_lock:
            task_id = self.progress.add_task(
                "download",
                start=False,
                completed=completed,
                total=None,
                url=label,
            )
            self.progress.start_task(task_id)
            return task_id

    def set_progress_label_factory(self, factory: Optional[LabelFactory]) -> None:
        self._progress_label_factory = factory or self._default_progress_label

    def _default_progress_label(self, task: DownloadTask) -> str:
        return str(task.metadata.get("progress_label") or task.url)

    def _remove_progress_task(self, task_id: TaskID) -> None:
        with self._progress_lock:
            self.progress.remove_task(task_id)

    def _map_http_error(self, code: int) -> DownloadError:
        if code in (404, 410):
            return NonRecoverableDownloadError(f"Remote file not found (HTTP {code})")
        if 400 <= code < 500:
            return NonRecoverableDownloadError(f"Client error (HTTP {code})")
        if 500 <= code < 600:
            return RecoverableDownloadError(f"Server error (HTTP {code})")
        return RecoverableDownloadError(f"Unexpected HTTP status {code}")

    def _map_pycurl_error(self, exc: pycurl.error, url: str) -> DownloadError:
        err_no = exc.args[0]
        recoverable_codes = {
            pycurl.E_COULDNT_RESOLVE_HOST,
            pycurl.E_COULDNT_CONNECT,
            pycurl.E_COULDNT_RESOLVE_PROXY,
            pycurl.E_OPERATION_TIMEDOUT,
            pycurl.E_PARTIAL_FILE,
            pycurl.E_SEND_ERROR,
            pycurl.E_RECV_ERROR,
        }
        if err_no == pycurl.E_REMOTE_FILE_NOT_FOUND:
            return NonRecoverableDownloadError(f"File not found on remote host: {url}")
        if err_no in recoverable_codes:
            return RecoverableDownloadError(f"Network disruption for {url}: {exc}")
        return RecoverableDownloadError(f"Pycurl error {err_no} for {url}: {exc}")

    def _determine_resume_offset(
        self, task: DownloadTask, output_path: Path
    ) -> tuple[int, Optional[int]]:
        remote_size_known = bool(task.metadata.get(_REMOTE_SIZE_KNOWN_KEY, False))
        remote_size: Optional[int] = (
            task.metadata.get(_REMOTE_SIZE_CACHE_KEY) if remote_size_known else None
        )

        if not output_path.exists():
            return 0, remote_size

        local_size = output_path.stat().st_size
        if local_size == 0:
            return 0, remote_size

        if not remote_size_known:
            try:
                remote_size = self._probe_remote_size(task)
            except NonRecoverableDownloadError:
                raise
            except DownloadError as exc:
                self.console.log(
                    f"[yellow]Unable to probe remote size for {task.url}: {exc}"
                )
                return local_size, None
            task.metadata[_REMOTE_SIZE_CACHE_KEY] = remote_size
            task.metadata[_REMOTE_SIZE_KNOWN_KEY] = True

        if remote_size is None:
            return local_size, None
        if local_size > remote_size:
            self.console.log(
                f"[yellow]Local file larger than remote for {task.url}, restarting download"
            )
            try:
                output_path.unlink()
            except FileNotFoundError:
                pass
            return 0, remote_size
        return local_size, remote_size

    def _probe_remote_size(self, task: DownloadTask) -> Optional[int]:
        curl = pycurl.Curl()
        try:
            curl.setopt(pycurl.VERBOSE, 0)
            curl.setopt(pycurl.URL, task.url)
            curl.setopt(pycurl.NOBODY, True)
            curl.setopt(pycurl.HEADERFUNCTION, _NULL_HEADER_SINK)
            curl.setopt(pycurl.FOLLOWLOCATION, True)
            curl.setopt(pycurl.MAXREDIRS, self.max_redirects)
            curl.setopt(pycurl.CONNECTTIMEOUT, self.connect_timeout)
            if self.transfer_timeout:
                curl.setopt(pycurl.TIMEOUT, self.transfer_timeout)
            curl.setopt(
                pycurl.PROTOCOLS,
                pycurl.PROTO_HTTP | pycurl.PROTO_HTTPS | pycurl.PROTO_FTP,
            )
            curl.setopt(pycurl.FTP_RESPONSE_TIMEOUT, self.ftp_response_timeout)

            headers = [f"{k}: {v}" for k, v in task.headers.items()]
            if headers:
                curl.setopt(pycurl.HTTPHEADER, headers)

            proxy = task.proxy or self.default_proxy
            if proxy:
                curl.setopt(pycurl.PROXY, proxy)

            curl.perform()
            self._validate_after_perform(curl)

            size = curl.getinfo(pycurl.CONTENT_LENGTH_DOWNLOAD)
            if size is None or size < 0:
                return None
            return int(size)
        except pycurl.error as exc:
            raise self._map_pycurl_error(exc, task.url) from exc
        finally:
            curl.close()


class _SafeFileWriter:
    def __init__(self, handle):
        self._handle = handle

    def write(self, data: bytes) -> int:
        try:
            return self._handle.write(data)
        except OSError as exc:
            if exc.errno in (errno.ENOSPC,):
                raise FatalDownloadError("Disk space exhausted") from exc
            if exc.errno in (errno.EACCES,):
                raise FatalDownloadError("Permission denied") from exc
            raise


# EOF
