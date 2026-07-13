"""Cross-process singleton lock for the private execution lifecycle."""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


class ExecutionLockError(RuntimeError):
    """Raised when another runner already owns private execution authority."""


_REGISTRY_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.Lock] = {}


def _thread_lock(path: str) -> threading.Lock:
    with _REGISTRY_GUARD:
        return _THREAD_LOCKS.setdefault(path, threading.Lock())


def _lock_file(handle) -> None:
    handle.seek(0, os.SEEK_END)
    if handle.tell() == 0:
        handle.write(b"\0")
        handle.flush()
        os.fsync(handle.fileno())
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise ExecutionLockError(
                "another process owns the execution lock"
            ) from exc
    else:
        import fcntl

        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise ExecutionLockError(
                "another process owns the execution lock"
            ) from exc


def _unlock_file(handle) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def exclusive_execution_lock(ledger_path: str) -> Iterator[None]:
    """Hold one non-blocking thread and OS lock beside the execution ledger."""

    resolved_ledger = Path(ledger_path).resolve()
    resolved_ledger.parent.mkdir(parents=True, exist_ok=True)
    lock_path = str(resolved_ledger.with_suffix(resolved_ledger.suffix + ".lock"))
    local_lock = _thread_lock(lock_path)
    if not local_lock.acquire(blocking=False):
        raise ExecutionLockError("another thread owns the execution lock")
    handle = None
    try:
        handle = open(lock_path, "a+b")
        _lock_file(handle)
        try:
            yield
        finally:
            _unlock_file(handle)
    finally:
        if handle is not None:
            handle.close()
        local_lock.release()


__all__ = ["ExecutionLockError", "exclusive_execution_lock"]
