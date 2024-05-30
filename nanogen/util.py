# coding: utf-8

from __future__ import annotations

__all__: list[str] = []

import os
import time
import contextlib
from typing import Any

import law  # type: ignore[import-untyped]


def expand_path(*path: str, abs: bool = False, real: bool = False, dir: bool = False) -> str:
    p = os.path.join(*map(str, path))
    p = os.path.expandvars(os.path.expanduser(p))
    if abs:
        p = os.path.abspath(p)
    if real:
        p = os.path.realpath(p)
    if dir:
        p = os.path.dirname(p)
    return p


class DCacheTarget(law.FileSystemTarget):

    def __init__(
        self,
        path: str,
        wlcg_fs: str,
        local_fs: str,
        wlcg_kwargs: dict[str, Any] | None = None,
        local_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ):
        # subclasses must have created the wlcg and local targets already
        assert getattr(self, "wlcg", None) is not None
        assert getattr(self, "local", None) is not None

        self._force_fs = None
        self._wlcg_fs = wlcg_fs
        self._local_fs = local_fs

        super().__init__(path, **kwargs)

    def _parent_args(self):
        parent_kwargs = {
            "wlcg_fs": self._wlcg_fs,
            "local_fs": self._local_fs,
            "wlcg_kwargs": self.wlcg._parent_args()[1],
            "local_kwargs": self.local._parent_args()[1],
        }

        return (), parent_kwargs

    @property
    def dirname(self):
        return self.local.dirname

    @property
    def abs_dirname(self):
        return self.local.abs_dirname

    @property
    def basename(self):
        return self.local.basename

    def stat(self, *args, **kwargs):
        return (
            self.local.stat(*args, **kwargs)
            if self.local.exists()
            else self.wlcg.stat(*args, **kwargs)
        )

    def exists(self, *args, **kwargs):
        return self.local.exists(*args, **kwargs) or self.wlcg.exists(*args, **kwargs)

    def remove(self, *args, **kwargs):
        return self.wlcg.remove(*args, **kwargs)

    def chmod(self, *args, **kwargs):
        return self.wlcg.chmod(*args, **kwargs)

    @property
    def fs(self):
        if self._force_fs is not None:
            return self._force_fs

        return self.local.fs if self.local.exists() else self.wlcg.fs

    @contextlib.contextmanager
    def force_fs(self, fs):
        with law.util.patch_object(self, "_force_fs", fs):
            yield

    @property
    def abspath(self):
        return self.local.abspath if self.local.exists() else self.wlcg.abspath

    def uri(self, *args, **kwargs):
        return (
            self.local.uri(*args, **kwargs)
            if self.local.exists()
            else self.wlcg.uri(*args, **kwargs)
        )

    def copy_to(self, *args, **kwargs):
        return (
            self.local.copy_to(*args, **kwargs)
            if self.local.exists()
            else self.wlcg.copy_to(*args, **kwargs)
        )

    def copy_from(self, *args, **kwargs):
        return self.wlcg.copy_from(*args, **kwargs)

    def move_to(self, *args, **kwargs):
        return self.wlcg.move_to(*args, **kwargs)

    def move_from(self, *args, **kwargs):
        return self.wlcg.move_from(*args, **kwargs)

    def copy_to_local(self, *args, **kwargs):
        return (
            self.local.copy_to_local(*args, **kwargs)
            if self.local.exists()
            else self.wlcg.copy_to_local(*args, **kwargs)
        )

    def copy_from_local(self, *args, **kwargs):
        return self.wlcg.copy_from_local(*args, **kwargs)

    def move_to_local(self, *args, **kwargs):
        return self.wlcg.move_to_local(*args, **kwargs)

    def move_from_local(self, *args, **kwargs):
        return self.wlcg.move_from_local(*args, **kwargs)

    def localize(self, mode="r", **kwargs):
        return (
            self.local.localize(mode, **kwargs)
            if mode == "r" and self.local.exists()
            else self.wlcg.localize(mode, **kwargs)
        )

    def load(self, *args, **kwargs):
        return (
            self.local.load(*args, **kwargs)
            if self.local.exists()
            else self.wlcg.load(*args, **kwargs)
        )

    def dump(self, *args, **kwargs):
        return self.wlcg.dump(*args, **kwargs)


class DCacheFileTarget(law.FileSystemFileTarget, DCacheTarget):

    def __init__(
        self,
        path: str,
        wlcg_fs: str,
        local_fs: str,
        wlcg_kwargs: dict[str, Any] | None = None,
        local_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ):
        # create two file targets
        path = path.lstrip(os.sep)
        wlcg_kwargs = wlcg_kwargs.copy() if wlcg_kwargs else {}
        wlcg_kwargs["fs"] = wlcg_fs
        self.wlcg = law.wlcg.WLCGFileTarget(os.sep + path, **wlcg_kwargs)
        local_kwargs = local_kwargs.copy() if local_kwargs else {}
        local_kwargs["fs"] = local_fs
        self.local = law.LocalFileTarget(path, **local_kwargs)

        super().__init__(path, wlcg_fs, local_fs, **kwargs)

    def touch(self, *args, **kwargs):
        return self.wlcg.touch(*args, **kwargs)

    def open(self, mode, **kwargs):
        return (
            self.local.open(mode, **kwargs)
            if mode == "r" and self.local.exists()
            else self.wlcg.open(mode, **kwargs)
        )


class DCacheDirectoryTarget(law.FileSystemDirectoryTarget, DCacheTarget):

    def __init__(
        self,
        path: str,
        wlcg_fs: str,
        local_fs: str,
        wlcg_kwargs: dict[str, Any] | None = None,
        local_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ):
        # create two directory targets
        path = path.lstrip(os.sep)
        wlcg_kwargs = wlcg_kwargs.copy() if wlcg_kwargs else {}
        wlcg_kwargs["fs"] = wlcg_fs
        self.wlcg = law.wlcg.WLCGDirectoryTarget(os.sep + path, **wlcg_kwargs)
        local_kwargs = local_kwargs.copy() if local_kwargs else {}
        local_kwargs["fs"] = local_fs
        self.local = law.LocalDirectoryTarget(path, **local_kwargs)

        super().__init__(path, wlcg_fs, local_fs, **kwargs)

    def _child_args(self, path):
        child_kwargs = {
            "wlcg_fs": self._wlcg_fs,
            "local_fs": self._local_fs,
            "wlcg_kwargs": self.wlcg._child_args(path)[1],
            "local_kwargs": self.local._child_args(path)[1],
        }

        return (), child_kwargs

    def child(self, *args, **kwargs):
        return (
            self.local.child(*args, **kwargs)
            if self.local.exists()
            else self.wlcg.child(*args, **kwargs)
        )

    def listdir(self, *args, **kwargs):
        return (
            self.local.listdir(*args, **kwargs)
            if self.local.exists()
            else self.wlcg.listdir(*args, **kwargs)
        )

    def glob(self, *args, **kwargs):
        return (
            self.local.glob(*args, **kwargs)
            if self.local.exists()
            else self.wlcg.glob(*args, **kwargs)
        )

    def walk(self, *args, **kwargs):
        return (
            self.local.walk(*args, **kwargs)
            if self.local.exists()
            else self.wlcg.walk(*args, **kwargs)
        )

    def touch(self, *args, **kwargs):
        return self.wlcg.touch(*args, **kwargs)


DCacheTarget.file_class = DCacheFileTarget
DCacheTarget.directory_class = DCacheDirectoryTarget


@law.decorator.factory(missing=False, accept_generator=True)
def maybe_wait_for_dcache(fn, opts, task, *args, **kwargs):
    def before_call() -> None:
        # no need for a state
        return None

    def call(state: None) -> Any:
        return fn(task, *args, **kwargs)

    def after_call(state: None) -> None:
        # do nothing if the mount is not existing
        if not os.path.isdir("/pnfs/desy.de/cms/tier2"):
            return

        # get dcache outputs
        dcache_outputs = [
            outp for outp in law.util.flatten(task.output())
            if isinstance(outp, DCacheTarget)
        ]
        if not dcache_outputs:
            return

        with task.publish_step("waiting for DCache to sync ...", scheduler=False):
            for outp in dcache_outputs:
                sleep_counter = 0
                while outp.local.exists() == opts["missing"] and sleep_counter < 90:
                    time.sleep(1.0)
                    sleep_counter += 1

    return before_call, call, after_call
