# coding: utf-8

from __future__ import annotations

__all__: list[str] = []

import os

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


def maybe_local_target(
    target: law.FileSystemFileTarget,
    local_fs: str = "local_fs_desy_store",
    only_existing: bool = True,
) -> law.FileSystemFileTarget:
    # only handle WLCG targets
    if not isinstance(target, law.wlcg.WLCGFileTarget):
        return target

    # convert to local target
    local_target = law.LocalFileTarget(target.path.lstrip(os.sep), fs=local_fs)

    return local_target if (not only_existing or local_target.exists()) else target
