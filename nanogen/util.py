# coding: utf-8

from __future__ import annotations

__all__: list[str] = []

import os


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
