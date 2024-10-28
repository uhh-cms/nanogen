# coding: utf-8

from __future__ import annotations

__all__: list[str] = []

import os
import time
from typing import Any

import law  # type: ignore[import-untyped]


is_remote_env = law.util.flag_to_bool(os.getenv("NG_REMOTE_ENV", "") or False)


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


@law.decorator.factory(missing=False, seconds=120, accept_generator=True)
def maybe_wait_for_dcache(fn, opts, task, *args, **kwargs):
    def before_call() -> None:
        # no need for a state
        return None

    def call(state: None) -> Any:
        return fn(task, *args, **kwargs)

    def after_call(state: None) -> None:
        # do nothing in remote envs or if the mount is not existing
        if is_remote_env or not os.path.isdir("/pnfs/desy.de/cms/tier2"):
            return

        # get dcache outputs
        dcache_outputs = [
            outp for outp in law.util.flatten(task.output())
            if isinstance(outp, law.MirroredTarget)
        ]
        if not dcache_outputs:
            return

        with task.publish_step("waiting for DCache to sync ...", scheduler=False):
            for outp in dcache_outputs:
                sleep_counter = 0
                while (
                    outp.local_target.exists() == opts["missing"] and
                    sleep_counter < opts["seconds"]
                ):
                    time.sleep(1.0)
                    sleep_counter += 1

    return before_call, call, after_call
