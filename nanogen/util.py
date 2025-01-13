# coding: utf-8

from __future__ import annotations

__all__: list[str] = []

import os
import time
import subprocess
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


def wget(src: str, dst: str, force: bool = False) -> str:
    """
    Downloads a file from a remote *src* to a local destination *dst*, creating intermediate
    directories when needed. When *dst* refers to an existing file, an exception is raised unless
    *force* is *True*.

    The full, normalized destination path is returned.
    """
    # check if the target directory exists
    dst = expand_path(dst)
    if os.path.isdir(dst):
        dst = os.path.join(dst, os.path.basename(src))
    else:
        dst_dir = os.path.dirname(dst)
        if not os.path.exists(dst_dir):
            raise IOError(f"target directory '{dst_dir}' does not exist")

    # remove existing dst or complain
    if os.path.exists(dst):
        if force:
            os.remove(dst)
        else:
            raise IOError(f"target '{dst}' already exists")

    # actual download
    cmd = ["wget", src, "-O", dst]
    code, _, error = law.util.interruptable_popen(
        law.util.quote_cmd(cmd),
        shell=True,
        executable="/bin/bash",
        stderr=subprocess.PIPE,
        kill_timeout=2,
    )
    if code != 0:
        raise Exception(f"wget failed: {error}")

    return dst


@law.decorator.factory(missing=False, seconds=150, accept_generator=True)
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
