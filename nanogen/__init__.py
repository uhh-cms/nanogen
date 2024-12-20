# coding: utf-8

from __future__ import annotations

import os

import law  # type: ignore[import-untyped]

law.contrib.load("tasks", "wlcg", "gfal", "git", "cms", "htcondor", "slurm", "root", "awkward")


# global flags from environment variables
env_is_remote = law.util.flag_to_bool(os.getenv("NG_REMOTE_ENV", "0"))
env_is_local = not env_is_remote
env_is_htcondor = law.util.flag_to_bool(os.getenv("NG_ON_HTCONDOR", "0"))
