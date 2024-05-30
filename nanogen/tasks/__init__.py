# coding: utf-8

import law  # type: ignore[import-untyped]

# pre-import modules for task indexing
import nanogen.tasks.base  # noqa: F401
import nanogen.tasks.remote  # noqa: F401
import nanogen.tasks.external  # noqa: F401
import nanogen.tasks.nano  # noqa: F401
import nanogen.tasks.export  # noqa: F401
import nanogen.tasks.custom  # noqa: F401


# initialize wlcg file systems once so that their cache cleanup is triggered if configured
for section in law.config.sections():
    if section.startswith("wlcg_fs"):
        fs = law.wlcg.WLCGFileSystem(section)
