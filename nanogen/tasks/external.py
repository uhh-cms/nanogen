# coding: utf-8

"""
Tasks dealing with external data.
"""

from __future__ import annotations

import subprocess

import law  # type: ignore[import-untyped]

from nanogen.tasks.base import ConfigTask, DatasetTask, wrapper_factory


class GetDatasetLFNs(DatasetTask, law.tasks.TransferLocalFile):

    version = None

    sandbox = "bash::/cvmfs/cms.cern.ch/cmsset_default.sh"

    def single_output(self) -> law.target.file.FileSystemFileTarget:
        # required by law.tasks.TransferLocalFile
        return self.target(f"lfns_{law.util.create_hash(self.dataset.key)}.json")

    @law.decorator.log
    def run(self):
        # build and run the command
        cmd = f"dasgoclient --query='file dataset={self.dataset.key}' --limit=0"
        self.publish_message(f"cmd: {cmd}")
        code, out, _ = law.util.interruptable_popen(
            cmd,
            stdout=subprocess.PIPE,
            shell=True,
            executable="/bin/bash",
        )
        if code != 0:
            raise Exception(f"dasgoclient query failed:\n{out}")

        # extract lfns
        lfns = [line.strip() for line in out.strip().split("\n") if line.strip().endswith(".root")]
        self.publish_message(f"found {len(lfns)} LFNs for dataset {self.dataset.key}")

        # sort them to always deal with a deterministic order
        lfns.sort()

        # save and upload
        tmp = law.LocalFileTarget(is_tmp=True)
        tmp.dump(lfns, indent=4, formatter="json")
        self.transfer(tmp)


GetDatasetLFNsWrapper = wrapper_factory(
    base_cls=ConfigTask,
    require_cls=GetDatasetLFNs,
    cls_name="GetDatasetLFNsWrapper",
    enable=["datasets", "skip_datasets"],
    attributes={"version": None},
)
