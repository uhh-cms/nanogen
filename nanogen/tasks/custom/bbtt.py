# coding: utf-8

"""
Custom bbtt tasks.
"""

from __future__ import annotations

import luigi  # type: ignore[import-untyped]
import law  # type: ignore[import-untyped]

from nanogen.tasks.base import ConfigTask, wrapper_factory
from nanogen.tasks.remote import RemoteWorkflow
from nanogen.tasks.nano import NanoDatasetWorkflow, CreateNano


class ReduceNano(NanoDatasetWorkflow, RemoteWorkflow):

    task_namespace = "bbtt"

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["nano"] = CreateNano.req(self)
        return reqs

    def requires(self):
        reqs = super().requires()
        reqs["nano"] = CreateNano.req(self)
        return reqs

    workflow_condition = NanoDatasetWorkflow.workflow_condition.copy()

    @workflow_condition.output
    def output(self):
        return self.target(f"output_{self.branch}.parquet")

    @law.decorator.log
    def run(self):
        import uproot
        import awkward as ak
        # TODO: implement
        from IPython import embed; embed()


ReduceNanoWrapper = wrapper_factory(
    base_cls=ConfigTask,
    require_cls=ReduceNano,
    cls_name="ReduceNanoWrapper",
    enable=["datasets", "skip_datasets"],
)
