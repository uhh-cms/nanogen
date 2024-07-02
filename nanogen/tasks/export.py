# coding: utf-8

"""
Tasks that export information about the nano production.
"""

from __future__ import annotations

import os
import re

import luigi  # type: ignore[import-untyped]
import law  # type: ignore[import-untyped]

from nanogen.tasks.base import (
    ConfigTask, DatasetTask, CMSSWSandboxTask, wrapper_factory, user_parameter,
)
from nanogen.tasks.nano import CreateNano, MergeNano
from nanogen.nano_util import load_dataset_stats, mini_to_nano_dataset, load_lfn_stats
from nanogen.util import expand_path


class GenerateNanoDocs(DatasetTask, CMSSWSandboxTask):

    user = user_parameter

    def requires(self):
        return CreateNano.req(self, branch=0)

    def output(self):
        return law.util.DotDict({
            "docs": self.target("docs.html"),
            "sizes": self.target("sizes.html"),
        })

    @law.decorator.log
    @law.decorator.localize
    def run(self):
        # find the inspection script
        for inspection_script in [
            "$CMSSW_BASE/src/PhysicsTools/NanoAOD/test/inspectNanoFile.py",
            "$CMSSW_RELEASE_BASE/src/PhysicsTools/NanoAOD/test/inspectNanoFile.py",
        ]:
            inspection_script = expand_path(inspection_script, abs=True)
            if os.path.exists(inspection_script):
                break
        else:
            raise Exception("could not find inspectNanoFile.py in usual locations")

        # prepare the command
        cmds = []
        outputs = self.output()
        cmds.append(
            f"{inspection_script}"
            f" -d {outputs.docs.abspath}"
            f" -s {outputs.sizes.abspath}"
            f" {self.input().abspath}",
        )

        # once created, some lines have to be updated in the output files
        cms_docs_url = "https://cms-nanoaod-integration.web.cern.ch"
        replace = [
            ("http://gpetrucc.web.cern.ch/gpetrucc/micro", cms_docs_url),
            ("http://gpetrucc.web.cern.ch/gpetrucc", cms_docs_url),
        ]
        for outp in outputs.values():
            for src, dst in replace:
                cmds.append(f"sed -i 's|{src}|{dst}|g' {outp.abspath}")

        # run them
        cmd = " && ".join(cmds)
        self.publish_message(f"cmd: {cmd}")
        code = law.util.interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
        if code != 0:
            raise Exception(f"command failed with exit code {code}")


GenerateNanoDocsWrapper = wrapper_factory(
    base_cls=ConfigTask,
    require_cls=GenerateNanoDocs,
    cls_name="GenerateNanoDocsWrapper",
    enable=["datasets", "skip_datasets"],
)


class CreateDBEntry(DatasetTask, law.tasks.RunOnceTask):

    merged_size = MergeNano.merged_size
    skip_shifts = luigi.BoolParameter(
        default=False,
        description="whether to skip systematic shifts; default: False",
    )
    skip_extensions = luigi.BoolParameter(
        default=False,
        description="whether to skip dataset extensions; default: False",
    )
    user = user_parameter

    sandbox = "bash::/cvmfs/cms.cern.ch/cmsset_default.sh"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # systematic shifts are not allowed
        if re.match(r"^.*_(up|down)$", self.dataset_name):
            raise Exception(f"systematic variations are not allowed, got '{self.dataset_name}'")

        # extensions are not allowed
        if re.match(r"^.*_ext\d+$", self.dataset_name):
            raise Exception(f"dataset extensions are not allowed, got '{self.dataset_name}'")

    def requires(self):
        def maybe_include_extensions(dataset_name):
            yield dataset_name
            if not self.skip_extensions:
                for _dataset_name in self.datasets:
                    if re.match(rf"^{dataset_name}_ext\d+$", _dataset_name):
                        yield _dataset_name

        # nominal dataset, plus extensions
        reqs = law.util.DotDict.wrap({
            "nominal": {
                dataset_name: MergeNano.req(self, dataset_name=dataset_name)
                for dataset_name in maybe_include_extensions(self.dataset_name)
            },
        })

        # add systematic shifts
        if not self.skip_shifts:
            for dataset_name in self.datasets.keys():
                if not (m := re.match(rf"^{self.dataset_name}_([^_]+)_(up|down)$", dataset_name)):
                    continue
                shift_name = "_".join(m.groups())
                reqs[shift_name] = {
                    _dataset_name: MergeNano.req(self, dataset_name=_dataset_name)
                    for _dataset_name in maybe_include_extensions(dataset_name)
                }

        return reqs

    def output(self):
        postfixes = []
        if self.skip_shifts:
            postfixes.append("noshifts")
        if self.skip_extensions:
            postfixes.append("noext")
        postfix = ("_" + "_".join(postfixes)) if postfixes else ""
        return self.target(f"cmsdb_entry{postfix}.txt")

    @law.decorator.log
    def run(self):
        reqs = self.requires()
        inputs = self.input()

        # helper to determine the number of files and events
        def get_stats(dataset_name, shift_name):
            # get das stats
            stats = load_dataset_stats(self.datasets[dataset_name].key)

            # sanity check: the collection should be as long as the number of files minus lfns to
            # skip, otherwise DAS might have temporarily returned a shorter list (which happens)
            req = reqs[shift_name][dataset_name]
            inp = inputs[shift_name][dataset_name]
            skipped_lfns = self.datasets[dataset_name].get("skip_lfns", [])
            n_skipped = len(skipped_lfns)
            n_unskipped = sum(len(branch_data) for branch_data in req.branch_map.values())
            if n_unskipped + n_skipped != stats["n_files"]:
                raise Exception(
                    f"number of files in original collection ({n_unskipped}) plus skipped lfns "
                    f"({n_skipped}) does not match number of files in DAS ({stats['n_files']})",
                )

            # query DAS for the number of events in skipped lfns
            n_skipped_events = 0
            for lfn in skipped_lfns:
                n_skipped_events += load_lfn_stats(lfn)["n_events"]
            n_unskipped_events = stats["n_events"] - n_skipped_events

            return len(inp.collection), n_unskipped_events

        # helper to create the nano dataset key based on a dataset name
        def nano_key(dataset_name):
            dataset = self.datasets[dataset_name]
            campaign_postfix = dataset.get("campaign_postfix", self.config.campaign_postfix)
            return mini_to_nano_dataset(dataset.key, campaign_postfix=campaign_postfix)

        # get the id of the original dataset
        dataset_id = load_dataset_stats(self.dataset.key)["dataset_id"]

        # estimate the process name
        if self.nano_info.data:
            process_name = "_".join(self.dataset_name.split("_")[:2])
        else:
            process_name = re.sub(r"_(powheg|madgraph|amcatnlo|pythia)$", "", self.dataset_name)

        # helper to format summation of numbers
        fmt_sum = lambda nums: " + ".join(f"{n:_}" for n in nums)

        # start creating the entry
        entry = "cpn.add_dataset(\n"
        entry += f"    name=\"{self.dataset_name}\",\n"  # noqa: Q003
        entry += f"    id={dataset_id},\n"
        if self.nano_info.data:
            entry += "    is_data=True,\n"
        entry += f"    processes=[procs.{process_name}],\n"
        # keys, n_files and n_events, depending on wether the dataset has variations
        if set(inputs.keys()) == {"nominal"}:
            # prepare values
            n_files, n_events = law.util.unzip([
                get_stats(dataset_name, "nominal")
                for dataset_name in inputs["nominal"]
            ])
            # add entries
            entry += "    keys=[\n"
            for dataset_name in inputs["nominal"]:
                entry += f"        \"{nano_key(dataset_name)}\",  # noqa\n"  # noqa: Q003
            entry += "    ],\n"
            entry += f"    n_files={fmt_sum(n_files)},\n"
            entry += f"    n_events={fmt_sum(n_events)},\n"
        else:
            entry += "    info=dict(\n"
            for shift_name in inputs:
                # prepare values
                n_files, n_events = law.util.unzip([
                    get_stats(dataset_name, shift_name)
                    for dataset_name in inputs[shift_name]
                ])
                # add entries
                entry += f"        {shift_name}=DatasetInfo(\n"
                entry += "            keys=[\n"
                for dataset_name in inputs[shift_name]:
                    entry += f"                \"{nano_key(dataset_name)}\",  # noqa\n"
                entry += "            ],\n"
                entry += f"            n_files={fmt_sum(n_files)},\n"
                entry += f"            n_events={fmt_sum(n_events)},\n"
                entry += "        ),\n"
            entry += "    ),\n"
        # auxiliary info
        entry += "    aux={\n"
        # merging factors
        entry += "        \"merging_factors\": {\n"  # noqa: Q003
        for shift_name, _reqs in reqs.items():
            for i, (dataset_name, req) in enumerate(_reqs.items()):
                factor = len(req.branch_map[0])
                key = shift_name
                # add extension postfix
                if i > 0:
                    key += f"_{dataset_name.rsplit('_', 1)[-1]}"
                entry += f"            \"{key}\": {factor},\n"  # noqa: Q003
        entry += "        },\n"
        # data era
        if self.nano_info.data:
            era = self.dataset_name.split("_")[-1].upper()
            entry += f"        \"era\": \"{era}\",\n"  # noqa: Q003
        entry += "    },\n"
        entry += ")\n"

        # save and print the entry
        self.output().dump(entry, formatter="text")
        self.publish_message("\n" + entry + "\n")


def db_entry_wrapper_reduce_params(self, params):
    # remove variations and extensions
    return [
        (config_name, dataset_name)
        for config_name, dataset_name in params
        if not re.match(r"^.*_(up|down|ext\d+)$", dataset_name)
    ]


CreateDBEntryWrapper = wrapper_factory(
    base_cls=ConfigTask,
    require_cls=CreateDBEntry,
    cls_name="CreateDBEntryWrapper",
    enable=["datasets", "skip_datasets"],
    reduce_params=db_entry_wrapper_reduce_params,
)
