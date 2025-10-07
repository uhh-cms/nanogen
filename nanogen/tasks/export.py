# coding: utf-8

"""
Tasks that export information about the nano production.
"""

from __future__ import annotations

import os
import re
import functools

import luigi  # type: ignore[import-untyped]
import law  # type: ignore[import-untyped]

from nanogen.tasks.base import (
    ConfigTask, DatasetTask, CMSSWSandboxTask, wrapper_factory, user_parameter,
)
from nanogen.tasks.nano import CreateNano, MergeNano
from nanogen.nano_util import (
    DatasetInfo, das_query, load_dataset_stats, mini_to_nano_dataset, load_lfn_stats,
)
from nanogen.util import expand_path


class GenerateNanoDocs(DatasetTask, CMSSWSandboxTask):

    user = user_parameter

    def requires(self):
        return CreateNano.req(self, branch=0, _prefer_cli={"branch"})

    def output(self):
        return law.util.DotDict({
            "docs": self.target("docs.html"),
            "sizes": self.target("sizes.html"),
        })

    @law.decorator.notify
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


class ExportCentralNanoKey(DatasetTask):

    version = None

    def store_parts(self):
        parts = super().store_parts()
        parts.pop("dataset")
        return parts

    def output(self):
        return self.target(f"nano__{self.dataset_name}.txt")

    def run(self):
        # check the prompt flag for data

        # get the nano dataset key from DAS using the "child" attribute
        nano_keys = das_query(f"child dataset={self.mini_info.dataset_key}").split()

        # now, select the correct one, using some heuristics in case of multiple results
        is_data = self.mini_info.data
        is_prompt = self.dataset.get("prompt", None)
        fallback_key = self.dataset.get("nano_key", None)
        if is_prompt is None:
            if is_data:
                raise ValueError("is_prompt must be specified for data")
            is_prompt = False

        # build info objects for simplified key parsing
        infos = [DatasetInfo.from_key(k) for k in nano_keys]

        # generic error message
        err_missing = (
            f"no valid nano key{'(s)' if is_data else ''} found for dataset '{self.dataset_name}' "
            f"with mini key '{self.mini_info.dataset_key}', got das response: {nano_keys}"
        )
        err_fallback = f"using fallback nano key '{fallback_key}'"

        # selection behavior depends heavily on data/mc
        if is_data:
            # campaign version should not start with "BTV" or "JME"
            infos = [
                info for info in infos
                if not info.campaign_version.startswith(("BTV", "JME"))
            ]

            # as per PPD, use all versions for prompt and only the latest for reprocessed datasets
            # !!! NOTE: this might be more difficult in case there are two central nanos assigned to
            # !!!       the same mini sample, so let's assume this never happens
            if len(infos) > 1:
                raise NotImplementedError("selection if latest re-reco dataset not implemented yet")

            # combine back to keys
            if infos:
                nano_key = infos[0].dataset_key
            elif fallback_key:
                self.logger.error(err_missing)
                self.logger.warning(err_fallback)
                nano_key = fallback_key
            else:
                raise ValueError(err_missing)

        else:  # mc
            # campaign version should not start with "BTV" or "JME"
            infos = [
                info for info in infos
                if not info.campaign_version.startswith(("BTV", "JME"))
            ]

            # further heuristics can be added here ...

            # only one objects should remain
            if len(infos) > 1:
                raise ValueError(err_missing)
            if infos:
                nano_key = infos[0].dataset_key
            elif fallback_key:
                self.logger.error(err_missing)
                self.logger.warning(err_fallback)
                nano_key = fallback_key
            else:
                raise ValueError(err_missing)

        # write it
        self.output().dump(nano_key, formatter="text")


ExportCentralNanoKeyWrapper = wrapper_factory(
    base_cls=ConfigTask,
    require_cls=ExportCentralNanoKey,
    cls_name="ExportCentralNanoKeyWrapper",
    enable=["datasets", "skip_datasets"],
    attributes={"version": None},
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
    central = luigi.BoolParameter(
        default=False,
        description="whether the created entry should be based on the central nanos rather than "
        "custom ones; default: False",
    )
    recreate = luigi.BoolParameter(
        default=False,
        description="whether to recreate existing entries before printing them; default: False",
    )
    user = user_parameter

    @classmethod
    def modify_param_args(cls, params, args, kwargs):
        if kwargs.get("central", False):
            kwargs["version"] = law.NO_STR
        return params, args, kwargs

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # systematic shifts are not allowed
        if re.match(r"^.*_(up|down)$", self.dataset_name):
            raise Exception(f"systematic variations are not allowed, got '{self.dataset_name}'")

        # extensions are not allowed
        if re.match(r"^.*_ext\d+$", self.dataset_name):
            raise Exception(f"dataset extensions are not allowed, got '{self.dataset_name}'")

        if self.central and self.dataset_is_private:
            raise Exception("central entries cannot be provided private datasets by construction")

    def requires(self):
        # no requirements if not recreating and output exists
        if not self.recreate and self.output().exists():
            return []

        def maybe_include_extensions(dataset_name):
            yield dataset_name
            if not self.skip_extensions:
                for _dataset_name in self.datasets:
                    if re.match(rf"^{dataset_name}_ext\d+$", _dataset_name):
                        yield _dataset_name

        # require MergeNano or ExportCentralNanoKey depending on the mode
        dep = ExportCentralNanoKey if self.central else MergeNano

        # nominal dataset, plus extensions
        reqs = law.util.DotDict.wrap({
            "nominal": {
                dataset_name: dep.req(self, dataset_name=dataset_name)
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
                    _dataset_name: dep.req(self, dataset_name=_dataset_name)
                    for _dataset_name in maybe_include_extensions(dataset_name)
                }

        return reqs

    def output(self):
        postfixes = []
        if self.central:
            postfixes.append("central")
        if self.skip_shifts:
            postfixes.append("noshifts")
        if self.skip_extensions:
            postfixes.append("noext")
        postfix = ("_" + "_".join(postfixes)) if postfixes else ""
        return self.target(f"cmsdb_entry{postfix}.txt")

    def complete(self):
        return law.tasks.RunOnceTask.complete(self)

    @law.decorator.notify
    @law.decorator.log
    @law.tasks.RunOnceTask.complete_on_success
    def run(self):
        output = self.output()

        # print existing entry if not recreating
        if not self.recreate and output.exists():
            self.publish_message("showing previously created entry")
            self.publish_message(f"\n{output.load(formatter='text')}\n")
            return

        # prepare requirements and inputs
        reqs = self.requires()
        inputs = self.input()

        # dataset stats loading cached by arguments (dataset key)
        load_dataset_stats_cached = functools.cache(load_dataset_stats)

        # helper to determine the number of files and events
        def get_stats(dataset_name, shift_name):
            # divert to central behavior if needed
            if self.central:
                stats = load_dataset_stats_cached(get_nano_key(dataset_name, shift_name))
                return stats["n_files"], stats["n_events"], 0, 0

            # divert to private behavior if needed
            if isinstance(self.datasets[dataset_name].get("private", None), dict):
                if shift_name != "nominal":
                    raise NotImplementedError(f"get_stats with shift '{shift_name}' not supported")
                col = inputs["nominal"][dataset_name].collection
                n_files = len(col)
                n_events = sum(
                    target.load(formatter="uproot")["Events"].num_entries
                    for target in col.targets.values()
                )
                return n_files, n_events

            # get das stats
            stats = load_dataset_stats_cached(self.datasets[dataset_name].key)

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

            return len(inp.collection), n_unskipped_events, n_skipped, n_skipped_events

        # helper to create the nano dataset key based on a dataset name
        def get_nano_key(dataset_name, shift_name):
            if self.central:
                return inputs[shift_name][dataset_name].load(formatter="text").strip()
            dataset = self.datasets[dataset_name]
            campaign_postfix = dataset.get("campaign_postfix", self.config.campaign_postfix)
            return mini_to_nano_dataset(dataset.key, campaign_postfix=campaign_postfix)

        # helper to format summation of numbers
        def fmt_sum(nums):
            return " + ".join(f"{n:_}" for n in nums)

        # get the id of the original dataset
        if self.dataset_is_private:
            dataset_id = self.dataset.private.id
        elif self.central:
            nano_key = inputs["nominal"][self.dataset_name].load(formatter="text").strip()
            dataset_id = load_dataset_stats_cached(nano_key)["dataset_id"]
        else:
            load_dataset_stats_cached(self.dataset.key)["dataset_id"]

        # estimate the process name
        if self.mini_info.data:
            process_name = "_".join(self.dataset_name.split("_")[:-1])
            # drop parking prefix
            process_name = process_name.replace("_parking", "")
        else:
            # start with the dataset name
            process_name = self.dataset_name
            # drop private marker
            if self.dataset_is_private:
                process_name = process_name.replace("_prv_", "_")
            # drop generators
            process_name = re.sub(r"_(powheg|madgraph|amcatnlo|pythia)$", "", process_name)
            # drop flavor schemes
            process_name = re.sub(r"_(4|5)f($|_)", r"\2", process_name)
            # drop fixes
            process_name = re.sub(r"_fix\d+($|_)", r"\1", process_name)

        # start creating the entry
        entry = "cpn.add_dataset(\n"
        entry += f"    name=\"{self.dataset_name}\",\n"  # noqa: Q003
        entry += f"    id={dataset_id},\n"
        if self.mini_info.data:
            entry += "    is_data=True,\n"
        entry += f"    processes=[procs.{process_name}],\n"
        # keys, n_files and n_events, depending on wether the dataset has variations
        if set(inputs.keys()) == {"nominal"}:
            # prepare values
            n_files, n_events, n_skipped_files, n_skipped_events = law.util.unzip([
                get_stats(dataset_name, "nominal")
                for dataset_name in inputs["nominal"]
            ])
            # add entries
            entry += "    keys=[\n"
            for dataset_name in inputs["nominal"]:
                key_line = f"        \"{get_nano_key(dataset_name, 'nominal')}\","  # noqa: Q003
                entry += key_line + ("  # noqa" if len(key_line) > 120 else "") + "\n"
            entry += "    ],\n"
            entry += f"    n_files={fmt_sum(n_files)},"
            if sum(n_skipped_files) > 0:
                entry += f"  # {fmt_sum(n_skipped_files)} skipped"
            entry += "\n"
            entry += f"    n_events={fmt_sum(n_events)},"
            if sum(n_skipped_events) > 0:
                entry += f"  # {fmt_sum(n_skipped_events)} skipped"
            entry += "\n"
        else:
            entry += "    info=dict(\n"
            for shift_name in inputs:
                # prepare values
                n_files, n_events, n_skipped_files, n_skipped_events = law.util.unzip([
                    get_stats(dataset_name, shift_name)
                    for dataset_name in inputs[shift_name]
                ])
                # add entries
                entry += f"        {shift_name}=DatasetInfo(\n"
                entry += "            keys=[\n"
                for dataset_name in inputs[shift_name]:
                    key_line = f"                \"{get_nano_key(dataset_name, shift_name)}\","  # noqa: Q003, E501
                    entry += key_line + ("  # noqa" if len(key_line) > 120 else "") + "\n"
                entry += "            ],\n"
                entry += f"            n_files={fmt_sum(n_files)},"
                if sum(n_skipped_files) > 0:
                    entry += f"  # {fmt_sum(n_skipped_files)} skipped"
                entry += "\n"
                entry += f"            n_events={fmt_sum(n_events)},"
                if sum(n_skipped_events) > 0:
                    entry += f"  # {fmt_sum(n_skipped_events)} skipped"
                entry += "\n"
                entry += "        ),\n"
            entry += "    ),\n"
        # auxiliary info
        if not self.central or self.mini_info.data or self.dataset_is_private:
            entry += "    aux={\n"
            # prompt flag and data era
            if self.mini_info.data:
                if (is_prompt := self.dataset.get("prompt", None)) is None:
                    raise ValueError(
                        f"dataset '{self.dataset_name}' misses the 'prompt' flag in the config, "
                        "please add it before trying to export a cmsdb entry",
                    )
                entry += f"        \"prompt\": {is_prompt},\n"  # noqa: Q003
                # the era is usually at the end, except for versioned datasets
                parts = self.dataset_name.split("_")
                era = (parts[-1] if parts[-1].isalpha() else parts[-2]).upper()
                entry += f"        \"era\": \"{era}\",\n"  # noqa: Q003
                if "jec_era" in self.dataset:
                    entry += f"        \"jec_era\": \"{self.dataset['jec_era']}\",\n"  # noqa: Q003
            if not self.central:
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
            # private flag, only when actually true
            if self.dataset_is_private:
                entry += "        \"private\": True,\n"  # noqa: Q003
            entry += "    },\n"
        entry += ")\n"

        # save and print the entry
        output.dump(entry, formatter="text")
        self.publish_message(f"\n{entry}\n")


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
