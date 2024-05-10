# coding: utf-8

"""
Tasks for MiniAOD to NanoAOD conversion.
"""

from __future__ import annotations

import os
import re

import luigi  # type: ignore[import-untyped]
import law  # type: ignore[import-untyped]

from nanogen.tasks.base import ConfigTask, DatasetTask, CMSSWSandboxTask, wrapper_factory
from nanogen.tasks.remote import RemoteWorkflow
from nanogen.tasks.external import GetDatasetLFNs, FetchLFN
from nanogen.nano_util import (
    SkimConfig, inject_customizations, nano_file_hash, skim_nano_file, locate_lfn, fetch_lfn,
    load_dataset_stats, mini_to_nano_dataset, MissingLFNException,
)
from nanogen.util import expand_path, maybe_wait_for_dcache


class CreateCMSRunConfig(CMSSWSandboxTask):

    dataset_kind = luigi.ChoiceParameter(
        choices=["data", "mc"],
        description="the kind of dataset to create a config for; choices: data,mc",
    )

    # versioning not required
    version = None

    def output(self):
        return self.target(f"nano_cfg_{self.dataset_kind}.py")

    @law.decorator.log
    @maybe_wait_for_dcache
    def run(self):
        # create the cmsDriver command that generates the nano config
        # https://gitlab.cern.ch/cms-nanoAOD/nanoaod-doc/-/wikis/Instructions/Private-production
        is_mc = self.dataset_kind == "mc"
        tier = "NANOAOD" + ("SIM" if is_mc else "")
        driver_cmd = (
            "cmsDriver.py NANO"
            " -s NANO"
            f" --{self.dataset_kind}"
            f" --eventcontent {tier}"
            f" --datatier {tier}"
            f" --conditions {self.config.global_tag[self.dataset_kind]}"
            f" --era {self.config.era[self.dataset_kind]}"
            " -n -1"
            " --no_exec"
            " --customise_commands=\""  # noqa: Q003
            "process.add_(cms.Service('InitRootHandlers', EnableIMT=cms.untracked.bool(False)));"
            "\""  # noqa: Q003
        )

        # run the command in temporary directory
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()
        self.publish_message(f"cmd: {driver_cmd}")
        self.publish_message(f"cwd: {tmp_dir.abspath}")
        law.util.interruptable_popen(driver_cmd, shell=True, cwd=tmp_dir.abspath)

        # copy the file to the output
        self.output().copy_from_local(tmp_dir.child("NANO_NANO.py"))


class NanoDatasetWorkflow(DatasetTask, law.LocalWorkflow, RemoteWorkflow):

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        return reqs

    @law.workflow_property(cache=True)
    def lfns(self) -> list[str]:
        # get the lfns target
        lfns_input = self.requires().lfns.output().lfns
        if not lfns_input.exists():
            raise Exception(f"{self.task_family} requires GetDatasetLFNs to be already complete")

        # load lfn list
        lfns = lfns_input.load(formatter="json")

        # potentially skip some lfns
        for lfn in self.dataset.get("skip_lfns", []):
            if lfn in lfns:
                lfns.remove(lfn)
            else:
                print(
                    f"LFN {lfn} confiured to be skipped for dataset {self.dataset_name}, but not "
                    "found in list of LFNs!",
                )

        return lfns

    def lfns_per_task(self, n_lfns: int) -> int:
        return self.dataset.get("lfns_per_task", 1)

    def create_branch_map(self):
        n_lfns = len(self.lfns)
        return list(law.util.iter_chunks(range(n_lfns), self.lfns_per_task(n_lfns)))

    def requires(self):
        return law.util.DotDict({"lfns": GetDatasetLFNs.req(self)})

    def htcondor_destination_info(self, info):
        info = super().htcondor_destination_info(info)
        info["config"] = self.config_name
        info["dataset"] = self.dataset_name
        return info


class CreateNano(NanoDatasetWorkflow, CMSSWSandboxTask):

    n_events = luigi.IntParameter(
        default=-1,
        description="maximum number of events to process; -1 for all; default: -1",
    )
    cms_store = luigi.BoolParameter(
        default=True,
        description="whether to store the output in the CMS-style /store/... format; default: True",
    )
    fetch_lfns = luigi.BoolParameter(
        default=False,
        significant=False,
        description="whether to persistently prefetch input files rather than streaming them; "
        "default: False",
    )
    tmp_fetch_lfns = luigi.ChoiceParameter(
        default="auto",
        choices=["True", "False", "auto"],
        significant=False,
        description="whether to temporarily prefetch input files rather than streaming them; "
        "this only works for xrootd targets since gfal plugins fail inside the cmssw sandbox; "
        "when 'auto', only lfns located at sites known to be unstable are prefetched; default: auto",
    )

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs.cfg = CreateCMSRunConfig.req(self, dataset_kind=self.mini_info.kind)
        return reqs

    def requires(self):
        reqs = super().requires()
        reqs.cfg = CreateCMSRunConfig.req(self, dataset_kind=self.mini_info.kind)

        # check if lfns were maybe already fetched, and if so, use them
        # otherwise make the decision dependent on the fetch_lfns flag
        lfns = self.lfns
        reqs.fetched_lfns = {
            i: task
            for i, task in ((i, FetchLFN.req(self, lfn=lfns[i])) for i in self.branch_data)
            if self.fetch_lfns or task.complete()
        }

        return reqs

    def output(self):
        if self.cms_store:
            # when only a single input lfn is processed and the number of events is unlimited,
            # reuse the nano file hash, otherwise create a new one
            if len(self.branch_data) == 1 and self.n_events < 0:
                name = os.path.splitext(os.path.basename(self.lfns[self.branch_data[0]]))[0]
            else:
                hash_parts = [[self.lfns[i] for i in self.branch_data]]
                if self.n_events >= 0:
                    hash_parts.append(f"n{self.n_events}")
                name = nano_file_hash(hash_parts)
        else:
            postfix_parts = [f"{min(self.branch_data)}To{max(self.branch_data) + 1}"]
            if self.n_events >= 0:
                postfix_parts.append(f"n{self.n_events}")
            name = f"nano_{'_'.join(map(str, postfix_parts))}"

        return self.target(f"{name}.root", cms_store=self.cms_store)

    @law.decorator.log
    @maybe_wait_for_dcache
    def run(self):
        inputs = self.input()

        # temporary directory to run in
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

        # get the input files to process
        lfns = self.lfns
        input_files = [
            inputs.fetched_lfns[i].uri() if i in inputs.fetched_lfns else lfns[i]
            for i in self.branch_data
        ]
        self.publish_message(f"processing files {', '.join(input_files)}")

        # check if lfns need to be fetched temporarily
        if self.tmp_fetch_lfns.lower() != "false":
            for i, lfn in enumerate(list(input_files)):
                # skip non-lfns
                if not lfn.startswith("/store/"):
                    continue
                # locate it
                try:
                    lfn_locations = locate_lfn(lfn, logger=self.logger)
                except MissingLFNException:
                    continue
                # select only xrootd locations
                lfn_locations = list(filter((lambda l: l.scheme == "root"), lfn_locations))
                if not lfn_locations:
                    continue
                # based on the first location, determine if the site is deemed unstable, or skip
                # TODO: it might happen that there is another site first in the list, but later on
                # during streaming it turns out to be unreliable and cmssw switches to a bad site,
                # so one might proactively identify these cases here through some kind of heuristic
                # and fetch the file anyway
                if self.tmp_fetch_lfns.lower() == "auto":
                    tier, country, _ = lfn_locations[0].site.split("_", 2)
                    # so far, only fetch from US and KR sites
                    if country.lower() not in {"us", "kr"}:
                        continue
                # fetch
                input_files[i] = fetch_lfn(
                    lfn, tmp_dir.abspath, logger=self.logger, _lfn_locations=lfn_locations,
                )

        # determine custom hook and arguments from config, or dataset of they exist
        custom_hook = (
            self.nano_config.customization.module,
            self.nano_config.customization.function,
        )
        custom_kwargs = self.nano_config.customization.arguments
        if "customization" in self.dataset and "module" in self.dataset.customization:
            custom_hook = (
                self.dataset.customization.module,
                self.dataset.customization.function,
            )
            custom_kwargs = self.dataset.customization.get("arguments", {})

        # fetch and adjust the cmsRun config
        cfg = tmp_dir.child("nano_cfg.py", type="f")
        inputs.cfg.copy_to_local(cfg)
        inject_customizations(
            cfg.abspath,
            hook=("nanogen.nano_util", "customize_nano_process"),
            dataset_kind=self.mini_info.kind,
            input_files=input_files,
            output_file="nano.root",
            compression=("ZSTD", 1),
            max_events=self.n_events,
            custom_hook=custom_hook,
            custom_kwargs=dict(custom_kwargs),
        )

        # build and run the command
        with self.publish_step("mini to nano conversion ..."):
            cmd = f"cmsRun {cfg.basename}"
            self.publish_message(f"cmd: {cmd}")
            self.publish_message(f"cwd: {tmp_dir.abspath}")
            code = law.util.interruptable_popen(
                cmd,
                shell=True,
                executable="/bin/bash",
                cwd=tmp_dir.abspath,
            )[0]
            if code != 0:
                raise Exception(f"cmsRun failed with exit code {code}")

        # log the size
        nano_file = tmp_dir.child("nano.root")
        nano_size = law.util.human_bytes(nano_file.stat().st_size, fmt=True)
        self.publish_message(f"nano size is {nano_size}")

        # skimming
        if self.nano_config:
            # create skim configs
            skim_configs = [SkimConfig.from_dict(d) for d in self.nano_config.skim]
            # start the skimming procedure
            with self.publish_step("skimming ..."):
                skim_nano_file(
                    input_file=nano_file.abspath,
                    skim_configs=skim_configs,
                    bypass_objects=self.nano_config.get("bypass", []),
                    compression=("ZSTD", 1),
                )
            # log the size again
            nano_size = law.util.human_bytes(nano_file.stat().st_size, fmt=True)
            self.publish_message(f"nano size after skimming is {nano_size}")

        # move the output
        # hotfix: in slc7, the webdav and xrood gfal plugins do not work inside the cmssw sandbox
        # so in case the output is accessible via xrootd, copy the file via xrdcp for now, and
        # otherwise let the configured target protocol handle the move
        uri = self.output().uri(base_name="xrootd")
        if law.target.file.get_scheme(uri) == "root":
            self.publish_message("copying output via xrdcp")
            cmd = f"xrdcp {nano_file.abspath} {uri}"
            code = law.util.interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
            if code == 0:
                return
        self.output().move_from_local(nano_file)


CreateNanoWrapper = wrapper_factory(
    base_cls=ConfigTask,
    require_cls=CreateNano,
    cls_name="CreateNanoWrapper",
    enable=["datasets", "skip_datasets"],
)


class GenerateNanoDocs(DatasetTask, CMSSWSandboxTask):

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

    skip_shifts = luigi.BoolParameter(
        default=False,
        description="whether to skip systematic shifts; default: False",
    )

    sandbox = "bash::/cvmfs/cms.cern.ch/cmsset_default.sh"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # systematic shifts are not allowed
        if self.dataset_name.endswith(("_up", "_down")):
            raise Exception(f"systematic variations are not allowed, got '{self.dataset_name}'")

    def requires(self):
        reqs = law.util.DotDict({(self.dataset_name, "nominal"): CreateNano.req(self)})

        # add systematic shifts
        if not self.skip_shifts:
            for dataset_name in self.datasets.keys():
                if (m := re.match(rf"^{self.dataset_name}_([^_]+)_(up|down)$", dataset_name)):
                    key = (dataset_name, "_".join(m.groups()))
                    reqs[key] = CreateNano.req(self, dataset_name=dataset_name)

        return reqs

    def output(self):
        return self.target(f"cmsdb_entry{'_noshifts' if self.skip_shifts else ''}.txt")

    @law.decorator.log
    def run(self):
        inputs = self.input()

        # helper to determine the number of files and events
        def get_stats(dataset_name, shift):
            # get das stats
            _das_stats = das_stats
            if shift != "nominal":
                _das_stats = load_dataset_stats(self.datasets[dataset_name].key)
            # when there are as many files in the collection as reported by das, also take
            # the number of events from there since CreateNano converts files one to one
            inp = inputs[(dataset_name, shift)]
            if len(inp.collection) == _das_stats["n_files"] or 1:
                return _das_stats["n_files"], _das_stats["n_events"]
            # otherwise, count events manually
            raise NotImplementedError("reduced number of files require manual event counting")

        # helper to create the nano dataset key based on a dataset name
        def nano_key(dataset_name):
            dataset = self.datasets[dataset_name]
            version_postfix = dataset.get(
                "campaign_version_postfix",
                self.config.campaign_version_postfix,
            )
            return mini_to_nano_dataset(dataset.key, campaign_version_postfix=version_postfix)

        # get the id of the original dataset
        das_stats = load_dataset_stats(self.dataset.key)
        dataset_id = das_stats["dataset_id"]

        # estimate the process name
        if self.nano_info.data:
            process_name = "_".join(self.dataset_name.split("_")[:2])
        else:
            process_name = re.sub(r"_(powheg|madgraph|amcatnlo)$", "", self.dataset_name)

        # start creating the entry
        entry = "cpn.add_dataset(\n"
        entry += f"    name=\"{self.dataset_name}\",\n"  # noqa: Q003
        entry += f"    id={dataset_id},\n"
        if self.nano_info.data:
            entry += "    is_data=True,\n"
        entry += f"    processes=[procs.{process_name}]\n"
        if set(inputs.keys()) == {(self.dataset_name, "nominal")}:
            n_files, n_events = get_stats(self.dataset_name, "nominal")
            entry += "    keys=[\n"
            entry += f"        \"{nano_key(self.dataset_name)}\",  # noqa\n"  # noqa: Q003
            entry += "    ],\n"
            entry += f"    n_files={n_files:_},\n"
            entry += f"    n_events={n_events:_},\n"
        else:
            entry += "    info=dict(\n"
            for dataset_name, shift in inputs.keys():
                n_files, n_events = get_stats(dataset_name, shift)
                entry += f"        {shift}=DatasetInfo(\n"
                entry += "            keys=[\n"
                entry += f"                \"{nano_key(dataset_name)}\",  # noqa\n"
                entry += "            ],\n"
                entry += f"            n_files={n_files:_},\n"
                entry += f"            n_events={n_events:_},\n"
                entry += "        ),\n"
            entry += "    ),\n"
        if self.nano_info.data:
            era = self.dataset_name.split("_")[-1].upper()
            entry += "    aux={\n"
            entry += f"        \"era\": \"{era}\",\n"  # noqa: Q003
            entry += "    },\n"
        entry += ")"

        # save and print the entry
        self.output().dump(entry, formatter="text")
        self.publish_message("\n" + entry + "\n")


CreateDBEntryWrapper = wrapper_factory(
    base_cls=ConfigTask,
    require_cls=CreateDBEntry,
    cls_name="CreateDBEntryWrapper",
    enable=["datasets", "skip_datasets"],
)
