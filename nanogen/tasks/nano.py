# coding: utf-8

"""
Tasks for MiniAOD to NanoAOD conversion.
"""

from __future__ import annotations

import os

import luigi  # type: ignore[import-untyped]
import law  # type: ignore[import-untyped]

from nanogen.tasks.base import ConfigTask, DatasetTask, CMSSWSandboxTask, wrapper_factory
from nanogen.tasks.remote import RemoteWorkflow
from nanogen.tasks.external import GetDatasetLFNs, FetchLFN
from nanogen.nano_util import SkimConfig, inject_customizations, nano_file_hash, skim_nano_file
from nanogen.util import expand_path, maybe_local_target


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


class NanoDatasetWorkflow(DatasetTask, law.LocalWorkflow):

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["lfns"] = GetDatasetLFNs.req(self)
        return reqs

    @law.dynamic_workflow_condition
    def workflow_condition(self):
        return maybe_local_target(self.input().lfns).exists()

    def lfns_per_task(self, n_lfns: int) -> int:
        return self.dataset.get("lfns_per_task", 1)

    @workflow_condition.create_branch_map
    def create_branch_map(self):
        n_lfns = len(maybe_local_target(self.input().lfns).load(formatter="json"))
        return list(law.util.iter_chunks(range(n_lfns), self.lfns_per_task(n_lfns)))

    def requires(self):
        return law.util.DotDict({"lfns": GetDatasetLFNs.req(self)})


class CreateNano(NanoDatasetWorkflow, CMSSWSandboxTask, RemoteWorkflow):

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
        description="whether to prefetch input files rather then streaming them them; "
        "default: False",
    )

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["cfg"] = CreateCMSRunConfig.req(self, dataset_kind=self.mini_info.kind)
        return reqs

    def requires(self):
        reqs = super().requires()
        reqs["cfg"] = CreateCMSRunConfig.req(self, dataset_kind=self.mini_info.kind)
        # edge case: if the inputs are to be fetched, the "lfns" requirement must be present
        if self.fetch_lfns:
            if not reqs["lfns"].complete():
                raise Exception("fetch_lfn requires GetDatasetLFNs to be already complete")
            lfns = maybe_local_target(reqs["lfns"].output()).load(formatter="json")
            reqs["files"] = [FetchLFN.req(self, lfn=lfns[i]) for i in self.branch_data]
        return reqs

    workflow_condition = NanoDatasetWorkflow.workflow_condition.copy()

    @workflow_condition.output
    def output(self):
        if self.cms_store:
            hash_parts = [self.dataset.key, self.branch]
            if self.n_events >= 0:
                hash_parts.append(f"n{self.n_events}")
            name = nano_file_hash(hash_parts)
        else:
            postfix_parts = [self.branch]
            if self.n_events >= 0:
                postfix_parts.append(f"n{self.n_events}")
            name = f"nano_{'_'.join(map(str, postfix_parts))}"

        return self.target(f"{name}.root", cms_store=self.cms_store)

    def run(self):
        inputs = self.input()

        # get the input files to process
        if self.fetch_lfns:
            input_files = [maybe_local_target(t).uri() for t in inputs.files]
        else:
            lfns = maybe_local_target(inputs.lfns).load(formatter="json")
            input_files = [lfns[i] for i in self.branch_data]
        self.publish_message(f"processing files {', '.join(input_files)}")

        # temporary directory to run in
        tmp_dir = law.LocalDirectoryTarget(is_tmp=True)
        tmp_dir.touch()

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
            f" {maybe_local_target(self.input()).abspath}",
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
