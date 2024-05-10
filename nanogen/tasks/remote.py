# coding: utf-8

"""
Base classes and tools for working with remote tasks and targets.
"""

import os
import math

import luigi  # type: ignore[import-untyped]
import law  # type: ignore[import-untyped]

from nanogen.tasks.base import Task


class BundleRepo(Task, law.git.BundleGitRepository, law.tasks.TransferLocalFile):

    replicas = luigi.IntParameter(
        default=3,
        description="number of replicas to generate; default: 3",
    )
    version = None

    exclude_files = [".law", ".github"]

    def get_repo_path(self):
        # required by BundleGitRepository
        return os.environ["NG_BASE"]

    def single_output(self):
        repo_base = os.path.basename(self.get_repo_path())
        return self.target(f"{repo_base}.{self.checksum}.tgz")

    def get_file_pattern(self):
        path = os.path.expandvars(os.path.expanduser(self.single_output().path))
        return self.get_replicated_path(path, i=None if self.replicas <= 0 else "*")

    def output(self):
        return law.tasks.TransferLocalFile.output(self)

    @law.decorator.log
    @law.decorator.safe_output
    def run(self):
        # create the bundle
        bundle = law.LocalFileTarget(is_tmp="tgz")
        self.bundle(bundle)

        # log the size
        self.publish_message(f"size is {law.util.human_bytes(bundle.stat().st_size, fmt=True)}")

        # transfer the bundle
        self.transfer(bundle)


default_htcondor_flavor = law.config.get_expanded("analysis", "htcondor_flavor", law.NO_STR)


class HTCondorWorkflow(Task, law.htcondor.HTCondorWorkflow):

    transfer_logs = luigi.BoolParameter(
        default=True,
        significant=False,
        description="transfer job logs to the output directory; default: True",
    )
    max_runtime = law.DurationParameter(
        default=2.0,
        unit="h",
        significant=False,
        description="maximum runtime; default unit is hours; default: 2",
    )
    htcondor_logs = luigi.BoolParameter(
        default=False,
        significant=False,
        description="transfer htcondor internal submission logs to the output directory; "
        "default: False",
    )
    htcondor_memory = law.BytesParameter(
        default=law.NO_FLOAT,
        unit="MB",
        significant=False,
        description="requested memeory in MB; empty value leads to the cluster default setting; "
        "empty default",
    )
    htcondor_flavor = luigi.ChoiceParameter(
        default=default_htcondor_flavor,
        choices=("naf", "cern", "cern_el7", "cern_el8", "cern_el9", law.NO_STR),
        significant=False,
        description="the 'flavor' (i.e. configuration name) of the batch system; choices: "
        f"naf,cern,cern_el7,cern_el8,cern_el9,NO_STR; default: '{default_htcondor_flavor}'",
    )

    exclude_params_branch = {"max_runtime", "htcondor_logs", "htcondor_memory", "htcondor_flavor"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cached BundleRepo requirement to avoid race conditions during checksum calculation
        self.bundle_repo_req = BundleRepo.req(self)

    def htcondor_workflow_requires(self):
        reqs = law.htcondor.HTCondorWorkflow.htcondor_workflow_requires(self)

        reqs["repo"] = self.bundle_repo_req
        reqs["repo"].checksum

        return reqs

    def htcondor_output_directory(self):
        # the directory where submission meta data and logs should be stored
        return self.local_target(dir=True)

    def htcondor_bootstrap_file(self):
        return law.JobInputFile(
            "$NG_BASE/nanogen/tasks/remote_bootstrap.sh",
            share=True,
            render_job=True,
        )

    def htcondor_job_config(self, config, job_num, branches):
        # add the law config
        rel_path = os.path.relpath(os.environ["LAW_CONFIG_FILE"], os.environ["NG_BASE"])
        if not rel_path.startswith(".."):
            config.render_variables["law_config_file"] = os.path.join("$NG_BASE", rel_path)
        else:
            config.input_files["law_config_file"] = law.JobInputFile(
                "$LAW_CONFIG_FILE",
                share=True,
                render=False,
            )

        # forward voms proxy
        vomsproxy_file = law.wlcg.get_vomsproxy_file()
        if not law.wlcg.check_vomsproxy_validity(proxy_file=vomsproxy_file):
            raise Exception("voms proxy not valid, submission aborted")
        config.input_files["vomsproxy_file"] = law.JobInputFile(
            vomsproxy_file,
            share=True,
            render=False,
        )

        # add repo bundle pattern
        config.render_variables["ng_repo_pattern"] = os.path.join(
            self.bundle_repo_req.output().dir.uri(base_name="filecopy", scheme=False),
            os.path.basename(self.bundle_repo_req.get_file_pattern()),
        )

        # some htcondor setups require a "log" config, but we can safely use /dev/null by default
        config.log = "log.txt" if self.htcondor_logs else "/dev/null"

        # select the correct env at CERN
        # https://batchdocs.web.cern.ch/local/submit.html#os-selection-via-containers
        if self.htcondor_flavor.startswith("cern"):
            cern_os = {"cern_el7": "el7", "cern_el8": "el8"}.get(self.htcondor_flavor, "el9")
            config.custom_content.append(("MY.WantOS", cern_os))

        # use cc7 on naf
        # https://confluence.desy.de/display/IS/BIRD
        if self.htcondor_flavor == "naf":
            config.custom_content.append(("requirements", "(OpSysAndVer == \"CentOS7\")"))  # noqa

        # maximum runtime, compatible with multiple batch systems
        if self.max_runtime is not None and self.max_runtime > 0:
            max_runtime = int(math.floor(self.max_runtime * 3600)) - 1
            config.custom_content.append(("+MaxRuntime", max_runtime))
            config.custom_content.append(("+RequestRuntime", max_runtime))

        # request memory
        if self.htcondor_memory is not None and self.htcondor_memory > 0:
            config.custom_content.append(("Request_Memory", self.htcondor_memory))

        # render variables
        config.render_variables["ng_bootstrap_name"] = "htcondor"
        if self.htcondor_flavor not in ("", law.NO_STR):
            config.render_variables["ng_htcondor_flavor"] = self.htcondor_flavor
        config.render_variables.setdefault("ng_pre_setup_command", "")
        config.render_variables.setdefault("ng_post_setup_command", "")
        for var in [
            "NG_CERN_USER",
            "NG_CERN_USER_DCACHE_STORE",
            "NG_SOFTWARE_BASE",
            "NG_STORE_LOCAL",
            "NG_LOCAL_SCHEDULER",
            "NG_DASMAPS_BASE",
        ]:
            config.render_variables[var.lower()] = os.environ[var]

        return config

    def htcondor_use_local_scheduler(self):
        # remote jobs should not communicate with ther central scheduler but with a local one
        return True


default_slurm_flavor = law.config.get_expanded("analysis", "slurm_flavor", "maxwell")
default_slurm_partition = law.config.get_expanded("analysis", "slurm_partition", "cms-uhh")


class SlurmWorkflow(Task, law.slurm.SlurmWorkflow):

    transfer_logs = luigi.BoolParameter(
        default=True,
        significant=False,
        description="transfer job logs to the output directory; default: True",
    )
    max_runtime = law.DurationParameter(
        default=2.0,
        unit="h",
        significant=False,
        description="maximum runtime; default unit is hours; default: 2",
    )
    slurm_partition = luigi.Parameter(
        default=default_slurm_partition,
        significant=False,
        description=f"target queue partition; default: {default_slurm_partition}",
    )
    slurm_flavor = luigi.ChoiceParameter(
        default=default_slurm_flavor,
        choices=("maxwell",),
        significant=False,
        description="the 'flavor' (i.e. configuration name) of the batch system; choices: "
        f"maxwell; default: '{default_slurm_flavor}'",
    )

    exclude_params_branch = {"max_runtime", "slurm_partition", "slurm_flavor"}

    def slurm_output_directory(self):
        # the directory where submission meta data and logs should be stored
        return self.local_target(dir=True)

    def slurm_bootstrap_file(self):
        return law.JobInputFile(
            "$NG_BASE/nanogen/tasks/remote_bootstrap.sh",
            share=True,
            render_job=True,
        )

    def slurm_job_config(self, config, job_num, branches):
        # forward voms proxy
        vomsproxy_file = law.wlcg.get_vomsproxy_file()
        if not law.wlcg.check_vomsproxy_validity(proxy_file=vomsproxy_file):
            raise Exception("voms proxy not valid, submission aborted")
        config.input_files["vomsproxy_file"] = law.JobInputFile(
            vomsproxy_file,
            share=True,
            render=False,
        )

        # forward kerberos proxy
        kfile = os.environ["KRB5CCNAME"]
        kerberos_proxy_file = os.sep + kfile.split(os.sep, 1)[-1]
        if os.path.exists(kerberos_proxy_file):
            config.input_files["kerberosproxy_file"] = law.JobInputFile(
                kerberos_proxy_file,
                share=True,
                render=False,
            )
            config.render_variables["ng_pre_setup_command"] = "aklog"

        # set job time
        if self.max_runtime is not None:
            job_time = law.util.human_duration(
                seconds=int(math.floor(self.max_runtime * 3600)) - 1,
                colon_format=True,
            )
            config.custom_content.append(("time", job_time))

        # set nodes
        config.custom_content.append(("nodes", 1))

        # custom, flavor dependent settings
        if self.slurm_flavor == "maxwell":
            # nothing yet
            pass

        # render variales
        config.render_variables["ng_bootstrap_name"] = "slurm"
        config.render_variables.setdefault("ng_pre_setup_command", "")
        config.render_variables.setdefault("ng_post_setup_command", "")
        for var in [
            "NG_BASE",
            "NG_DASMAPS_BASE",
        ]:
            config.render_variables[var.lower()] = os.environ[var]

        # custom tmp dir since slurm uses the job submission dir as the main job directory, and law
        # puts the tmp directory in this job directory which might become quite long; then,
        # python's default multiprocessing puts socket files into that tmp directory which comes
        # with the restriction of less then 80 characters that would be violated, and potentially
        # would also overwhelm the submission directory
        config.render_variables["law_job_tmp"] = "/tmp/law_$( basename \"$LAW_JOB_HOME\" )"  # noqa

        return config


class RemoteWorkflow(HTCondorWorkflow, SlurmWorkflow):
    """
    Workflow that can be submitted to a remote batch system like HTCondor or Slurm.
    """
