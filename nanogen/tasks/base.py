# coding: utf-8

"""
Generic tools and base tasks that are defined along typical objects in an analysis.
"""

from __future__ import annotations

import os
import re
import enum
import getpass
import inspect
from typing import Type, Callable

import luigi  # type: ignore[import-untyped]
import law  # type: ignore[import-untyped]

from nanogen.nano_util import DatasetInfo, mini_to_nano_dataset
from nanogen.util import is_remote_env, expand_path, maybe_wait_for_dcache


default_config = law.config.get_expanded("analysis", "default_config")
default_dataset = law.config.get_expanded("analysis", "default_dataset")


user_parameter = luigi.Parameter(
    default=getpass.getuser(),
    description="the user running the current task, mainly for central schedulers to distinguish "
    "between tasks that should or should not be run in parallel by multiple users; "
    "default: current user",
)


class OutputLocation(enum.Enum):
    """
    Output location flag.
    """

    config = "config"
    local = "local"
    wlcg = "wlcg"
    dcache = "dcache"


class Task(law.SandboxTask):

    version: luigi.Parameter | None = luigi.Parameter(
        description="mandatory version that is encoded into output paths",
    )

    allow_empty_sandbox = True
    sandbox: str | None = None

    task_namespace: str | None = None
    message_cache_size = 25
    local_workflow_require_branches = False
    output_collection_cls = law.SiblingFileCollection

    # defaults for targets
    default_local_fs = "local_fs_store"
    default_wlcg_fs = "wlcg_fs"
    default_output_location = "config"

    @classmethod
    def modify_param_values(cls, params: dict) -> dict:
        params = super().modify_param_values(params)
        params = cls.resolve_param_values(params)
        return params

    @classmethod
    def resolve_param_values(cls, params: dict) -> dict:
        return params

    @classmethod
    def req_params(cls, inst: Task, **kwargs) -> dict:
        # always prefer certain parameters given as task family parameters (--TaskFamily-parameter)
        _prefer_cli = law.util.make_set(kwargs.get("_prefer_cli", [])) | {
            "version", "workflow", "job_workers", "poll_interval", "walltime", "max_runtime",
            "retries", "acceptance", "tolerance", "parallel_jobs", "shuffle_jobs", "htcondor_cpus",
            "htcondor_gpus", "htcondor_memory", "htcondor_pool",
        }
        kwargs["_prefer_cli"] = _prefer_cli

        # build the params
        return super().req_params(inst, **kwargs)

    @maybe_wait_for_dcache(missing=True)
    def _remove_output(self, *args, **kwargs):
        return super()._remove_output(*args, **kwargs)

    def store_parts(self) -> law.util.InsertableDict:
        parts = law.util.InsertableDict()

        parts["task_family"] = self.task_family

        # add the version when set
        if self.version is not None:
            parts["version"] = self.version

        return parts

    def store_parts_cms(self) -> law.util.InsertableDict:
        parts = law.util.InsertableDict()

        parts["task_family"] = self.task_family

        # add the version when set
        if self.version is not None:
            parts["version"] = self.version

        return parts

    def store_path(self, *path, cms_store=False):
        store_parts = self.store_parts_cms() if cms_store else self.store_parts()

        # concatenate all parts that make up the path and join them
        parts = tuple(store_parts.values()) + path
        path = os.path.join(*[str(part).strip(os.sep) for part in parts])

        return path

    def local_target(self, *path, dir=False, cms_store=False, fs=None, **kwargs):
        if fs is None:
            fs = self.default_local_fs

        # select the target class
        cls = law.LocalDirectoryTarget if dir else law.LocalFileTarget

        # create the path
        path = self.store_path(*path, cms_store=cms_store)

        # create the target instance and return it
        return cls(path, fs=fs, **kwargs)

    def wlcg_target(self, *path, dir=False, cms_store=False, fs=None, **kwargs):
        if fs is None:
            fs = self.default_wlcg_fs

        # select the target class
        cls = law.wlcg.WLCGDirectoryTarget if dir else law.wlcg.WLCGFileTarget

        # create the path
        path = self.store_path(*path, cms_store=cms_store)

        # create the target instance and return it
        return cls(path, fs=fs, **kwargs)

    def get_task_output_location_options(self):
        return [self.task_family]

    def target(self, *path, **kwargs):
        # get the default location
        location = kwargs.pop("location", self.default_output_location)

        # parse it and obtain config values if necessary
        if isinstance(location, str):
            location = OutputLocation[location]
        if location == OutputLocation.config:
            for opt in self.get_task_output_location_options():
                location = law.config.get_expanded("analysis", opt, None, split_csv=True)
                if location:
                    break
            else:
                self.logger.debug(
                    f"no option 'analysis::{self.task_family}' found in law.cfg to obtain target "
                    "location, falling back to 'local'",
                )
                location = ["local"]
            location[0] = OutputLocation[location[0]]
        location = law.util.make_list(location)

        # forward to correct function
        if location[0] == OutputLocation.local:
            # get other options
            (loc,) = (location[1:] + [None])[:1]
            loc_key = "fs" if (loc and law.config.has_section(loc)) else "store"
            kwargs.setdefault(loc_key, loc)
            return self.local_target(*path, **kwargs)

        if location[0] == OutputLocation.wlcg:
            # get other options
            (fs,) = (location[1:] + [None])[:1]
            kwargs.setdefault("fs", fs)
            return self.wlcg_target(*path, **kwargs)

        if location[0] == OutputLocation.dcache:
            # get other options
            loc, wlcg_fs = (location[1:] + [None, None])[:2]
            # create the wlcg target
            wlcg_kwargs = kwargs.copy()
            wlcg_kwargs.setdefault("fs", wlcg_fs or self.default_wlcg_fs)
            wlcg_target = self.wlcg_target(*path, **wlcg_kwargs)
            # in remote envs, mirrored targets are not supported and default to the wlcg component
            if is_remote_env:
                return wlcg_target
            # create the local target
            local_kwargs = kwargs.copy()
            loc_key = "fs" if (loc and law.config.has_section(loc)) else "store"
            local_kwargs.setdefault(loc_key, loc or "local_fs_dcache_store")
            local_target = self.local_target(*path, **local_kwargs)
            # build the mirrored target from these two
            mirrored_target_cls = (
                law.MirroredFileTarget
                if isinstance(local_target, law.LocalFileTarget)
                else law.MirroredDirectoryTarget
            )
            return mirrored_target_cls(
                path=local_target.path,
                remote_target=wlcg_target,
                local_target=local_target,
            )

        raise Exception(f"cannot determine output location based on '{location}'")


# cache for loaded configs
_config_cache: dict[str, tuple[str, dict, str, dict, str, dict]] = {}


class ConfigTask(Task):

    config_name = luigi.Parameter(
        default=f"{default_config}",
        description="name the config yaml (preferred) or json file, but without the file extension; "
        f"resolved relative to $NG_BASE/config; default: {default_config}",
    )

    @classmethod
    def resolve_param_values(cls, params: dict) -> dict:
        # prepend "config_" to config name if needed
        if "config_name" in params and not params["config_name"].startswith("config_"):
            params["config_name"] = f"config_{params['config_name']}"
        # resolve the config file path
        if "config_file" in params:
            params["config_file"] = os.path.splitext(params["config_file"])[0]

        return params

    @classmethod
    def resolve_config_file(cls, config_name: str) -> str:
        for ext in ["yaml", "yml", "json"]:
            path = expand_path(os.path.join("$NG_BASE", "config", f"{config_name}.{ext}"), abs=True)
            if os.path.exists(path):
                return path
        raise IOError(f"could not resolve config file for '{config_name}' to existing path")

    @classmethod
    def resolve_dataset_config(cls, config_file: str, config: dict) -> str:
        config_dir = os.path.dirname(config_file)
        return expand_path(config_dir, config["dataset_config"], abs=True)

    @classmethod
    def resolve_nano_config(cls, config_file: str, config: dict) -> str:
        config_dir = os.path.dirname(config_file)
        return expand_path(config_dir, config["nano_config"], abs=True)

    @classmethod
    def load_configs(cls, config_name: str) -> tuple[str, dict, str, dict, str, dict]:
        # resolve the config file and check if it was already
        config_file = cls.resolve_config_file(config_name)
        if config_file not in _config_cache:
            # load the config
            config = law.util.DotDict.wrap(
                law.LocalFileTarget(config_file).load(formatter="yaml"),
            )

            # resolve and load the dataset config
            dataset_config_file = cls.resolve_dataset_config(config_file, config)
            datasets = law.util.DotDict.wrap(
                law.LocalFileTarget(dataset_config_file).load(formatter="yaml"),
            )

            # resolve and load the nano config
            nano_config_file = cls.resolve_nano_config(config_file, config)
            nano_config = law.util.DotDict.wrap(
                law.LocalFileTarget(nano_config_file).load(formatter="yaml"),
            )

            # store in cache
            _config_cache[config_file] = (
                config_file, config,
                dataset_config_file, datasets,
                nano_config_file, nano_config,
            )

        # return from cache
        return _config_cache[config_file]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # load configs
        (
            self.config_file,
            self.config,
            self.dataset_config_file,
            self.datasets,
            self.nano_config_file,
            self.nano_config,
        ) = self.load_configs(self.config_name)

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        parts.insert_before("version", "config", self.config_name)
        return parts

    def store_parts_cms(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        parts.insert_before("version", "config", self.config_name)
        return parts

    def get_task_output_location_options(self):
        opts = [f"{self.task_family}_{self.config_name}"]
        opts += super().get_task_output_location_options()
        return opts


class CMSSWSandboxTask(ConfigTask):

    def sandbox_stagein(self, sandbox_inputs):
        def can_read_locally(inp):
            if isinstance(inp, (law.LocalTarget, law.MirroredTarget)):
                return True
            if isinstance(inp, law.SiblingFileCollection):
                return (
                    isinstance(inp.dir, law.LocalDirectoryTarget) or
                    (isinstance(inp.dir, law.MirroredDirectoryTarget) and inp.dir._local_root_exists())  # noqa: E501
                )
            if isinstance(inp, law.TargetCollection):
                raise NotImplementedError("generic collections are not supported")
            return False

        # stage all inputs that are not locally accessible
        return law.util.map_struct(lambda inp: not can_read_locally(inp), sandbox_inputs)

    def sandbox_stageout(self, sandbox_outputs):
        def can_write_locally(outp):
            if isinstance(outp, law.LocalTarget):
                return True
            if isinstance(outp, law.SiblingFileCollection):
                return isinstance(outp.dir, law.LocalDirectoryTarget)
            if isinstance(outp, law.TargetCollection):
                raise NotImplementedError("generic collections are not supported")
            return False

        # stage all outputs that are not locally accessible
        return law.util.map_struct(lambda outp: not can_write_locally(outp), sandbox_outputs)

    @property
    def sandbox(self) -> str:  # type: ignore[override]
        return f"bash::$NG_BASE/sandboxes/{self.cmssw_sandbox_script}"

    def sandbox_pre_setup_cmds(self) -> list[str]:
        env = {
            "NG_CMSSW_VERSION": self.cmssw_sandbox_version,
            "NG_SCRAM_ARCH": self.cmssw_sandbox_arch,
            "NG_CMSSW_ENV_NAME": self.cmssw_sandbox_name,
        }
        return self.sandbox_inst._build_export_commands(env)

    def sandbox_post_setup_cmds(self) -> list[str]:
        if not self.sandbox_inst or not self.sandbox_inst.sandbox_type:
            return []

        # env variables to be set in the sandbox
        env = {
            # potentially updated cms related paths
            "CMS_PATH": "${NG_CMS_PATH}",
            "SITECONFIG_PATH": "${NG_CMS_PATH}/SITECONF/local",
        }

        # for old cmssw versions, use a custom gfal plugin dir with plugins removed that lead to
        # plenty of warnings
        cmssw_major = int(self.cmssw_sandbox_version.split("_", 2)[1])
        if cmssw_major < 14:
            env["GFAL_PLUGIN_DIR"] = "${NG_CONDA_BASE}/lib/gfal2-plugins-cmssw"

        return self.sandbox_inst._build_export_commands(env)

    @property
    def cmssw_sandbox_version(self) -> str:
        # version to install
        return self.config.cmssw_version

    @property
    def cmssw_sandbox_arch(self) -> str:
        # architecture to install for
        return self.config.scram_arch

    @property
    def cmssw_sandbox_name(self) -> str:
        # name of the environment, inserted into $NG_CMSSW_BASE/<the_name>/CMSSW_VERSION
        return self.config.cmssw_env_name

    @property
    def cmssw_sandbox_script(self) -> str:
        # bash script that sets up the cmssw env
        return self.config.sandbox_script


class DatasetTask(ConfigTask):

    dataset_name = luigi.Parameter(
        default=f"{default_dataset}",
        description=f"name of the dataset to process; default: {default_dataset}",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # load the dataset config
        self.dataset = self.datasets[self.dataset_name]

        # parse dataset mini info
        self.mini_info = DatasetInfo.from_key(self.dataset.key)

        # convert mini-based dataset key info nano-based one and store info
        campaign_postfix = self.dataset.get("campaign_postfix", self.config.campaign_postfix)
        nano_key = mini_to_nano_dataset(self.dataset.key, campaign_postfix=campaign_postfix)
        self.nano_info = DatasetInfo.from_key(nano_key)

    def store_parts(self) -> law.util.InsertableDict:
        parts = super().store_parts()
        parts.insert_before("version", "dataset", self.dataset_name)
        return parts

    def store_parts_cms(self) -> law.util.InsertableDict:
        parts = super().store_parts()

        # add the nano-based cms store
        parts["cms_store"] = os.path.join(self.nano_info.cms_store.lstrip("/"), "0")

        return parts

    def get_task_output_location_options(self):
        opts = [f"{self.task_family}_{self.config_name}_{self.dataset_name}"]
        opts += super().get_task_output_location_options()
        return opts


def wrapper_factory(
    base_cls: Type[Task],
    require_cls: Type[Task],
    enable: list[str],
    cls_name: str | None = None,
    attributes: dict | None = None,
    docs: str | None = None,
    reduce_params: Callable[[Type[Task], list[tuple]], list[tuple]] | None = None,
) -> Type[Task]:
    # check known features
    known_features = ["configs", "datasets", "skip_datasets"]
    for feature in enable:
        if feature not in known_features:
            raise ValueError(
                f"unknown enabled feature '{feature}', known features are "
                f"'{','.join(known_features)}'",
            )

    # treat base_cls as a tuple
    base_classes = law.util.make_tuple(base_cls)
    base_cls = base_classes[0]

    # define wrapper feature flags
    has_configs = "configs" in enable
    has_datasets = "datasets" in enable
    has_skip_datasets = has_datasets and "skip_datasets" in enable

    # some flags should be enabled
    if not has_configs and not has_datasets:
        raise ValueError("at least one of 'configs' or 'datasets' must be enabled")

    # helper to check if enabled features are compatible with required and base class
    def check_class_compatibility(name, min_require_cls, max_base_cls):
        if not issubclass(require_cls, min_require_cls):
            raise TypeError(
                f"when the '{name}' feature is enabled, require_cls must inherit from "
                f"{min_require_cls}, but {require_cls} does not",
            )
        if issubclass(base_cls, min_require_cls):
            raise TypeError(
                f"when the '{name}' feature is enabled, base_cls must not inherit from "
                f"{min_require_cls}, but {base_cls} does",
            )
        if not issubclass(max_base_cls, base_cls):
            raise TypeError(
                f"when the '{name}' feature is enabled, base_cls must be a super class of "
                f"{max_base_cls}, but {base_cls} is not",
            )

    # check classes
    if has_configs:
        check_class_compatibility("configs", ConfigTask, Task)
    if has_datasets:
        check_class_compatibility("datasets", DatasetTask, Task if has_configs else ConfigTask)

    # create the class
    class Wrapper(*base_classes, law.WrapperTask):  # type: ignore[misc]

        if has_configs:
            config_names = law.CSVParameter(
                default=(default_config,),
                description=f"comma-separated names of configs to use; default: {default_config}",
                brace_expand=True,
            )
        if has_datasets:
            dataset_names = law.CSVParameter(
                default=("*",),
                description="names or name patterns of datasets to use; default: ('*',)",
                brace_expand=True,
            )
        if has_skip_datasets:
            skip_dataset_names = law.CSVParameter(
                default=(),
                description="names or name patterns of datasets to skip; empty default",
                brace_expand=True,
            )

        user = user_parameter

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            # store wrapper flags
            self.wrapper_require_cls = require_cls
            self.wrapper_has_config_names = has_configs and self.config_names
            self.wrapper_has_dataset_names = has_datasets and self.dataset_names
            self.wrapper_has_skip_datasets = has_skip_datasets

            # store wrapped fields
            self.wrapper_fields = ["config_name"]
            if self.wrapper_has_dataset_names:
                self.wrapper_fields.append("dataset_name")

            # build the parameter space
            self.wrapper_parameters = self._build_wrapper_parameters()

        def _build_wrapper_parameters(self):
            # get the target config instances
            if self.wrapper_has_config_names:
                config_names = law.util.make_unique([
                    os.path.splitext(elem)[0]
                    for elem in os.listdir(expand_path("$NG_BASE/config", abs=True))
                    if re.match(r"^config_.+\.(yaml|yml|json)", elem)
                ])
            else:
                config_names = [self.config_name]

            # per config, find datasets
            selected_dataset_names = {}
            if self.wrapper_has_dataset_names:
                def load_dataset_names(config_name):
                    config_file = ConfigTask.resolve_config_file(config_name)
                    config = law.LocalFileTarget(config_file).load(formatter="yaml")
                    dataset_config_file = ConfigTask.resolve_dataset_config(config_file, config)
                    dataset_config = law.LocalFileTarget(dataset_config_file).load(formatter="yaml")
                    return list(dataset_config.keys())

                def filter_dataset_names(dataset_names: list[str]) -> list[str]:
                    return [
                        name for name in dataset_names
                        if (
                            law.util.multi_match(name, self.dataset_names, mode=any) and
                            not law.util.multi_match(name, self.skip_dataset_names, mode=any)
                        )
                    ]

                # load and filter dataset names
                selected_dataset_names = {
                    config_name: filter_dataset_names(load_dataset_names(config_name))
                    for config_name in config_names
                }

            # build config-dataset combinations
            if selected_dataset_names:
                # full combinatorics
                params = [
                    (config_name, dataset_name)
                    for config_name, dataset_names in selected_dataset_names.items()
                    for dataset_name in dataset_names
                ]
            else:
                # just use the configs
                params = [(config_name,) for config_name in config_names]

            # hook to reduce the parameter space
            params = self.reduce_wrapper_parameters(params)

            return params

        def reduce_wrapper_parameters(self, params):
            return reduce_params(self, params) if callable(reduce_params) else params

        def requires(self) -> dict:
            # build all requirements based on the parameter space
            reqs: dict[tuple, Type[Task]] = {}

            for values in self.wrapper_parameters:
                params = dict(zip(self.wrapper_fields, values))

                # allow custom checks and updates
                params = self.update_wrapper_params(params)
                if not params:
                    continue

                # add the requirement if not present yet
                req = self.wrapper_require_cls.req(self, **params)
                if req not in reqs.values():
                    reqs[values] = req

            return reqs

        def update_wrapper_params(self, params):
            return params

        # add additional class-level members
        if attributes:
            locals().update(attributes)

    # overwrite __module__ to point to the module of the calling stack
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    if module:
        Wrapper.__module__ = module.__name__

    # overwrite __name__
    Wrapper.__name__ = cls_name or require_cls.__name__ + "Wrapper"

    # set docs
    if docs:
        Wrapper.__docs__ = docs

    return Wrapper
