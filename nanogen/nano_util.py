# coding: utf-8

"""
Utility functions for dealing with nano files, cmsRun configs and cmssw process customizations.
"""

from __future__ import annotations

__all__: list[str] = []

import os
import re
import time
import shutil
import logging
from importlib import import_module
from fnmatch import fnmatch
from copy import deepcopy
from collections import Counter
from dataclasses import dataclass
from tempfile import mkstemp
from typing import Any

import law  # type: ignore[import-untyped]
from law.target.file import has_scheme, add_scheme  # type: ignore[import-untyped]

from nanogen.util import expand_path


@dataclass
class DatasetInfo(object):
    name: str
    campaign: str
    campaign_version: str
    dataset_version: str
    tier: str
    mc: bool

    @classmethod
    def from_key(cls, dataset_key: str) -> DatasetInfo:
        """
        Splits a dataset given by its *dataset_key* into its parts according to the format
        "/<name>/<campaign>-<campaign_version>-<dataset_version>/<tier>AOD<sim?>".
        """
        # split
        m = re.match(r"^/([^/]+)/([^/-]+)-([^/-]+)-([^/-]+)/(.+)AOD(SIM)?$", dataset_key)  # noqa
        if not m:
            raise ValueError(f"invalid dataset key: {dataset_key}")

        return cls(
            name=m.group(1),
            campaign=m.group(2),
            campaign_version=m.group(3),
            dataset_version=m.group(4),
            tier=m.group(5).lower(),
            mc=m.group(6) == "SIM",
        )

    @property
    def data(self) -> bool:
        return not self.mc

    @property
    def kind(self) -> str:
        return "mc" if self.mc else "data"

    @property
    def dataset_key(self) -> str:
        return (
            f"/{self.name}"
            f"/{self.campaign}-{self.campaign_version}-{self.dataset_version}"
            f"/{self.tier.upper()}AOD{'SIM' if self.mc else ''}"
        )

    @property
    def cms_store(self) -> str:
        return (
            "/store"
            f"/{self.kind}"
            f"/{self.campaign}"
            f"/{self.name}"
            f"/{self.tier.upper()}AOD{'SIM' if self.mc else ''}"
            f"/{self.campaign_version}-{self.dataset_version}"
        )

    def copy(self, **kwargs) -> DatasetInfo:
        attrs = deepcopy(self.__dict__)
        attrs.update(kwargs)
        return DatasetInfo(**attrs)


@dataclass
class SkimConfig(object):
    input_tree: str
    output_tree: str
    selection: str
    column_filters: list[str]

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> SkimConfig:
        return cls(
            input_tree=d["input_tree"],
            output_tree=d["output_tree"],
            selection=d.get("selection", "return true;"),
            column_filters=d.get("column_filters", []),
        )


def mini_to_nano_dataset(dataset_key: str, campaign_version_postfix: str | None = None) -> str:
    """
    Converts a MiniAOD dataset key into a NanoAOD dataset key.
    When *campaign_version_postfix* is given, it is appended to the campaign version.
    """
    # parse
    info = DatasetInfo.from_key(dataset_key)

    # update tier
    info = info.copy(tier="nano")

    # update campaign version
    if campaign_version_postfix:
        info.campaign_version += f"_{campaign_version_postfix}"

    return info.dataset_key


def nano_file_hash(hash_parts: list[Any]) -> str:
    """
    Creates a hash for a nano file based on the given *hash_parts*.
    """
    h = list(law.util.create_hash(hash_parts, l=32).upper())
    for i in [20, 16, 12, 8]:
        h.insert(i, "-")
    return "".join(h)


def fetch_lfn(
    lfn: str,
    dst: str,
    wlcg_fs: str | list[str] | None = None,
    retries: int = 2,
    fetch_local: bool = False,
    logger: logging.Logger | None = None,
) -> str:
    """
    Fetchs a *lfn* and stores it at *dst*. When *dst* is an existing directory, the file is stored
    with its original name. Itermediate, missing directories are created. The absolute path to the
    stored file is returned.
    """
    # prepare logging
    log_debug = log_info = lambda msg: None
    if logger:
        log_debug = logger.debug
        log_info = logger.info

    # prepare the output file
    abs_dst = expand_path(dst, abs=True)
    if os.path.isdir(abs_dst):
        dst_dir = abs_dst
        dst_file = os.path.basename(lfn)
        abs_dst = os.path.join(dst_dir, dst_file)
    elif os.path.isfile(abs_dst):
        dst_dir, dst_file = os.path.split(abs_dst)
        os.remove(abs_dst)
    elif abs_dst.endswith(".root"):
        dst_dir, dst_file = os.path.split(abs_dst)
    else:
        dst_dir = abs_dst
        dst_file = os.path.basename(lfn)
        abs_dst = os.path.join(dst_dir, dst_file)
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)

    # prepare fs list
    wlcg_fs_list = (
        law.util.make_list(wlcg_fs)
        if wlcg_fs
        else law.config.get_expanded("analysis", "lfn_sources", split_csv=True)
    )

    # loop over repeated fs
    log_info(f"determining location of {lfn} ...")
    for fs in wlcg_fs_list:
        log_debug(f"checking fs {fs}")

        # check if the fs is really remote or local
        is_local = True
        if fs.startswith("wlcg_fs"):
            fs_base = law.config.get_expanded(fs, "base")
            is_local = law.target.file.get_scheme(fs_base) in (None, "file")
        target_cls = law.LocalFileTarget if is_local else law.wlcg.WLCGFileTarget

        # measure the time required to perform a stat query
        input_file = target_cls(lfn, fs=fs)
        attempt = 1
        while attempt <= retries:
            t1 = time.perf_counter()
            input_stat = input_file.exists(stat=True)
            duration = time.perf_counter() - t1
            if input_stat:
                log_debug(f"stat query took {duration:.2f}s")
                break
            attempt += 1
        log_debug(f"file {lfn} does{'' if input_stat else ' not'} exist at fs {fs}")

        # stop when found
        if input_stat:
            log_info(f"found {input_file.uri()}")
            break
    else:
        raise Exception(f"LFN {lfn} not found at any of {wlcg_fs_list}")

    # when local and local fetching is not allowed, just return the path
    if is_local and not fetch_local:
        return input_file.abspath

    # fetch the file
    log_info(f"fetching {input_file} ...")
    input_file.copy_to_local(abs_dst)

    return abs_dst


def inject_customizations(
    cfg_file: str,
    *,
    hook: tuple[str, str],
    **hook_kwargs,
):
    """
    Takes a cmsRun *cfg_file* and injects a customization hook that is defined by the given *hook*
    which should be a 2-tuple given a module to import and a function name.
    All additional keyword arguments are passed to the hook function.
    """
    # read the file content
    _cfg_file = expand_path(cfg_file)
    with open(_cfg_file, "r") as f:
        lines = [line.rstrip() for line in f.readlines()]

    # inject hook
    injected_hook = False
    for i, line in enumerate(lines):
        if not injected_hook and line == "# End of customisation functions":
            lines[i] = f"""
# nanogen customization hook
from {hook[0]} import {hook[1]}
process = {hook[1]}(process, **{hook_kwargs})

{line}"""
            injected_hook = True

    # check success
    if not injected_hook:
        raise Exception(f"could not inject customization hook into {cfg_file}")

    # uncomment for cms process debugging
    # lines.append("from IPython import embed; embed()")

    # write the lines again
    with open(_cfg_file, "w") as f:
        for line in lines:
            f.write(f"{line}\n")


def customize_nano_process(
    process,
    *,
    dataset_kind: str,
    input_files: list[str] | None = None,
    output_file: str | None = None,
    compression: tuple[str, int] | None = None,
    report_every: int = 500,
    max_events: int = -1,
    custom_hook: tuple[str, str] | None = None,
    custom_kwargs: dict[str, Any] | None = None,
):
    """
    Customization function for the NanoAOD cmsRun process.
    """
    import FWCore.ParameterSet.Config as cms  # type: ignore[import-not-found]

    # set input files
    if input_files:
        process.source.fileNames = cms.untracked.vstring([
            (
                input_file
                if has_scheme(input_file) or input_file.startswith("/store")
                else add_scheme(input_file, "file")
            )
            for input_file in input_files
        ])

    # set output file
    out_module = process.NANOAODSIMoutput if dataset_kind == "mc" else process.NANOAODoutput
    if output_file:
        out_module.fileName = cms.untracked.string(output_file)

    # set compression
    if compression:
        out_module.compressionAlgorithm = cms.untracked.string(compression[0])
        out_module.compressionLevel = cms.untracked.int32(compression[1])

    # set max events
    process.maxEvents.input = cms.untracked.int32(max_events)
    process.configurationMetadata.annotation = cms.untracked.string(f"NANO evts:{max_events}")

    # set reporting frequency
    process.MessageLogger.cerr.FwkReport.reportEvery = report_every

    # invoke the custom hook
    if custom_hook:
        # import the module
        try:
            module = import_module(custom_hook[0])
        except Exception as e:
            e.args = (f"failed to import module '{custom_hook[0]}': {e}",)
            raise e
        # get the function
        missing = object()
        func = getattr(module, custom_hook[1], missing)
        if func == missing:
            raise ValueError(f"function '{custom_hook[1]}' not found in module '{custom_hook[0]}'")
        # update custom hook arguments
        custom_kwargs = deepcopy(custom_kwargs) or {}
        custom_kwargs.update({"dataset_kind": dataset_kind, "max_events": max_events})
        # call it
        process = func(process, **custom_kwargs)  # type: ignore[operator]

    return process


def skim_nano_file(
    input_file: str,
    skim_configs: list[SkimConfig],
    output_file: str | None = None,
    bypass_objects: list[str] | None = None,
    n_threads: int = 1,
    compression: tuple[str, int] | None = None,
) -> str:
    # prepare input and output tree names and check for duplicate outputs
    input_tree_names = [c.input_tree for c in skim_configs]
    output_tree_names = [c.output_tree for c in skim_configs]
    if duplicates := [name for name, c in Counter(output_tree_names).items() if c > 1]:
        raise ValueError(f"duplicate output tree names found in skim configs: {duplicates}")

    # import ROOT with the usual side-effects disabled
    ROOT = law.root.import_ROOT()

    # prepare the output file
    input_file = expand_path(input_file)
    if output_file:
        output_file = expand_path(output_file)
    if output_file and expand_path(output_file, real=True) == expand_path(input_file, real=True):
        output_file = None
    orig_output_file = output_file
    if not output_file:
        output_file = mkstemp(suffix="_" + os.path.basename(input_file))[1]
    elif os.path.isdir(output_file):
        output_file = os.path.join(output_file, os.path.basename(input_file))
    elif not os.path.exists(output_dir := os.path.dirname(output_file)):
        os.makedirs(output_dir)
    elif os.path.exists(output_file):
        os.remove(output_file)

    # prepare output compression
    compression_flag = ROOT.RCompressionSetting.EDefaults.kUseCompiledDefault
    if compression:
        compression_algo = getattr(ROOT.ROOT, f"k{compression[0]}")
        compression_level = compression[1]
        compression_flag = compression_algo * 100 + compression_level

    # open input and check that all source trees exist
    t_input = ROOT.TFile(input_file, "READ")
    existing_names = [key.GetName() for key in t_input.GetListOfKeys()]
    missing_inputs = [name for name in input_tree_names if name not in existing_names]
    if missing_inputs:
        raise ValueError(f"input tree(s) not found in input file: {missing_inputs}")

    # open output
    t_output = ROOT.TFile(output_file, "RECREATE", "", compression_flag)

    # copy objects to bypass
    # note: this assumes a flat input file structure without nested directories
    saved_names = []
    if bypass_objects:
        for key in t_input.GetListOfKeys():
            name = key.GetName()
            if not law.util.multi_match(name, bypass_objects, mode=any):
                continue
            obj = key.ReadObj()
            t_output.cd()
            # copy trees, use other objects "as is"
            copy = obj.CloneTree(-1, "FAST") if isinstance(obj, ROOT.TTree) else obj
            copy.Write(name)
            saved_names.append(name)

    # check that output trees do not collide with names of bypassed objects
    colliding_names = [name for name in output_tree_names if name in saved_names]
    if colliding_names:
        raise ValueError(f"output tree names collide with bypassed object names: {colliding_names}")

    # ROOT data frames cannot write into opened files, so close it
    t_output.Close()

    # enable multi-threading of RDataFrame event loop
    if n_threads > 1:
        ROOT.ROOT.EnableImplicitMT(n_threads)

    # loop over skim configs and apply them to extract trees via data frames
    for skim_config in skim_configs:
        # get the input tree and create a data frame
        in_tree = t_input.Get(skim_config.input_tree)
        df = ROOT.RDataFrame(in_tree)

        # filter
        df = df.Filter(skim_config.selection)

        # determine branches to save
        branches = [b.GetName() for b in in_tree.GetListOfBranches()]
        if skim_config.column_filters:
            n_before = len(branches)
            branches = filter_branches(branches, skim_config.column_filters)
            n_drop = n_before - len(branches)
            print(f"tree '{skim_config.output_tree}': drop {n_drop} / {n_before} branches")

        # save the snapshot
        opts = ROOT.RDF.RSnapshotOptions()
        opts.fMode = "UPDATE"
        if compression:
            opts.fCompressionAlgorithm = compression_algo
            opts.fCompressionLevel = compression_level
        df.Snapshot(skim_config.output_tree, output_file, branches, opts)

    # close files
    t_input.Close()

    # move the output file if tmp
    if not orig_output_file:
        shutil.move(output_file, input_file)
        output_file = input_file

    return output_file


def filter_branches(branches: list[str], column_filters: list[str]) -> list[str]:
    """
    Filters a list of *branches* by applying *column_filters*. A filter is a string in the format
    "(keep|drop) pattern" where the pattern can be a glob pattern or a regular expression. To be
    interpreted as a regular expression, the pattern must start with "^" and/or end with "$".
    """
    # strategy
    # - keep list of selected and discarded branches
    # - go through filters and move branches between lists
    # - finally sort according to original order
    orig_branches = branches
    selected = list(branches)
    discarded: list[str] = []

    filter_str_cre = re.compile(r"^(keep|drop)\s+(.+)$")
    for filter_str in column_filters:
        filter_str = filter_str.strip()

        # parse filter string
        if not (m := filter_str_cre.match(filter_str)):
            raise ValueError(f"invalid column filter format: {filter_str}")
        action, pattern = m.groups()

        # define a matching function
        is_re = pattern.startswith("^") or pattern.endswith("$")
        match = (lambda s: bool(re.match(pattern, s))) if is_re else (lambda s: fnmatch(s, pattern))

        # determine src and dst lists
        src, dst = (discarded, selected) if action == "keep" else (selected, discarded)

        # move branches in-place
        moved_indices = []
        for i, branch in enumerate(src):
            if match(branch):
                dst.append(branch)
                moved_indices.append(i)
        for i in reversed(moved_indices):
            src.pop(i)

    # sort according to original order
    selected.sort(key=orig_branches.index)

    return selected
