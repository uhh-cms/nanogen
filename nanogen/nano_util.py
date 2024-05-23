# coding: utf-8

"""
Utility functions for dealing with nano files, cmsRun configs and cmssw process customizations.
"""

from __future__ import annotations

__all__: list[str] = []

import os
import re
import gc
import math
import json
import shutil
import logging
import subprocess
import urllib.parse
import contextlib
from importlib import import_module
from fnmatch import fnmatch
from copy import deepcopy
from collections import Counter
from dataclasses import dataclass
from tempfile import mkstemp
from typing import Any, Callable

import law  # type: ignore[import-untyped]
from law.target.file import has_scheme, get_scheme, add_scheme, remove_scheme  # type: ignore[import-untyped] # noqa

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


def das_query(
    query: str,
    args: str | list[str] | None = None,
    log: Callable[[str], Any] | None = None,
) -> str:
    log = log or print

    # build the command
    cmd = f"dasgoclient -query='{query}'"
    if args:
        cmd += f" {law.util.quote_cmd(law.util.make_list(args))}"
    if "-limit" not in cmd:
        cmd += " -limit=0"
    if "-dasmaps" not in cmd and (dasmaps := os.getenv("NG_DASMAPS_BASE")):
        cmd += f" -dasmaps={dasmaps}"

    # run it
    log(f"cmd: {cmd}")
    code, out, _ = law.util.interruptable_popen(
        cmd,
        stdout=subprocess.PIPE,
        shell=True,
        executable="/bin/bash",
    )
    if code != 0:
        raise Exception(f"dasgoclient query failed:\n{out}")

    return out.strip()


def load_dataset_stats(dataset_key: str) -> dict[str, Any]:
    stats = {}
    found_info = False
    found_summaries = False

    for entry in json.loads(das_query(f"dataset={dataset_key}", args="-json")):
        # dataset info
        if not found_info and "dbs3:dataset_info" in entry.get("das", {}).get("services", []):
            info = entry["dataset"][0]
            stats["dataset_id"] = info["dataset_id"]
            found_info = True

        # file summaries
        if not found_summaries and "dbs3:filesummaries" in entry.get("das", {}).get("services", []):
            summary = entry["dataset"][0]
            stats["size"] = summary["size"]
            stats["n_files"] = summary["nfiles"]
            stats["n_events"] = summary["nevents"]
            found_summaries = True

    if not found_info:
        raise Exception(f"dataset info not found for {dataset_key}")
    if not found_summaries:
        raise Exception(f"file summaries not found for {dataset_key}")

    return stats


def load_lfn_stats(lfn: str) -> dict[str, Any]:
    file_info = json.loads(das_query(f"file={lfn}", args="-json"))[0]["file"][0]
    return {
        "n_events": file_info["nevents"],
        "size": file_info["size"],
        "checksum": file_info["adler32"],
    }


class MissingLFNException(Exception):

    def __init__(self, lfn: str, reason: str | None = None):
        msg = f"LFN {lfn} not found"
        if reason:
            msg += f", {reason}"
        super().__init__(msg)


def sort_sites_opinionated(sites: list[str]) -> list[str]:
    # sort DESY -> DE -> CH -> Rest -> US -> T3 -> TW -> IN -> RU
    return sorted(sites, key=lambda l: (
        -("DESY" in l),
        -((country := l.split("_")[1]) == "DE"),
        -(country == "CH"),
        # rest goes here
        +(country == "RU"),
        +(country == "IN"),
        +(country == "TW"),
        +(l.split("_")[0] not in {"T1", "T2"}),
        +(country == "US"),
    ))


def resolve_lfn_to_site(lfn: str, site: str) -> list[str]:
    # helper to determine a storage settings path
    def get_settings_path(site: str) -> str:
        return os.path.join("/cvmfs/cms.cern.ch/SITECONF", site, "storage.json")

    # determine the settings path and consider cases where a file for that specific site is not
    # available but a more general file is used (e.g. T1_UK_RAL_Disk isin T1_UK_RAL), and in these
    # cases, enforce that the so-called rse entry of the settings must match the requested site
    settings_path = get_settings_path(site)
    must_match_rse = False
    if not os.path.exists(settings_path) and (m := re.match(r"^(T\d_[^_]+_[^_]+)_.+$", site)):
        must_match_rse = True
        settings_path = get_settings_path(m.group(1))
    if not os.path.exists(settings_path):
        raise Exception(f"no storage settings found for site {site}")

    # load the settings
    with open(settings_path, "r") as f:
        site_settings = json.load(f)

    # apply rules per protocol and collect pfns
    pfns = []
    for entry in site_settings:
        # skip certain storage types
        if entry.get("type", "").lower() == "tape":
            continue

        # skip certain volumes
        vol = entry.get("volume", "")
        if vol.lower().endswith(("_xcache", "_tape")):
            continue

        # optionally require the rse entry to match
        if must_match_rse and entry.get("rse", "") != site:
            continue

        # select pfns per protocol
        proto_pfns = []
        for proto_entry in entry.get("protocols", []):
            # skip certain protocols (mixed refers to xcache, xrootd-module and file to site local)
            proto = proto_entry.get("protocol", "")
            if proto.lower() in {"mixed", "mixed-fuse", "mixed-xrootd", "xrootd-module", "file"}:
                continue

            # skip certain access types
            if proto_entry.get("access", "").lower() in {"virtual"}:
                continue

            if "prefix" in proto_entry:
                # when there is a prefix defined, use it as is if it's not nocal
                if get_scheme(proto_entry["prefix"]) == "file":
                    continue
                proto_pfns.append(proto_entry["prefix"] + lfn)

            elif "rules" in proto_entry:
                # check which rules applies
                for rule in proto_entry["rules"]:
                    # skip rules resulting in local paths
                    if get_scheme(rule["pfn"]) == "file" or rule["pfn"].startswith("/"):
                        continue
                    # match the lfn
                    if re.match(rule["lfn"], lfn):
                        proto_pfns.append(re.sub(rule["lfn"], rule["pfn"].replace("$", "\\"), lfn))
                        break

            # opinionated sorting: place xrootd first
            xrootd_pfns = [pfn for pfn in proto_pfns if get_scheme(pfn) == "root"]
            proto_pfns = list(filter((lambda pfn: pfn not in xrootd_pfns), proto_pfns))

            pfns.extend(xrootd_pfns)

    return pfns


@dataclass
class LFNLocation(object):
    lfn: str
    pfn: str | None = None  # optional in init, but always set
    fs: str | None = None
    site: str | None = None
    stat: os.stat_result | None = None

    def __post_init__(self) -> None:
        # either fs or site can be given
        if self.fs and self.site:
            raise ValueError("fs and site cannot be given at the same time")

        # pfn can be extracted from fs, otherwise it must be given
        if not self.pfn:
            if not self.fs:
                raise ValueError("a pfn must be given when no fs is set")
            self.pfn = self.create_target().uri(base_name="filecopy")

    def __str__(self) -> str:
        return f"LFNLocation({self.fs or self.site or self.pfn})"

    def __repr__(self) -> str:
        return str(self)

    @property
    def scheme(self) -> str:
        return get_scheme(self.pfn)

    @property
    def is_local(self) -> bool:
        base = law.config.get_expanded(self.fs, "base") if self.fs else self.pfn
        return get_scheme(base) in (None, "file")

    def create_target(self) -> law.LocalFileTarget | law.wlcg.WLCGFileTarget:
        if self.fs:
            target_cls = law.LocalFileTarget if self.is_local else law.wlcg.WLCGFileTarget
            return target_cls(self.lfn, fs=self.fs)

        # extract info from the pfn and register a law fs for the site if not already existing
        url = urllib.parse.urlparse(self.pfn)
        fs = f"wlcg_fs_{str(url.scheme)}_{str(url.hostname)}"  # type: ignore[union-attr]
        if not law.config.has_section(fs):
            law.config.update({fs: {"base": add_scheme(url.netloc, url.scheme)}})
        return law.wlcg.WLCGFileTarget("/" + str(url.path).lstrip("/"), fs=fs)


def locate_lfn(
    lfn: str,
    locations: str | list[str] | None = None,
    enable_xrd: bool = True,
    logger: logging.Logger | Callable | None = None,
) -> list[LFNLocation]:
    # prepare logging
    log_debug = log_info = print
    if logger:
        log_debug = getattr(logger, "debug", logger)  # type: ignore[arg-type]
        log_info = getattr(logger, "info", logger)  # type: ignore[arg-type]

    # get default locations from DAS
    if not locations:
        # get non-tape locations
        locations = das_query(f"site file={lfn}", log=log_debug).split("\n")
        # remove tapes
        locations = [l for l in locations if not l.lower().endswith("_tape")]
        # sort
        locations = sort_sites_opinionated(locations)
        # complain when there are no locations to check
        if not locations:
            raise MissingLFNException(lfn, "DAS reported no available sites")
        log_info(f"DAS reported {len(locations)} location(s): {', '.join(locations)}")

    # create location objects
    lfn_locations = []
    for location in locations:
        # site, fs, or a schemed prefix (e.g. root://...)?
        if re.match(r"^T[0-9]_\w{2}_.+$", location):
            # expand available protocols
            for pfn in resolve_lfn_to_site(lfn, location):
                lfn_locations.append(LFNLocation(lfn=lfn, pfn=pfn, site=location))
        elif has_scheme(location):
            lfn_locations.append(LFNLocation(lfn=lfn, pfn=location + lfn))
        else:
            lfn_locations.append(LFNLocation(lfn=lfn, fs=location))

    if not lfn_locations:
        raise MissingLFNException(lfn, "no locations found")

    return lfn_locations


def fetch_lfn(
    lfn: str,
    dst: str,
    locations: str | list[str] | None = None,
    attempts: int = 2,
    fetch_local: bool = False,
    enable_xrd: bool = True,
    logger: logging.Logger | None = None,
    _lfn_locations: list[LFNLocation] | None = None,
) -> str:
    """
    Fetchs a *lfn* and stores it at *dst*. When *dst* is an existing directory, the file is stored
    with its original name. Itermediate, missing directories are created. The absolute path to the
    stored file is returned.
    """
    # prepare logging
    log_info = log_warning = print
    if logger:
        log_info = getattr(logger, "info", logger)  # type: ignore[arg-type]
        log_warning = getattr(logger, "warning", logger)  # type: ignore[arg-type]

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

    # locate the lfn
    lfn_locations = _lfn_locations
    if not lfn_locations:
        lfn_locations = locate_lfn(lfn, locations=locations, enable_xrd=enable_xrd, logger=logger)

    # when there is a local location, just return the path
    for lfn_location in lfn_locations:
        if lfn_location.is_local and not fetch_local:
            return remove_scheme(lfn_location.pfn)

    # fetch the file
    for attempt, lfn_location in sum([list(enumerate(attempts * [l])) for l in lfn_locations], []):
        log_info(f"fetching {lfn_location.pfn} to {abs_dst}, attempt {attempt} ...")
        if lfn_location.scheme == "root" and enable_xrd:
            cmd = f"xrdcp -f {lfn_location.pfn} {abs_dst}"
            code = law.util.interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
            if code == 0:
                break
            log_warning(f"xrdcp failed for {lfn_location.pfn}")

        else:
            try:
                lfn_location.create_target().copy_to_local(abs_dst)
                break
            except Exception as e:
                log_warning(f"failed to fetch {lfn_location.pfn}: {e}")
    else:
        raise MissingLFNException(lfn, "all locations failed")

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
    for i, line in enumerate(lines):
        if line == "# End of customisation functions":
            lines[i] = f"""
# nanogen customization hook
from {hook[0]} import {hook[1]}
process = {hook[1]}(process, **{hook_kwargs})

{line}"""
            break
    else:
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
    want_summary: bool = False,
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

    # set reporting frequency
    process.MessageLogger.cerr.FwkReport.reportEvery = report_every

    # set summary
    process.options.wantSummary = cms.untracked.bool(want_summary)

    # set max events
    process.maxEvents.input = cms.untracked.int32(max_events)
    process.configurationMetadata.annotation = cms.untracked.string(f"NANO evts:{max_events}")

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


def iter_root_coffea_events(
    source: Any | law.FileSystemFileTarget | list[Any | law.FileSystemFileTarget],
    treepath: str = "Events",
    chunk_size: int = 30_000,
    branches: list[str] | None = None,
    callback: Callable[[int, int, int, int], Any] | None = None,
):
    """
    TODO: coffea.nanoevents.NanoEventsFactory leaks memory, which is known but unresolved?
    """
    import uproot  # type: ignore[import-untyped]
    import coffea.nanoevents  # type: ignore[import-untyped]

    # helper context to localize and uproot open a file target
    @contextlib.contextmanager
    def uproot_open_target(target):
        with target.localize("r") as tmp:
            # print(f"localized {target.uri()}")
            try:
                yield uproot.open(tmp.abspath)
            except:
                print(f"error occurred while processing {target.uri()}")
                raise

    sources = source if isinstance(source, list) else [source]
    for i, _source in enumerate(sources):
        context = (
            uproot_open_target
            if isinstance(_source, law.FileSystemFileTarget)
            else law.util.empty_context
        )

        with context(_source) as uproot_dir:
            # get the number of entries
            n_entries = uproot_dir[treepath].num_entries
            n_chunks = int(math.ceil(n_entries / chunk_size))

            # start iterating
            for j in range(0, n_chunks):
                yield coffea.nanoevents.NanoEventsFactory.from_root(
                    uproot_dir,
                    treepath=treepath,
                    entry_start=j * chunk_size,
                    entry_stop=min((j + 1) * chunk_size, n_entries),
                    delayed=False,
                    runtime_cache=None,
                    persistent_cache=None,
                    iteritems_options={"filter_name": branches},
                ).events()

                gc.collect()

                if callable(callback):
                    callback(i, len(sources), j, n_chunks)
