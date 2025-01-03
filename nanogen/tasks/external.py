# coding: utf-8

"""
Tasks dealing with external data.
"""

from __future__ import annotations

import os
from collections import defaultdict
from multiprocessing.dummy import Pool as ThreadPool

import luigi  # type: ignore[import-untyped]
import law  # type: ignore[import-untyped]

from nanogen.tasks.base import Task, ConfigTask, DatasetTask, wrapper_factory, user_parameter
from nanogen.nano_util import (
    das_query, load_dataset_stats, locate_lfn, fetch_lfn, sort_sites_opinionated,
    MissingLFNException,
)
from nanogen.util import wget, maybe_wait_for_dcache


class ListDatasetStats(ConfigTask, law.tasks.RunOnceTask):

    dataset_names = law.CSVParameter(
        default=(),
        description="comma-separated list of dataset names or patterns; when empty, all datasets "
        "in the config are considered; no default",
        brace_expand=True,
    )
    skip_dataset_names = law.CSVParameter(
        default=(),
        description="comma-separated list of dataset names or patterns to skip; no default",
        brace_expand=True,
    )
    sites = luigi.BoolParameter(
        default=False,
        significant=False,
        description="whether to list the sites where the datasets are available; default: False",
    )
    table_format = luigi.Parameter(
        default="fancy_grid",
        significant=False,
        description="the format of the 'tabular' table; default: fancy_grid",
    )
    user = user_parameter

    version = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get selected datasets
        self.selected_dataset_names = [
            name for name in list(self.datasets.keys())
            if (
                law.util.multi_match(name, self.dataset_names or ["*"], mode=any) and
                not law.util.multi_match(name, self.skip_dataset_names, mode=any)
            )
        ]

    def output(self):
        return law.SiblingFileCollection({
            dataset_name: law.util.DotDict({
                key: self.target(f"sites_{dataset_name}.json")
                for key in ["stats", "sites"]
                if key == "stats" or self.sites
            })
            for dataset_name in self.selected_dataset_names
        })

    @law.decorator.notify
    @law.decorator.log
    @law.tasks.RunOnceTask.complete_on_success
    def run(self):
        from tabulate import tabulate  # type: ignore[import-untyped]

        # prepare cache outputs
        outputs = self.output().targets

        # helper to load stats, either from cache or by querying DAS
        def load_stats(dataset_name):
            if outputs[dataset_name].stats.exists():
                stats = outputs[dataset_name].stats.load(formatter="json")
            else:
                # fetch info
                dataset_key = self.datasets[dataset_name].key
                stats = load_dataset_stats(dataset_key)
                outputs[dataset_name].stats.dump(stats, indent=4, formatter="json")

            # add sites
            if self.sites:
                if outputs[dataset_name].sites.exists():
                    sites = outputs[dataset_name].sites.load(formatter="json")
                else:
                    sites = [
                        site
                        for site in das_query(f"site dataset={dataset_key}").split("\n")
                        if not site.lower().endswith(("_tape", "_disk"))
                    ]
                    outputs[dataset_name].sites.dump(sites, indent=4, formatter="json")
                stats["sites"] = sites

            return stats

        # load stats in parallel
        with ThreadPool(4) as pool:
            stats = dict(zip(
                self.selected_dataset_names,
                pool.map(load_stats, self.selected_dataset_names),
            ))

        # helper to convert from bytes to GB
        gb = lambda b: b / 1024**3

        # fill table rows
        rows = []
        for dataset_name, s in stats.items():
            rows.append([
                dataset_name,
                "-" if s["size"] is None else gb(s["size"]),
                "-" if s["n_files"] is None else s["n_files"],
                "-" if s["n_events"] is None else s["n_events"],
            ])
            if self.sites:
                rows[-1].append(
                    "-" if not s["sites"] else ",".join(sort_sites_opinionated(s["sites"])),
                )

        # add a summary line
        if len(rows) > 1:
            rows.append([
                "Total",
                gb(sum(s["size"] for s in stats.values() if s["size"] is not None)),
                sum(s["n_files"] for s in stats.values() if s["n_files"] is not None),
                sum(s["n_events"] for s in stats.values() if s["n_events"] is not None),
            ])
            if self.sites:
                rows[-1].append(
                    len(set(sum((s["sites"] for s in stats.values() if s["sites"]), []))),
                )

        # create the table
        headers = ["Dataset", "Size / GB", "Files", "Events"]
        if self.sites:
            headers.append("Sites")
        print(tabulate(
            rows, headers=headers, tablefmt=self.table_format, floatfmt="_.1f", intfmt="_",
        ))


class GetDatasetLFNs(DatasetTask):

    validate = luigi.BoolParameter(
        default=False,
        description="whether to validate the presence of every lfn in the dataset and remove them "
        "in case they are not available; default: False",
    )
    user = user_parameter

    version = None

    def output(self):
        outputs = law.util.DotDict()
        outputs.lfns = self.target("lfns.json")
        if self.validate:
            outputs.missing = self.target("missing.json")
        return outputs

    @law.decorator.notify
    @law.decorator.log
    @maybe_wait_for_dcache
    def run(self):
        # get lfns
        lfns = [
            line.strip()
            for line in das_query(
                f"file dataset={self.dataset.key}",
                log=self.publish_message,
            ).split("\n")
            if line.strip().endswith(".root")
        ]
        self.publish_message(f"found {len(lfns)} LFNs for dataset {self.dataset.key}")
        if not lfns:
            raise Exception("no LFNs found")

        # sort them to always deal with a deterministic order
        lfns.sort()

        # validate
        outputs = self.output()
        missing_lfns = set()
        if self.validate:
            missing_data = {
                "n_total": len(lfns),
                "n_missing": None,
                "missing": [],
            }
            for lfn in self.iter_progress(lfns, len(lfns), msg="validating LFNs ..."):
                try:
                    locate_lfn(lfn, logger=self.logger)
                    continue
                except MissingLFNException:
                    self.logger.warning(f"LFN {lfn} not found")
                    # store info
                    missing_lfns.add(lfn)
                    missing_data["missing"].append(lfn)

            # reduce lfns and show some logs
            n_missing = missing_data["n_missing"] = len(missing_lfns)
            missing_frac = n_missing / len(lfns) * 100
            self.publish_message(f"{n_missing} of {len(lfns)} LFNs missing ({missing_frac:.2f}%)")
            if missing_lfns:
                lfns = [lfn for lfn in lfns if lfn not in missing_lfns]
                sites = set(sum((d["sites"] for d in missing_data["missing"]), []))
                self.publish_message(f"sites falsely reporting existince: {', '.join(sites)}")
            outputs.missing.dump(missing_data, indent=4, formatter="json")

        # save lfns
        outputs.lfns.dump(lfns, indent=4, formatter="json")

        # in case of real data, issue a fatal warning
        if missing_lfns and self.mini_info.data:
            self.logger.fatal(
                "lfns, both present and missing, were saved but since this is real data make sure "
                "to follow-up on missing ones:",
            )
            for lfn in missing_lfns:
                self.logger.fatal(f"  {lfn}")


GetDatasetLFNsWrapper = wrapper_factory(
    base_cls=ConfigTask,
    require_cls=GetDatasetLFNs,
    cls_name="GetDatasetLFNsWrapper",
    enable=["datasets", "skip_datasets"],
    attributes={"version": None},
)


class FetchLumiMask(ConfigTask):

    user = user_parameter
    version = None

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.src_file = self.config.lumi_mask

    def output(self):
        return self.target(f"lumi_mask_{law.util.create_hash(self.src_file, 8)}.json")

    @law.decorator.notify
    @law.decorator.log
    def run(self):
        # prepare the output
        output = self.output()
        output.parent.touch()

        # fetch
        if self.src_file.startswith(("http://", "https://")):
            # download via wget
            wget(self.src_file, output.abspath)
        else:
            # must be a local file
            output.copy_from_local(self.src_file)
        self.publish_message(f"fetched {self.src_file}")


class PrepareForConfig(ConfigTask, law.WrapperTask):

    user = user_parameter
    version = None

    def requires(self):
        return {
            "lumi_mask": FetchLumiMask.req(self),
            "lfns": GetDatasetLFNsWrapper.req(self),
        }


class FetchLFN(Task):

    lfn = luigi.Parameter(
        description="the LFN to fetch",
    )
    locations = law.CSVParameter(
        default=(),
        significant=False,
        description="comma-separated locations to fetch the lfn from; can refer to a 'fs' in the "
        "law config, an uri of a location (including protocol), or a site name (e.g. T2_DE_DESY); "
        "when empty, DAS is queried for locations; no default",
    )

    version = None
    sandbox = "bash::/cvmfs/cms.cern.ch/cmsset_default.sh"
    priority = 15

    def output(self):
        return self.target(self.lfn)

    @law.decorator.notify
    @law.decorator.log
    @maybe_wait_for_dcache
    def run(self):
        # hotfix: if all wlcg protocols worked normally, we could just the larget localization
        # feature with mode "w" here, but since there are issues with webdav and xrootd plugins in
        # slc7, and with gsiftp on the desy dcache in general, fetch into a tmp file and move it
        # manually
        with self.publish_step("fetching LFN ..."):
            tmp = law.LocalFileTarget(is_tmp=".root")
            fetch_lfn(self.lfn, tmp.abspath, locations=self.locations or None, logger=self.logger)

        with self.publish_step("moving LFN to output location ..."):
            output = self.output()
            uri = output.uri(base_name="xrootd")
            if law.target.file.get_scheme(uri) == "root":
                cmd = f"xrdcp -f {tmp.abspath} {uri}"
                code = law.util.interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
                if code != 0:
                    raise Exception(f"failed to xrdcp {tmp.abspath} to {uri}")
            else:
                output.move_from_local(tmp)


class FetchLFNWrapper(Task, law.WrapperTask):

    lfns = law.CSVParameter(
        description="comma-separated list of LFNs to fetch",
    )
    locations = law.MultiCSVParameter(
        default=(),
        description="colon-separated sequences of comma-separated locations to fetch the lfns from; "
        "when only a single sequence is passed, it is used for all lfns; otherwise, the number of "
        "sequences must match the number of lfns; no default",
    )
    user = user_parameter

    version = None

    @classmethod
    def modify_param_values(cls, params):
        if "lfns" in params and "locations" in params:
            if len(params["locations"]) == 1:
                params["locations"] *= len(params["lfns"])
            elif len(params["locations"]) not in {0, len(params["lfns"])}:
                raise ValueError("number of location sequences must match number of LFNs")
        return params

    def requires(self):
        return {
            lfn: FetchLFN(lfn=lfn, locations=locs)
            for lfn, locs in zip(self.lfns, self.locations or ([()] * len(self.lfns)))
        }


class CheckLocalPFNs(DatasetTask, law.tasks.RunOnceTask):

    PREFIX_DESY = "/pnfs/desy.de/cms/tier2"

    prefix = luigi.Parameter(
        default=PREFIX_DESY,
        description=f"the prefix to prepend to lfns to obtain a pfn; default: {PREFIX_DESY}",
    )
    show_lfn = luigi.BoolParameter(
        default=False,
        significant=False,
        description="whether to show the lfns in the output rather than pfns; default: False",
    )
    only_missing = luigi.BoolParameter(
        default=False,
        significant=False,
        description="whether to only show missing pfns; default: False",
    )
    locate = luigi.BoolParameter(
        default=False,
        significant=False,
        description="whether to print locations of lfns; default: False",
    )
    user = user_parameter

    version = None

    def requires(self):
        return GetDatasetLFNs.req(self)

    @law.decorator.notify
    @law.decorator.log
    @law.tasks.RunOnceTask.complete_on_success
    def run(self):
        lfns = self.input().lfns.load(formatter="json")

        # color helper
        c = law.util.colored

        n = 0
        with self.publish_step(f"checking {len(lfns)} local PFNs ..."):
            for i, lfn in enumerate(lfns):
                pfn = f"{self.prefix.rstrip('/')}/{lfn.lstrip('/')}"
                exists = int(os.path.exists(pfn))
                if exists and self.only_missing:
                    continue
                if not exists:
                    n += 1
                key = c(
                    ["missing", "exists "][exists],
                    color=["red", "green"][exists],
                    style="bright",
                )
                self.publish_message(f"{key}: {lfn if self.show_lfn else pfn} ({i})")
                if self.locate:
                    locs = defaultdict(set)
                    for loc in locate_lfn(lfn, silent=True):
                        locs[loc.fs or loc.site].add(loc.scheme)
                    if locs:
                        msg = f"locations ({c(len(locs), color='green' if locs else 'red')}): "
                        msg += ", ".join(
                            f"{c(site, style='bright')}({'|'.join(filter(bool, schemes))})"
                            for site, schemes in locs.items()
                        )
                        if "T2_DE_DESY" not in locs:
                            msg += f" ({c('not at DESY', color='red')})"
                    else:
                        msg = c("no locations found", color="red", style="bright")
                    self.publish_message(f"  -> {msg}")

        n_str = c(n, color="red" if n else "green")
        self.publish_message(f"missing: {n_str} of {len(lfns)}")


CheckLocalPFNsWrapper = wrapper_factory(
    base_cls=ConfigTask,
    require_cls=CheckLocalPFNs,
    cls_name="CheckLocalPFNsWrapper",
    enable=["datasets", "skip_datasets"],
    attributes={"version": None},
)
