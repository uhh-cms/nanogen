# coding: utf-8

"""
Tasks dealing with external data.
"""

from __future__ import annotations

import json

import luigi  # type: ignore[import-untyped]
import law  # type: ignore[import-untyped]

from nanogen.tasks.base import Task, ConfigTask, DatasetTask, wrapper_factory
from nanogen.nano_util import das_query, locate_lfn, fetch_lfn, MissingLFNException
from nanogen.util import maybe_wait_for_dcache


class ListDatasetStats(ConfigTask, law.tasks.RunOnceTask):

    dataset_names = law.CSVParameter(
        default=(),
        description="comma-separated list of dataset names or patterns; when empty, all datasets "
        "in the config are considered; no default",
    )
    skip_dataset_names = law.CSVParameter(
        default=(),
        description="comma-separated list of dataset names or patterns to skip; no default",
    )

    version = None
    sandbox = "bash::/cvmfs/cms.cern.ch/cmsset_default.sh"

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
            dataset_name: self.target(f"stats_{dataset_name}.json")
            for dataset_name in self.selected_dataset_names
        })

    @law.decorator.log
    def run(self):
        from tabulate import tabulate  # type: ignore[import-untyped]

        # load stats, considering present outputs as caches
        stats = {}
        outputs = self.output().targets
        for dataset_name in self.selected_dataset_names:
            if outputs[dataset_name].exists():
                stats[dataset_name] = outputs[dataset_name].load(formatter="json")
                continue

            # fetch info
            dataset_key = self.datasets[dataset_name].key
            res = das_query(f"dataset={dataset_key}", args="--json", log=self.logger.debug)
            for entry in json.loads(res):
                # extract stats
                if "dbs3:filesummaries" in entry.get("das", {}).get("services", []):
                    summary = entry["dataset"][0]
                    stats[dataset_name] = {
                        "size": summary["size"],
                        "n_files": summary["nfiles"],
                        "n_events": summary["nevents"],
                    }
                    outputs[dataset_name].dump(stats[dataset_name], indent=4, formatter="json")
                    break
            else:
                self.logger.warning(f"no dbs3:filesummaries in response for dataset {dataset_name}")
                stats[dataset_name] = {"size": "-", "n_files": "-", "n_events": "-"}

        # fill table rows
        rows = []
        for dataset_name, _stats in stats.items():
            rows.append([
                dataset_name,
                law.util.human_bytes(_stats["size"], unit="GB")[0],
                _stats["n_files"],
                _stats["n_events"],
            ])

        # add a summary line
        if len(rows) > 1:
            rows.append([
                "Total",
                sum(row[1] for row in rows if row[1] != "-"),
                sum(row[2] for row in rows if row[2] != "-"),
                sum(row[3] for row in rows if row[3] != "-"),
            ])

        # create the table
        headers = ["Dataset", "Size / GB", "Files", "Events"]
        print(tabulate(rows, headers=headers, tablefmt="fancy_grid", floatfmt="_.1f", intfmt="_"))


class GetDatasetLFNs(DatasetTask):

    validate = luigi.BoolParameter(
        default=False,
        description="whether to validate the presence of every lfn in the dataset and remove them "
        "in case they are not available; default: False",
    )

    version = None
    sandbox = "bash::/cvmfs/cms.cern.ch/cmsset_default.sh"

    def output(self):
        outputs = law.util.DotDict()
        outputs.lfns = self.target("lfns.json")
        if self.validate:
            outputs.missing = self.target("missing.json")
        return outputs

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

                # get the names of sites where the file is reported to exist
                sites = [
                    line.strip()
                    for line in das_query(f"site file={lfn}", log=self.publish_message).split("\n")
                    if line.strip()
                ]
                # store info
                missing_lfns.add(lfn)
                missing_data["missing"].append({
                    "lfn": lfn,
                    "sites": sites,
                })

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


class FetchLFN(Task):

    lfn = luigi.Parameter(
        description="the LFN to fetch",
    )
    locations = law.CSVParameter(
        default=(),
        description="comma-separated locations to fetch the lfn from; can refer to a 'fs' in the "
        "law config, an uri of a location (including protocol), or a site name (e.g. T2_DE_DESY); "
        "when empty, DAS is queried for locations; no default",
    )

    version = None
    sandbox = "bash::/cvmfs/cms.cern.ch/cmsset_default.sh"

    def output(self):
        return self.target(self.lfn)

    @law.decorator.log
    @maybe_wait_for_dcache
    def run(self):
        with self.publish_step("fetching LFN ..."), self.output().localize("w") as out:
            fetch_lfn(self.lfn, out.abspath, locations=self.locations or None, logger=self.logger)
