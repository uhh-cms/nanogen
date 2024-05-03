# coding: utf-8

"""
Custom bbtt tasks.
"""

from __future__ import annotations

import functools
import operator

import law  # type: ignore[import-untyped]

from nanogen.tasks.base import ConfigTask, wrapper_factory
from nanogen.tasks.remote import RemoteWorkflow
from nanogen.tasks.nano import NanoDatasetWorkflow, CreateNano
from nanogen.nano_util import iter_root_coffea_events
from nanogen.util import maybe_local_target


class ReduceNano(NanoDatasetWorkflow, RemoteWorkflow):

    task_namespace = "bbtt"

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs["nano"] = CreateNano.req_different_branching(self)
        return reqs

    def lfns_per_task(self, n_lfns: int) -> int:
        # handle all in one
        return n_lfns

    def requires(self):
        reqs = super().requires()
        reqs["nano"] = CreateNano.req_different_branching(
            self,
            branch=-1,
            workflow="local",
            branches=tuple(self.branch_data),
        )
        return reqs

    workflow_condition = NanoDatasetWorkflow.workflow_condition.copy()

    @workflow_condition.output
    def output(self):
        return self.target(f"output_{self.branch}.parquet")

    @law.decorator.log
    def run(self):
        import awkward as ak  # type: ignore[import-untyped]

        # prepare inputs
        inputs = [maybe_local_target(inp) for inp in self.input().nano.collection.targets.values()]
        self.publish_message(f"processing {len(inputs)} inputs ...")

        # storage for output array chunks
        output = []

        # progress callback
        def progress_callback(i_source, n_sources, i_chunk, n_chunks):
            self.publish_progress(100 / n_sources * (i_source + i_chunk / n_chunks))

        # loop over input chunks
        n_in = 0
        for events in iter_root_coffea_events(
            source=inputs,
            branches=[
                "run", "luminosityBlock", "event",
                "nGenPart", "GenPart_*", "Jet_*", "Tau_*", "PFCandidate_*", "PFCandidateIndices_*",
            ],
            callback=progress_callback,
        ):
            n_in += len(events)

            # events should have at least two taus and to jets
            events = events[
                (ak.num(events.Tau) >= 2) &
                (ak.num(events.Jet) >= 2)
            ]

            # find Higgs bosons at the hard interaction vertex
            events["GenH"] = events.GenPart[
                (events.GenPart.pdgId == 25) &
                (events.GenPart.hasFlags("isHardProcess"))
            ]

            # find direct decay products (b quarks and tau leptons)
            events["GenHDecay"] = events.GenH.distinctChildrenDeep

            # create a mask to extract taus, to select only fully hadronically decaying pairs
            events["h_tau_mask"] = ak.any(abs(events.GenHDecay.pdgId) == 15, axis=2)
            events["GenHTau"] = events.GenHDecay[events.h_tau_mask][:, 0]
            had_mask = ak.max(abs(events.GenHTau.distinctChildrenDeep.pdgId), axis=2) >= 100
            di_had_mask = ak.all(had_mask, axis=1)
            events = events[di_had_mask]
            events["GenHTau"] = events.GenHTau[:, [0, 1]]

            # find matched reconstructed taus
            dr = events.Tau.metric_table(events.GenHTau)
            events["matched_tau_indices"] = ak.argmin(dr, axis=1)
            close_and_unique_tau_matches = (
                ak.all(ak.min(dr, axis=1) <= 0.3, axis=1) &
                (events.matched_tau_indices[:, 0] != events.matched_tau_indices[:, 1])
            )
            events["MatchedTau"] = events.Tau[events.matched_tau_indices]
            events = events[close_and_unique_tau_matches]

            # select b quarks and find matched reconstructed jets
            events["GenHB"] = events.GenHDecay[~events.h_tau_mask][:, 0]
            dr = events.Jet.metric_table(events.GenHB)
            events["matched_b_indices"] = ak.argmin(dr, axis=1)
            close_and_unique_b_matches = (
                ak.all(ak.min(dr, axis=1) <= 0.3, axis=1) &
                (events.matched_b_indices[:, 0] != events.matched_b_indices[:, 1])
            )
            events["MatchedJet"] = events.Jet[events.matched_b_indices]
            events = events[close_and_unique_b_matches]

            # find pf candidates of match jets and taus
            events["MatchedJetConstituents"] = ak.concatenate(
                [
                    events.PFCandidate[
                        events.PFCandidateIndices.jet == events.matched_b_indices[:, i]
                    ][:, None, ...]
                    for i in range(2)
                ],
                axis=1,
            )
            events["MatchedTauConstituents"] = ak.concatenate(
                [
                    events.PFCandidate[
                        events.PFCandidateIndices.tau == events.matched_tau_indices[:, i]
                    ][:, None, ...]
                    for i in range(2)
                ],
                axis=1,
            )

            # add the first four unmatched jets and all unmatched taus
            jet_index = ak.local_index(events.Jet.pt)
            events["UnmatchedJet"] = events.Jet[
                ~functools.reduce(
                    operator.or_,
                    [jet_index == events.matched_b_indices[:, i] for i in range(2)],
                )
            ][:, :4]
            tau_index = ak.local_index(events.Tau.pt)
            events["UnmatchedTau"] = events.Tau[
                ~functools.reduce(
                    operator.or_,
                    [tau_index == events.matched_tau_indices[:, i] for i in range(2)],
                )
            ]

            # select and store columns to save
            output.append(ak.zip(
                {
                    field: ak.drop_none(events[field])
                    for field in [
                        "run", "luminosityBlock", "event",
                        "GenH", "GenHTau", "GenHB",
                        "MatchedJet", "MatchedTau",
                        "MatchedJetConstituents", "MatchedTauConstituents",
                        "UnmatchedJet", "UnmatchedTau",
                    ]
                },
                depth_limit=1,
            ))

        # concatenate and save
        output = ak.concatenate(output, axis=0)

        # save
        self.output().dump(
            output,
            formatter="awkward",
            compression="zstd",
            compression_level=1,
            parquet_dictionary_encoding=False,
        )
        self.publish_progress(100)

        # some logs
        size_in = sum(inp.stat().st_size for inp in inputs)
        size_out = self.output().stat().st_size
        n_out = len(output)
        self.publish_message(
            f"events saved: {n_out:_} ({law.util.human_bytes(size_out, fmt=True)}) of "
            f"{n_in:_} ({law.util.human_bytes(size_in, fmt=True)}) "
            f"-> {n_out / n_in * 100:.2f}%",
        )


ReduceNanoWrapper = wrapper_factory(
    base_cls=ConfigTask,
    require_cls=ReduceNano,
    cls_name="ReduceNanoWrapper",
    enable=["datasets", "skip_datasets"],
    attributes={"task_namespace": "bbtt"},
)
