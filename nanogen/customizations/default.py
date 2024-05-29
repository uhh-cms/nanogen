# coding: utf-8

"""
Default nano customizations.

Note that this module will be imported as part of the nano process customization and therefore,
all packages will be provided by - and therefore must exist in - the cmssw environment.
"""

from __future__ import annotations

import os
import enum

import FWCore.ParameterSet.Config as cms  # type: ignore[import-not-found]
from PhysicsTools.NanoAOD.common_cff import Var  # type: ignore[import-not-found]


# get the cmssw version triplet
cmssw_version = tuple(map(int, os.environ["CMSSW_VERSION"].split("_")[1:4]))


class NanoVersion(enum.Enum):
    V12 = "v12"
    V14 = "v14"


#
# High-level customization functions.
#

def no_customization(process, *, dataset_kind: str, **kwargs):
    """
    No customization and returns the process unchanged.
    """
    return process


def customize_v12_uhh(process, *, dataset_kind: str, pf_candidates: bool, **kwargs):
    # update gen particle selection
    if dataset_kind == "mc":
        process = update_gen_particles(process, NanoVersion.V12)

    # add tau variables
    process = add_tau_variables(process, NanoVersion.V12)

    # add met variables
    process = add_met_variables(process, NanoVersion.V12)

    # add PF candidates
    if pf_candidates:
        process = add_pf_candidates(process, NanoVersion.V12)

    return process


def customize_v14_uhh(process, *, dataset_kind: str, pf_candidates: bool, **kwargs):
    # update gen particle selection
    if dataset_kind == "mc":
        process = update_gen_particles(process, NanoVersion.V14)

    # add tau variables, lifetime variables already present in run 3 nano
    process = add_tau_variables(process, NanoVersion.V14)

    # add met variables
    process = add_met_variables(process, NanoVersion.V14)

    # add PF candidates
    if pf_candidates:
        process = add_pf_candidates(process, NanoVersion.V14)

    return process


#
# Utils.
#

def pdg_abs_or(pdgs: list[int]) -> str:
    abs_or = " || ".join([f"abs(pdgId) == {pdg}" for pdg in pdgs])
    return f"({abs_or})"


def var_f8(expr: str, *, doc: str = "") -> Var:
    return Var(expr, float, precision=8, **({"doc": doc} if doc else {}))


def var_f10(expr: str, *, doc: str = "") -> Var:
    return Var(expr, float, precision=10, **({"doc": doc} if doc else {}))


def var_i32(expr: str, *, doc: str = "") -> Var:
    return Var(expr, int, **({"doc": doc} if doc else {}))


def var_i16(expr: str, *, doc: str = "") -> Var:
    return Var(expr, "int16", **({"doc": doc} if doc else {}))


def var_ui32(expr: str, *, doc: str = "") -> Var:
    return Var(expr, "uint", **({"doc": doc} if doc else {}))


def var_b(expr: str, *, doc: str = "") -> Var:
    return Var(expr, bool, **({"doc": doc} if doc else {}))


#
# Lower-level modular customizations.
#

def update_gen_particles(process, nano_version: NanoVersion):
    """
    With infos taken from TAU POG.
    https://github.com/cms-tau-pog/NanoProd/blob/c66e12e738528f5155043472f51452853abe9b14/NanoProd/python/customize.py#L5
    """
    vip = pdg_abs_or([6, 21, 22, 23, 24, 25, 35, 36, 37, 39, 9990012, 9900012, 1000015])
    leptons = pdg_abs_or([11, 12, 13, 14, 15, 16])
    process.finalGenParticles.select += [
        # parents and daughers of important particles
        f"keep+ statusFlags().isLastCopy() && {vip}",
        f"+keep statusFlags().isFirstCopy() && {vip}",
        # parents and full decay history of leptons
        f"keep++ statusFlags().isLastCopy() && {leptons}",
        f"+keep statusFlags().isFirstCopy() && {leptons}",
    ]

    # store the production vertex coordinate
    for coord in ["x", "y", "z"]:
        new_var = var_f10(f"vertex().{coord}", doc=f"{coord} coordinate of the production vertex")
        setattr(process.genParticleTable.variables, f"v{coord}", new_var)

    return process


def add_tau_variables(process, nano_version: NanoVersion):
    """
    Taken from TAU POG.
    """
    # additional variables
    tau_vars = process.tauTable.variables
    tau_vars.dxyErr = var_f10("dxy_error", doc="dxy error")
    if cmssw_version < (14, 0, 0):
        tau_vars.dzErr = var_f10(
            "?leadChargedHadrCand.isNonnull() && leadChargedHadrCand.hasTrackDetails() ? leadChargedHadrCand.dzError() : 1000.0",  # noqa
            doc="dz error",
        )
    else:
        # as of 14_0_0, leadChargedHadrCand is not downcasted to a packed candidate but sticks with
        # a normal candidate, which does not have hasTrackDetails(), so skip the check here and
        # assume that dzError is returning sensible values
        tau_vars.dzErr = var_f10(
            "?leadChargedHadrCand.isNonnull() ? leadChargedHadrCand.dzError() : 1000.0",  # noqa
            doc="dz error",
        )
    tau_vars.ip3d = var_f10("ip3d", doc="3D impact parameter")
    tau_vars.ip3dErr = var_f10("ip3d_error", doc="3D impact parameter error")
    tau_vars.hasSV = var_b("hasSecondaryVertex", doc="has secondary vertex")
    tau_vars.flightLengthX = var_f10("flightLength().x()", doc="flight length x")
    tau_vars.flightLengthY = var_f10("flightLength().y()", doc="flight length y")
    tau_vars.flightLengthZ = var_f10("flightLength().z()", doc="flight length z")
    tau_vars.flightLengthSig = var_f10("flightLengthSig()", doc="flight length significance")  # noqa
    tau_vars.leadTkNormChi2 = var_f10("leadingTrackNormChi2()", doc="normalized chi2 of the leading track")  # noqa
    tau_vars.leadChCandEtaAtEcalEntrance = var_f10("etaAtEcalEntranceLeadChargedCand", doc="eta of the leading charged candidate at the entrance of the ECAL")  # noqa

    # lifetime variables
    # not available in V12, already present in V14
    # if add_lifetime_vars:
    #     from PhysicsTools.NanoAOD.leptonTimeLifeInfo_common_cff import addTimeLifeInfoToTaus  # type: ignore[import-not-found] # noqa
    #     addTimeLifeInfoToTaus(process)

    return process


def add_met_variables(process, nano_version: NanoVersion):
    """
    Adds additional (Puppi)MET variables.
    """
    puppimet_vars = getattr(getattr(process, "puppiMetTable", None), "variables", None)
    if puppimet_vars is None:
        print("no puppiMetTable found, skip adding additional variables")
        return process

    puppimet_vars.covXX = var_f8(
        "getSignificanceMatrix().At(0,0)",
        doc="xx element of met covariance matrix",
    )
    puppimet_vars.covXY = var_f8(
        "getSignificanceMatrix().At(0,1)",
        doc="xy element of met covariance matrix",
    )
    puppimet_vars.covYY = var_f8(
        "getSignificanceMatrix().At(1,1)",
        doc="yy element of met covariance matrix",
    )

    return process


def add_pf_candidates(
    process,
    nano_version: NanoVersion,
    *,
    collection_name: str = "PFCandidate",
    candidate_src: str = "packedPFCandidates",
    puppi_candidates: bool = False,
    jet_src: str = "linkedObjects:jets",
    fat_jet_src: str = "finalJetsAK8",
    tau_src: str = "linkedObjects:taus",
    boosted_tau_src: str = "linkedObjects:boostedTaus",
):
    # add candidate indexes to jets and taus their are contained in
    process.load("NanoGen.NanoGen.pfCandidateIndexer_cfi")
    process.pfCandidateIndexer.candidateCollection = cms.InputTag(candidate_src)
    process.pfCandidateIndexer.jetCollection = cms.InputTag(jet_src)
    process.pfCandidateIndexer.jetIndicesName = cms.string("jet")
    process.pfCandidateIndexer.fatJetCollection = cms.InputTag(fat_jet_src)
    process.pfCandidateIndexer.fatJetIndicesName = cms.string("fatjet")
    process.pfCandidateIndexer.tauCollection = cms.InputTag(tau_src)
    process.pfCandidateIndexer.tauIndicesName = cms.string("tau")
    process.pfCandidateIndexer.boostedTauCollection = cms.InputTag(boosted_tau_src)
    process.pfCandidateIndexer.boostedTauIndicesName = cms.string("boostedtau")
    process.pfCandidateIndexer.containedCandidatesName = cms.string("containedPFCandidates")
    process.nanoTableTaskCommon.add(process.pfCandidateIndexer)
    new_candidate_src = "pfCandidateIndexer:containedPFCandidates"

    # add the nano index table
    process.load("NanoGen.NanoGen.pfCandidateIndicesTable_cfi")
    process.pfCandidateIndicesTable.tableName = cms.string(f"{collection_name}Indices")
    process.pfCandidateIndicesTable.candidateCollection = cms.InputTag(new_candidate_src)
    process.pfCandidateIndicesTable.jetIndicesCollection = cms.InputTag("pfCandidateIndexer:jet")
    process.pfCandidateIndicesTable.fatJetIndicesCollection = cms.InputTag("pfCandidateIndexer:fatjet")  # noqa
    process.pfCandidateIndicesTable.tauIndicesCollection = cms.InputTag("pfCandidateIndexer:tau")
    process.pfCandidateIndicesTable.boostedTauIndicesCollection = cms.InputTag("pfCandidateIndexer:boostedtau")  # noqa
    process.nanoTableTaskCommon.add(process.pfCandidateIndicesTable)

    # define variables in a dict
    variables = dict(
        pt=var_f8("pt"),
        eta=var_f8("eta"),
        phi=var_f8("phi"),
        mass=var_f8("mass"),
        pdgId=var_i32("pdgId"),
        charge=var_i32("charge"),
        dxy=var_f8("dxy"),
        dz=var_f8("dz"),
    )
    if puppi_candidates:
        variables["puppiWeight"] = var_f8("puppiWeight")

    # create the table and add it to the common nano sequence
    if cmssw_version >= (14, 0, 0):
        # use the pat candidate specific table producer that exists since 14_0_0
        from PhysicsTools.NanoAOD.simplePATCandidateFlatTableProducer_cfi import simplePATCandidateFlatTableProducer as candidateTableProducer  # type: ignore[import-not-found] # noqa
    else:
        # fall back to the generic candidate table producer
        from PhysicsTools.NanoAOD.simpleCandidateFlatTableProducer_cfi import simpleCandidateFlatTableProducer as candidateTableProducer # type: ignore[import-not-found] # noqa

    process.pfCandidateTable = candidateTableProducer.clone(
        name=cms.string(collection_name),
        src=cms.InputTag(new_candidate_src),
        doc=cms.string(f"interesting {candidate_src}"),
        variables=cms.PSet(**variables),
    )
    process.nanoTableTaskCommon.add(process.pfCandidateTable)

    return process
