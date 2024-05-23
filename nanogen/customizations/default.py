# coding: utf-8

"""
Default nano customizations.

Note that this module will be imported as part of the nano process customization and therefore,
all packages will be provided by - and therefore must exist in - the cmssw environment.
"""

from __future__ import annotations

import FWCore.ParameterSet.Config as cms  # type: ignore[import-not-found]
from PhysicsTools.NanoAOD.common_cff import Var  # type: ignore[import-not-found]
from PhysicsTools.NanoAOD.simpleCandidateFlatTableProducer_cfi import simpleCandidateFlatTableProducer  # type: ignore[import-not-found] # noqa


#
# High-level customization functions.
#

def no_customization(process, *, dataset_kind: str, **kwargs):
    """
    No customization and returns the process unchanged.
    """
    return process


def customize_v12_uhh(process, *, dataset_kind: str, pf_candidates: bool, **kwargs):
    # reduce gen particle history info
    if dataset_kind == "mc":
        process = reduce_gen_particles(process)

    # add tau variables, lifetime variables not available in run 2 nano
    process = add_tau_variables(process, add_lifetime_vars=False)

    # add PF candidates
    if pf_candidates:
        process = add_pf_candidates(process)

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

def reduce_gen_particles(process):
    """
    Taken from TAU POG.
    https://github.com/cms-tau-pog/NanoProd/blob/c66e12e738528f5155043472f51452853abe9b14/NanoProd/python/customize.py#L5
    """
    leptons = pdg_abs_or([11, 12, 13, 14, 15, 16])
    important_particles = pdg_abs_or([6, 23, 24, 25, 35, 39, 9990012, 9900012, 1000015])

    process.finalGenParticles.select = [
        "drop *",
        f"keep++ statusFlags().isLastCopy() && {leptons}",
        f"+keep statusFlags().isFirstCopy() && {leptons}",
        f"keep+ statusFlags().isLastCopy() && {important_particles}",
        f"+keep statusFlags().isFirstCopy() && {important_particles}",
    ]

    for coord in ["x", "y", "z"]:
        new_var = var_f10(f"vertex().{coord}", doc=f"{coord} coordinate of the production vertex")
        setattr(process.genParticleTable.variables, f"v{coord}", new_var)

    return process


def add_tau_variables(
    process,
    *,
    add_lifetime_vars: bool = True,
):
    """
    Taken from TAU POG.
    """
    # additional variables
    tau_vars = process.tauTable.variables
    tau_vars.dxyErr = var_f10("dxy_error", doc="dxy error")
    tau_vars.dzErr = var_f10(
        "?leadChargedHadrCand.isNonnull() && leadChargedHadrCand.hasTrackDetails()?leadChargedHadrCand.dzError(): 0/0.",  # noqa
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
    if add_lifetime_vars:
        from PhysicsTools.NanoAOD.leptonTimeLifeInfo_common_cff import addTimeLifeInfoToTaus  # type: ignore[import-not-found] # noqa
        addTimeLifeInfoToTaus(process)

    return process


def add_pf_candidates(
    process,
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
    process.pfCandidateTable = simpleCandidateFlatTableProducer.clone(
        name=cms.string(collection_name),
        src=cms.InputTag(new_candidate_src),
        doc=cms.string(f"interesting {candidate_src}"),
        variables=cms.PSet(**variables),
    )
    process.nanoTableTaskCommon.add(process.pfCandidateTable)

    return process
