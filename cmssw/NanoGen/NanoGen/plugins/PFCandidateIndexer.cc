//
// Plugin that creates jet and tau assigns jet and tau indices to PF candidates.
//

#include <memory>

#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/stream/EDProducer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Utilities/interface/StreamID.h"
#include "DataFormats/PatCandidates/interface/PackedCandidate.h"
#include "DataFormats/PatCandidates/interface/Jet.h"
#include "DataFormats/PatCandidates/interface/Tau.h"

typedef std::vector<int32_t> CandidateIndices;
typedef std::vector<CandidateIndices*> CandidateIndicesList;
typedef std::vector<pat::PackedCandidate> Candidates;
typedef std::vector<pat::Jet> Jets;
typedef std::vector<pat::Tau> Taus;

class PFCandidateIndexer : public edm::stream::EDProducer<> {
public:
  explicit PFCandidateIndexer(const edm::ParameterSet&);

  static void fillDescriptions(edm::ConfigurationDescriptions& descriptions);

private:
  void produce(edm::Event&, const edm::EventSetup&) override;

  void fillJetIndices(const Candidates&, const Jets&, CandidateIndices&) const;
  void fillTauIndices(const Candidates&, const Taus&, CandidateIndices&) const;
  void fillContainedCandidates(const Candidates&, const CandidateIndicesList&, Candidates&) const;

  edm::EDGetTokenT<Candidates> candidateToken_;
  edm::EDGetTokenT<Jets> jetToken_;
  edm::EDGetTokenT<Taus> tauToken_;
  std::string jetIndicesName_;
  std::string tauIndicesName_;
  std::string containedCandidatesName_;

  edm::EDPutTokenT<CandidateIndices> jetIndicesToken_;
  edm::EDPutTokenT<CandidateIndices> tauIndicesToken_;
  edm::EDPutTokenT<Candidates> containedCandidatesToken_;
};

void PFCandidateIndexer::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
  edm::ParameterSetDescription desc;

  desc.add<edm::InputTag>("candidateCollection", edm::InputTag("packedPFCandidates"));
  desc.add<edm::InputTag>("jetCollection", edm::InputTag("slimmedJets"));
  desc.add<edm::InputTag>("tauCollection", edm::InputTag("slimmedTaus"));
  desc.add<std::string>("jetIndicesName", "jet");
  desc.add<std::string>("tauIndicesName", "tau");
  desc.add<std::string>("containedCandidatesName", "");

  descriptions.add("pfCandidateIndexer", desc);
}

PFCandidateIndexer::PFCandidateIndexer(const edm::ParameterSet& pset)
    : candidateToken_(consumes<Candidates>(pset.getParameter<edm::InputTag>("candidateCollection"))),
      jetToken_(consumes<Jets>(pset.getParameter<edm::InputTag>("jetCollection"))),
      tauToken_(consumes<Taus>(pset.getParameter<edm::InputTag>("tauCollection"))),
      jetIndicesName_(pset.getParameter<std::string>("jetIndicesName")),
      tauIndicesName_(pset.getParameter<std::string>("tauIndicesName")),
      containedCandidatesName_(pset.getParameter<std::string>("containedCandidatesName")) {
  // produce jet indices if name is provided
  if (!jetIndicesName_.empty()) {
    jetIndicesToken_ = produces<CandidateIndices>(jetIndicesName_);
  }
  // produce tau indices if name is provided
  if (!tauIndicesName_.empty()) {
    tauIndicesToken_ = produces<CandidateIndices>(tauIndicesName_);
  }
  // produce a new collection of candidates that are contained in jets or taus if name is provided
  if (!containedCandidatesName_.empty()) {
    containedCandidatesToken_ = produces<Candidates>(containedCandidatesName_);
  }
}

void PFCandidateIndexer::produce(edm::Event& event, const edm::EventSetup& setup) {
  // read candidates
  auto const& candidates = event.get(candidateToken_);

  // produce jet indices if configured
  auto jetIndices = std::make_unique<CandidateIndices>();
  if (!jetIndicesName_.empty()) {
    auto const& jets = event.get(jetToken_);
    fillJetIndices(candidates, jets, *jetIndices);
  }

  // produce tau indices if configured
  auto tauIndices = std::make_unique<CandidateIndices>();
  if (!tauIndicesName_.empty()) {
    auto const& taus = event.get(tauToken_);
    fillTauIndices(candidates, taus, *tauIndices);
  }

  // produce new candidates if configured
  auto containedCandidates = std::make_unique<Candidates>();
  if (!containedCandidatesName_.empty()) {
    CandidateIndicesList indices({&(*jetIndices), &(*tauIndices)});
    fillContainedCandidates(candidates, indices, *containedCandidates);
  }

  // add to event
  if (!jetIndicesName_.empty()) {
    event.put(jetIndicesToken_, std::move(jetIndices));
  }
  if (!tauIndicesName_.empty()) {
    event.put(tauIndicesToken_, std::move(tauIndices));
  }
  if (!containedCandidatesName_.empty()) {
    event.put(containedCandidatesToken_, std::move(containedCandidates));
  }
}

void PFCandidateIndexer::fillJetIndices(const Candidates& candidates,
                                        const Jets& jets,
                                        CandidateIndices& indices) const {
  // create a hash map that associates pointers of jet constituents to the corresponding jet index
  std::map<const pat::PackedCandidate*, int32_t> candidateMap;
  for (size_t iJet = 0; iJet < jets.size(); ++iJet) {
    const auto& jet = jets[iJet];
    for (size_t iCand = 0; iCand < jet.numberOfDaughters(); ++iCand) {
      const auto pc = dynamic_cast<const pat::PackedCandidate*>(jet.daughterPtr(iCand).get());
      candidateMap[pc] = (int32_t)iJet;
    }
  }

  // loop through candidates and find the corresponding jet index
  indices.resize(candidates.size(), -1);
  for (size_t iCand = 0; iCand < candidates.size(); ++iCand) {
    const auto it = candidateMap.find(&candidates[iCand]);
    if (it != candidateMap.end()) {
      indices[iCand] = it->second;
    }
  }
}

void PFCandidateIndexer::fillTauIndices(const Candidates& candidates,
                                        const Taus& taus,
                                        CandidateIndices& indices) const {
  // create a hash map that associates pointers of tau constituents to the corresponding tau index
  std::map<const pat::PackedCandidate*, int32_t> candidateMap;
  for (size_t iTau = 0; iTau < taus.size(); ++iTau) {
    const auto& tau = taus[iTau];
    for (size_t iCand = 0; iCand < tau.numberOfSourceCandidatePtrs(); ++iCand) {
      const auto pc = dynamic_cast<const pat::PackedCandidate*>(tau.sourceCandidatePtr(iCand).get());
      candidateMap[pc] = (int32_t)iTau;
    }
  }

  // loop through candidates and find the corresponding tau index
  indices.resize(candidates.size(), -1);
  for (size_t iCand = 0; iCand < candidates.size(); ++iCand) {
    const auto it = candidateMap.find(&candidates[iCand]);
    if (it != candidateMap.end()) {
      indices[iCand] = it->second;
    }
  }
}

void PFCandidateIndexer::fillContainedCandidates(const Candidates& candidates,
                                                 const CandidateIndicesList& indicesList,
                                                 Candidates& containedCandidates) const {
  // strategy:
  // - loop through candidates and check if they have at least one index value set (!= -1)
  // - if so, add the candidate and remember the corresponding indices in seperates indices
  // - reassign the indices in-place to the reduced ones so that their sizes are consistent

  // create reduced indices list
  CandidateIndicesList reducedIndicesList;
  for (size_t i = 0; i < indicesList.size(); ++i) {
    reducedIndicesList.push_back(new CandidateIndices());
  }

  // evaluate contained candidates
  containedCandidates.clear();
  for (size_t iCand = 0; iCand < candidates.size(); ++iCand) {
    bool keep = false;
    for (const auto& indices : indicesList) {
      if (indices->at(iCand) >= 0) {
        keep = true;
        break;
      }
    }
    if (keep) {
      containedCandidates.push_back(candidates[iCand]);
      for (size_t i = 0; i < indicesList.size(); ++i) {
        reducedIndicesList[i]->push_back(indicesList[i]->at(iCand));
      }
    }
  }

  // reassign reduced indices list
  for (size_t i = 0; i < indicesList.size(); ++i) {
    indicesList[i]->assign(reducedIndicesList[i]->begin(), reducedIndicesList[i]->end());
    delete reducedIndicesList[i];
  }
}

//define this as a plug-in
DEFINE_FWK_MODULE(PFCandidateIndexer);
