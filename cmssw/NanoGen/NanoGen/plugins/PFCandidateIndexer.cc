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

typedef int16_t CandidateIndexType;
typedef std::vector<CandidateIndexType> CandidateIndices;
typedef std::vector<CandidateIndices*> CandidateIndicesList;
typedef std::vector<pat::PackedCandidate> Candidates;
typedef edm::View<pat::PackedCandidate> InputCandidates;
typedef edm::View<pat::Jet> InputJets;
typedef edm::View<pat::Tau> InputTaus;

class PFCandidateIndexer : public edm::stream::EDProducer<> {
public:
  explicit PFCandidateIndexer(const edm::ParameterSet&);

  static void fillDescriptions(edm::ConfigurationDescriptions& descriptions);

private:
  void produce(edm::Event&, const edm::EventSetup&) override;

  void fillJetIndices(const InputCandidates&, const InputJets&, CandidateIndices&) const;
  void fillTauIndices(const InputCandidates&, const InputTaus&, CandidateIndices&) const;
  void fillContainedCandidates(const InputCandidates&, const CandidateIndicesList&, Candidates&) const;

  // parameters
  edm::EDGetTokenT<InputCandidates> candidateToken_;
  std::string jetIndicesName_;
  std::string fatJetIndicesName_;
  std::string tauIndicesName_;
  std::string boostedTauIndicesName_;
  std::string containedCandidatesName_;

  // get tokens
  edm::EDGetTokenT<InputJets> jetToken_;
  edm::EDGetTokenT<InputJets> fatJetToken_;
  edm::EDGetTokenT<InputTaus> tauToken_;
  edm::EDGetTokenT<InputTaus> boostedTauToken_;

  // put tokens
  edm::EDPutTokenT<CandidateIndices> jetIndicesToken_;
  edm::EDPutTokenT<CandidateIndices> fatJetIndicesToken_;
  edm::EDPutTokenT<CandidateIndices> tauIndicesToken_;
  edm::EDPutTokenT<CandidateIndices> boostedTauIndicesToken_;
  edm::EDPutTokenT<Candidates> containedCandidatesToken_;
};

void PFCandidateIndexer::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
  edm::ParameterSetDescription desc;

  desc.add<edm::InputTag>("candidateCollection", edm::InputTag("packedPFCandidates"));
  desc.add<edm::InputTag>("jetCollection", edm::InputTag("slimmedJets"));
  desc.add<edm::InputTag>("fatJetCollection", edm::InputTag("slimmedJetsAK8"));
  desc.add<edm::InputTag>("tauCollection", edm::InputTag("slimmedTaus"));
  desc.add<edm::InputTag>("boostedTauCollection", edm::InputTag("slimmedTausBoosted"));
  desc.add<std::string>("jetIndicesName", "jet");
  desc.add<std::string>("fatJetIndicesName", "fatjet");
  desc.add<std::string>("tauIndicesName", "tau");
  desc.add<std::string>("boostedTauIndicesName", "boostedtau");
  desc.add<std::string>("containedCandidatesName", "");

  descriptions.add("pfCandidateIndexer", desc);
}

PFCandidateIndexer::PFCandidateIndexer(const edm::ParameterSet& pset)
    : candidateToken_(consumes<InputCandidates>(pset.getParameter<edm::InputTag>("candidateCollection"))),
      jetIndicesName_(pset.getParameter<std::string>("jetIndicesName")),
      fatJetIndicesName_(pset.getParameter<std::string>("fatJetIndicesName")),
      tauIndicesName_(pset.getParameter<std::string>("tauIndicesName")),
      boostedTauIndicesName_(pset.getParameter<std::string>("boostedTauIndicesName")),
      containedCandidatesName_(pset.getParameter<std::string>("containedCandidatesName")) {
  // consume jets, produce indices
  auto jetCollection = pset.getParameter<edm::InputTag>("jetCollection");
  if (jetCollection.encode().empty()) {
    jetIndicesName_ = "";
  }
  if (!jetIndicesName_.empty()) {
    jetToken_ = consumes<InputJets>(jetCollection);
    jetIndicesToken_ = produces<CandidateIndices>(jetIndicesName_);
  }

  // consume fat jets, produce indices
  auto fatJetCollection = pset.getParameter<edm::InputTag>("fatJetCollection");
  if (fatJetCollection.encode().empty()) {
    fatJetIndicesName_ = "";
  }
  if (!fatJetIndicesName_.empty()) {
    fatJetToken_ = consumes<InputJets>(fatJetCollection);
    fatJetIndicesToken_ = produces<CandidateIndices>(fatJetIndicesName_);
  }

  // consume taus, produce indices
  auto tauCollection = pset.getParameter<edm::InputTag>("tauCollection");
  if (tauCollection.encode().empty()) {
    tauIndicesName_ = "";
  }
  if (!tauIndicesName_.empty()) {
    tauToken_ = consumes<InputTaus>(tauCollection);
    tauIndicesToken_ = produces<CandidateIndices>(tauIndicesName_);
  }

  // consume boosted taus, produce indices
  auto boostedTauCollection = pset.getParameter<edm::InputTag>("boostedTauCollection");
  if (boostedTauCollection.encode().empty()) {
    boostedTauIndicesName_ = "";
  }
  if (!boostedTauIndicesName_.empty()) {
    boostedTauToken_ = consumes<InputTaus>(boostedTauCollection);
    boostedTauIndicesToken_ = produces<CandidateIndices>(boostedTauIndicesName_);
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

  // produce fat jet indices if configured
  auto fatJetIndices = std::make_unique<CandidateIndices>();
  if (!fatJetIndicesName_.empty()) {
    auto const& fatJets = event.get(fatJetToken_);
    fillJetIndices(candidates, fatJets, *fatJetIndices);
  }

  // produce tau indices if configured
  auto tauIndices = std::make_unique<CandidateIndices>();
  if (!tauIndicesName_.empty()) {
    auto const& taus = event.get(tauToken_);
    fillTauIndices(candidates, taus, *tauIndices);
  }

  // produce boosted tau indices if configured
  auto boostedTauIndices = std::make_unique<CandidateIndices>();
  if (!boostedTauIndicesName_.empty()) {
    auto const& boostedTaus = event.get(boostedTauToken_);
    fillTauIndices(candidates, boostedTaus, *boostedTauIndices);
  }

  // produce new candidates if configured
  auto containedCandidates = std::make_unique<Candidates>();
  if (!containedCandidatesName_.empty()) {
    CandidateIndicesList indices;
    if (!jetIndicesName_.empty()) {
      indices.push_back(&(*jetIndices));
    }
    if (!fatJetIndicesName_.empty()) {
      indices.push_back(&(*fatJetIndices));
    }
    if (!tauIndicesName_.empty()) {
      indices.push_back(&(*tauIndices));
    }
    if (!boostedTauIndicesName_.empty()) {
      indices.push_back(&(*boostedTauIndices));
    }
    fillContainedCandidates(candidates, indices, *containedCandidates);
  }

  // add to event
  if (!jetIndicesName_.empty()) {
    event.put(jetIndicesToken_, std::move(jetIndices));
  }
  if (!fatJetIndicesName_.empty()) {
    event.put(fatJetIndicesToken_, std::move(fatJetIndices));
  }
  if (!tauIndicesName_.empty()) {
    event.put(tauIndicesToken_, std::move(tauIndices));
  }
  if (!boostedTauIndicesName_.empty()) {
    event.put(boostedTauIndicesToken_, std::move(boostedTauIndices));
  }
  if (!containedCandidatesName_.empty()) {
    event.put(containedCandidatesToken_, std::move(containedCandidates));
  }
}

void PFCandidateIndexer::fillJetIndices(const InputCandidates& candidates,
                                        const InputJets& jets,
                                        CandidateIndices& indices) const {
  // create a hash map that associates pointers of jet constituents to the corresponding jet index
  std::map<const pat::PackedCandidate*, CandidateIndexType> candidateMap;
  for (size_t iJet = 0; iJet < jets.size(); ++iJet) {
    const auto& jet = jets[iJet];
    for (size_t iCand = 0; iCand < jet.numberOfDaughters(); ++iCand) {
      const auto pc = dynamic_cast<const pat::PackedCandidate*>(jet.daughterPtr(iCand).get());
      candidateMap[pc] = (CandidateIndexType)iJet;
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

void PFCandidateIndexer::fillTauIndices(const InputCandidates& candidates,
                                        const InputTaus& taus,
                                        CandidateIndices& indices) const {
  // create a hash map that associates pointers of tau constituents to the corresponding tau index
  std::map<const pat::PackedCandidate*, CandidateIndexType> candidateMap;
  for (size_t iTau = 0; iTau < taus.size(); ++iTau) {
    const auto& tau = taus[iTau];
    for (size_t iCand = 0; iCand < tau.numberOfSourceCandidatePtrs(); ++iCand) {
      const auto pc = dynamic_cast<const pat::PackedCandidate*>(tau.sourceCandidatePtr(iCand).get());
      candidateMap[pc] = (CandidateIndexType)iTau;
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

void PFCandidateIndexer::fillContainedCandidates(const InputCandidates& candidates,
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
