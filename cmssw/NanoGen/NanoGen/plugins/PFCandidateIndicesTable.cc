//
// Plugin that assigns jet and tau indices to PF candidates.
//

#include <memory>

#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Framework/interface/stream/EDProducer.h"
#include "FWCore/Framework/interface/Event.h"
#include "FWCore/Framework/interface/MakerMacros.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Utilities/interface/StreamID.h"
#include "DataFormats/NanoAOD/interface/FlatTable.h"
#include "DataFormats/PatCandidates/interface/PackedCandidate.h"
#include "DataFormats/PatCandidates/interface/Jet.h"
#include "DataFormats/PatCandidates/interface/Tau.h"

typedef std::vector<int32_t> CandidateIndices;
typedef edm::View<pat::PackedCandidate> InputCandidates;

class PFCandidateIndicesTable : public edm::stream::EDProducer<> {
public:
  explicit PFCandidateIndicesTable(const edm::ParameterSet&);

  static void fillDescriptions(edm::ConfigurationDescriptions& descriptions);

private:
  void produce(edm::Event&, const edm::EventSetup&) override;

  std::string tableName_;
  edm::EDPutTokenT<nanoaod::FlatTable> tableToken_;
  edm::EDGetTokenT<InputCandidates> candidateToken_;
  edm::InputTag jetIndicesCollection_;
  edm::InputTag fatJetIndicesCollection_;
  edm::InputTag tauIndicesCollection_;
  edm::InputTag boostedTauIndicesCollection_;
  std::string jetIndicesName_;
  std::string fatJetIndicesName_;
  std::string tauIndicesName_;
  std::string boostedTauIndicesName_;

  edm::EDGetTokenT<CandidateIndices> jetIndicesToken_;
  edm::EDGetTokenT<CandidateIndices> fatJetIndicesToken_;
  edm::EDGetTokenT<CandidateIndices> tauIndicesToken_;
  edm::EDGetTokenT<CandidateIndices> boostedTauIndicesToken_;
};

void PFCandidateIndicesTable::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
  edm::ParameterSetDescription desc;

  desc.add<std::string>("tableName", "PFCandidateIndices");
  desc.add<edm::InputTag>("candidateCollection", edm::InputTag("packedPFCandidates"));
  desc.add<edm::InputTag>("jetIndicesCollection", edm::InputTag("pfCandidateIndexer", "jet"));
  desc.add<edm::InputTag>("fatJetIndicesCollection", edm::InputTag("pfCandidateIndexer", "fatjet"));
  desc.add<edm::InputTag>("tauIndicesCollection", edm::InputTag("pfCandidateIndexer", "tau"));
  desc.add<edm::InputTag>("boostedTauIndicesCollection", edm::InputTag("pfCandidateIndexer", "boostedtau"));

  descriptions.add("pfCandidateIndicesTable", desc);
}

PFCandidateIndicesTable::PFCandidateIndicesTable(const edm::ParameterSet& pset)
    : tableName_(pset.getParameter<std::string>("tableName")),
      tableToken_(produces<nanoaod::FlatTable>(tableName_)),
      candidateToken_(consumes<InputCandidates>(pset.getParameter<edm::InputTag>("candidateCollection"))),
      jetIndicesCollection_(pset.getParameter<edm::InputTag>("jetIndicesCollection")),
      fatJetIndicesCollection_(pset.getParameter<edm::InputTag>("fatJetIndicesCollection")),
      tauIndicesCollection_(pset.getParameter<edm::InputTag>("tauIndicesCollection")),
      boostedTauIndicesCollection_(pset.getParameter<edm::InputTag>("boostedTauIndicesCollection")),
      jetIndicesName_(jetIndicesCollection_.instance()),
      fatJetIndicesName_(fatJetIndicesCollection_.instance()),
      tauIndicesName_(tauIndicesCollection_.instance()),
      boostedTauIndicesName_(boostedTauIndicesCollection_.instance()) {
  // consume jet indices if collection and indices name are provided
  if (jetIndicesCollection_.encode().empty()) {
    jetIndicesName_ = "";
  }
  if (!jetIndicesName_.empty()) {
    jetIndicesToken_ = consumes<CandidateIndices>(jetIndicesCollection_);
  }

  // consume fat jet indices if collection and indices name are provided
  if (fatJetIndicesCollection_.encode().empty()) {
    fatJetIndicesName_ = "";
  }
  if (!fatJetIndicesName_.empty()) {
    fatJetIndicesToken_ = consumes<CandidateIndices>(fatJetIndicesCollection_);
  }

  // consume tau indices if collection and indices name are provided
  if (tauIndicesCollection_.encode().empty()) {
    tauIndicesName_ = "";
  }
  if (!tauIndicesName_.empty()) {
    tauIndicesToken_ = consumes<CandidateIndices>(tauIndicesCollection_);
  }

  // consume boosted tau indices if collection and indices name are provided
  if (boostedTauIndicesCollection_.encode().empty()) {
    boostedTauIndicesName_ = "";
  }
  if (!boostedTauIndicesName_.empty()) {
    boostedTauIndicesToken_ = consumes<CandidateIndices>(boostedTauIndicesCollection_);
  }
}

void PFCandidateIndicesTable::produce(edm::Event& event, const edm::EventSetup& setup) {
  // read candidates
  auto const& candidates = event.get(candidateToken_);

  // create table
  auto tab = std::make_unique<nanoaod::FlatTable>(candidates.size(), tableName_, false);

  // fill jet indices if configured
  if (!jetIndicesName_.empty()) {
    auto const& jetIndices = event.get(jetIndicesToken_);
    tab->addColumn<int8_t>(jetIndicesName_, jetIndices, jetIndicesName_ + " index");
  }

  // fill fat jet indices if configured
  if (!fatJetIndicesName_.empty()) {
    auto const& fatJetIndices = event.get(fatJetIndicesToken_);
    tab->addColumn<int8_t>(fatJetIndicesName_, fatJetIndices, fatJetIndicesName_ + " index");
  }

  // fill tau indices if configured
  if (!tauIndicesName_.empty()) {
    auto const& tauIndices = event.get(tauIndicesToken_);
    tab->addColumn<int8_t>(tauIndicesName_, tauIndices, tauIndicesName_ + " index");
  }

  // fill boosted tau indices if configured
  if (!boostedTauIndicesName_.empty()) {
    auto const& boostedTauIndices = event.get(boostedTauIndicesToken_);
    tab->addColumn<int8_t>(boostedTauIndicesName_, boostedTauIndices, boostedTauIndicesName_ + " index");
  }

  // write the table
  event.put(tableToken_, std::move(tab));
}

DEFINE_FWK_MODULE(PFCandidateIndicesTable);
