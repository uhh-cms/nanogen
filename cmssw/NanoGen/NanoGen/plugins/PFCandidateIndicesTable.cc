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
typedef std::vector<pat::PackedCandidate> Candidates;

class PFCandidateIndicesTable : public edm::stream::EDProducer<> {
public:
  explicit PFCandidateIndicesTable(const edm::ParameterSet&);

  static void fillDescriptions(edm::ConfigurationDescriptions& descriptions);

private:
  void produce(edm::Event&, const edm::EventSetup&) override;

  std::string tableName_;
  edm::EDPutTokenT<nanoaod::FlatTable> tableToken_;
  edm::EDGetTokenT<Candidates> candidateToken_;
  edm::InputTag jetIndicesCollection_;
  edm::InputTag tauIndicesCollection_;
  std::string jetIndicesName_;
  std::string tauIndicesName_;

  edm::EDGetTokenT<CandidateIndices> jetIndicesToken_;
  edm::EDGetTokenT<CandidateIndices> tauIndicesToken_;
};

void PFCandidateIndicesTable::fillDescriptions(edm::ConfigurationDescriptions& descriptions) {
  edm::ParameterSetDescription desc;

  desc.add<std::string>("tableName", "PFCandidateIndices");
  desc.add<edm::InputTag>("candidateCollection", edm::InputTag("packedPFCandidates"));
  desc.add<edm::InputTag>("jetIndicesCollection", edm::InputTag("pfCandidateIndexer", "jet"));
  desc.add<edm::InputTag>("tauIndicesCollection", edm::InputTag("pfCandidateIndexer", "tau"));

  descriptions.add("pfCandidateIndicesTable", desc);
}

PFCandidateIndicesTable::PFCandidateIndicesTable(const edm::ParameterSet& pset)
    : tableName_(pset.getParameter<std::string>("tableName")),
      tableToken_(produces<nanoaod::FlatTable>(tableName_)),
      candidateToken_(consumes<Candidates>(pset.getParameter<edm::InputTag>("candidateCollection"))),
      jetIndicesCollection_(pset.getParameter<edm::InputTag>("jetIndicesCollection")),
      tauIndicesCollection_(pset.getParameter<edm::InputTag>("tauIndicesCollection")),
      jetIndicesName_(jetIndicesCollection_.instance()),
      tauIndicesName_(tauIndicesCollection_.instance()) {
  // consume jet indices if collection is provided
  if (!jetIndicesName_.empty()) {
    jetIndicesToken_ = consumes<CandidateIndices>(jetIndicesCollection_);
  }
  // consume tau indices if collection is provided
  if (!tauIndicesName_.empty()) {
    tauIndicesToken_ = consumes<CandidateIndices>(tauIndicesCollection_);
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

  // fill tau indices if configured
  if (!tauIndicesName_.empty()) {
    auto const& tauIndices = event.get(tauIndicesToken_);
    tab->addColumn<int8_t>(tauIndicesName_, tauIndices, tauIndicesName_ + " index");
  }

  // write the table
  event.put(tableToken_, std::move(tab));
}

DEFINE_FWK_MODULE(PFCandidateIndicesTable);
