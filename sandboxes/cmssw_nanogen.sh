#!/use/bin/env bash

# the following variables can be expected to exist:
#   - NG_CMSSW_VERSION (e.g. CMSSW_14_...)
#   - NG_SCRAM_ARCH (e.g. el9_amd64_gcc12)
#   - NG_CMSSW_ENV_NAME (e.g. "nanogen")
#   - NG_NANO_VERSION (e.g. "14")

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # custom install function that symlinks the custom NanoGen subsystem
    ng_cmssw_custom_install() {
        # v14 customizations
        # see https://github.com/cms-tau-pog/NanoProd/blob/ed454d471583ab99c9a42bb6ad0fd4bd8430b49f/env.sh#L51-L67
        if [ "${NG_NANO_VERSION}" = "14" ] && [ "${NG_CMSSW_ENV_NAME}" = "nanogen_taupog" ]; then
            echo "setting up TAU POG customizations for V14"
            git cms-merge-topic cms-tau-pog:CMSSW_14_0_X_HLepRare_skim_2025_v1 || return "$?"
            git cms-addpkg RecoBTag/Combined || return "$?"
            git cms-addpkg RecoJets/JetProducers || return "$?"
            wget https://github.com/cms-tau-pog/RecoTauTag-TrainingFiles/raw/refs/heads/BoostedDeepTau_v2/BoostedDeepTauId/boosteddeepTau_RunIIv2p0_{core,inner,outer}.pb -P RecoTauTag/TrainingFiles/data/BoostedDeepTauId/ || return "$?"
            wget https://github.com/cms-tau-pog/RecoTauTag-TrainingFiles/raw/refs/heads/deepTau_v2p5_noDomainAdaptation/DeepTauId/deepTau_2018v2p5_noDomainAdaptation_{core,inner,outer}.pb -P RecoTauTag/TrainingFiles/data/DeepTauId/ || return "$?"
            wget https://github.com/kandrosov/RecoBTag-Combined/raw/refs/heads/GloParT_V02/GlobalParticleTransformerAK8/PUPPI/V02/model.onnx -P RecoBTag/Combined/data/GlobalParticleTransformerAK8/PUPPI/V02/ || return "$?"
            wget https://github.com/kandrosov/RecoBTag-Combined/raw/refs/heads/GloParT_V02/GlobalParticleTransformerAK8/PUPPI/V02/preprocess.json -P RecoBTag/Combined/data/GlobalParticleTransformerAK8/PUPPI/V02/ || return "$?"
            wget https://github.com/kandrosov/RecoBTag-Combined/raw/refs/heads/GloParT_V02/GlobalParticleTransformerAK8/PUPPI/V02/preprocess_corr.json -P RecoBTag/Combined/data/GlobalParticleTransformerAK8/PUPPI/V02/ || return "$?"
        fi

        echo "symlinking ${NG_BASE}/cmssw/NanoGen into ${PWD}"
        ln -s "${NG_BASE}/cmssw/NanoGen" .
    }

    # invoke the common setup
    source "${this_dir}/cmssw.sh" "$@"
}
action "$@"
