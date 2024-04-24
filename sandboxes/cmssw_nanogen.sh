#!/use/bin/env bash

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"

    # custom install function that symlinks the custom NanoGen subsystem
    ng_cmssw_custom_install() {
        echo "symlinking ${NG_BASE}/cmssw/NanoGen into ${PWD}"
        ln -s "${NG_BASE}/cmssw/NanoGen" .
    }

    # invoke the common setup
    source "${this_dir}/cmssw.sh" "$@"
}
action "$@"
