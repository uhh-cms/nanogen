#!/usr/bin/env bash

# Script that installs, removes and / or sources a CMSSW environment. Distinctions are made
# depending on whether the installation is already present, and whether the script is called as part
# of a remote (law) job (NG_REMOTE_ENV=1).
#
# In case a function or executable named "ng_cmssw_custom_install" is defined, it is invoked inside
# the src directory of the CMSSW checkout after a first "scram build" pass and followed by a second
# pass. If its refers to a file, that file is sourced. Likewise, if defined, a function or
# executable named "ng_cmssw_custom_setup" is invoked after the sandbox is activated.
#
# Six environment variables are expected to be set before this script is called:
#   NG_CMSSW_VERSION
#       The desired CMSSW version to setup.
#   NG_SCRAM_ARCH
#       The scram architecture string.
#   NG_CMSSW_ENV_NAME
#       The name of the environment to prevent collisions between multiple environments using the
#       same CMSSW version.
#   NG_CMSSW_BASE
#       The location where the CMSSW environment should be installed.
#
# Arguments:
#   1. mode
#      The setup mode. Different values are accepted:
#        - ''/install: The CMSSW environment is installed when not existing yet and sourced.
#        - clear:      The CMSSW environment is removed when existing.
#        - reinstall:  The CMSSW environment is removed first, then reinstalled and sourced.
#        - update:     The CMSSW environment is removed first in case it is outdated, then
#                      reinstalled and sourced.
#      Please note that if the mode is empty ('') and the environment variable NG_SANDBOX_SETUP_MODE
#      is defined, its value is used instead.
#
# Note on remote jobs:
# When the NG_REMOTE_ENV variable is found to be "1" (usually set by a remote job bootstrap script),
# no mode is supported and no installation will happen but the desired CMSSW setup must be present

setup_cmssw() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local orig_dir="$( pwd )"

    # zsh options
    if ${shell_is_zsh}; then
        emulate -L bash
        setopt globdots
    fi


    #
    # get and check arguments
    #

    local mode="${1:-}"

    # default mode
    if [ -z "${mode}" ]; then
        mode="install"
        [ ! -z "${NG_SANDBOX_SETUP_MODE}" ] && mode="${NG_SANDBOX_SETUP_MODE}"
    fi

    # force install mode for remote jobs
    [ "${NG_REMOTE_ENV}" = "1" ] && mode="install"

    # value checks
    if [ "${mode}" != "install" ] && [ "${mode}" != "clear" ] && [ "${mode}" != "reinstall" ] && [ "${mode}" != "update" ]; then
        >&2 echo "unknown CMSSW setup mode '${mode}'"
        return "1"
    fi


    #
    # check required global variables
    #

    if [ -z "${NG_CMSSW_VERSION}" ]; then
        >&2 echo "NG_CMSSW_VERSION is not set but required by ${this_file} to setup CMSSW"
        return "10"
    fi
    if [ -z "${NG_SCRAM_ARCH}" ]; then
        >&2 echo "NG_SCRAM_ARCH is not set but required by ${this_file} to setup CMSSW"
        return "11"
    fi
    if [ -z "${NG_CMSSW_ENV_NAME}" ]; then
        >&2 echo "NG_CMSSW_ENV_NAME is not set but required by ${this_file} to setup CMSSW"
        return "12"
    fi
    if [ -z "${NG_CMSSW_BASE}" ]; then
        >&2 echo "NG_CMSSW_BASE is not set but required by ${this_file} to setup CMSSW"
        return "13"
    fi


    #
    # define variables
    #

    local install_base="${NG_CMSSW_BASE}/${NG_CMSSW_ENV_NAME}"
    local install_path="${install_base}/${NG_CMSSW_VERSION}"
    local install_path_repr="\$NG_CMSSW_BASE/${NG_CMSSW_ENV_NAME}/${NG_CMSSW_VERSION}"
    local pending_flag_file="${NG_CMSSW_BASE}/pending_${NG_CMSSW_ENV_NAME}_${NG_CMSSW_VERSION}"


    #
    # start the setup
    #

    # ensure NG_CMSSW_BASE exists
    mkdir -p "${NG_CMSSW_BASE}"

    if [ "${NG_REMOTE_ENV}" != "1" ]; then
        # optionally remove the current installation
        if [ "${mode}" = "clear" ] || [ "${mode}" = "reinstall" ]; then
            echo "removing current installation at ${install_path} (mode '${mode}')"
            rm -rf "${install_path}"

            # optionally stop here
            [ "${mode}" = "clear" ] && return "0"
        fi

        # in local environments, install from scratch
        if [ ! -d "${install_path}" ]; then
            # from here onwards, files and directories could be created and in order to prevent race
            # conditions from multiple processes, guard the setup with the pending_flag_file and
            # sleep for a random amount of seconds between 0 and 10 to further reduce the chance of
            # simultaneously starting processes getting here at the same time
            local sleep_counter="0"
            sleep "$( python3 -c 'import random;print(random.random() * 10)')"
            # when the file is older than 30 minutes, consider it a dangling leftover from a
            # previously failed installation attempt and delete it.
            if [ -f "${pending_flag_file}" ]; then
                local flag_file_age="$(( $( date +%s ) - $( date +%s -r "${pending_flag_file}" )))"
                [ "${flag_file_age}" -ge "1800" ] && rm -f "${pending_flag_file}"
            fi
            # start the sleep loop
            while [ -f "${pending_flag_file}" ]; do
                # wait at most 20 minutes
                sleep_counter="$(( $sleep_counter + 1 ))"
                if [ "${sleep_counter}" -ge 120 ]; then
                    >&2 echo "${NG_CMSSW_VERSION} is setup in different process, but number of sleeps exceeded"
                    return "20"
                fi
                echo "${NG_CMSSW_VERSION} already being setup in different process, sleep ${sleep_counter} / 120"
                sleep 10
            done
        fi

        # create the pending_flag to express that the state might be changing
        touch "${pending_flag_file}"
        clear_pending() {
            rm -f "${pending_flag_file}"
        }

        # install when missing
        if [ ! -d "${install_path}" ]; then
            echo
            echo "installing ${NG_CMSSW_VERSION} on ${NG_SCRAM_ARCH} in ${install_base}"

            # first install pass
            (
                mkdir -p "${install_base}"
                cd "${install_base}"
                export SCRAM_ARCH="${NG_SCRAM_ARCH}"
                source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" &&
                scramv1 project CMSSW "${NG_CMSSW_VERSION}" &&
                cd "${NG_CMSSW_VERSION}/src" &&
                eval "$( scramv1 runtime -sh )" &&
                scram b
            )
            local ret="$?"
            [ "${ret}" != "0" ] && clear_pending && return "${ret}"

            # custom install hook and second install pass
            (
                cd "${install_path}/src"
                source "/cvmfs/cms.cern.ch/cmsset_default.sh" ""
                eval "$( scramv1 runtime -sh )"

                if command -v ng_cmssw_custom_install &> /dev/null; then
                    echo -e "\nrunning ng_cmssw_custom_install"
                    ng_cmssw_custom_install &&
                    cd "${install_path}/src" &&
                    scram b
                elif [ ! -z "${ng_cmssw_custom_install}" ] && [ -f "${ng_cmssw_custom_install}" ]; then
                    echo -e "\nsourcing ng_cmssw_custom_install file"
                    source "${ng_cmssw_custom_install}" "" &&
                    cd "${install_path}/src" &&
                    scram b
                fi
            )
            ret="$?"
            [ "${ret}" != "0" ] && clear_pending && return "${ret}"
        fi

        # remove the pending_flag
        clear_pending
    fi

    # at this point, the src path must exist
    if [ ! -d "${install_path}/src" ]; then
        >&2 echo "src directory not found in CMSSW installation at ${install_path}"
        return "30"
    fi

    # source it
    source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || return "$?"
    export SCRAM_ARCH="${NG_SCRAM_ARCH}"
    export CMSSW_VERSION="${NG_CMSSW_VERSION}"
    cd "${install_path}/src"
    eval "$( scramv1 runtime -sh )" || return "$?"

    # custom setup
    if command -v ng_cmssw_custom_setup &> /dev/null; then
        ng_cmssw_custom_setup || return "$?"
    elif [ ! -z "${ng_cmssw_custom_setup}" ] && [ -f "${ng_cmssw_custom_setup}" ]; then
        source "${ng_cmssw_custom_setup}" "" || return "$?"
    fi

    cd "${orig_dir}"

    # prepend persistent path fragments again to ensure priority for local packages and
    export PYTHONPATH="${NG_PERSISTENT_PYTHONPATH}:${PYTHONPATH}"
    export PATH="${NG_PERSISTENT_PATH}:${PATH}"

    # mark this as a bash sandbox for law
    export LAW_SANDBOX="bash::\$NG_BASE/sandboxes/$( basename "${this_file}" )"

    return "0"
}
setup_cmssw "$@"
