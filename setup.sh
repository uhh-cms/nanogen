#!/usr/bin/env bash

setup_ng() {
    # Runs the entire project setup, leading to a collection of environment variables starting with
    # "NG_", the installation of the software stack via virtual environments.

    #
    # prepare local variables
    #

    local shell_is_zsh=$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"
    local orig="${PWD}"
    local micromamba_url="https://micro.mamba.pm/api/micromamba/linux-64/latest"
    local pyv="3.9"
    local remote_env="$( [ "${NG_REMOTE_ENV}" = "1" ] && echo "true" || echo "false" )"


    #
    # check for mandatory variables
    #

    if [ -z "${NG_CERN_USER}" ]; then
        >&2 echo "NG_CERN_USER empty, please set it to your CERN username before sourcing"
        return "1"
    fi


    #
    # global variables
    # (NG = NanoGen)
    #

    # start exporting variables
    export NG_BASE="${this_dir}"
    export NG_CERN_USER="${NG_CERN_USER}"
    export NG_CERN_USER_DCACHE_STORE="${NG_CERN_USER_DCACHE_STORE:-${NG_CERN_USER}}"
    export NG_CMS_SITE="${NG_CMS_SITE:-T2_DE_DESY}"
    export NG_DATA_BASE="${NG_DATA_BASE:-${NG_BASE}/data}"
    export NG_SOFTWARE_BASE="${NG_SOFTWARE_BASE:-${NG_DATA_BASE}/software}"
    export NG_CONDA_BASE="${NG_CONDA_BASE:-${NG_SOFTWARE_BASE}/conda}"
    export NG_VENV_BASE="${NG_VENV_BASE:-${NG_SOFTWARE_BASE}/venvs}"
    export NG_CMSSW_BASE="${NG_CMSSW_BASE:-${NG_SOFTWARE_BASE}/cmssw}"
    export NG_JOB_BASE="${NG_JOB_BASE:-${NG_DATA_BASE}/jobs}"
    export NG_STORE_LOCAL="${NG_STORE_LOCAL:-${NG_DATA_BASE}/store}"
    export NG_LOCAL_SCHEDULER="${NG_LOCAL_SCHEDULER:-true}"
    export NG_SCHEDULER_HOST="${NG_SCHEDULER_HOST:-naf-cms-gpu01.desy.de}"
    export NG_SCHEDULER_PORT="${NG_SCHEDULER_PORT:-8088}"
    export NG_WORKER_KEEP_ALIVE="${NG_WORKER_KEEP_ALIVE:-$( ${remote_env} && echo "false" || echo "false" )}"
    export NG_HTCONDOR_FLAVOR="${NG_HTCONDOR_FLAVOR:-naf}"
    export NG_SLURM_FLAVOR="${NG_SLURM_FLAVOR:-maxwell}"
    export NG_SLURM_PARTITION="${NG_SLURM_PARTITION:-allcpu}"
    export NG_WLCG_CACHE_ROOT="${NG_WLCG_CACHE_ROOT}"
    export NG_WLCG_USE_CACHE="${NG_WLCG_USE_CACHE:-$( [ -z "${NG_WLCG_CACHE_ROOT}" ] && echo "false" || echo "true" )}"
    export NG_WLCG_CACHE_CLEANUP="${NG_WLCG_CACHE_CLEANUP:-false}"
    export NG_DASMAPS_BASE="${NG_DASMAPS_BASE:-${HOME}}"
    export NG_DUST_ROOT="$( [ -d "/data/dust/cms/user/riegerma" ] && echo "/data/dust" || echo "/nfs/dust" )"

    # external variables
    export LANGUAGE="${LANGUAGE:-en_US.UTF-8}"
    export LANG="${LANG:-en_US.UTF-8}"
    export LC_ALL="${LC_ALL:-en_US.UTF-8}"
    export X509_USER_PROXY="${X509_USER_PROXY:-/tmp/x509up_u$( id -u )}"
    export PYTHONWARNINGS="ignore"
    export GLOBUS_THREAD_MODEL="none"
    export VIRTUAL_ENV_DISABLE_PROMPT="${VIRTUAL_ENV_DISABLE_PROMPT:-1}"
    export X509_CERT_DIR="${X509_CERT_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/certificates}"
    export X509_VOMS_DIR="${X509_VOMS_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/vomsdir}"
    export X509_VOMSES="${X509_VOMSES:-/cvmfs/grid.cern.ch/etc/grid-security/vomses}"
    export VOMS_USERCONF="${VOMS_USERCONF:-${X509_VOMSES}}"
    export MAMBA_ROOT_PREFIX="${NG_CONDA_BASE}"
    export MAMBA_EXE="${MAMBA_ROOT_PREFIX}/bin/micromamba"
    export GFAL_PLUGIN_DIR="${NG_CONDA_BASE}/lib/gfal2-plugins"


    #
    # minimal local software setup
    #

    ulimit -s unlimited

    # remove parts of the software stack if requested
    if [ "${NG_REINSTALL_CONDA}" = "1" ] || ( [ -z "${NG_REINSTALL_CONDA}" ] && [ "${NG_REINSTALL_SOFTWARE}" = "1" ] ); then
        echo "removing conda/micromamba at ${NG_CONDA_BASE}"
        rm -rf "${NG_CONDA_BASE}"
    fi
    if [ "${NG_REINSTALL_VENV}" = "1" ] || ( [ -z "${NG_REINSTALL_VENV}" ] && [ "${NG_REINSTALL_SOFTWARE}" = "1" ] ); then
        echo "removing venvs at ${NG_VENV_BASE}"
        rm -rf "${NG_VENV_BASE}"
    fi
    if [ "${NG_REINSTALL_CMSSW}" = "1" ] || ( [ -z "${NG_REINSTALL_CMSSW}" ] && [ "${NG_REINSTALL_SOFTWARE}" = "1" ] ); then
        echo "removing cmssw at ${NG_CMSSW_BASE}"
        rm -rf "${NG_CMSSW_BASE}"
    fi

    # empty the PYTHONPATH and LD_LIBRARY_PATH
    export PYTHONPATH=""
    export LD_LIBRARY_PATH=""

    # persistent PATH and PYTHONPATH parts that should be
    # priotized over any additions made in sandboxes later on
    export NG_PERSISTENT_PATH="${NG_BASE}/bin:${NG_BASE}/modules/law/bin"
    export NG_PERSISTENT_PYTHONPATH="${NG_BASE}:${NG_BASE}/modules/law"
    # also append the conda path for propagation to sandboxes that bring their own python (e.g. cmssw)
    export NG_PERSISTENT_PYTHONPATH="${NG_PERSISTENT_PYTHONPATH}:${NG_CONDA_BASE}/lib/python${pyv}/site-packages"

    # prepend them
    export PATH="${NG_PERSISTENT_PATH}:${PATH}"
    export PYTHONPATH="${NG_PERSISTENT_PYTHONPATH}:${PYTHONPATH}"

    # conda base environment
    local conda_missing="$( [ -d "${NG_CONDA_BASE}" ] && echo "false" || echo "true" )"
    if ${conda_missing}; then
        echo "installing conda/micromamba at ${NG_CONDA_BASE}"
        (
            mkdir -p "${NG_CONDA_BASE}"
            cd "${NG_CONDA_BASE}"
            curl -Ls "${micromamba_url}" | tar -xvj -C . "bin/micromamba"
            ./bin/micromamba shell hook -y --root-prefix="${NG_CONDA_BASE}" &> "micromamba.sh"
            mkdir -p "etc/profile.d"
            mv "micromamba.sh" "etc/profile.d"
            cat << EOF > ".mambarc"
changeps1: false
always_yes: true
channels:
  - conda-forge
EOF
        )
    fi

    # initialize conda
    source "${NG_CONDA_BASE}/etc/profile.d/micromamba.sh" "" || return "$?"
    micromamba activate || return "$?"
    echo "initialized conda/micromamba"

    # install packages
    if ${conda_missing}; then
        echo
        echo "setting up conda/micromamba environment"

        # conda packages (nothing so far)
        micromamba install \
            libgcc \
            bash \
            zsh \
            "python=${pyv}" \
            git \
            git-lfs \
            gfal2-util \
            python-gfal2 \
            conda-pack \
            || return "$?"
        micromamba clean --yes --all

        # update python base packages
        pip install --no-cache-dir -U \
            pip \
            setuptools \
            wheel \
            || return "$?"

        # additional packages
        pip install --no-cache-dir -U -r "${NG_BASE}/requirements.txt" || return "$?"

        # create a custom gfal plugin directory for use in cmssw sandboxes that might provide an
        # incompatible glibcxx version (still present on el9)
        local cmssw_suffix
        for cmssw_suffix in legacy 14; do
            local gfal_plugins_cmssw="${NG_CONDA_BASE}/lib/gfal2-plugins-cmssw-${cmssw_suffix}"
            mkdir -p "${gfal_plugins_cmssw}"
            (
                cd "${gfal_plugins_cmssw}"
                for f in $( find ../gfal2-plugins -name "*.so" ); do
                    ln -s "${f}" .
                done
                # the http plugin never works
                rm -f libgfal_plugin_http.so
                # the xrootd plugin does not work in legacy setups
                [ "${cmssw_suffix}" != "legacy" ] || rm -f libgfal_plugin_xrootd.so
            ) || return "$?"
        done
    fi


    #
    # CMS site setup
    # (only needed if /cvmfs/cms.cern.ch/SITECONF/local is undefined)
    #

    export NG_CMS_PATH="/cvmfs/cms.cern.ch"
    local local_conf="/cvmfs/cms.cern.ch/SITECONF/local"
    if [ "${NG_ON_HTCONDOR}" != "1" ]; then
        if [ ! -d "${local_conf}" ] || [ -z "$( readlink "${local_conf}" )" ]; then
            local cms_path="${NG_SOFTWARE_BASE}/cms"
            local dst_conf="${cms_path}/SITECONF/local"
            if [ ! -d "${dst_conf}" ]; then
                local src_conf="$( dirname "${local_conf}" )/${NG_CMS_SITE}"
                if [ ! -d "${src_conf}" ]; then
                    >&2 echo "CMS site configuration not found at ${src_conf}"
                    return "1"
                fi
                echo "local SITECONF not found, creating symlink"
                echo "${src_conf} -> ${dst_conf}"
                mkdir -p "$( dirname "${dst_conf}" )"
                ln -s "${src_conf}" "${dst_conf}"
            fi
            export NG_CMS_PATH="${cms_path}"
        fi
    fi


    #
    # initialize / update submodules
    #

    if ! ${remote_env}; then
        for mpath in modules/law; do
            # do nothing when the path does not exist or it is not a submodule
            if [ ! -d "${mpath}" ] || [ ! -f "${mpath}/.git" ] ; then
                continue
            fi

            # initialize the submodule when the directory is empty
            if [ "$( ls -1q "${mpath}" | wc -l )" = "0" ]; then
                git submodule update --init --recursive "${mpath}"
            else
                # update when not on a working branch and there are no changes
                local detached_head="$( ( cd "${mpath}"; git symbolic-ref -q HEAD &> /dev/null ) && echo "true" || echo "false" )"
                local changed_files="$( cd "${mpath}"; git status --porcelain=v1 2> /dev/null | wc -l )"
                if ! ${detached_head} && [ "${changed_files}" = "0" ]; then
                    git submodule update --init --recursive "${mpath}"
                fi
            fi
        done
    fi


    #
    # law setup
    #

    export LAW_HOME="${NG_BASE}/.law"
    export LAW_CONFIG_FILE="${NG_BASE}/law.cfg"

    if ! ${remote_env} && which law &> /dev/null; then
        # source law's bash completion scipt
        source "$( law completion )" ""

        # silently index
        law index -q
    fi
}

action() {
    if setup_ng "$@"; then
        echo -e "\x1b[0;49;35mnanogen successfully setup\x1b[0m"
        return "0"
    else
        local code="$?"
        echo -e "\x1b[0;49;31mnanogen setup failed with code ${code}\x1b[0m"
        return "${code}"
    fi
}
action "$@"
