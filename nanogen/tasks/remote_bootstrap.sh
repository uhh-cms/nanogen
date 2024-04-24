#!/usr/bin/env bash

# Bootstrap file that is executed in remote jobs submitted by law to set up the environment.
# So-called render variables, denoted by "{{name}}", are replaced with variables configured in the
# remote workflow tasks, e.g. in HTCondorWorkflow.htcondor_job_config() upon job submission.

# bootstrap function for standalone htcondor jobs
bootstrap_htcondor() {
    # set env variables
    export NG_ON_HTCONDOR="1"
    export NG_REMOTE_ENV="1"
    export NG_CERN_USER="{{ng_cern_user}}"
    export NG_CERN_USER_FIRSTCHAR="${NG_CERN_USER:0:1}"
    export NG_BASE="${LAW_JOB_HOME}/repo"
    export NG_DATA_BASE="${LAW_JOB_HOME}/ng_data"
    export NG_SOFTWARE_BASE="{{ng_software_base}}"
    export NG_STORE_LOCAL="{{ng_store_local}}"
    export NG_LOCAL_SCHEDULER="{{ng_local_scheduler}}"
    export NG_WLCG_CACHE_ROOT="${LAW_JOB_HOME}/ng_wlcg_cache"
    export NG_WLCG_CACHE_CLEANUP="true"
    export NG_WLCG_TOOLS="{{wlcg_tools}}"
    export LAW_CONFIG_FILE="{{law_config_file}}"
    [ ! -z "{{vomsproxy_file}}" ] && export X509_USER_PROXY="${PWD}/{{vomsproxy_file}}"

    # load the repo bundle
    echo -e "\nfetching repository bundle ..."
    mkdir -p "${NG_BASE}"
    local bundle_src="$( ls -1 {{ng_repo_pattern}} | shuf | head -n 1 )"
    if [ -z "${bundle_src}" ]; then
        >&2 echo "could not determine repo bundle to fetch from '{{ng_repo_pattern}}'"
        return "1"
    fi
    (
        cd "${NG_BASE}" &&
        cp "${bundle_src}" "repo.tgz" &&
        tar -xzf "repo.tgz" &&
        rm "repo.tgz"
    ) || return "$?"
    echo "done fetching repository bundle"

    # optional custom command before the setup is sourced
    {{ng_pre_setup_command}}

    # source the default repo setup
    echo -e "\nsource repository setup ..."
    source "${NG_BASE}/setup.sh" "" || return "$?"
    echo "done sourcing repository setup"

    # optional custom command after the setup is sourced
    {{ng_post_setup_command}}

    return "0"
}

bootstrap_slurm() {
    # set env variables
    export NG_ON_SLURM="1"
    export NG_REMOTE_ENV="1"
    export NG_BASE="{{ng_base}}"
    export NG_WLCG_CACHE_ROOT="${LAW_JOB_HOME}/ng_wlcg_cache"
    export NG_WLCG_CACHE_CLEANUP="true"
    export KRB5CCNAME="FILE:{{kerberosproxy_file}}"
    [ ! -z "{{vomsproxy_file}}" ] && export X509_USER_PROXY="{{vomsproxy_file}}"

    # optional custom command before the setup is sourced
    {{ng_pre_setup_command}}

    # source the default repo setup
    echo -e "\nsource repository setup ..."
    source "${NG_BASE}/setup.sh" "" || return "$?"
    echo "done sourcing repository setup"

    # optional custom command after the setup is sourced
    {{ng_post_setup_command}}
}

# job entry point
bootstrap_{{ng_bootstrap_name}} "$@"
