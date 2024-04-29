# nanogen

## Setup

Set environment variables before sourcing `setup.sh` and consider creating an alias.

```shell
export NG_CERN_USER="your_cern_username"
export NG_DATA_BASE="/nfs/dust/cms/user/$( whoami )/nanogen"  # e.g. dust
source setup.sh ""
```

## Tasks

```mermaid
flowchart TD
    CN([CreateNano])
    GetDatasetLFNs --> CN
    CreateCMSRunConfig --> CN
    BundleRepo -- for remote<br />workflows --> CN
    CN -. optional .-> GenerateNanoDocs
```

## References

- General nano docs: https://gitlab.cern.ch/cms-nanoAOD/nanoaod-doc
- Private productions: https://gitlab.cern.ch/cms-nanoAOD/nanoaod-doc/-/wikis/Instructions/Private-production
- CMS DAS: https://cmsweb.cern.ch/das
