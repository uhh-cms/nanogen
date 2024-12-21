# coding: utf-8

"""
Script that uses CMSSW FWLite to loop through a file, tries to unpack a certain product and
remembers the run number and event number pairs for which the unpacking failed.

At the end, failed events are written to stderr to be able to pipe them into a file via "2>".

Certain variables below are hardcoded and need to be adapted to the specific case.
"""

import os
import sys
import re
import subprocess
import tempfile
from collections import deque


# the file to be checked
# pfn = "/pnfs/desy.de/cms/tier2/store/user/mrieger/nanogen_store/FetchLFN/store/data/Run2022C/JetMET/MINIAOD/19Dec2023-v1/2560000/7a5aa4c8-3b72-4da3-8d95-ec5ee18f765c.root"  # noqa
pfn = "/pnfs/desy.de/cms/tier2/store/user/mrieger/nanogen_store/FetchLFN/store/data/Run2023C/Muon1/MINIAOD/22Sep2023_v4-v2/2820000/10f0bd36-0f37-4144-8be1-61a0c7109d4f.root"  # noqa

# the objects to be unpacked
handles = {
    # "pfcands": {
    #     "type": "std::vector<pat::PackedCandidate>",
    #     "label": ("packedPFCandidates",),
    # },
    "jets": {
        "type": "std::vector<pat::Jet>",
        "label": ("slimmedJets",),
    },
}


# a function to be called to unpack and use the objects
# (this should not print anything!)
def unpack(event, products):
    cands = products["jets"]
    # cands.at(0).pt()
    # cands.at(0).eta()
    # cands.at(0).phi()
    sum(cands.at(i).pt() for i in range(cands.size()))
    sum(cands.at(i).eta() for i in range(cands.size()))
    sum(cands.at(i).phi() for i in range(cands.size()))


# --- nothing needs to be configured below this line -----------------------------------------------


def fwlite_loop(path, handle_data=None, start=0, end=-1, object_type="Event", msg=None):
    """
    Opens one or more ROOT files defined by *path* and yields the FWLite event. When *handle_data*
    is not *None*, it is supposed to be a dictionary ``key -> {"type": ..., "label": ...}``. In that
    case, the handle products are yielded as well in a dictionary, mapped to the key, as
    ``(event, objects dict)``.
    """
    import ROOT  # type: ignore
    ROOT.PyConfig.IgnoreCommandLineOptions = True
    ROOT.gROOT.SetBatch()
    ROOT.gSystem.Load("libFWCoreFWLite.so")
    ROOT.gSystem.Load("libDataFormatsFWLite.so")
    ROOT.FWLiteEnabler.enable()
    from DataFormats.FWLite import Events, Runs, Handle  # type: ignore  # noqa

    paths = path if isinstance(path, (list, tuple)) else [path]
    handles = {}
    if handle_data:
        for key, data in handle_data.items():
            handles[key] = Handle(data["type"])

    objects = locals()[object_type + "s"](paths)

    for i, obj in enumerate(objects):
        if i < start:
            continue
        if end >= 0 and i >= end:
            break
        if msg is not None:
            print(msg)
            sys.stdout.flush()
        if handle_data:
            products = {}
            for key, data in handle_data.items():
                obj.getByLabel(data["label"], handles[key])
                products[key] = handles[key].product()
            yield obj, products
        else:
            yield obj


def run_fwlite_loop():
    for event, products in fwlite_loop(pfn, handles, msg="NEXT"):
        nr = event.eventAuxiliary().run()
        ne = event.eventAuxiliary().event()
        print(f"EVENT {nr} {ne}")
        try:
            unpack(event, products)
        except Exception as e:
            print(f"\n{e}")
        sys.stdout.flush()
        sys.stderr.flush()


def main():
    env_var = "NG_FIND_BROKEN_EVENTS_TRIGGERED"

    # when set, just run the fwlite loop
    if os.getenv(env_var) == "1":
        run_fwlite_loop()
        return 0

    # trigger _this_ file and pipe its output into a temporary file
    this_file = os.path.abspath(__file__)
    with tempfile.NamedTemporaryFile(mode="w") as tmp:
        try:
            subprocess.run(
                [sys.executable, this_file],
                env={**os.environ, env_var: "1"},
                stdout=tmp,
                stderr=subprocess.STDOUT,
                check=True,
            )
        except subprocess.CalledProcessError:
            with open(tmp.name) as tmp:
                print(tmp.read(), file=sys.stderr)
            return 1
        # read the file content
        tmp.seek(0)
        with open(tmp.name) as tmp:
            lines = deque(line.strip() for line in tmp.readlines())

    # parse the output and extract events that failed
    failed = []
    while lines:
        # get lines until the next END
        block = []
        while lines and (l := lines.popleft()) != "NEXT":
            block.append(l)
        if not block:
            continue
        # parse the block
        run_event = None
        errs = []
        for line in block:
            if m := re.match(r"EVENT (\d+) (\d+)$", line):
                run_event = int(m.group(1)), int(m.group(2))
            else:
                errs.append(line)
        if not run_event:
            raise RuntimeError(f"no run/event found in block: {block}")
        if errs:
            # some error messages are just warnings
            is_warning = any(
                (
                    "file probably overwritten" in err or
                    "stopping reporting error messages" in err
                )
                for err in errs
            )
            if not is_warning:
                failed.append(run_event)

    # print results
    if failed:
        print(f"found {len(failed)} broken events:")
        # for r, e in failed:
        #     print(f"({r}, {e}),", file=sys.stderr)
        print("--skip-events parameter:")
        print(":".join("{},{}".format(*tpl) for tpl in failed))

    return 0


if __name__ == "__main__":
    sys.exit(main())
