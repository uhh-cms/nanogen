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
pfn = "/pnfs/desy.de/cms/tier2/store/user/mrieger/nanogen_store/FetchLFN/store/data/Run2022C/JetMET/MINIAOD/19Dec2023-v1/2560000/7a5aa4c8-3b72-4da3-8d95-ec5ee18f765c.root"  # noqa

# the objects to be unpacked
handles = {
    "pfcands": {
        "type": "std::vector<pat::PackedCandidate>",
        "label": ("packedPFCandidates",),
    },
}


# a function to be called to unpack and use the objects
# (this should not print anything!)
def unpack(event, products):
    cands = products["pfcands"]
    cands.at(0).pt()
    # sum(cands.at(i).pt() for i in range(cands.size()))


# --- nothing needs to be configured below this line -----------------------------------------------


def fwlite_loop(path, handle_data=None, start=0, end=-1, object_type="Event"):
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
        if handle_data:
            products = {}
            for key, data in handle_data.items():
                obj.getByLabel(data["label"], handles[key])
                products[key] = handles[key].product()
            yield obj, products
        else:
            yield obj


def run_fwlite_loop():
    for event, products in fwlite_loop(pfn, handles):
        nr = event.eventAuxiliary().run()
        ne = event.eventAuxiliary().event()
        print(f"EVENT {nr} {ne}")
        try:
            unpack(event, products)
        except Exception as e:
            print(f"\n{e}")
        sys.stdout.flush()
        sys.stderr.flush()
        print("END")


def main():
    env_var = "NG_FIND_BROKEN_EVENTS_TRIGGERED"

    # when set, just run the fwlite loop
    if os.getenv(env_var):
        run_fwlite_loop()
        return

    # trigger _this_ file and pipe its output into a temporary file
    this_file = os.path.abspath(__file__)
    with tempfile.NamedTemporaryFile(mode="w") as tmp:
        subprocess.run(
            [sys.executable, this_file],
            env={**os.environ, env_var: "1"},
            stdout=tmp,
            stderr=subprocess.STDOUT,
            check=True,
        )
        # read the file content
        tmp.seek(0)
        with open(tmp.name) as tmp:
            lines = deque(line.strip() for line in tmp.readlines())

    # parse the output and extract events that failed
    failed = []
    while lines:
        if not (m := re.match(r"EVENT (\d+) (\d+)$", lines.popleft())):
            continue
        bad = lines.popleft() != "END" if lines else True
        if bad:
            failed.append((int(m.group(1)), int(m.group(2))))

    # print results
    if failed:
        print(f"found {len(failed)} events:")
        for r, e in failed:
            print(f"({r}, {e}),", file=sys.stderr)


if __name__ == "__main__":
    main()
