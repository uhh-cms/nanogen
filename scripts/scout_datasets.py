from __future__ import annotations

import os
import subprocess

import law

from nanogen.nano_util import DatasetInfo, load_dataset_stats, das_query


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="find MC dataset extensions or invalid datasets and suggest fixes")
    parser.add_argument("datasets_file", help="path to a datasets_*.yaml file")
    parser.add_argument("--datasets", nargs="+", help="names or patterns of datasets to check")
    parser.add_argument("--skip-datasets", nargs="+", help="names or patterns of datasets to skip")
    parser.add_argument("--replace-invalid", action="store_true", help="replace invalid datasets with valid ones")
    parser.add_argument("--check-extensions", action="store_true", help="check for dataset extensions")
    args = parser.parse_args()

    # skipping helpers
    def do_skip(dataset_name: str) -> bool:
        if args.skip_datasets:
            return law.util.multi_match(dataset_name, args.skip_datasets)
        if args.datasets:
            return not law.util.multi_match(dataset_name, args.datasets)
        return False

    # open the file and loop, keeping track of dataset info
    datasets_file = os.path.abspath(os.path.expanduser(os.path.expandvars(args.datasets_file)))
    content = law.LocalFileTarget(datasets_file).load()
    known_keys = set(dataset["key"] for dataset in content.values())
    missing: dict[str, dict] = {}
    valid: dict[str, tuple[dict, dict, list[str]]] = {}
    invalid: dict[str, tuple[dict, dict, list[str]]] = {}
    for dataset_name, dataset in content.items():
        if do_skip(dataset_name):
            continue
        print(f"checking {dataset_name} ...")

        # skip private datasets
        if dataset.get("private", False):
            print(law.util.colored("private!\n", "cyan"))
            continue

        # fetch stats
        dataset_key = dataset["key"]
        stats = load_dataset_stats(dataset_key, silent=True)

        # should not be empty
        if stats is None:
            print(law.util.colored("missing!\n", "magenta", style="bright"))
            missing[dataset_name] = dataset
            continue

        # parse the key and create a wildcard variant of it
        di = DatasetInfo.from_key(dataset_key)
        wildcard_key = di.copy(
            campaign_version=di.campaign_version.rsplit("_", 1)[0] + "_*",
            dataset_version="*",
        ).dataset_key

        # handle valid ones
        if stats["valid"]:
            print(law.util.colored("valid!", "green", style="bright"))
            valid[dataset_name] = (dataset, stats, [])
            # check for potential extensions
            if args.check_extensions:
                print("looking for extensions ...")
                out = das_query(f"dataset={wildcard_key}", log=(lambda msg: None))
                extensions = set(out.split()) - known_keys
                if extensions:
                    print(law.util.colored(f"found {len(extensions)} extension(s):", "blue", style="bright"))
                    valid[dataset_name][2].extend(extensions)
                else:
                    print("no extensions found")
        else:  # handle invalid ones
            print(law.util.colored("invalid!", "red", style="bright"))
            invalid[dataset_name] = (dataset, stats, [])

            # look for valid datasets with a wildcard for the dataset version
            print("looking for updated versions ...")
            out = das_query(f"dataset={wildcard_key}", log=(lambda msg: None))
            if out:
                alternatives = out.split()
                alternatives_str = "\n  - ".join(alternatives)
                print(law.util.colored(f"found {len(alternatives)} alternative(s):", "green"))
                print(f"  - {alternatives_str}")
                invalid[dataset_name][2].extend(alternatives)
            else:
                print(law.util.colored("no alternatives found", "yellow", style="bright"))
        print()

    # print final overview
    print(f"summary:")
    print(f"  - missing datasets: {len(missing)}")
    print(f"  - valid datasets: {len(valid)}")
    print(f"  - invalid datasets: {len(invalid)}")
    print()
    if args.check_extensions:
        print(f"possible extensions of valid datasets:")
        for dataset_name, (dataset, stats, extensions) in valid.items():
            if not extensions:
                continue
            extensions_str = "\n    - " + "\n    - ".join(extensions)
            print(f"  - {dataset_name}:{extensions_str}")
        print()
    print(f"possible replacements of invalid datasets:")
    for dataset_name, (dataset, stats, alternatives) in invalid.items():
        if alternatives:
            alternatives_str = "\n    - " + "\n    - ".join(alternatives)
        else:
            alternatives_str = law.util.colored(" none", "red", style="bright")
        if args.replace_invalid and len(alternatives) == 1:
            escaped = alternatives[0].replace("/", "\\/")
            cmd = f"sed -i \'/{dataset_name}:/{{n;s/.*/  key: {escaped}/}}\' {datasets_file}"
            subprocess.run(cmd, shell=True, executable="/bin/bash")
            alternatives_str = law.util.colored(" (replaced)", "green") + alternatives_str
        print(f"  - {dataset_name}:{alternatives_str}")



if __name__ == "__main__":
    main()
