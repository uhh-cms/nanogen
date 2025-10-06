#!/usr/bin/env python
# coding: utf-8

"""
Script to parse the size of a dataset from the DAS json output.
Example:

> dasgoclient -query="dataset=..." -json | parse_das_size.py
"""

# read json from STDIN
import sys
import json
data = json.load(sys.stdin)

# get the entries that has the "dbs3:filesummaries" service in the "das" section
for entry in data:
    if (
        "das" in entry and
        "services" in entry["das"] and
        "dbs3:filesummaries" in entry["das"]["services"]
    ):
        break
else:
    print("No entry with dbs3:filesummaries found", file=sys.stderr)
    sys.exit(1)

# check that the entry has a single dataset section
if not (
    isinstance(entry.get("dataset"), list) and
    len(entry["dataset"]) == 1
):
    print("Entry does not have a single dataset section", file=sys.stderr)
    sys.exit(1)

# print the size in bytes
print(entry["dataset"][0]["size"])
