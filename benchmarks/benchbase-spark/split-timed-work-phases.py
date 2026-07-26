#!/usr/bin/env python3
import argparse
import copy
import xml.etree.ElementTree as element_tree
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Split a timed BenchBase workload into one phase per query."
    )
    parser.add_argument("--config", type=Path, required=True)
    return parser.parse_args()


def split_timed_work_phases(config_path):
    tree = element_tree.parse(config_path)
    works = tree.getroot().find("works")
    if works is None:
        raise ValueError("BenchBase config does not contain <works>")

    phases = works.findall("work")
    if len(phases) != 1:
        raise ValueError(
            "timed query splitting requires exactly one <work> phase"
        )

    template = phases[0]
    if template.find("time") is None:
        raise ValueError("timed query splitting requires <time>")

    weights_element = template.find("weights")
    if weights_element is None or not weights_element.text:
        raise ValueError("timed query splitting requires <weights>")

    try:
        weights = [
            float(value.strip())
            for value in weights_element.text.split(",")
        ]
    except ValueError as error:
        raise ValueError("query weights must be numeric") from error

    if any(weight < 0 for weight in weights):
        raise ValueError("query weights must be non-negative")

    active_queries = [
        index for index, weight in enumerate(weights) if weight > 0
    ]
    if not active_queries:
        raise ValueError("timed workload must select at least one query")

    works.remove(template)
    for phase_index, active_query in enumerate(active_queries):
        phase = copy.deepcopy(template)
        phase.find("weights").text = ",".join(
            "100" if index == active_query else "0"
            for index in range(len(weights))
        )
        warmup = phase.find("warmup")
        if phase_index > 0 and warmup is not None:
            warmup.text = "0"
        works.append(phase)

    tree.write(config_path, encoding="utf-8", xml_declaration=True)
    return len(active_queries)


def main():
    args = parse_args()
    print(split_timed_work_phases(args.config))


if __name__ == "__main__":
    main()
