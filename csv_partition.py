"""
Splits a CSV file based on the unique values of a specified column and
outputs those rows into separate files based on that value into the 
specified directory.

Useful as a preprocessing step for splitting CSV files that contain 
multiple kinds of messages, like Campbell Scientific CSV.
"""

import os
import argparse
import pandas as pd
from pathlib import Path

def main(prog_args):
    output_files = {}

    source = open(prog_args.source_file.strip(), "r")
    index_col = prog_args.column

    for line in source:
        row = line.strip().split(",")
        column = row[index_col]

        if column in output_files:
            output_files[column].append(row)
        else:
            output_files[column] = []
            output_files[column].append(row)

    source.close

    for partition in output_files:
        output_path = "%s/%s.csv" % (prog_args.output.strip(), partition)

        # check output path and create directory paths that do not exist
        if not Path(os.path.dirname(output_path)).exists():
            os.makedirs(os.path.dirname(output_path))

        df = pd.DataFrame.from_dict(output_files[partition])
        df.to_csv(output_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "source_file",
        help="Source file to be partitioned",
        action="store",
    )

    parser.add_argument(
        "-c",
        "--column",
        help="Column that contains unique values to partition into sepearate files",
        default=0,
        type=int,
        action="store",
    )

    parser.add_argument(
        "-o",
        "--output",
        help="Path to store generated output files",
        default=".",
        action="store",
    )

    prog_args = parser.parse_args()

    main(prog_args)
