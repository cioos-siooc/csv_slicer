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
import json
from pathlib import Path

def main(prog_args):
    output_files = {}

    source = open(prog_args.source_file.strip(), "r")
    partition_column = prog_args.column
    
    try:
        formats = json.loads(prog_args.format.strip())
    except AttributeError:
        formats = {}

    try:
        labels = json.loads(prog_args.labels)
    except TypeError:
        labels = {}
    

    for line in source:
        # Separate NMEA Checksum from final value
        if prog_args.nema_checksum:
             line = line.replace("*", ",*", 1)
             line = line.replace("@&2C", ",")

        row = line.strip().split(",")

                    
        # https://stackoverflow.com/questions/20003290/output-different-precision-by-column-with-pandas-dataframe-to-csv
        for column, format_str in formats.items():
            col_int = int(column)
            row[col_int] = prep_value(value=row[col_int], format_str=format_str)


        try:
            column = row[partition_column]
        except IndexError as ie:
            print(f"Index of partition column ({partition_column}) could not be located - skipping...")
            print(f"Line content: {row}")
            continue

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

        headers = False
        if labels and partition in labels:
            headers = labels[partition].split(",")

        df.to_csv(output_path, index=False, header=headers)

def prep_value(value, format_str):
    if format_str['type'] == 'int':
        value = int(value)
    elif format_str['type'] == 'float':
        value = float(value)

    return format_str['output'].format(value)

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
        help="Index of column that contains unique values to partition into separate files (zero-based).  Default: 0",
        default=0,
        type=int,
        action="store",
    )

    parser.add_argument(
        "-f",
        "--format",
        # Inspired by this answer https://stackoverflow.com/a/62546734
        help="A JSON formatted list of columns (zero-based) and how they should be formatted according to python string formatting - applies to ALL rows before partitioning.\r\n  Example: {\"0\":{\"type\":\"float\",\"output\":\"{:1.2f}\"}} would format the value in the first column to 2 decimal places.",
        action="store",
    )

    parser.add_argument(
        "-l",
        "--labels",
        help="A JSON formatted list of column labels corresponding to 1 or more of the values related to the partition column. Example: {\"value1\":[\"col_a\",\"col_b\", \"col_c\"], ... ,\"valueN\":[\"label_a\",\"label_b\"]}",
        action="store",
    )

    parser.add_argument(
        "-o",
        "--output",
        help="Path to store generated output files.  Default: .",
        default=".",
        action="store",
    )

    parser.add_argument(
        "-n",
        "--nema-checksum",
        help="Accounts for NMEA checksums at he end of a line, which need to be split with an '*' to separate the final value from the checksum",
        action="store_true",
    )

    prog_args = parser.parse_args()

    main(prog_args)
