"""
Merges two or more CSV files with the same column structure, sorts their 
contents by the specified column, drops duplicates based on the unique values 
of a specified column and writes the output to a specified path which supports 
python date/time string formatting.

Can take in multiple files directly named or paths with wildcards.
"""
import os
from numpy.lib.utils import source
import pandas as pd
from datetime import datetime
import argparse
from pathlib import Path

def main(prog_args):
    file_list = []

    for file_arg in prog_args.source_files:
        src_dir = os.path.dirname(file_arg)
        file_path = os.path.basename(file_arg)

        search_list = Path(src_dir).glob(file_path)
        print("%s : %s" % (file_arg, search_list))

        for source_file in search_list:
            file_list.append(source_file)

    print("files to merge: %s" % (file_list))

    merged_df = merge_files(prog_args, file_list)
    output_merged_file(prog_args, merged_df)

def merge_files(prog_args, file_list):
    merged_df = pd.DataFrame()

    for source_file in file_list:
        df = pd.read_csv(source_file)

        if merged_df.empty:
            merged_df = df
        else:
            df.columns = merged_df.columns
            merged_df = merged_df.append(df, ignore_index=True)

        print("\r\nSource DataFrame: %s" % (source_file))
        print(df.info())
        print(df.head())
        print(df.tail())
        print("\r\nMerged DataFrame:")
        print(merged_df.info())
        print(merged_df.head())
        print(merged_df.tail())

    index_col = merged_df.columns[prog_args.column]

    sort_col_idx, sort_dir = prog_args.sort.split(",")
    sort_col = merged_df.columns[int(sort_col_idx)]

    ascending = None
    if sort_dir.strip().upper() == 'ASC':
        ascending = True
    elif sort_dir.strip().upper() == 'DESC':
        ascending = False

    merged_df = merged_df.sort_values(by=sort_col, ascending=ascending)

    deduped_df = merged_df.drop_duplicates(subset=index_col)
    deduped_df.set_index(index_col, inplace=True)

    return deduped_df

def output_merged_file(prog_args, output_df):
    try:
        output_file_path = datetime.now().strftime(prog_args.output.strip())
    except:
        output_file_path = prog_args.output.strip()

    output_df.to_csv(output_file_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "source_files",
        help="Source file to be partitioned, minimum two files, can accept wildcards. Headers from the first file will be assigned to all subsequent files for consistency.",
        action="store",
        nargs="+"
    )

    parser.add_argument(
        "-c",
        "--column",
        help="Column that contains unique values to merge and filter on",
        default=0,
        type=int,
        action="store",
    )

    parser.add_argument(
        "-s",
        "--sort",
        help="Column that contains the value to sort rows on and direction (ASC or DESC).  Default: 0|ASC",
        default="0,ASC",
        action="store",
    )

    parser.add_argument(
        "-o",
        "--output",
        help="Path to store generated output files",
        default="./merged_%Y-%m-%dT%H%M%S.csv",
        action="store",
    )

    prog_args = parser.parse_args()

    main(prog_args)
