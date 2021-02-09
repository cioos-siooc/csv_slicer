from datetime import timedelta
import os
import importlib
import pandas as pd
import argparse
from pathlib import Path

def main(prog_args):
    # Check if headers and data begins at a set row
    try:
        skip_rows = int(prog_args.data_begins)

    # If CSV file is more complex and specific rows should be skipped then 
    # accept a list of 0-based row numbers that should be skipped instead
    except ValueError:
        skip_rows = map(int, prog_args.data_begins.strip().split(","))

    header_row = int(prog_args.names.strip())
    if header_row < 0:
        header_row = None

    # Open source file using provided source path, header row number and skip 
    # rows arguments
    csv_data = pd.read_csv(
        filepath_or_buffer=prog_args.source.strip(),
        header=header_row,
        skiprows=skip_rows,
    )

    # Remove "Unnamed" (i.e. blank columns with no header name) columns from CSV
    csv_data = csv_data.loc[:, ~csv_data.columns.str.contains('^Unnamed')]

    if prog_args.column_names:
        column_names = prog_args.column_names.strip().split(",")
        csv_data.columns = column_names

    # Write files out in perscribed format
    write_files(prog_args, csv_data)
    

def write_files(prog_args, csv_data):
    split_method, interval_format = prog_args.method.strip().split(':')

    # Get index column from program args
    index_col = prog_args.column.strip()
    
    #importlib.import_module(('methods/%s.py' % split_method))

    # Currently only method of slicing is "date", translate index into date/time
    if split_method == 'date':
        if prog_args.adjust_tz: # if datetime is not UTC adjust accordingly
            adjust_tz, destination_tz = prog_args.adjust_tz.strip().split(":")
            
            csv_data[index_col] = pd.to_datetime(csv_data[index_col]) + timedelta(hours=float(adjust_tz))
                    
            # set index, necessary grouping rows by interval format
            csv_data.set_index(index_col, inplace=True)

            csv_data.index = csv_data.index.tz_localize(destination_tz)
        else: # assume datetime is UTC
            csv_data[index_col] = pd.to_datetime(csv_data[index_col])
        
            # set index, necessary grouping rows by interval format
            csv_data.set_index(index_col, inplace=True)

        # generate path names using output path and file name format arguments
        file_path = '%s/%s' % (prog_args.output.strip(), prog_args.filename_format.strip())

        # create list of data files and their full paths
        log_files = csv_data.index.strftime(file_path).unique()

        # create a list of unique days present in the master dataframe
        # use this to create subsets of each days worth of data
        log_file_index = csv_data.index.strftime(interval_format).unique()

    else:
        print("ERROR: Unable to create index.")
        log_file_index = []

    # Loop through list of log file names
    for index, log_file in enumerate(log_files):
        date_key = log_file_index[index]

        if Path(log_file).exists():
            # load exisitng data
            df_tmp = pd.read_csv(log_file, index_col=index_col, parse_dates=True)

            # add all data from this day to existing dataframe
            df_tmp = df_tmp.append(csv_data.loc[date_key], sort=False)

            df_tmp.index = df_tmp.index.floor('T')

            # Remove duplicates.
            # 
            # Description of duplicated() method use here:
            # https://stackoverflow.com/a/34297689/2112410
            # 
            # Once appending new records to an existing file it is likely 
            # duplicated records will be added, this method will return a 
            # subset of the dataframe with duplicate indexes (timestamps) 
            # removed.
            df_tmp = df_tmp[~df_tmp.index.duplicated(keep='first')]
        else:
            # if no current file exists then create a dataframe from a subset
            # of the master dataframe and write its contents out to the file
            df_tmp = csv_data.loc[date_key]

        # sort data by index (timestamp) in ascending order
        df_write = df_tmp.sort_index()

        # check output path and create directory paths that do not exist
        if not Path(os.path.dirname(log_file)).exists():
            os.makedirs(os.path.dirname(log_file))

        # write out to data file
        df_write.to_csv(log_file)

        # destroy temporary DataFrames
        df_tmp = None
        df_write = None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--source",
        help="Source CSV file to be sliced into smaller files.",
        action="store",
    )

    parser.add_argument(
        "-o",
        "--output",
        help="Destination directory for output files, default: .",
        default=".",
        action="store",
    )

    parser.add_argument(
        "-f",
        "--filename_format",
        help="How output file names should be formatted, can and should contain date formatting string.  Example: test_data_%Y-%m-%d.csv",
        action="store",
    )

    parser.add_argument(
        "-c",
        "--column",
        help="Column name that contains the date/time information to slice the data on.",
        action="store",
    )

    parser.add_argument(
        "-m",
        "--method",
        help="How column should be used to split data by date.  Specify in terms for python date formatting string. Example: date:%Y%m%d",
        default="date:%Y%m%d",
        action="store",
    )

    parser.add_argument(
        "-n",
        "--names",
        help="Row that contains column names, default: 0",
        default="0",
        action="store",
    )

    parser.add_argument(
        "-t",
        "--column-names",
        help="A comma seperated list of values that will be assigned to the columns in order",
        action="store",
    )

    parser.add_argument(
        "-d",
        "--data-begins",
        help="Row that contains beginning of data, default: 1",
        default="1",
        action="store",
    )

    parser.add_argument(
        "-z",
        "--adjust-tz",
        help="Specifies how date/times should be adjusted, in hours, and what timezone the data should be localized to.  Example: 3.5:UTC",
        action="store",
    )

    prog_args = parser.parse_args()

    main(prog_args)
