from datetime import timedelta
import os
import importlib
import pandas as pd
import argparse
from pathlib import Path

def main(prog_args):
    file_source = prog_args.source_file.strip()
    src_dir = os.path.dirname(file_source)
    file_path = os.path.basename(file_source)

    for source_file in Path(src_dir).glob(file_path):
        process_source_file(prog_args, source_file)

def process_source_file(prog_args, source_file):
    # Check if headers and data begins at a set row
    try:
        skip_rows = int(prog_args.data_begins)

    # Default is None, keep it that way
    except TypeError:
        skip_rows = prog_args.data_begins

    # If CSV file is more complex and specific rows should be skipped then 
    # accept a list of 0-based row numbers that should be skipped instead
    except ValueError:
        skip_rows = map(int, prog_args.data_begins.strip().split(","))

    header_row = int(prog_args.names.strip())
    if header_row < 0:
        header_row = None

    try:
        index_column, rename_index = prog_args.column.strip().split(":")

    except ValueError:
        index_column = prog_args.column.strip()
        rename_index = None

    except AttributeError:
        index_column = prog_args.column
        rename_index = None

    # Open source file using provided source path, header row number and skip 
    # rows arguments
    csv_data = pd.read_csv(
        filepath_or_buffer=source_file,
        header=header_row,
        skiprows=skip_rows,
        parse_dates=True,
        index_col=index_column
    )

    # Remove "Unnamed" (i.e. blank columns with no header name) columns from CSV by default,
    # keep them if specifically specified by the user
    # 
    # NOTE: This behaviour can conflict with setting the column names - the 
    #       "Unnamed:<column_index>" column labels are generated by pandas when reading in a
    #       CSV that has an empty column name
    if not prog_args.keep_empty:
        csv_data = csv_data.loc[:, ~csv_data.columns.str.contains('^Unnamed')]

    if rename_index:
        csv_data.index.name = rename_index

    if prog_args.column_names:
        column_names = prog_args.column_names.strip().split(",")
        csv_data.columns = column_names

    if prog_args.drop_columns:
        csv_data.drop(labels=prog_args.drop_columns.strip().split(","), axis=1, inplace=True)

    # Write files out in perscribed format
    write_files(prog_args, csv_data)

def write_files(prog_args, csv_data):
    split_method, interval_format = prog_args.method.strip().split(':')

    # Get index column from program args
    try:
        index_column, rename_index = prog_args.column.strip().split(":")
    except ValueError:
        index_column = prog_args.column.strip()
        rename_index = None

    except AttributeError:
        index_column = prog_args.column
        rename_index = None

    if rename_index:
        index_column = rename_index

    #importlib.import_module(('methods/%s.py' % split_method))

    # Currently only method of slicing is "date", translate index into date/time
    if split_method == 'date':
        if prog_args.adjust_tz: # if datetime is not UTC adjust accordingly
            adjust_tz, destination_tz = prog_args.adjust_tz.strip().split(":")
            
            # if index is not already a DateTimeIndex then recreate it as one
            if not isinstance(csv_data.index, pd.DatetimeIndex):
                csv_data[index_column] = pd.to_datetime(csv_data[index_column]) + timedelta(hours=float(adjust_tz))
                        
                # set index, necessary grouping rows by interval format
                csv_data.set_index(index_column, inplace=True)

                csv_data.index = csv_data.index.tz_localize(destination_tz)
            # index is already a DateTimeIndex but timezones need to be adjusted
            else:
                # DateTimeIndex can be timezone aware (has one) or timezone 
                # naive (doesn't have a tz)
                try:
                    csv_data.index = csv_data.index.tz_convert(destination_tz)
                except TypeError as err:
                    new_index = csv_data.index + timedelta(hours=float(adjust_tz))

                    csv_data.index = new_index.tz_localize(destination_tz)

        elif not isinstance(csv_data.index, pd.DatetimeIndex):
            csv_data[index_column] = pd.to_datetime(csv_data[index_column])
        
            # set index, necessary grouping rows by interval format
            csv_data.set_index(index_column, inplace=True)

        # generate path names using output path and file name format arguments
        file_path = '%s/%s' % (prog_args.output.strip(), prog_args.filename_format.strip())

        # create list of data files and their full paths
        log_files = csv_data.index.strftime(file_path).unique()

        # create a list of unique days present in the master dataframe
        # use this to create subsets of each days worth of data
        log_file_index = csv_data.index.strftime(interval_format).unique()

    # Split source file by an arbitrary number of rows
    elif  split_method == 'chunk':
        pass

    else:
        print("ERROR: Unable to create index.")
        log_file_index = []

    # Loop through list of log file names
    for index, log_file in enumerate(log_files):
        date_key = log_file_index[index]

        if Path(log_file).exists():
            # load exisitng data
            df_tmp = pd.read_csv(log_file, index_col=index_column, parse_dates=True)

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
        df_write.to_csv(log_file, date_format=prog_args.date_format_out)

        # destroy temporary DataFrames
        df_tmp = None
        df_write = None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "source_file",
        help="Source CSV file to be sliced into smaller files.  This argument also accepts wildcards to allow the processing of multiple files in one call.  NOTE: All files should have the same CSV structure.",
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
        help="How output file names should be formatted, can and should contain date formatting string.  Example: test_data_%%Y-%%m-%%d.csv",
        action="store",
    )

    parser.add_argument(
        "-c",
        "--column",
        help="Column name that contains the date/time information to slice the data on.",
        action="store",
        default=0
    )

    parser.add_argument(
        "-k",
        "--keep-empty",
        help="Override default behaviour to drop unnamed columns from final output.  NOTE: The process occurs before the column names are assigned.  As a result, this setting can complement or interfere with the column names option depending on how it's used.",
        action="store_true",
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
        help="A comma seperated list of values that will be assigned to the columns in order.  NOTE:  This operation occurs AFTER the dropping of \"Unnamed\" columns.  If you have columns, without names, that you want to assign using this option, use the --keep-empty flag to ensure they aren't dropped.",
        action="store",
    )

    parser.add_argument(
        "-d",
        "--data-begins",
        help="Row number that contains beginning of data (zero-based).  If the file is more complex and requires multiple rows to be skipped a comma seperated list can be supplied, default: None",
        default=None,
        action="store",
    )

    parser.add_argument(
        "-x",
        "--drop-columns",
        help="List of columns to drop from the output",
        action="store",
    )

    parser.add_argument(
        "-z",
        "--adjust-tz",
        help="Specifies how date/times should be adjusted, in hours, and what timezone the data should be localized to.  Example: 3.5:UTC",
        action="store",
    )

    parser.add_argument(
        "--date-format-out",
        help="Specifies how date/times should be formatted in the resulting files.  By default, this uses ISO 8601: %Y-%m-%dT%H:%M:%S%z",
        action="store",
        default='%Y-%m-%dT%H:%M:%S%z'
    )

    prog_args = parser.parse_args()

    main(prog_args)
