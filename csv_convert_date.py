from datetime import timedelta, datetime
import re
import os
import json
from json import JSONDecodeError
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
        parse_dates_arg = json.loads(prog_args.timestamp.strip())
    except JSONDecodeError as ex:
        print(ex)
        parse_dates_arg = prog_args.timestamp.strip().lower() == 'true'

    index_col = prog_args.column.strip()

    # Open source file using provided source path, header row number and skip 
    # rows arguments
    csv_data = pd.read_csv(
        filepath_or_buffer=source_file,
        header=header_row,
        skiprows=skip_rows,
        parse_dates=parse_dates_arg,
        infer_datetime_format=True, 
        keep_date_col=True
    )

    csv_data[index_col] = csv_data[index_col].map(lambda date_str: parse_dates(date_str, prog_args))

    if prog_args.adjust_tz: # if datetime is not UTC adjust accordingly
        adjust_tz, destination_tz = prog_args.adjust_tz.strip().split(":")
        
        csv_data[index_col] = pd.to_datetime(csv_data[index_col]) + timedelta(hours=float(adjust_tz))
                
        # set index, necessary grouping rows by interval format
        csv_data.set_index(index_col, inplace=True)

        csv_data.index = csv_data.index.tz_localize(destination_tz)

    if prog_args.drop_columns:
        csv_data.drop(labels=prog_args.drop_columns.strip().split(","), axis=1, inplace=True)

    # Write files out in perscribed format
    write_files(prog_args, csv_data)

def parse_dates(date_str, prog_args):
    new_dt = None
    parse_format = prog_args.in_format.strip()

    try:
        new_dt = datetime.strptime(date_str, parse_format)
    except ValueError:
        # Correct for 2400 time, which is actually 12AM the next day
        print("Invalid Date/time string and parsing format: %s | %s" % (date_str, parse_format))
        
        find_time = None
        replace_time = None

        # Two common ways that this problematic time may be represented
        if date_str.find('2400') > -1:
            find_time = '2400'
            replace_time = '0000'
        elif date_str.find('24:00') > -1:
            find_time = '24:00'
            replace_time = '00:00'

        date_str = re.sub(find_time, replace_time, date_str)
        new_dt = datetime.strptime(date_str, parse_format)
        new_dt = new_dt + timedelta(days=1)

        print("Auto-correcting for unparsable time: %s" % (new_dt))

    return new_dt

def write_files(prog_args, csv_data):
    print(csv_data.info())
    print(csv_data)

    # generate path names using output path and file name format arguments
    file_path = '%s/%s' % (prog_args.output.strip(), prog_args.filename_format.strip())

    # check output path and create directory paths that do not exist
    if not Path(os.path.dirname(file_path)).exists():
        os.makedirs(os.path.dirname(file_path))

    write_index = False
    if prog_args.adjust_tz:
        write_index = True

    csv_data.to_csv(file_path, index=write_index)

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
        "-n",
        "--names",
        help="Row that contains column names, default: 0",
        default="0",
        action="store",
    )

    parser.add_argument(
        "-d",
        "--data-begins",
        help="Row that contains beginning of data, default: None",
        default=None,
        action="store",
    )

    parser.add_argument(
        "-t",
        "--timestamp",
        help="Boolean to tell pandas to parse dates (default: true) or a JSON formatted list of columns that can be combined to create a timestamp. NOTE: refer to the pandas read_csv() parse_dates option for further details.",
        default="true",
        action="store",
    )

    parser.add_argument(
        "-c",
        "--column",
        help="Name of the new timestamp column",
        default="timestamp",
        action="store",
    )

    parser.add_argument(
        "-i",
        "--in-format",
        help="Format of the timestamp column for date/time parsing",
        default="%%Y-%%m-%%dT%%H:%%M:%%S.000Z",
        action="store",
    )

    parser.add_argument(
        "-w",
        "--out-format",
        help="Output format of the timestamp column after it has been parsed",
        default="%%Y-%%m-%%dT%%H:%%M:%%S.000Z",
        action="store",
    )

    parser.add_argument(
        "-p",
        "--position",
        help="Index position of the newly created timestamp column.  Use -1 to append to the end, Default: 0",
        default="0",
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

    prog_args = parser.parse_args()

    main(prog_args)
